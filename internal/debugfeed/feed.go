// Package debugfeed records what one replica's event loop does, the
// Multi-Paxos messages it sends and receives, its role changes and its
// commit and apply progress, in a fixed-size ring buffer, and serves the
// buffer read-only as JSON on GET /debug/events, so that a visualiser can
// follow a live cluster. arena enables it with -debug-feed.
//
// The feed exposes protocol traffic without authentication: ballots, slots,
// and for every command its operation name and identifiers. It never
// records values beyond that summary (no idempotency keys, deal seeds,
// verifiers, scores or rules), but it belongs on a loopback listener only.
//
// Events are flat and numeric, with no maps and no polymorphism, so that a
// Unity client can decode them with JsonUtility.
package debugfeed

import (
	"fmt"
	"time"

	"github.com/oguzhanozfe/paxos-arena/internal/paxos"
	"github.com/oguzhanozfe/paxos-arena/internal/replica"
	"github.com/oguzhanozfe/paxos-arena/internal/replog"
	"github.com/oguzhanozfe/paxos-arena/internal/tournament"
)

// Capacity is the number of events a Feed keeps; older events are
// overwritten.
const Capacity = 4096

// MaxSummarisedValue is the longest value a Feed decodes to name the command
// it carries. A longer value is described by its size, so that recording an
// Accept or a Learn costs the event loop a bounded amount of work.
const MaxSummarisedValue = 16 << 10

// Event kinds.
const (
	// KindSend is a message this replica handed to its transport.
	KindSend = "send"
	// KindRecv is a message this replica's event loop took from its inbox.
	KindRecv = "recv"
	// KindRole is a change of this replica's role, of the leader it knows
	// or of that leader's ballot.
	KindRole = "role"
	// KindCommit is an advance of this replica's commit index.
	KindCommit = "commit"
	// KindApplied is one slot this replica's state machine applied.
	KindApplied = "applied"
)

// Event is one thing a replica did. Every field is always present in the
// JSON encoding; a field that does not apply to the kind is 0 or "".
type Event struct {
	// Seq numbers the events of one replica from 1, without gaps; it starts
	// again at 1 when the replica restarts.
	Seq uint64 `json:"seq"`
	// AtMs is the wall-clock time of recording in Unix milliseconds.
	AtMs int64 `json:"at_ms"`
	// Node is the replica that recorded the event.
	Node paxos.NodeID `json:"node"`
	// Kind is one of send, recv, role, commit and applied.
	Kind string `json:"kind"`
	// From is the sender of a message, 0 for other kinds.
	From paxos.NodeID `json:"from"`
	// To is the receiver of a message, 0 for other kinds.
	To paxos.NodeID `json:"to"`
	// Type is the message type (prepare, promise, accept, accepted, nack,
	// learn, learn_request, heartbeat, heartbeat_ack) for send and recv, the
	// new role (leader, follower, candidate) for role, and "" otherwise.
	Type string `json:"type"`
	// BallotRound and BallotNode are the message's ballot; for role, commit
	// and applied events, the ballot of the leader this replica knows.
	BallotRound uint64       `json:"ballot_round"`
	BallotNode  paxos.NodeID `json:"ballot_node"`
	// Slot is the slot of an accept, accepted, nack or learn, the first slot
	// asked about by a prepare or a learn_request, and the applied slot of an
	// applied event.
	Slot paxos.Slot `json:"slot"`
	// CommitIndex is the sender's commit index on a promise, heartbeat and
	// heartbeat_ack, and this replica's commit index on role, commit and
	// applied events.
	CommitIndex paxos.Slot `json:"commit_index"`
	// Value summarises the command of an accept, a learn or an applied slot
	// by its operation name and identifiers ("join t1/p2"), or is "noop".
	Value string `json:"value"`
	// Detail is short free text: for example the promise a nack reports,
	// the result code of an applied command, or the previous commit index.
	Detail string `json:"detail"`
}

// heartbeat reports whether the event is a heartbeat or its ack, which
// readers skip unless they ask for them.
func (e *Event) heartbeat() bool {
	return (e.Kind == KindSend || e.Kind == KindRecv) && (e.Type == "heartbeat" || e.Type == "heartbeat_ack")
}

func (e *Event) setBallot(b paxos.Ballot) {
	e.BallotRound, e.BallotNode = b.Round, b.Node
}

// Feed records the events of one replica. Its methods implement
// replica.Observer; install it with Runner.Observe before Run. Safe for
// concurrent use.
type Feed struct {
	self paxos.NodeID
	ring *Ring
	now  func() time.Time
}

var _ replica.Observer = (*Feed)(nil)

// New returns an empty feed for replica self that keeps Capacity events.
func New(self paxos.NodeID) *Feed {
	return &Feed{self: self, ring: NewRing(Capacity), now: time.Now}
}

// Ring returns the buffer the feed records into.
func (f *Feed) Ring() *Ring { return f.ring }

// Sent records an outbound message.
func (f *Feed) Sent(env replog.Envelope) { f.record(messageEvent(KindSend, env)) }

// Received records an inbound message.
func (f *Feed) Received(env replog.Envelope) { f.record(messageEvent(KindRecv, env)) }

// Changed records a role event when the role, the known leader or its ballot
// changed, a commit event when the commit index advanced, and one applied
// event per applied slot.
func (f *Feed) Changed(prev, cur replica.Status, applied []replica.Applied) {
	if cur.Role != prev.Role || cur.Leader != prev.Leader || cur.Ballot != prev.Ballot {
		ev := Event{Kind: KindRole, Type: cur.Role.String(), CommitIndex: cur.CommitIndex, Detail: "leader unknown"}
		ev.setBallot(cur.Ballot)
		if cur.Leader != 0 {
			ev.Detail = fmt.Sprintf("leader %d", cur.Leader)
		}
		f.record(ev)
	}
	if cur.CommitIndex > prev.CommitIndex {
		ev := Event{Kind: KindCommit, CommitIndex: cur.CommitIndex, Detail: fmt.Sprintf("from %d", prev.CommitIndex)}
		ev.setBallot(cur.Ballot)
		f.record(ev)
	}
	for _, a := range applied {
		ev := Event{Kind: KindApplied, Slot: a.Slot, CommitIndex: cur.CommitIndex}
		ev.setBallot(cur.Ballot)
		switch {
		case a.Err != nil:
			ev.Value, ev.Detail = "undecodable", "skipped"
		case a.NoOp:
			ev.Value = "noop"
		default:
			ev.Value = SummariseOp(a.Command.Op)
			ev.Detail = string(a.Result.Code)
			if a.Result.Replayed {
				ev.Detail += ", replayed"
			}
		}
		f.record(ev)
	}
}

func (f *Feed) record(ev Event) {
	ev.Node = f.self
	ev.AtMs = f.now().UnixMilli()
	f.ring.Append(ev)
}

// messageEvent describes one envelope. Its work is bounded by
// MaxSummarisedValue: a promise's accepted values are counted, not read.
func messageEvent(kind string, env replog.Envelope) Event {
	ev := Event{Kind: kind, From: env.From, To: env.To, Type: replog.TypeName(env.Msg)}
	switch m := env.Msg.(type) {
	case replog.Prepare:
		ev.setBallot(m.Ballot)
		ev.Slot = m.FromSlot
	case replog.Promise:
		ev.setBallot(m.Ballot)
		ev.CommitIndex = m.CommitIndex
		ev.Detail = fmt.Sprintf("accepted %d", len(m.Accepted))
		if n := len(m.Accepted); n > 0 {
			ev.Detail += fmt.Sprintf(", slots %d-%d", m.Accepted[0].Slot, m.Accepted[n-1].Slot)
		}
	case replog.Accept:
		ev.setBallot(m.Ballot)
		ev.Slot = m.Slot
		ev.Value = SummariseValue(m.Value)
	case replog.Accepted:
		ev.setBallot(m.Ballot)
		ev.Slot = m.Slot
	case replog.Nack:
		ev.setBallot(m.Ballot)
		ev.Slot = m.Slot
		ev.Detail = "promised " + m.Promised.String()
		if m.LeaseRemaining > 0 {
			ev.Detail += fmt.Sprintf(", lease %dms", m.LeaseRemaining.Milliseconds())
		}
	case replog.Learn:
		ev.setBallot(m.Ballot)
		ev.Slot = m.Slot
		ev.Value = SummariseValue(m.Value)
	case replog.LearnRequest:
		ev.Slot = m.FromSlot
		ev.Detail = fmt.Sprintf("max %d", m.MaxCount)
	case replog.Heartbeat:
		ev.setBallot(m.Ballot)
		ev.CommitIndex = m.CommitIndex
		if m.ReadSeq != 0 {
			ev.Detail = fmt.Sprintf("read_seq %d", m.ReadSeq)
		}
	case replog.HeartbeatAck:
		ev.setBallot(m.Ballot)
		ev.CommitIndex = m.CommitIndex
		ev.Detail = "promised " + m.Promised.String()
		if m.ReadSeq != 0 {
			ev.Detail += fmt.Sprintf(", read_seq %d", m.ReadSeq)
		}
	default:
		ev.Type = fmt.Sprintf("%T", env.Msg)
	}
	return ev
}

// SummariseValue names the command a log value carries: "noop" for the
// empty value, the command's SummariseOp when it decodes, "undecodable" when
// it does not, and its size when it is longer than MaxSummarisedValue.
func SummariseValue(v paxos.Value) string {
	switch {
	case len(v) == 0:
		return "noop"
	case len(v) > MaxSummarisedValue:
		return fmt.Sprintf("value %d bytes", len(v))
	}
	cmd, err := tournament.Decode(v)
	if err != nil {
		return "undecodable"
	}
	return SummariseOp(cmd.Op)
}

// SummariseOp names an operation and the identifiers it addresses, and
// nothing else: "create_tournament t1", "join t1/p2", "play_move t1/p2 r1
// m4". Keys, seeds, verifiers, scores, moves and rules are left out.
func SummariseOp(op tournament.Op) string {
	name := tournament.OpName(op)
	switch o := op.(type) {
	case tournament.CreateTournament:
		return name + " " + shortID(string(o.ID))
	case tournament.Join:
		return name + " " + pair(o.Tournament, o.Player.ID)
	case tournament.SubmitScore:
		return name + " " + pair(o.Tournament, o.Player)
	case tournament.Close:
		return name + " " + shortID(string(o.Tournament))
	case tournament.Settle:
		return name + " " + shortID(string(o.Tournament))
	case tournament.OpenSession:
		return name + " " + shortID(string(o.Player))
	case tournament.Enter:
		return name + " " + pair(o.Tournament, o.Player)
	case tournament.StartRound:
		return fmt.Sprintf("%s %s r%d", name, pair(o.Tournament, o.Player), o.Round)
	case tournament.PlayMove:
		return fmt.Sprintf("%s %s r%d m%d", name, pair(o.Tournament, o.Player), o.Round, o.MoveIndex)
	case tournament.FinishRound:
		return fmt.Sprintf("%s %s r%d", name, pair(o.Tournament, o.Player), o.Round)
	case tournament.ClaimPayout:
		return name + " " + pair(o.Tournament, o.Player)
	}
	return "unknown"
}

func pair(t tournament.TournamentID, p tournament.PlayerID) string {
	return shortID(string(t)) + "/" + shortID(string(p))
}

// shortID cuts an identifier to tournament.MaxIDLen bytes. The state machine
// rejects longer ones, but a value in flight has not been validated yet.
func shortID(s string) string {
	if len(s) > tournament.MaxIDLen {
		return s[:tournament.MaxIDLen] + "..."
	}
	return s
}
