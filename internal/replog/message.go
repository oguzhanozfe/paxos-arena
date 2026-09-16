package replog

import (
	"errors"
	"fmt"
	"time"

	"github.com/oguzhanozfe/paxos-arena/internal/paxos"
)

// Entry is one chosen slot of the log.
type Entry struct {
	// Slot is the log position.
	Slot paxos.Slot `json:"slot"`
	// Ballot is the ballot under which the slot was chosen.
	Ballot paxos.Ballot `json:"ballot"`
	// Value is the chosen value; empty for a no-op.
	Value paxos.Value `json:"value"`
}

// NoOp reports whether the entry carries the empty value: a gap fill or a
// leadership marker that the state machine skips.
func (e Entry) NoOp() bool { return len(e.Value) == 0 }

// Envelope is one protocol message with its sender and receiver.
type Envelope struct {
	// From is the sending node.
	From paxos.NodeID
	// To is the receiving node.
	To paxos.NodeID
	// Msg is the message, stored by value as one of the nine message types.
	Msg Message
}

// Message is one of the nine protocol messages: Prepare, Promise, Accept,
// Accepted, Nack, Learn, LearnRequest, Heartbeat and HeartbeatAck. Messages
// are stored by value in Envelope.Msg.
type Message interface{ msg() }

// Prepare is Phase 1, sent by a Candidate to every peer. One Prepare covers
// every slot >= FromSlot, the candidate's commit index + 1.
type Prepare struct {
	// Ballot is the candidate's new ballot.
	Ballot paxos.Ballot `json:"ballot"`
	// FromSlot is the first slot the candidate asks about.
	FromSlot paxos.Slot `json:"from_slot"`
}

// Promise is the Phase 1 reply. Accepted lists the acceptor's most recently
// accepted pvalue for every slot >= FromSlot, in slot order. CommitIndex is
// diagnostic: it tells the new leader how far this acceptor has learned.
type Promise struct {
	// Ballot is the ballot being promised.
	Ballot paxos.Ballot `json:"ballot"`
	// Accepted holds the acceptor's accepted pvalues from the requested
	// slot on, in slot order.
	Accepted []paxos.PValue `json:"accepted"`
	// CommitIndex is the acceptor's commit index at the time of the reply.
	CommitIndex paxos.Slot `json:"commit_index"`
}

// Accept is Phase 2 for one slot.
type Accept struct {
	// Ballot is the leader's ballot.
	Ballot paxos.Ballot `json:"ballot"`
	// Slot is the slot being proposed.
	Slot paxos.Slot `json:"slot"`
	// Value is the proposed value; empty for a no-op.
	Value paxos.Value `json:"value"`
}

// Accepted is the Phase 2 reply.
type Accepted struct {
	// Ballot is the ballot of the accepted proposal.
	Ballot paxos.Ballot `json:"ballot"`
	// Slot is the accepted slot.
	Slot paxos.Slot `json:"slot"`
}

// Nack refuses a Prepare (Slot == 0) or an Accept (Slot set). Promised is
// the acceptor's current promise. LeaseRemaining > 0 means the refusal came
// from the lease rule and the sender should wait that long before retrying.
type Nack struct {
	// Ballot is the refused ballot.
	Ballot paxos.Ballot `json:"ballot"`
	// Promised is the acceptor's current promise.
	Promised paxos.Ballot `json:"promised"`
	// Slot is the refused slot for an Accept, 0 for a Prepare.
	Slot paxos.Slot `json:"slot"`
	// LeaseRemaining is how long the acceptor's lease for another leader
	// still runs; 0 when the refusal was not from the lease rule.
	LeaseRemaining time.Duration `json:"lease_remaining"`
}

// Learn announces a chosen slot. The leader sends it when a slot is chosen
// and any node sends it in reply to LearnRequest.
type Learn struct {
	// Slot is the chosen slot.
	Slot paxos.Slot `json:"slot"`
	// Ballot is the ballot under which the slot was chosen.
	Ballot paxos.Ballot `json:"ballot"`
	// Value is the chosen value; empty for a no-op.
	Value paxos.Value `json:"value"`
}

// LearnRequest asks for chosen entries from FromSlot, at most MaxCount. A
// replica sends it when a Heartbeat shows the leader's commit index ahead
// of its own.
type LearnRequest struct {
	// FromSlot is the first slot wanted.
	FromSlot paxos.Slot `json:"from_slot"`
	// MaxCount bounds the number of Learn replies.
	MaxCount int `json:"max_count"`
}

// Heartbeat is sent by the leader every HeartbeatInterval and on every
// ReadIndex call. ReadSeq is the highest read sequence number the leader
// wants acknowledged, 0 when none is pending.
type Heartbeat struct {
	// Ballot is the leader's ballot.
	Ballot paxos.Ballot `json:"ballot"`
	// CommitIndex is the leader's commit index.
	CommitIndex paxos.Slot `json:"commit_index"`
	// ReadSeq is the highest pending read sequence number, or 0.
	ReadSeq uint64 `json:"read_seq"`
}

// HeartbeatAck answers a Heartbeat. Ballot is the heartbeat's ballot,
// Promised the acceptor's current promise. An ack counts toward a read
// barrier only when Promised equals the leader's ballot.
type HeartbeatAck struct {
	// Ballot is the heartbeat's ballot.
	Ballot paxos.Ballot `json:"ballot"`
	// Promised is the acceptor's current promise.
	Promised paxos.Ballot `json:"promised"`
	// CommitIndex is the acceptor's commit index.
	CommitIndex paxos.Slot `json:"commit_index"`
	// ReadSeq echoes the heartbeat's ReadSeq.
	ReadSeq uint64 `json:"read_seq"`
}

func (Prepare) msg()      {}
func (Promise) msg()      {}
func (Accept) msg()       {}
func (Accepted) msg()     {}
func (Nack) msg()         {}
func (Learn) msg()        {}
func (LearnRequest) msg() {}
func (Heartbeat) msg()    {}
func (HeartbeatAck) msg() {}

// TypeName returns the wire type name of a message, or "" for a value that
// is not one of the nine messages.
func TypeName(m Message) string {
	switch m.(type) {
	case Prepare:
		return "prepare"
	case Promise:
		return "promise"
	case Accept:
		return "accept"
	case Accepted:
		return "accepted"
	case Nack:
		return "nack"
	case Learn:
		return "learn"
	case LearnRequest:
		return "learn_request"
	case Heartbeat:
		return "heartbeat"
	case HeartbeatAck:
		return "heartbeat_ack"
	}
	return ""
}

// Event is something the host must react to, drained with Node.Events.
type Event interface{ event() }

// LeaderChanged reports a change of the leader this node knows about. Self
// is true when this node became leader. Leader may be 0 when leadership was
// lost and no successor is known.
type LeaderChanged struct {
	// Leader is the node now believed to lead, or 0.
	Leader paxos.NodeID
	// Ballot is that leader's ballot.
	Ballot paxos.Ballot
	// Self is true when this node itself became leader.
	Self bool
}

// ReadReady reports that the read-index barrier for Seq is satisfied: state
// applied through Index reflects every command chosen before ReadIndex was
// called.
type ReadReady struct {
	// Seq is the sequence number ReadIndex returned.
	Seq uint64
	// Index is the commit index the read must wait for.
	Index paxos.Slot
}

// ReadFailed reports that the read-index barrier for Seq cannot complete,
// because this node stopped leading.
type ReadFailed struct {
	// Seq is the sequence number ReadIndex returned.
	Seq uint64
	// Err is the reason, a NotLeaderError naming the new leader if known.
	Err error
}

// LearnConflict reports that a Learn carried a value different from the one
// already chosen for the slot. It never happens when every node runs the
// protocol; the simulator treats it as a violation of invariant S1 and a
// production host logs it. The node keeps the value it had.
type LearnConflict struct {
	// Slot is the disputed slot.
	Slot paxos.Slot
	// Have is the entry this node had already chosen.
	Have Entry
	// Got is the conflicting entry the Learn carried.
	Got Entry
}

func (LeaderChanged) event() {}
func (ReadReady) event()     {}
func (ReadFailed) event()    {}
func (LearnConflict) event() {}

// NotLeaderError is returned by Propose and ReadIndex on a node that is not
// the leader. Leader is the leader this node knows about, 0 when unknown.
type NotLeaderError struct {
	// Leader is the node believed to lead, or 0.
	Leader paxos.NodeID
}

// Error implements error.
func (e NotLeaderError) Error() string {
	if e.Leader == 0 {
		return "replog: not the leader (leader unknown)"
	}
	return fmt.Sprintf("replog: not the leader (leader is node %d)", e.Leader)
}

// ErrNotReady is returned by ReadIndex while the leader's leadership no-op
// is not yet chosen, so its commit index may not cover every earlier ballot.
// It is unrelated to paxos.ErrNoQuorum of the single-decree proposer.
var ErrNotReady = errors.New("replog: leader has not committed its leadership no-op")

// ErrBusy is returned by Propose when Window slots are in flight and
// QueueLimit values are already queued. Nothing was proposed; the caller may
// retry later.
var ErrBusy = errors.New("replog: the leader's proposal queue is full")

// ErrValueTooLarge is returned, wrapped with the sizes, by Propose for a
// value longer than MaxValueBytes. Nothing was proposed.
var ErrValueTooLarge = errors.New("replog: value is too large to replicate")
