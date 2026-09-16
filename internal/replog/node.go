package replog

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"sort"
	"time"

	"github.com/oguzhanozfe/paxos-arena/internal/paxos"
)

// proposal is one slot a leader has in flight.
type proposal struct {
	value    paxos.Value
	accepts  map[paxos.NodeID]struct{}
	lastSent time.Duration
}

// phase1State is a candidate's Phase 1 bookkeeping.
type phase1State struct {
	fromSlot paxos.Slot
	promises map[paxos.NodeID]Promise
	deadline time.Duration
	lastSent time.Duration
}

// read is one pending read-index barrier.
type read struct {
	index paxos.Slot
	acks  map[paxos.NodeID]struct{}
}

// Node is one replica's participant in the replicated log: acceptor and
// learner for every slot, and candidate or leader when elected.
//
// Every driving method (Step, Tick, Propose, ReadIndex) returns the messages
// to send to other nodes. Messages addressed to the node itself are
// processed inline and never returned, so a leader counts itself as an
// acceptor without a round trip.
type Node struct {
	cfg    Config
	store  Store
	rng    *rand.Rand
	quorum int

	// Durable state, mirrored in store.
	promised paxos.Ballot
	maxRound uint64
	accepted map[paxos.Slot]paxos.PValue
	chosen   map[paxos.Slot]Entry

	// Volatile acceptor and learner state.
	commitIndex      paxos.Slot
	leaseHolder      paxos.NodeID
	leaseUntil       time.Duration
	leaderHint       paxos.NodeID
	leaderBallot     paxos.Ballot
	electionDeadline time.Duration
	started          bool
	learnReqAfter    time.Duration

	// Volatile candidate and leader state.
	role          Role
	ballot        paxos.Ballot
	phase1        phase1State
	proposals     map[paxos.Slot]*proposal
	nextSlot      paxos.Slot
	firstOwnSlot  paxos.Slot
	queue         []paxos.Value
	contacts      map[paxos.NodeID]time.Duration
	leaderSince   time.Duration
	lastHeartbeat time.Duration
	readSeq       uint64
	reads         map[uint64]*read

	out    []Envelope
	events []Event
	failed error
}

// New builds a node from cfg and the durable state in store. Zero timing
// fields of cfg take their defaults; LeaseDuration zero disables the lease.
// rng drives election timeouts only.
func New(cfg Config, store Store, rng *rand.Rand) (*Node, error) {
	cfg = cfg.withDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if store == nil {
		return nil, errors.New("replog: New: nil store")
	}
	if rng == nil {
		return nil, errors.New("replog: New: nil rng")
	}
	d, err := store.Load()
	if err != nil {
		return nil, fmt.Errorf("replog: New: load durable state: %w", err)
	}
	n := &Node{
		cfg:       cfg,
		store:     store,
		rng:       rng,
		quorum:    paxos.Quorum(len(cfg.Peers)),
		promised:  d.Promised,
		maxRound:  d.MaxRound,
		accepted:  make(map[paxos.Slot]paxos.PValue, len(d.Accepted)),
		chosen:    make(map[paxos.Slot]Entry, len(d.Chosen)),
		proposals: make(map[paxos.Slot]*proposal),
		reads:     make(map[uint64]*read),
	}
	if cfg.Unsafe == nil || !cfg.Unsafe.ForgetAcceptedOnRestart {
		for _, pv := range d.Accepted {
			n.accepted[pv.Slot] = pv
		}
	}
	for _, e := range d.Chosen {
		n.chosen[e.Slot] = e
	}
	n.advanceCommit()
	if n.maxRound < n.promised.Round {
		// A crash between SaveMaxRound and SavePromised leaves the promise
		// ahead of the round; the next election saves the higher round.
		n.maxRound = n.promised.Round
	}
	return n, nil
}

// Config returns the effective configuration, with defaults applied.
func (n *Node) Config() Config { return n.cfg }

// Self returns this node's identifier.
func (n *Node) Self() paxos.NodeID { return n.cfg.Self }

// Failed returns the store error that stopped this node, or nil. A failed
// node ignores every call; the host must discard it and rebuild a node from
// the same store, exactly as after a crash.
func (n *Node) Failed() error { return n.failed }

// Step processes one inbound message at time now and returns the messages
// to send. Messages from nodes outside Peers, or not addressed to this
// node, are ignored.
func (n *Node) Step(now time.Duration, env Envelope) []Envelope {
	if n.failed != nil {
		return nil
	}
	n.begin(now)
	if env.To == n.cfg.Self && n.isPeer(env.From) {
		n.handle(now, env)
	}
	return n.finish()
}

// Tick runs the timers at time now: election timeout for a follower, Phase
// 1 retry and deadline for a candidate, heartbeat, Accept retransmission and
// majority-contact check for a leader. Call it at least every
// HeartbeatInterval/2.
func (n *Node) Tick(now time.Duration) []Envelope {
	if n.failed != nil {
		return nil
	}
	n.begin(now)
	n.tick(now)
	return n.finish()
}

// Propose asks the leader to place v in the next free slot. It returns
// ErrNotLeader when this node is not the leader. Once Window slots are in
// flight, v is queued and proposed when a slot is chosen. A proposed value
// that loses its slot to another leader's value is not re-proposed by the
// log; the client's retry does that.
func (n *Node) Propose(now time.Duration, v paxos.Value) ([]Envelope, error) {
	if n.failed != nil {
		return nil, n.failed
	}
	n.begin(now)
	if n.role != Leader {
		return nil, ErrNotLeader{Leader: n.leaderHint}
	}
	if len(n.proposals) >= n.cfg.Window {
		n.queue = append(n.queue, v)
	} else {
		n.propose(now, n.nextSlot, v)
		n.nextSlot++
	}
	return n.finish(), nil
}

// ReadIndex starts a read-index barrier on the leader: it records the
// current commit index under a new sequence number and sends a Heartbeat
// carrying it. ReadReady{seq, index} is emitted when a quorum acknowledges
// at the leader's ballot; ReadFailed{seq} when leadership is lost first. It
// returns ErrNotLeader on a non-leader and ErrNotReady until the leadership
// no-op is chosen.
func (n *Node) ReadIndex(now time.Duration) (seq uint64, out []Envelope, err error) {
	if n.failed != nil {
		return 0, nil, n.failed
	}
	n.begin(now)
	if n.role != Leader {
		return 0, nil, ErrNotLeader{Leader: n.leaderHint}
	}
	if !n.Ready() {
		return 0, nil, ErrNotReady
	}
	n.readSeq++
	seq = n.readSeq
	n.reads[seq] = &read{index: n.commitIndex, acks: map[paxos.NodeID]struct{}{n.cfg.Self: {}}}
	n.lastHeartbeat = now
	n.broadcast(now, Heartbeat{Ballot: n.ballot, CommitIndex: n.commitIndex, ReadSeq: seq}, true)
	return seq, n.finish(), nil
}

// Events returns and clears the events emitted since the last call.
func (n *Node) Events() []Event {
	evs := n.events
	n.events = nil
	return evs
}

// Role returns the node's current role.
func (n *Node) Role() Role { return n.role }

// Ballot returns the ballot of the node's current or most recent candidacy.
// It is meaningful when Role is Candidate or Leader.
func (n *Node) Ballot() paxos.Ballot { return n.ballot }

// Leader returns the leader this node knows about and its ballot. ok is
// false when no leader is known.
func (n *Node) Leader() (paxos.NodeID, paxos.Ballot, bool) {
	if n.role == Leader {
		return n.cfg.Self, n.ballot, true
	}
	return n.leaderHint, n.leaderBallot, n.leaderHint != 0
}

// Promised returns the acceptor's current promise.
func (n *Node) Promised() paxos.Ballot { return n.promised }

// Accepted returns the most recently accepted pvalue for slot s.
func (n *Node) Accepted(s paxos.Slot) (paxos.PValue, bool) {
	pv, ok := n.accepted[s]
	return pv, ok
}

// Chosen returns the chosen entry for slot s, if this node has learned it.
func (n *Node) Chosen(s paxos.Slot) (Entry, bool) {
	e, ok := n.chosen[s]
	return e, ok
}

// CommitIndex returns the largest slot such that every slot up to it is
// chosen on this node.
func (n *Node) CommitIndex() paxos.Slot { return n.commitIndex }

// MaxRound returns the highest round this node has used or seen promised.
func (n *Node) MaxRound() uint64 { return n.maxRound }

// Ready reports whether this node is the leader and its leadership no-op is
// chosen, so that its commit index covers everything chosen under earlier
// ballots.
func (n *Node) Ready() bool {
	if n.role != Leader {
		return false
	}
	if n.cfg.Unsafe != nil && n.cfg.Unsafe.SkipLeadershipNoOp {
		return true
	}
	return n.commitIndex >= n.firstOwnSlot
}

// Pending returns the number of slots the leader has in flight and the
// number of queued values waiting for a free slot.
func (n *Node) Pending() (inFlight, queued int) {
	return len(n.proposals), len(n.queue)
}

// --- internal plumbing ---

func (n *Node) begin(now time.Duration) {
	if !n.started {
		n.started = true
		n.electionDeadline = now + n.randomElectionTimeout()
	}
}

func (n *Node) finish() []Envelope {
	if n.failed != nil {
		n.out = nil
		n.events = nil
		return nil
	}
	out := n.out
	n.out = nil
	return out
}

func (n *Node) fail(err error) {
	if n.failed == nil {
		n.failed = fmt.Errorf("replog: node %d store failure: %w", n.cfg.Self, err)
	}
}

func (n *Node) emit(ev Event) { n.events = append(n.events, ev) }

func (n *Node) isPeer(id paxos.NodeID) bool {
	for _, p := range n.cfg.Peers {
		if p == id {
			return true
		}
	}
	return false
}

func (n *Node) randomElectionTimeout() time.Duration {
	span := int64(n.cfg.ElectionTimeoutMax - n.cfg.ElectionTimeoutMin)
	return n.cfg.ElectionTimeoutMin + time.Duration(n.rng.Int64N(span+1))
}

// send queues a message for to, or processes it inline when to is Self.
func (n *Node) send(now time.Duration, to paxos.NodeID, m Message) {
	if n.failed != nil {
		return
	}
	env := Envelope{From: n.cfg.Self, To: to, Msg: m}
	if to == n.cfg.Self {
		n.handle(now, env)
		return
	}
	n.out = append(n.out, env)
}

// broadcast sends m to every peer, in configuration order, optionally
// including Self (processed inline).
func (n *Node) broadcast(now time.Duration, m Message, includeSelf bool) {
	for _, p := range n.cfg.Peers {
		if p == n.cfg.Self && !includeSelf {
			continue
		}
		n.send(now, p, m)
	}
}

func (n *Node) handle(now time.Duration, env Envelope) {
	if n.failed != nil {
		return
	}
	switch m := env.Msg.(type) {
	case Prepare:
		n.onPrepare(now, env.From, m)
	case Promise:
		n.onPromise(now, env.From, m)
	case Accept:
		n.onAccept(now, env.From, m)
	case Accepted:
		n.onAccepted(now, env.From, m)
	case Nack:
		n.onNack(now, env.From, m)
	case Learn:
		n.onLearn(now, env.From, m)
	case LearnRequest:
		n.onLearnRequest(now, env.From, m)
	case Heartbeat:
		n.onHeartbeat(now, env.From, m)
	case HeartbeatAck:
		n.onHeartbeatAck(now, env.From, m)
	}
}

// --- acceptor and learner rules (design 5.2) ---

// setPromised raises the promise to b, persisting the round first and the
// promise second. A candidate or leader whose own ballot is now below the
// promise steps down: it can neither win nor keep proposing.
func (n *Node) setPromised(now time.Duration, b paxos.Ballot) {
	if b.Round > n.maxRound {
		n.maxRound = b.Round
		if err := n.store.SaveMaxRound(n.maxRound); err != nil {
			n.fail(err)
			return
		}
	}
	n.promised = b
	if err := n.store.SavePromised(b); err != nil {
		n.fail(err)
		return
	}
	if n.role != Follower && n.ballot.Less(b) {
		n.stepDown(now, b.Node, b, 0)
	}
}

func (n *Node) refreshLease(now time.Duration, b paxos.Ballot) {
	n.leaseHolder = b.Node
	n.leaseUntil = now + n.cfg.LeaseDuration
}

// noteLeader records that another node is acting as leader or candidate at
// ballot b and resets the election timer.
func (n *Node) noteLeader(now time.Duration, b paxos.Ballot) {
	n.electionDeadline = now + n.randomElectionTimeout()
	if n.leaderHint != b.Node || n.leaderBallot != b {
		n.leaderHint = b.Node
		n.leaderBallot = b
		n.emit(LeaderChanged{Leader: b.Node, Ballot: b, Self: false})
	}
}

func (n *Node) onPrepare(now time.Duration, from paxos.NodeID, m Prepare) {
	b := m.Ballot
	if b.IsZero() {
		return
	}
	if b != n.promised {
		if !paxos.MayPromise(n.promised, b) {
			n.send(now, from, Nack{Ballot: b, Promised: n.promised})
			return
		}
		if n.cfg.LeaseDuration > 0 && now < n.leaseUntil && b.Node != n.leaseHolder {
			// Lease rule: refuse without changing the promise. Refusing a
			// request never affects safety.
			n.send(now, from, Nack{Ballot: b, Promised: n.promised, LeaseRemaining: n.leaseUntil - now})
			return
		}
		n.setPromised(now, b)
		if n.failed != nil {
			return
		}
	}
	// b == promised is a retransmitted Prepare: answering it again is safe
	// because the promise is unchanged and any value accepted since carries
	// a ballot >= b, which the reply reports.
	if from != n.cfg.Self {
		n.refreshLease(now, b)
		n.noteLeader(now, b)
	}
	n.send(now, from, Promise{Ballot: b, Accepted: n.acceptedFrom(m.FromSlot), CommitIndex: n.commitIndex})
}

// acceptedFrom returns the accepted pvalues for slots >= from in slot order.
func (n *Node) acceptedFrom(from paxos.Slot) []paxos.PValue {
	var out []paxos.PValue
	for s, pv := range n.accepted {
		if s >= from {
			out = append(out, pv)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Slot < out[j].Slot })
	return out
}

func (n *Node) onAccept(now time.Duration, from paxos.NodeID, m Accept) {
	b := m.Ballot
	if b.IsZero() || m.Slot == 0 {
		return
	}
	allow := paxos.MayAccept(n.promised, b)
	if n.cfg.Unsafe != nil && n.cfg.Unsafe.AcceptBelowPromise {
		allow = true
	}
	if !allow {
		n.send(now, from, Nack{Ballot: b, Promised: n.promised, Slot: m.Slot})
		return
	}
	if n.promised.Less(b) {
		n.setPromised(now, b)
		if n.failed != nil {
			return
		}
	}
	pv := paxos.PValue{Ballot: b, Slot: m.Slot, Value: m.Value}
	if err := n.store.SaveAccepted(pv); err != nil {
		n.fail(err)
		return
	}
	n.accepted[m.Slot] = pv
	n.refreshLease(now, b)
	if from != n.cfg.Self {
		n.noteLeader(now, b)
	}
	n.send(now, from, Accepted{Ballot: b, Slot: m.Slot})
}

func (n *Node) onHeartbeat(now time.Duration, from paxos.NodeID, m Heartbeat) {
	b := m.Ballot
	if b.IsZero() {
		return
	}
	if b.Less(n.promised) {
		n.send(now, from, HeartbeatAck{Ballot: b, Promised: n.promised, CommitIndex: n.commitIndex, ReadSeq: m.ReadSeq})
		return
	}
	if n.promised.Less(b) {
		n.setPromised(now, b)
		if n.failed != nil {
			return
		}
	}
	n.refreshLease(now, b)
	if from != n.cfg.Self {
		n.noteLeader(now, b)
	}
	n.send(now, from, HeartbeatAck{Ballot: b, Promised: n.promised, CommitIndex: n.commitIndex, ReadSeq: m.ReadSeq})
	if from != n.cfg.Self && m.CommitIndex > n.commitIndex && now >= n.learnReqAfter {
		n.learnReqAfter = now + n.cfg.HeartbeatInterval
		n.send(now, from, LearnRequest{FromSlot: n.commitIndex + 1, MaxCount: n.cfg.LearnBatch})
	}
}

func (n *Node) onLearn(now time.Duration, from paxos.NodeID, m Learn) {
	if m.Slot == 0 {
		return
	}
	got := Entry{Slot: m.Slot, Ballot: m.Ballot, Value: m.Value}
	if have, ok := n.chosen[m.Slot]; ok {
		if !paxos.ValueEqual(have.Value, m.Value) {
			n.emit(LearnConflict{Slot: m.Slot, Have: have, Got: got})
		}
		return
	}
	n.learn(now, got)
}

// learn records a chosen entry, persisting it first, and advances the
// commit index. A leader drops any proposal it had for the slot.
func (n *Node) learn(now time.Duration, e Entry) {
	if err := n.store.SaveChosen(e); err != nil {
		n.fail(err)
		return
	}
	n.chosen[e.Slot] = e
	delete(n.proposals, e.Slot)
	n.advanceCommit()
	if n.role == Leader {
		n.drainQueue(now)
	}
}

func (n *Node) advanceCommit() {
	for {
		if _, ok := n.chosen[n.commitIndex+1]; !ok {
			return
		}
		n.commitIndex++
	}
}

func (n *Node) onLearnRequest(now time.Duration, from paxos.NodeID, m LearnRequest) {
	if from == n.cfg.Self || m.FromSlot == 0 {
		return
	}
	count := m.MaxCount
	if count < 1 || count > n.cfg.LearnBatch {
		count = n.cfg.LearnBatch
	}
	for s := m.FromSlot; s < m.FromSlot+paxos.Slot(count); s++ {
		if e, ok := n.chosen[s]; ok {
			n.send(now, from, Learn{Slot: s, Ballot: e.Ballot, Value: e.Value})
		}
	}
}

// --- candidate and leader rules (design 5.3) ---

func (n *Node) tick(now time.Duration) {
	switch n.role {
	case Follower:
		if now >= n.electionDeadline && (n.cfg.LeaseDuration == 0 || now >= n.leaseUntil) {
			n.startElection(now)
		}
	case Candidate:
		if now >= n.phase1.deadline {
			n.role = Follower
			n.phase1 = phase1State{}
			n.electionDeadline = now + n.randomElectionTimeout()
			return
		}
		if now >= n.phase1.lastSent+n.cfg.HeartbeatInterval {
			n.phase1.lastSent = now
			for _, p := range n.cfg.Peers {
				if p == n.cfg.Self {
					continue
				}
				if _, ok := n.phase1.promises[p]; ok {
					continue
				}
				n.send(now, p, Prepare{Ballot: n.ballot, FromSlot: n.phase1.fromSlot})
			}
		}
	case Leader:
		if now >= n.lastHeartbeat+n.cfg.HeartbeatInterval {
			n.lastHeartbeat = now
			n.broadcast(now, Heartbeat{Ballot: n.ballot, CommitIndex: n.commitIndex, ReadSeq: n.maxPendingRead()}, true)
			if n.role != Leader {
				return
			}
		}
		// Retransmission runs on each proposal's own timer, independent of
		// the heartbeat timer: ReadIndex also sends heartbeats, and a busy
		// read path must not starve the Accepts a stuck slot needs.
		for _, s := range sortedSlots(n.proposals) {
			p := n.proposals[s]
			if now >= p.lastSent+n.cfg.HeartbeatInterval {
				p.lastSent = now
				n.broadcast(now, Accept{Ballot: n.ballot, Slot: s, Value: p.value}, false)
				if n.role != Leader {
					return
				}
			}
		}
		if now-n.quorumContact(now) > n.cfg.ElectionTimeoutMax {
			n.stepDown(now, 0, paxos.Ballot{}, 0)
		}
	}
}

func (n *Node) startElection(now time.Duration) {
	n.maxRound++
	if err := n.store.SaveMaxRound(n.maxRound); err != nil {
		n.fail(err)
		return
	}
	n.ballot = paxos.Ballot{Round: n.maxRound, Node: n.cfg.Self}
	n.role = Candidate
	n.phase1 = phase1State{
		fromSlot: n.commitIndex + 1,
		promises: make(map[paxos.NodeID]Promise),
		deadline: now + n.randomElectionTimeout(),
		lastSent: now,
	}
	n.broadcast(now, Prepare{Ballot: n.ballot, FromSlot: n.phase1.fromSlot}, true)
}

func (n *Node) onPromise(now time.Duration, from paxos.NodeID, m Promise) {
	if n.role != Candidate || m.Ballot != n.ballot {
		return
	}
	n.phase1.promises[from] = m
	if len(n.phase1.promises) >= n.quorum {
		n.becomeLeader(now)
	}
}

// becomeLeader runs the takeover of Paxos Made Simple section 3: it
// re-proposes every value reported in the promises at its own ballot, fills
// the gaps with no-ops, proposes one leadership no-op after the highest
// reported slot, and only then takes client values.
func (n *Node) becomeLeader(now time.Duration) {
	n.role = Leader
	n.leaderHint = n.cfg.Self
	n.leaderBallot = n.ballot
	n.leaderSince = now
	n.lastHeartbeat = now
	n.proposals = make(map[paxos.Slot]*proposal)
	n.reads = make(map[uint64]*read)
	n.contacts = make(map[paxos.NodeID]time.Duration, len(n.cfg.Peers))
	n.emit(LeaderChanged{Leader: n.cfg.Self, Ballot: n.ballot, Self: true})

	reports := make(map[paxos.Slot][]paxos.PValue)
	top := n.phase1.fromSlot - 1
	for _, from := range sortedNodes(n.phase1.promises) {
		n.contacts[from] = now
		for _, pv := range n.phase1.promises[from].Accepted {
			if pv.Slot < n.phase1.fromSlot {
				continue
			}
			reports[pv.Slot] = append(reports[pv.Slot], pv)
			if pv.Slot > top {
				top = pv.Slot
			}
		}
	}
	for s := range n.chosen {
		if s > top {
			top = s
		}
	}
	n.firstOwnSlot = top + 1
	n.nextSlot = n.firstOwnSlot + 1
	for s := n.phase1.fromSlot; s <= top; s++ {
		if _, ok := n.chosen[s]; ok {
			continue
		}
		var v paxos.Value
		if n.cfg.Unsafe == nil || !n.cfg.Unsafe.IgnorePhase1Reports {
			v = paxos.Choose(reports[s], nil)
		}
		n.propose(now, s, v)
		if n.failed != nil || n.role != Leader {
			return
		}
	}
	n.propose(now, n.firstOwnSlot, nil)
	if n.failed != nil || n.role != Leader {
		return
	}
	n.drainQueue(now)
}

// propose starts Phase 2 for slot s with value v at the leader's ballot.
// The leader's own acceptor handles the Accept inline.
func (n *Node) propose(now time.Duration, s paxos.Slot, v paxos.Value) {
	n.proposals[s] = &proposal{value: v, accepts: make(map[paxos.NodeID]struct{}), lastSent: now}
	n.broadcast(now, Accept{Ballot: n.ballot, Slot: s, Value: v}, true)
}

func (n *Node) drainQueue(now time.Duration) {
	for len(n.queue) > 0 && len(n.proposals) < n.cfg.Window && n.role == Leader && n.failed == nil {
		v := n.queue[0]
		n.queue = n.queue[1:]
		n.propose(now, n.nextSlot, v)
		n.nextSlot++
	}
}

func (n *Node) onAccepted(now time.Duration, from paxos.NodeID, m Accepted) {
	if n.role != Leader || m.Ballot != n.ballot {
		return
	}
	p, ok := n.proposals[m.Slot]
	if !ok {
		return
	}
	p.accepts[from] = struct{}{}
	if len(p.accepts) < n.quorum {
		return
	}
	e := Entry{Slot: m.Slot, Ballot: n.ballot, Value: p.value}
	n.learn(now, e)
	if n.failed != nil {
		return
	}
	n.broadcast(now, Learn{Slot: e.Slot, Ballot: e.Ballot, Value: e.Value}, false)
}

func (n *Node) onNack(now time.Duration, from paxos.NodeID, m Nack) {
	if n.role == Follower || m.Ballot != n.ballot {
		return
	}
	if !n.ballot.Less(m.Promised) {
		// A lease refusal from an acceptor whose promise is below our
		// ballot: keep retrying until the Phase 1 deadline.
		return
	}
	if m.Promised.Round > n.maxRound {
		n.maxRound = m.Promised.Round
		if err := n.store.SaveMaxRound(n.maxRound); err != nil {
			n.fail(err)
			return
		}
	}
	n.stepDown(now, m.Promised.Node, m.Promised, m.LeaseRemaining)
}

func (n *Node) onHeartbeatAck(now time.Duration, from paxos.NodeID, m HeartbeatAck) {
	if n.role == Follower {
		return
	}
	if n.ballot.Less(m.Promised) {
		if m.Promised.Round > n.maxRound {
			n.maxRound = m.Promised.Round
			if err := n.store.SaveMaxRound(n.maxRound); err != nil {
				n.fail(err)
				return
			}
		}
		n.stepDown(now, m.Promised.Node, m.Promised, 0)
		return
	}
	if n.role != Leader || m.Ballot != n.ballot {
		return
	}
	n.contacts[from] = now
	if m.Promised != n.ballot {
		return
	}
	for _, seq := range sortedSeqs(n.reads) {
		if seq > m.ReadSeq {
			continue
		}
		r := n.reads[seq]
		r.acks[from] = struct{}{}
		if len(r.acks) >= n.quorum {
			n.emit(ReadReady{Seq: seq, Index: r.index})
			delete(n.reads, seq)
		}
	}
}

// stepDown returns the node to Follower. Pending reads fail with
// ErrNotLeader; pending proposals and queued values are dropped (the host
// fails pending submits with its own error). The next election is delayed
// by wait, the lease time a Nack reported.
func (n *Node) stepDown(now time.Duration, leader paxos.NodeID, lb paxos.Ballot, wait time.Duration) {
	for _, seq := range sortedSeqs(n.reads) {
		n.emit(ReadFailed{Seq: seq, Err: ErrNotLeader{Leader: leader}})
	}
	n.role = Follower
	n.proposals = make(map[paxos.Slot]*proposal)
	n.reads = make(map[uint64]*read)
	n.queue = nil
	n.contacts = nil
	n.phase1 = phase1State{}
	n.electionDeadline = now + wait + n.randomElectionTimeout()
	n.leaderHint = leader
	n.leaderBallot = lb
	n.emit(LeaderChanged{Leader: leader, Ballot: lb, Self: false})
}

// quorumContact returns the most recent time at which a quorum of peers
// (counting Self as contacted now) had all been heard from.
func (n *Node) quorumContact(now time.Duration) time.Duration {
	times := make([]time.Duration, 0, len(n.cfg.Peers))
	for _, p := range n.cfg.Peers {
		if p == n.cfg.Self {
			times = append(times, now)
			continue
		}
		if t, ok := n.contacts[p]; ok {
			times = append(times, t)
		}
	}
	if len(times) < n.quorum {
		return n.leaderSince
	}
	sort.Slice(times, func(i, j int) bool { return times[i] > times[j] })
	return times[n.quorum-1]
}

func (n *Node) maxPendingRead() uint64 {
	var m uint64
	for seq := range n.reads {
		if seq > m {
			m = seq
		}
	}
	return m
}

func sortedSlots(m map[paxos.Slot]*proposal) []paxos.Slot {
	out := make([]paxos.Slot, 0, len(m))
	for s := range m {
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func sortedSeqs(m map[uint64]*read) []uint64 {
	out := make([]uint64, 0, len(m))
	for s := range m {
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func sortedNodes(m map[paxos.NodeID]Promise) []paxos.NodeID {
	out := make([]paxos.NodeID, 0, len(m))
	for id := range m {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}
