package sim

import (
	"fmt"

	"github.com/oguzhanozfe/paxos-arena/internal/paxos"
	"github.com/oguzhanozfe/paxos-arena/internal/replog"
)

// nodeRecord is what the checker remembers about one node between calls.
type nodeRecord struct {
	promised     paxos.Ballot // last observed promise (durable, survives restarts)
	commit       paxos.Slot   // last observed commit index
	prePromised  paxos.Ballot // promise before the call in progress
	preSlot      paxos.Slot   // slot of the Accept delivered by the call in progress, or 0
	preAccepted  paxos.PValue // accepted pvalue for preSlot before the call
	preHad       bool
	crashDurable *replog.Durable // durable state at the last crash, until restart
}

type slotValue struct {
	slot  paxos.Slot
	value string
}

type ballotSlot struct {
	ballot paxos.Ballot
	slot   paxos.Slot
}

// Checker observes every node call, every message the nodes send and every
// applied entry, and reports the first violation of the log's safety
// invariants S1 through S7 (design section 6.1). It has a global view of
// every node's state through the observation methods of replog.Node.
//
// The checks are incremental: each hook examines only what the current
// event could have changed, so the cost per event is small.
type Checker struct {
	// chosen records the value first observed chosen for each slot, from
	// Learn messages and from nodes' Chosen after they learn (S1, S2).
	chosen map[paxos.Slot]paxos.Value
	// chosenValues maps a non-empty value to the first slot it was chosen
	// in; the clients use it to know when a command is done.
	chosenValues map[string]paxos.Slot
	// hashAt records the state hash after applying each slot (S2).
	hashAt map[paxos.Slot][32]byte
	// majority records a value held by a quorum of acceptors for a slot (S1
	// derived from acceptor state rather than from Learn).
	majority map[paxos.Slot]string
	// proposedWhenMax records, for the first Accept seen for (slot, value),
	// the highest slot chosen anywhere at that moment (S6).
	proposedWhenMax map[slotValue]paxos.Slot
	// ballotOwner and ballotSlotValue enforce ballot uniqueness (S4).
	ballotOwner     map[paxos.Ballot]paxos.NodeID
	ballotSlotValue map[ballotSlot]string
	// maxChosen is the highest slot observed chosen anywhere; maxCommitSeen
	// the highest commit index observed on any node (S6, S7).
	maxChosen     paxos.Slot
	maxCommitSeen paxos.Slot
	nodes         []*nodeRecord
	err           error
}

func newChecker(r *Run) *Checker {
	c := &Checker{
		chosen:          make(map[paxos.Slot]paxos.Value),
		chosenValues:    make(map[string]paxos.Slot),
		hashAt:          make(map[paxos.Slot][32]byte),
		majority:        make(map[paxos.Slot]string),
		proposedWhenMax: make(map[slotValue]paxos.Slot),
		ballotOwner:     make(map[paxos.Ballot]paxos.NodeID),
		ballotSlotValue: make(map[ballotSlot]string),
	}
	for range r.nodes {
		c.nodes = append(c.nodes, &nodeRecord{})
	}
	return c
}

// fail records the first violation.
func (c *Checker) fail(r *Run, invariant, format string, args ...any) {
	if c.err != nil {
		return
	}
	c.err = &Violation{
		Invariant: invariant,
		Detail:    fmt.Sprintf(format, args...),
		Seed:      r.p.Seed,
		Step:      r.steps,
		Scenario:  r.p.Scenario,
		At:        r.clock,
	}
	r.tracef("VIOLATION %v", c.err)
}

// maxCommit returns the highest commit index observed on any node so far,
// crashed nodes included.
func (c *Checker) maxCommit() paxos.Slot { return c.maxCommitSeen }

// Chosen returns the recorded chosen value for a slot.
func (c *Checker) Chosen(s paxos.Slot) (paxos.Value, bool) {
	v, ok := c.chosen[s]
	return v, ok
}

// beforeCall snapshots what a node call may change: its promise, and the
// accepted pvalue of the slot an Accept being delivered names.
func (c *Checker) beforeCall(nd *simNode, delivered *replog.Envelope) {
	rec := c.nodes[int(nd.id)-1]
	rec.prePromised = nd.node.Promised()
	rec.preSlot = 0
	if delivered != nil {
		if a, ok := delivered.Msg.(replog.Accept); ok {
			rec.preSlot = a.Slot
			rec.preAccepted, rec.preHad = nd.node.Accepted(a.Slot)
		}
	}
}

// observeSend inspects a message a node is about to send: ballot ownership
// and one value per (ballot, slot) for S4, the proposal time of each
// (slot, value) for S6, and chosen values announced by Learn for S1.
func (c *Checker) observeSend(r *Run, nd *simNode, env replog.Envelope) {
	switch m := env.Msg.(type) {
	case replog.Accept:
		c.checkBallotOwner(r, m.Ballot, env.From)
		c.checkBallotSlotValue(r, m.Ballot, m.Slot, m.Value, env.From)
		if len(m.Value) > 0 {
			key := slotValue{m.Slot, string(m.Value)}
			if _, ok := c.proposedWhenMax[key]; !ok {
				c.proposedWhenMax[key] = c.maxChosen
			}
		}
	case replog.Prepare:
		c.checkBallotOwner(r, m.Ballot, env.From)
	case replog.Heartbeat:
		c.checkBallotOwner(r, m.Ballot, env.From)
	case replog.Learn:
		c.recordChosen(r, nd, m.Slot, replog.Entry{Slot: m.Slot, Ballot: m.Ballot, Value: m.Value}, "Learn sent by node")
	}
}

func (c *Checker) checkBallotOwner(r *Run, b paxos.Ballot, from paxos.NodeID) {
	if b.Node != from {
		c.fail(r, "S4", "node %d uses ballot %v whose node component is not its own", from, b)
		return
	}
	if owner, ok := c.ballotOwner[b]; ok && owner != from {
		c.fail(r, "S4", "ballot %v used by node %d and node %d", b, owner, from)
		return
	}
	c.ballotOwner[b] = from
}

func (c *Checker) checkBallotSlotValue(r *Run, b paxos.Ballot, s paxos.Slot, v paxos.Value, from paxos.NodeID) {
	key := ballotSlot{b, s}
	if prev, ok := c.ballotSlotValue[key]; ok {
		if prev != string(v) {
			c.fail(r, "S4", "two values proposed for ballot %v slot %d: %q and %q (node %d)", b, s, prev, v, from)
		}
		return
	}
	c.ballotSlotValue[key] = string(v)
}

// afterCall checks what one node call changed.
func (c *Checker) afterCall(r *Run, nd *simNode, delivered *replog.Envelope, outs []replog.Envelope) {
	rec := c.nodes[int(nd.id)-1]
	n := nd.node
	// S3: the promise never decreases.
	if n.Promised().Less(rec.prePromised) {
		c.fail(r, "S3", "node %d promise decreased from %v to %v", nd.id, rec.prePromised, n.Promised())
	}
	// S3: an acceptance made during this call is at or above the promise
	// in force when the call began.
	if rec.preSlot != 0 {
		if pv, ok := n.Accepted(rec.preSlot); ok {
			changed := !rec.preHad || pv.Ballot != rec.preAccepted.Ballot || !paxos.ValueEqual(pv.Value, rec.preAccepted.Value)
			if changed {
				if pv.Ballot.Less(rec.prePromised) {
					c.fail(r, "S3", "node %d accepted ballot %v for slot %d while promised %v", nd.id, pv.Ballot, pv.Slot, rec.prePromised)
				}
				c.checkBallotSlotValue(r, pv.Ballot, pv.Slot, pv.Value, pv.Ballot.Node)
				c.checkMajority(r, rec.preSlot)
			}
		}
	}
	// Commit index is monotone within one incarnation.
	if n.CommitIndex() < rec.commit {
		c.fail(r, "S2", "node %d commit index decreased from %d to %d", nd.id, rec.commit, n.CommitIndex())
	}
	rec.commit = n.CommitIndex()
	rec.promised = n.Promised()
	if rec.commit > c.maxCommitSeen {
		c.maxCommitSeen = rec.commit
	}
	// S4: a candidate or leader's ballot names itself and nobody else uses it.
	if n.Role() != replog.Follower {
		c.checkBallotOwner(r, n.Ballot(), nd.id)
	}
	// S1: what the node learned in this call agrees with the global record.
	if delivered != nil {
		if l, ok := delivered.Msg.(replog.Learn); ok {
			if e, ok := n.Chosen(l.Slot); ok {
				c.recordChosen(r, nd, l.Slot, e, "Chosen on node after Learn")
			}
		}
	}
	for _, o := range outs {
		if l, ok := o.Msg.(replog.Learn); ok {
			if e, ok := n.Chosen(l.Slot); ok {
				c.recordChosen(r, nd, l.Slot, e, "Chosen on node sending Learn")
			}
		}
	}
}

// checkMajority derives "chosen" from acceptor state: a (ballot, value)
// held by a quorum of live acceptors for a slot. Two different such values
// over time, or a difference from a learned value, violate S1.
func (c *Checker) checkMajority(r *Run, s paxos.Slot) {
	counts := make(map[string]int)
	values := make(map[string]string)
	for _, o := range r.nodes {
		if !o.alive {
			continue
		}
		pv, ok := o.node.Accepted(s)
		if !ok {
			continue
		}
		k := pv.Ballot.String() + "|" + string(pv.Value)
		counts[k]++
		values[k] = string(pv.Value)
	}
	for k, cnt := range counts {
		if cnt < r.quorum {
			continue
		}
		v := values[k]
		if prev, ok := c.majority[s]; ok && prev != v {
			c.fail(r, "S1", "slot %d: a quorum accepted %q after a quorum had accepted %q", s, v, prev)
			return
		}
		c.majority[s] = v
		if cv, ok := c.chosen[s]; ok && string(cv) != v {
			c.fail(r, "S1", "slot %d: a quorum accepted %q but %q was learned as chosen", s, v, cv)
			return
		}
	}
}

// recordChosen compares an observed chosen entry with the record, adding it
// on first sight, and applies the gap rule (S6).
func (c *Checker) recordChosen(r *Run, nd *simNode, s paxos.Slot, e replog.Entry, how string) {
	if v, ok := c.chosen[s]; ok {
		if !paxos.ValueEqual(v, e.Value) {
			c.fail(r, "S1", "slot %d chosen as %q (%s %d, ballot %v) but recorded as %q", s, e.Value, how, nd.id, e.Ballot, v)
		}
		return
	}
	c.chosen[s] = e.Value
	if !e.NoOp() {
		if _, ok := c.chosenValues[string(e.Value)]; !ok {
			c.chosenValues[string(e.Value)] = s
		}
		if m, ok := c.proposedWhenMax[slotValue{s, string(e.Value)}]; ok && m > s {
			c.fail(r, "S6", "slot %d chosen with command %q that was first proposed after slot %d was already chosen", s, e.Value, m)
		}
	}
	if mv, ok := c.majority[s]; ok && mv != string(e.Value) {
		c.fail(r, "S1", "slot %d learned as %q but a quorum had accepted %q", s, e.Value, mv)
	}
	if s > c.maxChosen {
		c.maxChosen = s
	}
}

// observeApply checks an applied entry against the chosen record and the
// state hash other nodes reached at the same slot (S2).
func (c *Checker) observeApply(r *Run, nd *simNode, e replog.Entry, hash [32]byte) {
	if v, ok := c.chosen[e.Slot]; ok {
		if !paxos.ValueEqual(v, e.Value) {
			c.fail(r, "S2", "node %d applied %q at slot %d but %q was chosen", nd.id, e.Value, e.Slot, v)
			return
		}
	} else {
		c.recordChosen(r, nd, e.Slot, e, "applied on node")
	}
	if h, ok := c.hashAt[e.Slot]; ok {
		if h != hash {
			c.fail(r, "S2", "node %d state hash after slot %d differs from the hash first recorded at that slot", nd.id, e.Slot)
		}
		return
	}
	c.hashAt[e.Slot] = hash
}

// observeCrash keeps the durable state at the moment of a crash.
func (c *Checker) observeCrash(nd *simNode, d replog.Durable) {
	c.nodes[int(nd.id)-1].crashDurable = &d
}

// observeRestart checks that a restarted node recovered its durable state
// (S5) and that its promise and commit index did not regress.
func (c *Checker) observeRestart(r *Run, nd *simNode) {
	rec := c.nodes[int(nd.id)-1]
	n := nd.node
	if d := rec.crashDurable; d != nil {
		if n.Promised().Less(d.Promised) {
			c.fail(r, "S5", "node %d restarted with promise %v below the durable %v", nd.id, n.Promised(), d.Promised)
		}
		if n.MaxRound() < d.MaxRound {
			c.fail(r, "S5", "node %d restarted with max round %d below the durable %d", nd.id, n.MaxRound(), d.MaxRound)
		}
		for _, pv := range d.Accepted {
			got, ok := n.Accepted(pv.Slot)
			if !ok {
				c.fail(r, "S5", "node %d lost the accepted pvalue for slot %d (%v) across a restart", nd.id, pv.Slot, pv.Ballot)
				break
			}
			if got.Ballot.Less(pv.Ballot) || (got.Ballot == pv.Ballot && !paxos.ValueEqual(got.Value, pv.Value)) {
				c.fail(r, "S5", "node %d restarted with accepted %v for slot %d, durable was %v", nd.id, got.Ballot, pv.Slot, pv.Ballot)
				break
			}
		}
		for _, e := range d.Chosen {
			got, ok := n.Chosen(e.Slot)
			if !ok || !paxos.ValueEqual(got.Value, e.Value) {
				c.fail(r, "S5", "node %d lost the chosen entry for slot %d across a restart", nd.id, e.Slot)
				break
			}
		}
		rec.crashDurable = nil
	}
	if n.Promised().Less(rec.promised) {
		c.fail(r, "S3", "node %d promise decreased across a restart from %v to %v", nd.id, rec.promised, n.Promised())
	}
	if n.CommitIndex() < rec.commit {
		c.fail(r, "S5", "node %d commit index regressed across a restart from %d to %d", nd.id, rec.commit, n.CommitIndex())
	}
	rec.promised = n.Promised()
	rec.commit = n.CommitIndex()
}

// observeReadReady checks S7: the index a read barrier returns is at least
// the highest commit index reached anywhere before the read was issued.
func (c *Checker) observeReadReady(r *Run, nd *simNode, pr pendingRead, index paxos.Slot) {
	if index < pr.maxCommit {
		c.fail(r, "S7", "node %d answered a read issued at t=%v with index %d, but commit index %d had been reached before the call",
			nd.id, pr.issued, index, pr.maxCommit)
	}
}

// AfterEvent returns the first violation found so far. The checks
// themselves run in the hooks the run loop calls during each event.
func (c *Checker) AfterEvent(r *Run) error { return c.err }

// AtEnd replays the chosen prefix into a fresh state machine and compares
// its hashes with those every node produced (S2), and checks every live
// node's chosen prefix against the record (S1).
func (c *Checker) AtEnd(r *Run) error {
	if c.err != nil {
		return c.err
	}
	st := newByteState()
	for s := paxos.Slot(1); ; s++ {
		v, ok := c.chosen[s]
		if !ok {
			break
		}
		h := st.apply(replog.Entry{Slot: s, Value: v})
		if rh, ok := c.hashAt[s]; ok && rh != h {
			c.fail(r, "S2", "replaying the chosen log gives a different state hash at slot %d than the nodes recorded", s)
			return c.err
		}
	}
	for _, nd := range r.nodes {
		if !nd.alive {
			continue
		}
		for s := paxos.Slot(1); s <= nd.node.CommitIndex(); s++ {
			e, ok := nd.node.Chosen(s)
			if !ok {
				c.fail(r, "S2", "node %d: commit index %d but slot %d is not chosen", nd.id, nd.node.CommitIndex(), s)
				return c.err
			}
			if v, ok := c.chosen[s]; ok && !paxos.ValueEqual(v, e.Value) {
				c.fail(r, "S1", "node %d holds %q at slot %d, recorded chosen value is %q", nd.id, e.Value, s, v)
				return c.err
			}
		}
	}
	return c.err
}
