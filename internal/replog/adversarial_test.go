package replog

// Adversarial tests. Each test encodes one attack on the safety argument of
// the replicated log: the execution is either constructed by hand, with full
// control over which message reaches which node and in what order, or found
// by a seeded random scheduler with an independent checker. Every test states
// what the protocol must guarantee under that execution; a failure is a
// safety bug. Liveness is asserted only where the execution ends with every
// node up and every link open.

import (
	"fmt"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/oguzhanozfe/paxos-arena/internal/paxos"
)

// --- helpers on the cluster harness ---

// deliverWhere delivers the queued envelopes for which keep returns true, in
// queue order, and leaves the others queued. Replies join the queue and are
// subject to keep on later passes, so the call returns only when no queued
// envelope matches.
func (c *cluster) deliverWhere(keep func(Envelope) bool) {
	for {
		idx := -1
		for i, env := range c.queue {
			if keep(env) {
				idx = i
				break
			}
		}
		if idx < 0 {
			return
		}
		env := c.queue[idx]
		c.queue = append(c.queue[:idx:idx], c.queue[idx+1:]...)
		if c.drop != nil && c.drop(env) {
			continue
		}
		if n, ok := c.nodes[env.To]; ok {
			c.enqueue(env.To, n.Step(c.now, env))
		}
	}
}

// tickOnly advances the clock by one step and ticks node id alone.
func (c *cluster) tickOnly(id paxos.NodeID) {
	c.now += tickStep
	if n, ok := c.nodes[id]; ok {
		c.enqueue(id, n.Tick(c.now))
	}
}

// candidateOnly ticks node id alone until it starts an election.
func (c *cluster) candidateOnly(id paxos.NodeID) *Node {
	c.t.Helper()
	n := c.nodes[id]
	for i := 0; i < 1000 && n.Role() != Candidate; i++ {
		c.tickOnly(id)
	}
	if n.Role() != Candidate {
		c.t.Fatalf("node %d never became a candidate", id)
	}
	return n
}

// conflicts returns every LearnConflict any node emitted so far.
func (c *cluster) conflicts() []LearnConflict {
	var out []LearnConflict
	for _, id := range c.ids {
		if n, ok := c.nodes[id]; ok {
			c.events[id] = append(c.events[id], n.Events()...)
		}
		for _, ev := range c.events[id] {
			if lc, ok := ev.(LearnConflict); ok {
				out = append(out, lc)
			}
		}
	}
	return out
}

// requireOneValuePerSlot fails when two live nodes hold different chosen
// values for one slot, or when any node reported a LearnConflict. It scans
// well past the committed prefix so that slots chosen above a gap count.
func (c *cluster) requireOneValuePerSlot() map[paxos.Slot]string {
	c.t.Helper()
	if lcs := c.conflicts(); len(lcs) > 0 {
		c.t.Fatalf("LearnConflict emitted: %+v", lcs[0])
	}
	limit := c.maxChosen() + 2*DefaultWindow
	values := make(map[paxos.Slot]string)
	for s := paxos.Slot(1); s <= limit; s++ {
		for _, id := range c.ids {
			n, ok := c.nodes[id]
			if !ok {
				continue
			}
			e, ok := n.Chosen(s)
			if !ok {
				continue
			}
			if prev, seen := values[s]; seen && prev != string(e.Value) {
				c.t.Fatalf("slot %d: node %d holds %q, another node holds %q", s, id, e.Value, prev)
			}
			values[s] = string(e.Value)
		}
	}
	return values
}

// others returns the identifiers other than id, in configuration order.
func (c *cluster) others(id paxos.NodeID) []paxos.NodeID {
	var out []paxos.NodeID
	for _, x := range c.ids {
		if x != id {
			out = append(out, x)
		}
	}
	return out
}

// --- attack 1: a value accepted by a majority but never learned ---

// TestAttackChosenButUnlearnedValueSurvivesTakeover: a value is accepted by
// exactly a quorum (the leader and two followers) and the Accepted replies
// are lost, so no node learns it; a higher slot is then chosen normally so
// the slot is a gap; the leader crashes. The new leader's Phase 1 quorum
// shares exactly one node with the accepting quorum. The takeover must fill
// the gap with the accepted value, never with a no-op, whichever of the two
// followers is the one that reports it (P2c of Paxos Made Simple).
func TestAttackChosenButUnlearnedValueSurvivesTakeover(t *testing.T) {
	for shared := 0; shared < 2; shared++ {
		t.Run(fmt.Sprintf("reporter=%d", shared), func(t *testing.T) {
			c := newCluster(t, 5, nil)
			l := c.electLeader()
			others := c.others(l.Self())
			a1, a2, cand, spare := others[0], others[1], others[2], others[3]
			reporter, silent := a1, a2
			if shared == 1 {
				reporter, silent = a2, a1
			}
			c.propose(l, paxos.Value("before"))
			target := l.CommitIndex() + 1
			want := paxos.Value("chosen-unlearned")
			c.drop = func(e Envelope) bool {
				switch m := e.Msg.(type) {
				case Accept:
					return m.Slot == target && e.To != a1 && e.To != a2
				case Accepted:
					return m.Slot == target
				}
				return false
			}
			outs, err := l.Propose(c.now, want)
			if err != nil {
				t.Fatal(err)
			}
			c.enqueue(l.Self(), outs)
			c.deliverAll()
			for _, id := range []paxos.NodeID{l.Self(), a1, a2} {
				pv, ok := c.nodes[id].Accepted(target)
				if !ok || !paxos.ValueEqual(pv.Value, want) {
					t.Fatalf("setup: node %d accepted %+v ok=%t, want %q", id, pv, ok, want)
				}
			}
			for _, id := range c.ids {
				if _, ok := c.nodes[id].Chosen(target); ok {
					t.Fatalf("setup: node %d learned slot %d although every Accepted was lost", id, target)
				}
			}
			// A higher slot is chosen normally, so the target is a gap that a
			// naive takeover would fill with a no-op.
			c.propose(l, paxos.Value("after"))
			if e, ok := l.Chosen(target + 1); !ok || string(e.Value) != "after" {
				t.Fatalf("setup: slot %d = %+v ok=%t, want after", target+1, e, ok)
			}
			if l.CommitIndex() != target-1 {
				t.Fatalf("setup: leader commit index %d, want %d (stuck at the gap)", l.CommitIndex(), target-1)
			}
			c.crash(l.Self())

			// The candidate's Phase 1 reaches cand, spare and reporter only.
			c.drop = func(e Envelope) bool {
				_, isPrepare := e.Msg.(Prepare)
				return isPrepare && e.To == silent
			}
			cn := c.candidateOnly(cand)
			c.now += DefaultLeaseDuration // every lease from the old leader has run out
			c.deliverWhere(func(e Envelope) bool { return true })
			if cn.Role() != Leader {
				t.Fatalf("node %d did not win with quorum {%d, %d, %d}: role %v", cand, cand, spare, reporter, cn.Role())
			}
			pv, ok := cn.Accepted(target)
			if !ok || pv.Ballot != cn.Ballot() || !paxos.ValueEqual(pv.Value, want) {
				t.Fatalf("takeover proposed %+v (ok=%t) for slot %d, want %q at %v", pv, ok, target, want, cn.Ballot())
			}
			c.drop = nil
			done := c.runUntil(5*time.Second, func() bool {
				for _, id := range c.ids {
					n, ok := c.nodes[id]
					if !ok {
						continue
					}
					if e, ok := n.Chosen(target); !ok || !paxos.ValueEqual(e.Value, want) {
						return false
					}
				}
				return true
			})
			if !done {
				t.Fatalf("slot %d not chosen as %q on every live node", target, want)
			}
			old := c.restart(l.Self())
			if !c.runUntil(5*time.Second, func() bool { _, ok := old.Chosen(target); return ok }) {
				t.Fatalf("restarted old leader never learned slot %d", target)
			}
			values := c.requireOneValuePerSlot()
			if values[target] != string(want) {
				t.Fatalf("slot %d = %q after convergence, want %q", target, values[target], want)
			}
		})
	}
}

// --- attack 2: stale, duplicated and foreign Accepted replies ---

// TestAttackStaleAcceptedNeverCountsTowardQuorum feeds a leader every
// Accepted it must not count: for a ballot of its earlier term, for a slot
// it never proposed, the same peer's reply three times over, and a reply
// from a node outside the membership. None may choose the slot; the genuine
// third acceptance then does.
func TestAttackStaleAcceptedNeverCountsTowardQuorum(t *testing.T) {
	ids := []paxos.NodeID{1, 2, 3, 4, 5}
	n, err := New(DefaultConfig(1, ids), NewMemStore(), rand.New(rand.NewPCG(9, 0)))
	if err != nil {
		t.Fatal(err)
	}
	var now time.Duration
	// First candidacy times out so that a lower round of this node exists.
	tickUntilCandidate(t, n, &now)
	first := n.Ballot()
	for n.Role() == Candidate {
		now += tickStep
		n.Tick(now)
	}
	outs := tickUntilCandidate(t, n, &now)
	b := n.Ballot()
	if !first.Less(b) {
		t.Fatalf("second candidacy ballot %v not above the first %v", b, first)
	}
	_ = outs
	n.Step(now, Envelope{From: 2, To: 1, Msg: Promise{Ballot: b}})
	outs = n.Step(now, Envelope{From: 3, To: 1, Msg: Promise{Ballot: b}})
	if n.Role() != Leader {
		t.Fatalf("role %v after a quorum of promises, want Leader", n.Role())
	}
	var slot paxos.Slot
	for _, e := range outs {
		if a, ok := e.Msg.(Accept); ok {
			slot = a.Slot
		}
	}
	if slot == 0 {
		t.Fatal("no Accept for the leadership no-op was sent")
	}
	n.Events()
	feed := func(from paxos.NodeID, m Accepted, times int) {
		t.Helper()
		for i := 0; i < times; i++ {
			for _, e := range n.Step(now, Envelope{From: from, To: 1, Msg: m}) {
				if _, ok := e.Msg.(Learn); ok {
					t.Fatalf("Learn sent after Accepted %+v from %d (x%d): the slot was chosen on bad evidence", m, from, times)
				}
			}
		}
		if _, ok := n.Chosen(slot); ok {
			t.Fatalf("slot %d chosen after Accepted %+v from %d (x%d)", slot, m, from, times)
		}
	}
	feed(2, Accepted{Ballot: first, Slot: slot}, 1)
	feed(3, Accepted{Ballot: first, Slot: slot}, 1)
	feed(4, Accepted{Ballot: first, Slot: slot}, 1)
	feed(5, Accepted{Ballot: first, Slot: slot}, 1)
	feed(2, Accepted{Ballot: b, Slot: slot + 7}, 1)
	feed(3, Accepted{Ballot: b, Slot: slot + 7}, 1)
	feed(2, Accepted{Ballot: b, Slot: slot}, 3) // self + node 2 = 2 < 3, however often
	feed(9, Accepted{Ballot: b, Slot: slot}, 1) // not a peer
	feed(1, Accepted{Ballot: b, Slot: slot}, 2) // a spoofed self reply changes nothing
	outs = n.Step(now, Envelope{From: 3, To: 1, Msg: Accepted{Ballot: b, Slot: slot}})
	if e, ok := n.Chosen(slot); !ok || !e.NoOp() || e.Ballot != b {
		t.Fatalf("slot %d = %+v ok=%t after the genuine third acceptance, want the no-op at %v", slot, e, ok, b)
	}
	learns := 0
	for _, e := range outs {
		if l, ok := e.Msg.(Learn); ok && l.Slot == slot {
			learns++
		}
	}
	if learns != len(ids)-1 {
		t.Fatalf("%d Learns sent for slot %d, want one per peer (%d)", learns, slot, len(ids)-1)
	}
}

// --- attack 3: stale, duplicated and foreign Promises ---

// TestAttackDuplicateAndStalePromisesDoNotElect feeds a candidate the
// promises it must not count: one peer's promise repeated, promises for its
// own earlier round, and a promise for another node's ballot. It must stay a
// candidate until two distinct peers promise its current ballot.
func TestAttackDuplicateAndStalePromisesDoNotElect(t *testing.T) {
	ids := []paxos.NodeID{1, 2, 3, 4, 5}
	n, err := New(DefaultConfig(1, ids), NewMemStore(), rand.New(rand.NewPCG(4, 0)))
	if err != nil {
		t.Fatal(err)
	}
	var now time.Duration
	tickUntilCandidate(t, n, &now)
	old := n.Ballot()
	// Promises for the old round arrive after the candidacy timed out and a
	// new one started.
	for n.Role() == Candidate {
		now += tickStep
		n.Tick(now)
	}
	tickUntilCandidate(t, n, &now)
	b := n.Ballot()
	if !old.Less(b) {
		t.Fatalf("new ballot %v not above old %v", b, old)
	}
	deliver := func(from paxos.NodeID, m Promise, times int) {
		t.Helper()
		for i := 0; i < times; i++ {
			n.Step(now, Envelope{From: from, To: 1, Msg: m})
		}
		if n.Role() != Candidate {
			t.Fatalf("role %v after Promise %+v from %d (x%d), want Candidate", n.Role(), m, from, times)
		}
	}
	deliver(2, Promise{Ballot: old}, 1)
	deliver(3, Promise{Ballot: old}, 1)
	deliver(4, Promise{Ballot: old}, 1)
	deliver(2, Promise{Ballot: b}, 3)
	deliver(3, Promise{Ballot: paxos.Ballot{Round: b.Round, Node: 3}}, 1)
	deliver(9, Promise{Ballot: b}, 1)
	deliver(1, Promise{Ballot: b}, 2)
	n.Step(now, Envelope{From: 3, To: 1, Msg: Promise{Ballot: b}})
	if n.Role() != Leader {
		t.Fatalf("role %v after promises from 2 and 3 at %v, want Leader", n.Role(), b)
	}
}

// --- attack 4: an isolated leader keeps leading and taking proposals ---

// TestAttackIsolatedLeaderNeverGetsItsValuesChosen isolates the leader
// completely. It keeps leading until it loses majority contact, and during
// that window it accepts client proposals into its own acceptor. Meanwhile
// the majority elects a new leader and chooses values in the same slots.
// After the links reopen, every slot must hold one value on every node, none
// of the isolated leader's proposals may be chosen, its read barrier must
// fail rather than complete, and a third election must not resurrect the
// stale acceptances it still holds. Run with the lease on and off.
func TestAttackIsolatedLeaderNeverGetsItsValuesChosen(t *testing.T) {
	for _, lease := range []time.Duration{DefaultLeaseDuration, 0} {
		t.Run(fmt.Sprintf("lease=%v", lease), func(t *testing.T) {
			c := newCluster(t, 5, func(cfg *Config) { cfg.LeaseDuration = lease })
			l := c.electLeader()
			c.propose(l, paxos.Value("shared"))
			c.drop = func(e Envelope) bool { return e.From == l.Self() || e.To == l.Self() }
			stale := map[string]bool{}
			for i := 0; i < 4; i++ {
				v := fmt.Sprintf("stale-%d", i)
				stale[v] = true
				outs, err := l.Propose(c.now, paxos.Value(v))
				if err != nil {
					t.Fatalf("isolated leader refused a proposal: %v", err)
				}
				c.enqueue(l.Self(), outs)
			}
			readSeq, outs, err := l.ReadIndex(c.now)
			if err != nil {
				t.Fatalf("isolated leader refused ReadIndex: %v", err)
			}
			c.enqueue(l.Self(), outs)
			var l2 *Node
			overlap := false
			elected := c.runUntil(3*time.Second, func() bool {
				for _, id := range c.others(l.Self()) {
					if n := c.nodes[id]; n.Role() == Leader && n.Ready() {
						l2 = n
						overlap = l.Role() == Leader
						return true
					}
				}
				return false
			})
			if !elected {
				t.Fatal("the majority never elected a new leader")
			}
			t.Logf("lease %v: new leader %d at %v; old leader still leading at that moment: %t", lease, l2.Self(), l2.Ballot(), overlap)
			for i := 0; i < 4; i++ {
				c.propose(l2, paxos.Value(fmt.Sprintf("fresh-%d", i)))
			}
			c.drop = nil
			converged := c.runUntil(5*time.Second, func() bool {
				if l.Role() == Leader {
					return false
				}
				ci := l2.CommitIndex()
				for _, id := range c.ids {
					if c.nodes[id].CommitIndex() != ci {
						return false
					}
				}
				return true
			})
			if !converged {
				t.Fatalf("nodes did not converge after the links reopened: old leader role %v", l.Role())
			}
			values := c.requireOneValuePerSlot()
			for s, v := range values {
				if stale[v] {
					t.Fatalf("slot %d holds %q, proposed by the isolated leader", s, v)
				}
			}
			var readFailed, readReady bool
			for _, ev := range c.events[l.Self()] {
				switch e := ev.(type) {
				case ReadFailed:
					readFailed = readFailed || e.Seq == readSeq
				case ReadReady:
					readReady = readReady || e.Seq == readSeq
				}
			}
			if readReady || !readFailed {
				t.Fatalf("isolated leader's read %d: ready=%t failed=%t, want failed only", readSeq, readReady, readFailed)
			}
			// The stale acceptances at the old leader must lose every later
			// Phase 1 to the values chosen at the higher ballot.
			c.crash(l2.Self())
			l3 := c.electLeader()
			if l3.Self() == l2.Self() {
				t.Fatal("crashed leader was elected")
			}
			c.propose(l3, paxos.Value("later"))
			after := c.requireOneValuePerSlot()
			for s, v := range values {
				if after[s] != v {
					t.Fatalf("slot %d changed from %q to %q across the third election", s, v, after[s])
				}
			}
		})
	}
}

// --- attack 5: a stale leader learns a slot at its next free slot ---

// TestAttackStaleLeaderSlotCollisionIsHarmless: a leader that has not yet
// heard of a higher ballot learns, through a Learn from the new leader, a
// value in the very slot it would use next, and then takes a client
// proposal. The proposal lands in the taken slot at the stale ballot. The
// stale leader must never announce its own value for that slot, must keep
// the learned value, and must step down on the first refusal.
func TestAttackStaleLeaderSlotCollisionIsHarmless(t *testing.T) {
	ids := threeNodes()
	n, err := New(DefaultConfig(1, ids), NewMemStore(), rand.New(rand.NewPCG(2, 0)))
	if err != nil {
		t.Fatal(err)
	}
	var now time.Duration
	tickUntilCandidate(t, n, &now)
	b1 := n.Ballot()
	outs := n.Step(now, Envelope{From: 2, To: 1, Msg: Promise{Ballot: b1}})
	if n.Role() != Leader {
		t.Fatalf("role %v, want Leader", n.Role())
	}
	var noop paxos.Slot
	for _, e := range outs {
		if a, ok := e.Msg.(Accept); ok {
			noop = a.Slot
		}
	}
	n.Step(now, Envelope{From: 2, To: 1, Msg: Accepted{Ballot: b1, Slot: noop}})
	if !n.Ready() {
		t.Fatal("leader not ready after its no-op was chosen")
	}
	next := noop + 1
	b2 := paxos.Ballot{Round: b1.Round + 1, Node: 3}
	learned := paxos.Value("from-new-leader")
	n.Step(now, Envelope{From: 3, To: 1, Msg: Learn{Slot: next, Ballot: b2, Value: learned}})
	if e, ok := n.Chosen(next); !ok || !paxos.ValueEqual(e.Value, learned) {
		t.Fatalf("slot %d = %+v ok=%t, want the learned value", next, e, ok)
	}
	if n.Role() != Leader {
		t.Fatalf("a Learn alone changed the role to %v; the test needs a stale leader", n.Role())
	}
	outs, err = n.Propose(now, paxos.Value("stale"))
	if err != nil {
		t.Fatal(err)
	}
	collided := false
	for _, e := range outs {
		switch m := e.Msg.(type) {
		case Accept:
			if m.Slot == next {
				collided = true
			}
		case Learn:
			if m.Slot == next && !paxos.ValueEqual(m.Value, learned) {
				t.Fatalf("stale leader announced %q for slot %d, which it had learned as %q", m.Value, next, learned)
			}
		}
	}
	if !collided {
		t.Skip("the proposal did not land in the learned slot; the collision cannot be exercised")
	}
	if e, _ := n.Chosen(next); !paxos.ValueEqual(e.Value, learned) {
		t.Fatalf("proposing into a learned slot changed the chosen value to %q", e.Value)
	}
	// The peers promised b2, so they refuse; one refusal is enough.
	outs = n.Step(now, Envelope{From: 2, To: 1, Msg: Nack{Ballot: b1, Promised: b2, Slot: next}})
	for _, e := range outs {
		if l, ok := e.Msg.(Learn); ok && l.Slot == next {
			t.Fatalf("Learn for slot %d sent after a refusal: %+v", next, l)
		}
	}
	if n.Role() != Follower {
		t.Fatalf("role %v after the refusal, want Follower", n.Role())
	}
	if e, _ := n.Chosen(next); !paxos.ValueEqual(e.Value, learned) {
		t.Fatalf("slot %d = %q after stepping down, want %q", next, e.Value, learned)
	}
	inFlight, queued := n.Pending()
	if inFlight != 0 || queued != 0 {
		t.Fatalf("pending after step-down = %d, %d; want 0, 0", inFlight, queued)
	}
}

// --- attack 6: promises must survive retransmission and the lease ---

// TestAttackRetransmittedPrepareReportsLaterAcceptance: an acceptor that
// re-answers a Prepare at exactly its promised ballot (ADR 0009 item 1) must
// report everything it accepted since, and must refuse the same Prepare once
// a higher ballot has been accepted.
func TestAttackRetransmittedPrepareReportsLaterAcceptance(t *testing.T) {
	cfg := DefaultConfig(1, threeNodes())
	cfg.LeaseDuration = 0
	n, err := New(cfg, NewMemStore(), rand.New(rand.NewPCG(1, 0)))
	if err != nil {
		t.Fatal(err)
	}
	b := paxos.Ballot{Round: 3, Node: 2}
	outs := n.Step(0, Envelope{From: 2, To: 1, Msg: Prepare{Ballot: b, FromSlot: 1}})
	if p, ok := outs[0].Msg.(Promise); !ok || len(p.Accepted) != 0 {
		t.Fatalf("first reply %+v, want an empty Promise", outs[0].Msg)
	}
	n.Step(0, Envelope{From: 2, To: 1, Msg: Accept{Ballot: b, Slot: 4, Value: paxos.Value("v4")}})
	n.Step(0, Envelope{From: 2, To: 1, Msg: Accept{Ballot: b, Slot: 2, Value: paxos.Value("v2")}})
	outs = n.Step(0, Envelope{From: 2, To: 1, Msg: Prepare{Ballot: b, FromSlot: 1}})
	p, ok := outs[0].Msg.(Promise)
	if !ok {
		t.Fatalf("retransmitted Prepare answered with %T, want Promise", outs[0].Msg)
	}
	if len(p.Accepted) != 2 || p.Accepted[0].Slot != 2 || p.Accepted[1].Slot != 4 ||
		string(p.Accepted[0].Value) != "v2" || string(p.Accepted[1].Value) != "v4" ||
		p.Accepted[0].Ballot != b || p.Accepted[1].Ballot != b {
		t.Fatalf("re-Promise reports %+v, want slots 2 and 4 at %v in order", p.Accepted, b)
	}
	outs = n.Step(0, Envelope{From: 2, To: 1, Msg: Prepare{Ballot: b, FromSlot: 3}})
	if p := outs[0].Msg.(Promise); len(p.Accepted) != 1 || p.Accepted[0].Slot != 4 {
		t.Fatalf("re-Promise from slot 3 reports %+v, want slot 4 only", p.Accepted)
	}
	higher := paxos.Ballot{Round: 4, Node: 3}
	n.Step(0, Envelope{From: 3, To: 1, Msg: Accept{Ballot: higher, Slot: 4, Value: paxos.Value("w4")}})
	outs = n.Step(0, Envelope{From: 2, To: 1, Msg: Prepare{Ballot: b, FromSlot: 1}})
	nk, ok := outs[0].Msg.(Nack)
	if !ok || nk.Promised != higher {
		t.Fatalf("Prepare %v after accepting %v answered with %+v, want Nack promising %v", b, higher, outs[0].Msg, higher)
	}
	if pv, _ := n.Accepted(4); pv.Ballot != higher || string(pv.Value) != "w4" {
		t.Fatalf("slot 4 accepted %+v, want w4 at %v", pv, higher)
	}
}

// TestAttackLeaseRefusalNeverLowersPromise: a lease refusal changes nothing,
// and an Accept at the refused ballot is still honoured because the ballot
// is above the promise; afterwards the former lease holder's own ballot is
// refused. The lease can only delay elections, never re-order ballots.
func TestAttackLeaseRefusalNeverLowersPromise(t *testing.T) {
	c := newCluster(t, 3, nil)
	l := c.electLeader()
	f := c.nodes[c.others(l.Self())[0]]
	other := c.others(l.Self())[1]
	high := paxos.Ballot{Round: l.Ballot().Round + 5, Node: other}
	before := f.Promised()
	outs := f.Step(c.now, Envelope{From: other, To: f.Self(), Msg: Prepare{Ballot: high, FromSlot: 1}})
	if nk, ok := outs[0].Msg.(Nack); !ok || nk.LeaseRemaining <= 0 {
		t.Fatalf("expected a lease refusal, got %+v", outs[0].Msg)
	}
	if f.Promised() != before {
		t.Fatalf("lease refusal changed the promise from %v to %v", before, f.Promised())
	}
	slot := f.CommitIndex() + 1
	outs = f.Step(c.now, Envelope{From: other, To: f.Self(), Msg: Accept{Ballot: high, Slot: slot, Value: paxos.Value("h")}})
	if _, ok := outs[0].Msg.(Accepted); !ok {
		t.Fatalf("Accept at %v above the promise %v was answered with %+v, want Accepted", high, before, outs[0].Msg)
	}
	if f.Promised() != high {
		t.Fatalf("promise after accepting %v is %v", high, f.Promised())
	}
	outs = f.Step(c.now, Envelope{From: l.Self(), To: f.Self(), Msg: Accept{Ballot: l.Ballot(), Slot: slot, Value: paxos.Value("stale")}})
	if nk, ok := outs[0].Msg.(Nack); !ok || nk.Promised != high {
		t.Fatalf("Accept at the old leader's ballot answered with %+v, want Nack promising %v", outs[0].Msg, high)
	}
	if pv, _ := f.Accepted(slot); string(pv.Value) != "h" {
		t.Fatalf("slot %d accepted %q, the lower ballot overwrote the higher one", slot, pv.Value)
	}
}

// --- attack 7: ballots after a restart ---

// TestAttackRestartNeverReusesBallot: a node restarted from a store whose
// promise is ahead of its round counter must start above the promise; a node
// that led at round r must start above r, so Accepted replies of its old
// term cannot count for its new one.
func TestAttackRestartNeverReusesBallot(t *testing.T) {
	ids := threeNodes()
	st := NewMemStore()
	if err := st.SaveMaxRound(3); err != nil {
		t.Fatal(err)
	}
	if err := st.SavePromised(paxos.Ballot{Round: 7, Node: 2}); err != nil {
		t.Fatal(err)
	}
	n, err := New(DefaultConfig(1, ids), st, rand.New(rand.NewPCG(1, 0)))
	if err != nil {
		t.Fatal(err)
	}
	var now time.Duration
	tickUntilCandidate(t, n, &now)
	if n.Ballot().Round <= 7 {
		t.Fatalf("first ballot %v after restart does not exceed the stored promise r7", n.Ballot())
	}
	d, _ := st.Load()
	if d.MaxRound != n.Ballot().Round {
		t.Fatalf("store max round %d, ballot round %d: the round was not saved before Prepare", d.MaxRound, n.Ballot().Round)
	}

	// A leader crashes with a proposal in flight and restarts.
	st2 := NewMemStore()
	n2, err := New(DefaultConfig(1, ids), st2, rand.New(rand.NewPCG(2, 0)))
	if err != nil {
		t.Fatal(err)
	}
	now = 0
	tickUntilCandidate(t, n2, &now)
	oldBallot := n2.Ballot()
	outs := n2.Step(now, Envelope{From: 2, To: 1, Msg: Promise{Ballot: oldBallot}})
	var slot paxos.Slot
	for _, e := range outs {
		if a, ok := e.Msg.(Accept); ok {
			slot = a.Slot
		}
	}
	n3, err := New(DefaultConfig(1, ids), st2, rand.New(rand.NewPCG(3, 0)))
	if err != nil {
		t.Fatal(err)
	}
	if n3.Role() != Follower {
		t.Fatalf("restarted node role %v, want Follower", n3.Role())
	}
	now = 0
	tickUntilCandidate(t, n3, &now)
	if !oldBallot.Less(n3.Ballot()) {
		t.Fatalf("ballot after restart %v is not above the pre-crash ballot %v", n3.Ballot(), oldBallot)
	}
	n3.Step(now, Envelope{From: 2, To: 1, Msg: Promise{Ballot: n3.Ballot()}})
	if n3.Role() != Leader {
		t.Fatalf("role %v, want Leader", n3.Role())
	}
	for _, from := range []paxos.NodeID{2, 3} {
		n3.Step(now, Envelope{From: from, To: 1, Msg: Accepted{Ballot: oldBallot, Slot: slot}})
	}
	if e, ok := n3.Chosen(slot); ok && e.Ballot == oldBallot {
		t.Fatalf("slot %d chosen at the pre-crash ballot %v from stale Accepted replies", slot, oldBallot)
	}
}

// --- attack 8: a restart that forgets ---

// TestAttackRestartFromEmptyStoreBreaksAgreement runs one execution twice.
// A value is chosen: the leader and one follower accept it and the leader
// learns it, while the third node sees neither the Accept nor the Learn.
// The follower then restarts and the leader crashes, so the next leader's
// Phase 1 quorum is the follower and the third node. With the follower's
// store kept across the restart, its promise and acceptance are reported
// and the slot keeps its value everywhere. With the store replaced by an
// empty one, as a restarted per-process `arena` does today, the follower
// reports nothing, the slot is filled with a no-op, and the old leader
// returns holding a different chosen value: two values for one slot. The
// second half is not a bug in the log; it is the precondition the log's
// safety argument places on the host, made executable.
func TestAttackRestartFromEmptyStoreBreaksAgreement(t *testing.T) {
	run := func(t *testing.T, forget bool) (kept, filled string, conflicts int) {
		c := newCluster(t, 3, nil)
		l := c.electLeader()
		others := c.others(l.Self())
		a, x := others[0], others[1]
		c.propose(l, paxos.Value("before"))
		s := l.CommitIndex() + 1
		v := paxos.Value("chosen")
		c.drop = func(e Envelope) bool {
			switch m := e.Msg.(type) {
			case Accept:
				return m.Slot == s && e.To == x
			case Learn:
				return m.Slot == s
			}
			return false
		}
		outs, err := l.Propose(c.now, v)
		if err != nil {
			t.Fatal(err)
		}
		c.enqueue(l.Self(), outs)
		c.deliverAll()
		if e, ok := l.Chosen(s); !ok || !paxos.ValueEqual(e.Value, v) {
			t.Fatalf("setup: leader has slot %d = %+v ok=%t, want %q", s, e, ok, v)
		}
		if pv, ok := c.nodes[a].Accepted(s); !ok || !paxos.ValueEqual(pv.Value, v) {
			t.Fatalf("setup: follower %d accepted %+v ok=%t", a, pv, ok)
		}
		if _, ok := c.nodes[x].Accepted(s); ok {
			t.Fatalf("setup: node %d accepted slot %d although the Accept was dropped", x, s)
		}
		c.drop = nil
		c.crash(a)
		if forget {
			c.stores[a] = NewMemStore()
		}
		c.restart(a)
		c.crash(l.Self())
		l2 := c.electLeader()
		if l2.Self() == l.Self() {
			t.Fatal("crashed leader elected")
		}
		if !c.runUntil(5*time.Second, func() bool {
			_, ok1 := c.nodes[a].Chosen(s)
			_, ok2 := c.nodes[x].Chosen(s)
			return ok1 && ok2
		}) {
			t.Fatalf("slot %d never chosen on the survivors", s)
		}
		old := c.restart(l.Self())
		c.runUntil(2*time.Second, func() bool { return old.CommitIndex() >= l2.CommitIndex() })
		e1, _ := old.Chosen(s)
		e2, _ := l2.Chosen(s)
		return string(e1.Value), string(e2.Value), len(c.conflicts())
	}
	t.Run("store kept", func(t *testing.T) {
		kept, filled, conflicts := run(t, false)
		if kept != "chosen" || filled != "chosen" || conflicts != 0 {
			t.Fatalf("with a durable store: old leader holds %q, new leader holds %q, %d conflicts; want chosen, chosen, 0", kept, filled, conflicts)
		}
	})
	t.Run("store replaced", func(t *testing.T) {
		kept, filled, _ := run(t, true)
		if kept != "chosen" {
			t.Fatalf("the restarted old leader lost its own chosen entry: %q", kept)
		}
		if filled == "chosen" {
			t.Fatalf("slot filled with %q although the only surviving acceptor forgot it; the log cannot be this robust, check the test", filled)
		}
		t.Logf("store replaced: slot holds %q on the old leader and %q on the new one; a restart must reload the store", kept, filled)
	})
}

// --- attack 9: a Learn that disagrees ---

// TestAttackConflictingLearnKeepsFirstValue: a Learn carrying a second value
// for a chosen slot is reported and ignored; the entry, the commit index and
// the state read by the host do not change.
func TestAttackConflictingLearnKeepsFirstValue(t *testing.T) {
	n, err := New(DefaultConfig(1, threeNodes()), NewMemStore(), rand.New(rand.NewPCG(1, 0)))
	if err != nil {
		t.Fatal(err)
	}
	b := paxos.Ballot{Round: 1, Node: 2}
	n.Step(0, Envelope{From: 2, To: 1, Msg: Learn{Slot: 1, Ballot: b, Value: paxos.Value("one")}})
	n.Events()
	outs := n.Step(0, Envelope{From: 3, To: 1, Msg: Learn{Slot: 1, Ballot: paxos.Ballot{Round: 2, Node: 3}, Value: paxos.Value("two")}})
	if len(outs) != 0 {
		t.Fatalf("a conflicting Learn produced messages: %+v", outs)
	}
	evs := n.Events()
	if len(evs) != 1 {
		t.Fatalf("events = %+v, want one LearnConflict", evs)
	}
	lc, ok := evs[0].(LearnConflict)
	if !ok || lc.Slot != 1 || string(lc.Have.Value) != "one" || string(lc.Got.Value) != "two" {
		t.Fatalf("event = %+v, want LearnConflict{1, one, two}", evs[0])
	}
	if e, _ := n.Chosen(1); string(e.Value) != "one" || e.Ballot != b {
		t.Fatalf("slot 1 = %+v after the conflict, want one at %v", e, b)
	}
	// The same value under another ballot is a duplicate, not a conflict.
	n.Step(0, Envelope{From: 3, To: 1, Msg: Learn{Slot: 1, Ballot: paxos.Ballot{Round: 2, Node: 3}, Value: paxos.Value("one")}})
	if evs := n.Events(); len(evs) != 0 {
		t.Fatalf("a duplicate Learn with another ballot emitted %+v", evs)
	}
	if n.CommitIndex() != 1 {
		t.Fatalf("commit index %d, want 1", n.CommitIndex())
	}
}

// --- attack 10: adversarial scheduler with an independent checker ---

type arenaBallotSlot struct {
	b paxos.Ballot
	s paxos.Slot
}

type arenaRead struct {
	node paxos.NodeID
	gen  int
	seq  uint64
}

// arena drives one cluster under a seeded adversary: every message in flight
// sits in an unordered pool, so delivery order is arbitrary; any message may
// be duplicated or lost; any link may be blocked in one direction; any node
// may crash, losing the messages addressed to it, and later restart from its
// store. Nodes share one clock. An independent checker evaluates, after
// every call into a node, the acceptor rules (promise monotone across
// restarts, one value per ballot and slot, ballot ownership), agreement (one
// chosen value per slot across every node's durable store, a majority-held
// value never displaced, no LearnConflict) and the read barrier (index at
// least the highest commit index reached anywhere before the call).
type arena struct {
	t       *testing.T
	seed    uint64
	rng     *rand.Rand
	ids     []paxos.NodeID
	cfgs    map[paxos.NodeID]Config
	stores  map[paxos.NodeID]*MemStore
	nodes   map[paxos.NodeID]*Node
	gen     map[paxos.NodeID]int
	pool    []Envelope
	blocked map[[2]paxos.NodeID]bool
	now     time.Duration
	quorum  int
	nextVal int

	chosen    map[paxos.Slot]string
	majority  map[paxos.Slot]string
	promised  map[paxos.NodeID]paxos.Ballot
	commit    map[paxos.NodeID]paxos.Slot
	owner     map[paxos.Ballot]paxos.NodeID
	slotValue map[arenaBallotSlot]string
	reads     map[arenaRead]paxos.Slot
	maxSlot   paxos.Slot // highest slot named in any Accept, chosen or not
	maxChosen paxos.Slot // highest slot observed chosen anywhere

	steps, delivered, dups, drops, crashes, elections, readsReady, proposals int

	// tornP is the probability that one Save call fails as a crash in the
	// middle of a write would: the store keeps its previous record, the node
	// halts, and the arena crashes it. Zero draws nothing from rng.
	tornP     float64
	tornFired map[paxos.NodeID]bool
	tornCount int
}

// arenaStore wraps a node's MemStore and fails a Save with probability
// tornP, leaving the MemStore untouched.
type arenaStore struct {
	a  *arena
	id paxos.NodeID
}

func (s arenaStore) tear() bool {
	if s.a.tornP == 0 || s.a.rng.Float64() >= s.a.tornP {
		return false
	}
	s.a.tornFired[s.id] = true
	return true
}

var errArenaTorn = fmt.Errorf("arena: torn write")

func (s arenaStore) Load() (Durable, error) { return s.a.stores[s.id].Load() }
func (s arenaStore) SavePromised(b paxos.Ballot) error {
	if s.tear() {
		return errArenaTorn
	}
	return s.a.stores[s.id].SavePromised(b)
}
func (s arenaStore) SaveMaxRound(r uint64) error {
	if s.tear() {
		return errArenaTorn
	}
	return s.a.stores[s.id].SaveMaxRound(r)
}
func (s arenaStore) SaveAccepted(pv paxos.PValue) error {
	if s.tear() {
		return errArenaTorn
	}
	return s.a.stores[s.id].SaveAccepted(pv)
}
func (s arenaStore) SaveChosen(e Entry) error {
	if s.tear() {
		return errArenaTorn
	}
	return s.a.stores[s.id].SaveChosen(e)
}

func newArena(t *testing.T, seed uint64, n int, lease time.Duration) *arena {
	a := &arena{
		t:         t,
		seed:      seed,
		rng:       rand.New(rand.NewPCG(seed, 0)),
		cfgs:      make(map[paxos.NodeID]Config),
		stores:    make(map[paxos.NodeID]*MemStore),
		nodes:     make(map[paxos.NodeID]*Node),
		gen:       make(map[paxos.NodeID]int),
		blocked:   make(map[[2]paxos.NodeID]bool),
		quorum:    paxos.Quorum(n),
		chosen:    make(map[paxos.Slot]string),
		majority:  make(map[paxos.Slot]string),
		promised:  make(map[paxos.NodeID]paxos.Ballot),
		commit:    make(map[paxos.NodeID]paxos.Slot),
		owner:     make(map[paxos.Ballot]paxos.NodeID),
		slotValue: make(map[arenaBallotSlot]string),
		reads:     make(map[arenaRead]paxos.Slot),
		tornFired: make(map[paxos.NodeID]bool),
	}
	for i := 1; i <= n; i++ {
		a.ids = append(a.ids, paxos.NodeID(i))
	}
	for _, id := range a.ids {
		cfg := DefaultConfig(id, a.ids)
		cfg.LeaseDuration = lease
		a.cfgs[id] = cfg
		a.stores[id] = NewMemStore()
		a.start(id)
	}
	return a
}

func (a *arena) failf(format string, args ...any) {
	a.t.Helper()
	a.t.Fatalf("seed %d step %d t=%v: %s", a.seed, a.steps, a.now, fmt.Sprintf(format, args...))
}

func (a *arena) start(id paxos.NodeID) {
	n, err := New(a.cfgs[id], arenaStore{a: a, id: id}, a.rng)
	if err != nil {
		a.failf("New(%d): %v", id, err)
	}
	a.nodes[id] = n
	a.gen[id]++
	if n.Promised().Less(a.promised[id]) {
		a.failf("node %d restarted with promise %v below %v", id, n.Promised(), a.promised[id])
	}
	if n.CommitIndex() < a.commit[id] {
		a.failf("node %d restarted with commit index %d below %d", id, n.CommitIndex(), a.commit[id])
	}
	a.promised[id] = n.Promised()
	a.commit[id] = n.CommitIndex()
}

func (a *arena) crash(id paxos.NodeID) {
	if _, ok := a.nodes[id]; !ok {
		return
	}
	delete(a.nodes, id)
	kept := a.pool[:0]
	for _, e := range a.pool {
		if e.To != id {
			kept = append(kept, e)
		}
	}
	a.pool = kept
	a.crashes++
	for k := range a.reads {
		if k.node == id {
			delete(a.reads, k)
		}
	}
}

func (a *arena) maxCommit() paxos.Slot {
	var m paxos.Slot
	for _, id := range a.ids {
		if n, ok := a.nodes[id]; ok && n.CommitIndex() > m {
			m = n.CommitIndex()
		}
		if a.commit[id] > m {
			m = a.commit[id]
		}
	}
	return m
}

// call runs fn on node id with the checker's hooks around it.
func (a *arena) call(id paxos.NodeID, delivered *Envelope, fn func(n *Node) []Envelope) {
	n, ok := a.nodes[id]
	if !ok {
		return
	}
	pre := n.Promised()
	var preAccepted paxos.PValue
	var preHad bool
	var slot paxos.Slot
	if delivered != nil {
		if acc, ok := delivered.Msg.(Accept); ok {
			slot = acc.Slot
			preAccepted, preHad = n.Accepted(slot)
		}
	}
	outs := fn(n)
	if err := n.Failed(); err != nil {
		if !a.tornFired[id] {
			a.failf("node %d failed: %v", id, err)
		}
		// A torn write: nothing the call produced may leave the node.
		if len(outs) != 0 {
			a.failf("node %d failed on a torn write but returned %d envelopes", id, len(outs))
		}
		for _, ev := range n.Events() {
			a.failf("node %d failed on a torn write but emitted %T", id, ev)
		}
		delete(a.tornFired, id)
		a.tornCount++
		a.crash(id)
		for s, e := range a.stores[id].chosen {
			a.recordChosen(s, e.Value, fmt.Sprintf("store of node %d after a torn write", id))
		}
		return
	}
	delete(a.tornFired, id)
	if n.Promised().Less(pre) {
		a.failf("node %d promise decreased from %v to %v", id, pre, n.Promised())
	}
	if n.CommitIndex() < a.commit[id] {
		a.failf("node %d commit index decreased from %d to %d", id, a.commit[id], n.CommitIndex())
	}
	a.promised[id] = n.Promised()
	a.commit[id] = n.CommitIndex()
	if n.Role() != Follower {
		a.checkOwner(n.Ballot(), id)
	}
	if slot != 0 {
		if pv, ok := n.Accepted(slot); ok {
			changed := !preHad || pv.Ballot != preAccepted.Ballot || !paxos.ValueEqual(pv.Value, preAccepted.Value)
			if changed {
				if pv.Ballot.Less(pre) {
					a.failf("node %d accepted %v for slot %d while promised %v", id, pv.Ballot, slot, pre)
				}
				a.checkSlotValue(pv.Ballot, pv.Slot, pv.Value)
				a.checkMajority(slot)
			}
		}
	}
	for _, e := range outs {
		if e.From != id {
			a.failf("node %d sent an envelope from %d", id, e.From)
		}
		if e.To == id {
			a.failf("node %d sent an envelope to itself", id)
		}
		switch m := e.Msg.(type) {
		case Prepare:
			a.checkOwner(m.Ballot, id)
		case Heartbeat:
			a.checkOwner(m.Ballot, id)
		case Accept:
			a.checkOwner(m.Ballot, id)
			a.checkSlotValue(m.Ballot, m.Slot, m.Value)
			if m.Slot > a.maxSlot {
				a.maxSlot = m.Slot
			}
		case Learn:
			a.recordChosen(m.Slot, m.Value, fmt.Sprintf("Learn sent by node %d at %v", id, m.Ballot))
		}
		if a.blocked[[2]paxos.NodeID{e.From, e.To}] {
			continue
		}
		a.pool = append(a.pool, e)
	}
	for _, ev := range n.Events() {
		switch x := ev.(type) {
		case LearnConflict:
			a.failf("node %d LearnConflict at slot %d: have %q got %q", id, x.Slot, x.Have.Value, x.Got.Value)
		case LeaderChanged:
			if x.Self {
				a.elections++
			}
		case ReadReady:
			key := arenaRead{node: id, gen: a.gen[id], seq: x.Seq}
			if before, ok := a.reads[key]; ok {
				delete(a.reads, key)
				a.readsReady++
				if x.Index < before {
					a.failf("node %d read %d ready at index %d, but commit index %d was reached before the call", id, x.Seq, x.Index, before)
				}
			}
		case ReadFailed:
			delete(a.reads, arenaRead{node: id, gen: a.gen[id], seq: x.Seq})
		}
	}
	// Agreement over the node's durable chosen entries.
	for s, e := range a.stores[id].chosen {
		a.recordChosen(s, e.Value, fmt.Sprintf("store of node %d", id))
	}
}

func (a *arena) checkOwner(b paxos.Ballot, from paxos.NodeID) {
	if b.Node != from {
		a.failf("node %d uses ballot %v owned by another node", from, b)
	}
	if o, ok := a.owner[b]; ok && o != from {
		a.failf("ballot %v used by node %d and node %d", b, o, from)
	}
	a.owner[b] = from
}

func (a *arena) checkSlotValue(b paxos.Ballot, s paxos.Slot, v paxos.Value) {
	k := arenaBallotSlot{b, s}
	if prev, ok := a.slotValue[k]; ok {
		if prev != string(v) {
			a.failf("two values for ballot %v slot %d: %q and %q", b, s, prev, v)
		}
		return
	}
	a.slotValue[k] = string(v)
}

func (a *arena) recordChosen(s paxos.Slot, v paxos.Value, how string) {
	if prev, ok := a.chosen[s]; ok {
		if prev != string(v) {
			a.failf("slot %d chosen as %q (%s) but recorded as %q", s, v, how, prev)
		}
		return
	}
	a.chosen[s] = string(v)
	if s > a.maxChosen {
		a.maxChosen = s
	}
	if s > a.maxSlot {
		a.maxSlot = s
	}
	if mv, ok := a.majority[s]; ok && mv != string(v) {
		a.failf("slot %d learned as %q (%s) but a quorum had accepted %q", s, v, how, mv)
	}
}

// checkMajority counts, over every store (durable acceptances survive
// crashes), the nodes holding the same (ballot, value) for s.
func (a *arena) checkMajority(s paxos.Slot) {
	counts := make(map[string]int)
	vals := make(map[string]string)
	for _, id := range a.ids {
		pv, ok := a.stores[id].accepted[s]
		if !ok {
			continue
		}
		k := pv.Ballot.String() + "|" + string(pv.Value)
		counts[k]++
		vals[k] = string(pv.Value)
	}
	for k, cnt := range counts {
		if cnt < a.quorum {
			continue
		}
		v := vals[k]
		if prev, ok := a.majority[s]; ok && prev != v {
			a.failf("slot %d: a quorum accepted %q after a quorum had accepted %q", s, v, prev)
		}
		a.majority[s] = v
		if cv, ok := a.chosen[s]; ok && cv != v {
			a.failf("slot %d: a quorum accepted %q but %q was learned as chosen", s, v, cv)
		}
	}
}

func (a *arena) randomLive() (paxos.NodeID, bool) {
	var live []paxos.NodeID
	for _, id := range a.ids {
		if _, ok := a.nodes[id]; ok {
			live = append(live, id)
		}
	}
	if len(live) == 0 {
		return 0, false
	}
	return live[a.rng.IntN(len(live))], true
}

func (a *arena) step() {
	a.steps++
	switch r := a.rng.IntN(100); {
	case r < 48:
		if len(a.pool) == 0 {
			a.tick()
			return
		}
		i := a.rng.IntN(len(a.pool))
		env := a.pool[i]
		a.pool[i] = a.pool[len(a.pool)-1]
		a.pool = a.pool[:len(a.pool)-1]
		a.delivered++
		a.call(env.To, &env, func(n *Node) []Envelope { return n.Step(a.now, env) })
	case r < 72:
		a.tick()
	case r < 84:
		id, ok := a.randomLive()
		if !ok {
			return
		}
		if a.nodes[id].Role() != Leader {
			return
		}
		a.nextVal++
		v := paxos.Value(fmt.Sprintf("v%d-n%d", a.nextVal, id))
		a.proposals++
		a.call(id, nil, func(n *Node) []Envelope {
			outs, err := n.Propose(a.now, v)
			if err != nil {
				a.failf("Propose on leader %d: %v", id, err)
			}
			return outs
		})
	case r < 88:
		if len(a.pool) > 0 {
			a.pool = append(a.pool, a.pool[a.rng.IntN(len(a.pool))])
			a.dups++
		}
	case r < 91:
		if len(a.pool) > 0 {
			i := a.rng.IntN(len(a.pool))
			a.pool[i] = a.pool[len(a.pool)-1]
			a.pool = a.pool[:len(a.pool)-1]
			a.drops++
		}
	case r < 94:
		from := a.ids[a.rng.IntN(len(a.ids))]
		to := a.ids[a.rng.IntN(len(a.ids))]
		if from != to {
			k := [2]paxos.NodeID{from, to}
			if a.blocked[k] {
				delete(a.blocked, k)
			} else {
				a.blocked[k] = true
			}
		}
	case r < 95:
		if id, ok := a.randomLive(); ok {
			a.crash(id)
		}
	case r < 97:
		var down []paxos.NodeID
		for _, id := range a.ids {
			if _, ok := a.nodes[id]; !ok {
				down = append(down, id)
			}
		}
		if len(down) > 0 {
			a.start(down[a.rng.IntN(len(down))])
		}
	default:
		id, ok := a.randomLive()
		if !ok || a.nodes[id].Role() != Leader {
			return
		}
		before := a.maxCommit()
		a.call(id, nil, func(n *Node) []Envelope {
			seq, outs, err := n.ReadIndex(a.now)
			if err == nil {
				a.reads[arenaRead{node: id, gen: a.gen[id], seq: seq}] = before
			}
			return outs
		})
	}
}

func (a *arena) tick() {
	a.now += time.Duration(1+a.rng.IntN(10)) * time.Millisecond
	id, ok := a.randomLive()
	if !ok {
		return
	}
	a.call(id, nil, func(n *Node) []Envelope { return n.Tick(a.now) })
}

// settle removes every fault, restarts every node and runs until every node
// holds the same committed prefix that covers every slot chosen anywhere and
// one node is a ready leader. It reports whether that happened.
func (a *arena) settle(limit time.Duration) bool {
	a.tornP = 0
	a.blocked = make(map[[2]paxos.NodeID]bool)
	for _, id := range a.ids {
		if _, ok := a.nodes[id]; !ok {
			a.start(id)
		}
	}
	deadline := a.now + limit
	for a.now < deadline {
		for len(a.pool) > 0 {
			i := a.rng.IntN(len(a.pool))
			env := a.pool[i]
			a.pool[i] = a.pool[len(a.pool)-1]
			a.pool = a.pool[:len(a.pool)-1]
			a.call(env.To, &env, func(n *Node) []Envelope { return n.Step(a.now, env) })
		}
		a.now += tickStep
		for _, id := range a.ids {
			a.call(id, nil, func(n *Node) []Envelope { return n.Tick(a.now) })
		}
		if a.converged() {
			return true
		}
	}
	return a.converged()
}

func (a *arena) converged() bool {
	var leader *Node
	for _, id := range a.ids {
		n := a.nodes[id]
		if n.Role() == Leader && n.Ready() {
			if leader != nil {
				return false
			}
			leader = n
		}
	}
	if leader == nil {
		return false
	}
	// Every slot chosen anywhere must be in the committed prefix. Slots that
	// only a deposed leader's own acceptor ever held are not chosen and no
	// later leader is obliged to fill them.
	ci := leader.CommitIndex()
	if ci < a.maxChosen {
		return false
	}
	for _, id := range a.ids {
		n := a.nodes[id]
		if n.CommitIndex() != ci || n.Promised() != leader.Ballot() {
			return false
		}
	}
	return true
}

func (a *arena) describe() string {
	var s string
	for _, id := range a.ids {
		n, ok := a.nodes[id]
		if !ok {
			s += fmt.Sprintf(" node %d down;", id)
			continue
		}
		s += fmt.Sprintf(" node %d %s commit %d promised %v;", id, n.Role(), n.CommitIndex(), n.Promised())
	}
	return s
}

// TestAttackRandomInterleavingsMultiSlot runs the arena over many seeds,
// cluster sizes (including the even sizes the scenarios never use) and both
// lease settings. Any violation reported by the arena's checker fails the
// test; a run that does not converge after every fault is removed also
// fails, since with every node up and every link open the log must reach
// one committed prefix.
func TestAttackRandomInterleavingsMultiSlot(t *testing.T) {
	seeds := 40
	steps := 16000
	if testing.Short() {
		seeds, steps = 6, 4000
	}
	sizes := []int{3, 4, 5}
	for _, size := range sizes {
		for _, lease := range []time.Duration{DefaultLeaseDuration, 0} {
			for seed := 1; seed <= seeds; seed++ {
				size, lease, seed := size, lease, uint64(seed)
				t.Run(fmt.Sprintf("n=%d/lease=%v/seed=%d", size, lease, seed), func(t *testing.T) {
					t.Parallel()
					a := newArena(t, seed, size, lease)
					for i := 0; i < steps; i++ {
						a.step()
					}
					if !a.settle(10 * time.Second) {
						t.Fatalf("seed %d: no convergence after healing (max chosen %d, max proposed %d):%s", seed, a.maxChosen, a.maxSlot, a.describe())
					}
					if a.elections == 0 || a.proposals == 0 {
						t.Fatalf("seed %d: the schedule was too quiet: %d elections, %d proposals", seed, a.elections, a.proposals)
					}
					t.Logf("seed %d: %d delivered, %d dups, %d drops, %d crashes, %d elections, %d proposals, %d slots chosen, %d reads ready",
						seed, a.delivered, a.dups, a.drops, a.crashes, a.elections, a.proposals, len(a.chosen), a.readsReady)
				})
			}
		}
	}
}

// --- attack 11: torn writes under the adversarial scheduler ---

// TestAttackRandomInterleavingsWithTornWrites runs the arena with a store
// that fails a Save call now and then, as a crash in the middle of a write
// does: the store keeps its previous record, the node halts without sending
// what the call produced, and the arena crashes it and later restarts it
// from the store. The same checker applies; in addition a failed call must
// return no envelope and emit no event, and a restart must not lower the
// promise or the commit index the node had reached durably.
func TestAttackRandomInterleavingsWithTornWrites(t *testing.T) {
	seeds := 16
	steps := 12000
	if testing.Short() {
		seeds, steps = 4, 4000
	}
	for _, size := range []int{3, 5} {
		for _, lease := range []time.Duration{DefaultLeaseDuration, 0} {
			for seed := 1; seed <= seeds; seed++ {
				size, lease, seed := size, lease, uint64(seed)
				t.Run(fmt.Sprintf("n=%d/lease=%v/seed=%d", size, lease, seed), func(t *testing.T) {
					t.Parallel()
					a := newArena(t, 1000+seed, size, lease)
					a.tornP = 0.01
					for i := 0; i < steps; i++ {
						a.step()
					}
					if a.tornCount == 0 {
						t.Fatalf("seed %d: no torn write fired", seed)
					}
					if !a.settle(10 * time.Second) {
						t.Fatalf("seed %d: no convergence after healing (max chosen %d):%s", seed, a.maxChosen, a.describe())
					}
					t.Logf("seed %d: %d torn writes, %d crashes, %d elections, %d slots chosen", seed, a.tornCount, a.crashes, a.elections, len(a.chosen))
				})
			}
		}
	}
}

// --- attack 12: a read barrier fed with acks older than the read ---

// TestAttackReadBarrierIgnoresAcksFromBeforeTheRead: a leader completes one
// read, is then cut off without noticing (it is not ticked), and the other
// two nodes elect a new leader and choose a value. The old leader starts a
// second read at its stale commit index. Every HeartbeatAck it receives is a
// duplicate of the genuine, at-its-own-ballot acks of the first read, which
// arrived before the new ballot existed. None may complete the second read:
// the barrier must only count acks for heartbeats sent after the read began.
func TestAttackReadBarrierIgnoresAcksFromBeforeTheRead(t *testing.T) {
	c := newCluster(t, 3, func(cfg *Config) { cfg.LeaseDuration = 0 })
	l := c.electLeader()
	c.propose(l, paxos.Value("before"))
	var stale []Envelope
	c.drop = func(e Envelope) bool {
		if ack, ok := e.Msg.(HeartbeatAck); ok && e.To == l.Self() && ack.Promised == l.Ballot() {
			stale = append(stale, e)
		}
		return false
	}
	seq1, outs, err := l.ReadIndex(c.now)
	if err != nil {
		t.Fatal(err)
	}
	c.enqueue(l.Self(), outs)
	c.deliverAll()
	ready1 := false
	for _, ev := range c.events[l.Self()] {
		if r, ok := ev.(ReadReady); ok && r.Seq == seq1 {
			ready1 = true
		}
	}
	if !ready1 || len(stale) == 0 {
		t.Fatalf("setup: first read ready=%t with %d acks captured", ready1, len(stale))
	}
	staleIndex := l.CommitIndex()

	// Cut the leader off and elect another one without ticking it.
	c.drop = func(e Envelope) bool { return e.From == l.Self() || e.To == l.Self() }
	var l2 *Node
	for i := 0; i < 1000 && l2 == nil; i++ {
		c.now += tickStep
		for _, id := range c.others(l.Self()) {
			c.enqueue(id, c.nodes[id].Tick(c.now))
		}
		c.deliverAll()
		for _, id := range c.others(l.Self()) {
			if n := c.nodes[id]; n.Role() == Leader && n.Ready() {
				l2 = n
			}
		}
	}
	if l2 == nil {
		t.Fatal("setup: the majority never elected a new leader")
	}
	c.propose(l2, paxos.Value("after"))
	if l2.CommitIndex() <= staleIndex {
		t.Fatalf("setup: new leader commit %d not above the stale index %d", l2.CommitIndex(), staleIndex)
	}
	if l.Role() != Leader {
		t.Fatalf("setup: the old leader stepped down (%v); the attack needs a stale leader", l.Role())
	}

	seq2, _, err := l.ReadIndex(c.now) // its heartbeats are lost
	if err != nil {
		t.Fatalf("stale leader refused ReadIndex: %v", err)
	}
	for round := 0; round < 3; round++ {
		for _, e := range stale {
			l.Step(c.now, e)
		}
	}
	for _, ev := range l.Events() {
		if r, ok := ev.(ReadReady); ok && r.Seq == seq2 {
			t.Fatalf("read %d completed at index %d from acks sent before it began; slot %d was already chosen by leader %d",
				seq2, r.Index, l2.CommitIndex(), l2.Self())
		}
	}
}

// --- attack 13: configuration corners ---

// TestAttackRandomInterleavingsEdgeConfigs reruns the arena (with torn
// writes) on the configuration corners: a window of one slot, catch-up one
// entry at a time, election timeouts equal to the heartbeat interval so
// followers suspect a live leader constantly, and cluster sizes from one to
// seven.
func TestAttackRandomInterleavingsEdgeConfigs(t *testing.T) {
	seeds := 6
	steps := 12000
	if testing.Short() {
		seeds, steps = 3, 3000
	}
	for _, size := range []int{1, 2, 6, 7} {
		for variant := 0; variant < 2; variant++ {
			for seed := 1; seed <= seeds; seed++ {
				size, variant, seed := size, variant, uint64(seed)
				t.Run(fmt.Sprintf("n=%d/variant=%d/seed=%d", size, variant, seed), func(t *testing.T) {
					t.Parallel()
					a := newArena(t, 5000+seed, size, 0)
					for _, id := range a.ids {
						cfg := a.cfgs[id]
						cfg.Window = 1
						cfg.LearnBatch = 1
						if variant == 1 {
							cfg.HeartbeatInterval = 20 * time.Millisecond
							cfg.ElectionTimeoutMin = 20 * time.Millisecond
							cfg.ElectionTimeoutMax = 25 * time.Millisecond
							cfg.LeaseDuration = 20 * time.Millisecond
						}
						a.cfgs[id] = cfg
						a.crash(id)
						a.start(id)
					}
					a.crashes = 0
					a.tornP = 0.005
					for i := 0; i < steps; i++ {
						a.step()
					}
					t.Logf("seed %d: %d torn writes, %d crashes, %d elections, %d proposals, %d slots chosen, %d reads ready",
						seed, a.tornCount, a.crashes, a.elections, a.proposals, len(a.chosen), a.readsReady)
					if variant == 1 {
						// Leave the aggressive timers only for the fault phase:
						// with them, healing need not converge.
						return
					}
					if !a.settle(20 * time.Second) {
						t.Fatalf("seed %d: no convergence after healing (max chosen %d):%s", seed, a.maxChosen, a.describe())
					}
				})
			}
		}
	}
}
