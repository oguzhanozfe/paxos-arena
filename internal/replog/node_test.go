package replog

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/oguzhanozfe/paxos-arena/internal/paxos"
)

func threeNodes() []paxos.NodeID { return []paxos.NodeID{1, 2, 3} }

// tickUntilCandidate ticks a lone node until it starts an election.
func tickUntilCandidate(t *testing.T, n *Node, now *time.Duration) []Envelope {
	t.Helper()
	for i := 0; i < 1000; i++ {
		*now += tickStep
		outs := n.Tick(*now)
		if n.Role() == Candidate {
			return outs
		}
	}
	t.Fatalf("node %d never became a candidate", n.Self())
	return nil
}

func TestBallotIsUniquePerNode(t *testing.T) {
	ids := []paxos.NodeID{1, 2, 3, 4, 5}
	rng := rand.New(rand.NewPCG(7, 0))
	seen := make(map[paxos.Ballot]paxos.NodeID)
	for _, id := range ids {
		n, err := New(DefaultConfig(id, ids), NewMemStore(), rng)
		if err != nil {
			t.Fatal(err)
		}
		var now time.Duration
		// Two candidacies per node: the Phase 1 deadline passes with no
		// promises, the node falls back to follower and tries again.
		for attempt := 0; attempt < 2; attempt++ {
			tickUntilCandidate(t, n, &now)
			b := n.Ballot()
			if b.Node != id {
				t.Errorf("node %d uses ballot %v with a foreign node component", id, b)
			}
			if prev, dup := seen[b]; dup {
				t.Errorf("ballot %v used by node %d and node %d", b, prev, id)
			}
			seen[b] = id
			// Let the candidacy time out.
			for n.Role() == Candidate {
				now += tickStep
				n.Tick(now)
			}
		}
	}
	if len(seen) != 2*len(ids) {
		t.Fatalf("expected %d distinct ballots, saw %d", 2*len(ids), len(seen))
	}
}

func TestAcceptorMonotonic(t *testing.T) {
	cfg := DefaultConfig(1, threeNodes())
	cfg.LeaseDuration = 0 // isolate the promise rules from the lease
	n, err := New(cfg, NewMemStore(), rand.New(rand.NewPCG(1, 0)))
	if err != nil {
		t.Fatal(err)
	}
	b := func(r uint64, id paxos.NodeID) paxos.Ballot { return paxos.Ballot{Round: r, Node: id} }
	steps := []struct {
		name     string
		from     paxos.NodeID
		msg      Message
		wantType string
	}{
		{"prepare r1.n2", 2, Prepare{Ballot: b(1, 2), FromSlot: 1}, "promise"},
		{"prepare r1.n3 same round lower node", 3, Prepare{Ballot: b(1, 3), FromSlot: 1}, "promise"},
		{"prepare r1.n2 again is refused", 2, Prepare{Ballot: b(1, 2), FromSlot: 1}, "nack"},
		{"accept r1.n2 below promise", 2, Accept{Ballot: b(1, 2), Slot: 1, Value: paxos.Value("a")}, "nack"},
		{"accept r1.n3 at promise", 3, Accept{Ballot: b(1, 3), Slot: 1, Value: paxos.Value("b")}, "accepted"},
		{"accept r3.n2 above promise raises it", 2, Accept{Ballot: b(3, 2), Slot: 2, Value: paxos.Value("c")}, "accepted"},
		{"prepare r2.n3 below raised promise", 3, Prepare{Ballot: b(2, 3), FromSlot: 1}, "nack"},
		{"heartbeat r5.n3 raises promise", 3, Heartbeat{Ballot: b(5, 3)}, "heartbeat_ack"},
		{"accept r4.n2 below promise", 2, Accept{Ballot: b(4, 2), Slot: 3, Value: paxos.Value("d")}, "nack"},
		{"heartbeat r1.n2 stale", 2, Heartbeat{Ballot: b(1, 2)}, "heartbeat_ack"},
		{"prepare r5.n3 retransmitted is answered", 3, Prepare{Ballot: b(5, 3), FromSlot: 1}, "promise"},
	}
	var now time.Duration
	for _, s := range steps {
		before := n.Promised()
		acceptedBefore := make(map[paxos.Slot]paxos.PValue)
		for slot := paxos.Slot(1); slot <= 3; slot++ {
			if pv, ok := n.Accepted(slot); ok {
				acceptedBefore[slot] = pv
			}
		}
		now += time.Millisecond
		outs := n.Step(now, Envelope{From: s.from, To: 1, Msg: s.msg})
		if len(outs) != 1 {
			t.Fatalf("%s: got %d replies, want 1", s.name, len(outs))
		}
		if got := TypeName(outs[0].Msg); got != s.wantType {
			t.Errorf("%s: reply %s, want %s", s.name, got, s.wantType)
		}
		if n.Promised().Less(before) {
			t.Errorf("%s: promise decreased %v -> %v", s.name, before, n.Promised())
		}
		for slot := paxos.Slot(1); slot <= 3; slot++ {
			pv, ok := n.Accepted(slot)
			if !ok {
				continue
			}
			if prev, had := acceptedBefore[slot]; had && prev.Ballot == pv.Ballot && paxos.ValueEqual(prev.Value, pv.Value) {
				continue
			}
			if pv.Ballot.Less(before) {
				t.Errorf("%s: accepted %v for slot %d below the promise in force %v", s.name, pv.Ballot, slot, before)
			}
		}
		if s.wantType == "nack" {
			if nk := outs[0].Msg.(Nack); nk.Promised != n.Promised() {
				t.Errorf("%s: nack reports promise %v, node has %v", s.name, nk.Promised, n.Promised())
			}
		}
	}
	if n.Promised() != b(5, 3) {
		t.Errorf("final promise %v, want r5.n3", n.Promised())
	}
}

func TestLeaseRefusesPrepare(t *testing.T) {
	c := newCluster(t, 3, nil)
	l := c.electLeader()
	var f *Node
	for _, id := range c.ids {
		if id != l.Self() {
			f = c.nodes[id]
			break
		}
	}
	var other paxos.NodeID
	for _, id := range c.ids {
		if id != l.Self() && id != f.Self() {
			other = id
		}
	}
	// The follower heard the leader's heartbeat within the last tick, so its
	// lease is live.
	before := f.Promised()
	high := paxos.Ballot{Round: l.Ballot().Round + 100, Node: other}
	outs := f.Step(c.now, Envelope{From: other, To: f.Self(), Msg: Prepare{Ballot: high, FromSlot: 1}})
	if len(outs) != 1 {
		t.Fatalf("got %d replies, want 1", len(outs))
	}
	nk, ok := outs[0].Msg.(Nack)
	if !ok {
		t.Fatalf("reply is %T, want Nack", outs[0].Msg)
	}
	if nk.LeaseRemaining <= 0 || nk.LeaseRemaining > f.Config().LeaseDuration {
		t.Errorf("LeaseRemaining = %v, want in (0, %v]", nk.LeaseRemaining, f.Config().LeaseDuration)
	}
	if nk.Promised != before || f.Promised() != before {
		t.Errorf("lease refusal changed the promise: nack %v, node %v, before %v", nk.Promised, f.Promised(), before)
	}
	if nk.Slot != 0 || nk.Ballot != high {
		t.Errorf("nack = %+v, want Slot 0 and Ballot %v", nk, high)
	}
	// After the lease expires the same Prepare is promised.
	later := c.now + f.Config().LeaseDuration + time.Millisecond
	outs = f.Step(later, Envelope{From: other, To: f.Self(), Msg: Prepare{Ballot: high, FromSlot: 1}})
	if len(outs) != 1 {
		t.Fatalf("after lease: got %d replies, want 1", len(outs))
	}
	if _, ok := outs[0].Msg.(Promise); !ok {
		t.Fatalf("after lease: reply is %T, want Promise", outs[0].Msg)
	}
	if f.Promised() != high {
		t.Errorf("after lease: promise %v, want %v", f.Promised(), high)
	}
}

func TestLeaseDisabledPromisesImmediately(t *testing.T) {
	c := newCluster(t, 3, func(cfg *Config) { cfg.LeaseDuration = 0 })
	l := c.electLeader()
	var f *Node
	var other paxos.NodeID
	for _, id := range c.ids {
		if id == l.Self() {
			continue
		}
		if f == nil {
			f = c.nodes[id]
		} else {
			other = id
		}
	}
	high := paxos.Ballot{Round: l.Ballot().Round + 1, Node: other}
	outs := f.Step(c.now, Envelope{From: other, To: f.Self(), Msg: Prepare{Ballot: high, FromSlot: 1}})
	if len(outs) != 1 {
		t.Fatalf("got %d replies, want 1", len(outs))
	}
	if _, ok := outs[0].Msg.(Promise); !ok {
		t.Fatalf("reply is %T, want Promise with the lease disabled", outs[0].Msg)
	}
}

func TestTakeoverFillsGapsWithNoOps(t *testing.T) {
	c := newCluster(t, 3, nil)
	l := c.electLeader()
	first := l.CommitIndex() + 1
	values := []paxos.Value{
		paxos.Value("v1"), paxos.Value("v2"), paxos.Value("v3"), paxos.Value("v4"), paxos.Value("v5"),
	}
	gap := first + 2 // the slot of v3 never reaches the peers
	c.drop = func(e Envelope) bool {
		a, ok := e.Msg.(Accept)
		return ok && a.Slot == gap
	}
	for _, v := range values {
		c.propose(l, v)
	}
	// Higher slots are chosen while the gap stays open.
	if _, ok := l.Chosen(gap); ok {
		t.Fatalf("slot %d was chosen despite the drop filter", gap)
	}
	if _, ok := l.Chosen(gap + 2); !ok {
		t.Fatalf("slot %d (v5) not chosen while the gap was open", gap+2)
	}
	if l.CommitIndex() != gap-1 {
		t.Fatalf("leader commit index %d, want %d (stuck at the gap)", l.CommitIndex(), gap-1)
	}
	old := l.Self()
	oldBallot := l.Ballot()
	c.crash(old)
	c.drop = nil
	l2 := c.electLeader()
	if l2.Self() == old {
		t.Fatal("crashed node became leader")
	}
	if !oldBallot.Less(l2.Ballot()) {
		t.Errorf("new leader ballot %v not above old %v", l2.Ballot(), oldBallot)
	}
	// The gap is filled with a no-op at the new ballot; every other slot
	// keeps its value; a leadership no-op follows the highest reported slot.
	for _, id := range c.ids {
		n, ok := c.nodes[id]
		if !ok {
			continue
		}
		e, ok := n.Chosen(gap)
		if !ok {
			t.Fatalf("node %d: gap slot %d not chosen after takeover", id, gap)
		}
		if !e.NoOp() {
			t.Errorf("node %d: gap slot %d holds %q, want no-op", id, gap, e.Value)
		}
		if e.Ballot != l2.Ballot() {
			t.Errorf("node %d: gap slot %d chosen at %v, want %v", id, gap, e.Ballot, l2.Ballot())
		}
		for i, v := range values {
			s := first + paxos.Slot(i)
			if s == gap {
				continue
			}
			got, ok := n.Chosen(s)
			if !ok {
				t.Errorf("node %d: slot %d not chosen", id, s)
				continue
			}
			if !paxos.ValueEqual(got.Value, v) {
				t.Errorf("node %d: slot %d = %q, want %q", id, s, got.Value, v)
			}
		}
		lead, ok := n.Chosen(first + paxos.Slot(len(values)))
		if !ok || !lead.NoOp() {
			t.Errorf("node %d: leadership no-op not chosen after the highest reported slot", id)
		}
		if n.CommitIndex() < first+paxos.Slot(len(values)) {
			t.Errorf("node %d: commit index %d did not cover the takeover", id, n.CommitIndex())
		}
	}
	// A client value proposed by the new leader lands after the no-op.
	c.propose(l2, paxos.Value("v6"))
	e, ok := l2.Chosen(first + paxos.Slot(len(values)) + 1)
	if !ok || !paxos.ValueEqual(e.Value, paxos.Value("v6")) {
		t.Errorf("v6 not chosen in the first slot after the leadership no-op: %+v ok=%t", e, ok)
	}
}

func TestReadIndexRefusedBeforeLeadershipNoOp(t *testing.T) {
	ids := threeNodes()
	n, err := New(DefaultConfig(1, ids), NewMemStore(), rand.New(rand.NewPCG(3, 0)))
	if err != nil {
		t.Fatal(err)
	}
	var now time.Duration
	tickUntilCandidate(t, n, &now)
	if _, _, err := n.ReadIndex(now); !errors.As(err, &NotLeaderError{}) {
		t.Fatalf("ReadIndex as candidate: err = %v, want NotLeaderError", err)
	}
	// One remote promise plus the inline self promise is a quorum of 2.
	outs := n.Step(now, Envelope{From: 2, To: 1, Msg: Promise{Ballot: n.Ballot()}})
	if n.Role() != Leader {
		t.Fatalf("role after quorum of promises = %v, want Leader", n.Role())
	}
	if n.Ready() {
		t.Fatal("Ready() true before the leadership no-op is chosen")
	}
	if _, _, err := n.ReadIndex(now); !errors.Is(err, ErrNotReady) {
		t.Fatalf("ReadIndex before no-op: err = %v, want ErrNotReady", err)
	}
	var noop *Accept
	for _, e := range outs {
		if a, ok := e.Msg.(Accept); ok && e.To == 2 {
			noop = &a
		}
	}
	if noop == nil || len(noop.Value) != 0 {
		t.Fatalf("no leadership no-op Accept sent to peer 2: %+v", outs)
	}
	if noop.Slot != 1 {
		t.Fatalf("leadership no-op in slot %d, want 1 on an empty log", noop.Slot)
	}
	n.Step(now, Envelope{From: 2, To: 1, Msg: Accepted{Ballot: n.Ballot(), Slot: noop.Slot}})
	if !n.Ready() {
		t.Fatal("Ready() false after the leadership no-op was chosen")
	}
	if n.CommitIndex() != 1 {
		t.Errorf("commit index %d, want 1", n.CommitIndex())
	}
	seq, _, err := n.ReadIndex(now)
	if err != nil || seq != 1 {
		t.Fatalf("ReadIndex after no-op: seq=%d err=%v, want 1, nil", seq, err)
	}
}

func TestReadIndexNeedsMajorityAtOwnBallot(t *testing.T) {
	c := newCluster(t, 3, nil)
	l := c.electLeader()
	c.propose(l, paxos.Value("x"))
	for _, id := range c.ids {
		c.nodes[id].Events() // discard election events
	}
	var peers []paxos.NodeID
	for _, id := range c.ids {
		if id != l.Self() {
			peers = append(peers, id)
		}
	}
	seq, outs, err := l.ReadIndex(c.now)
	if err != nil {
		t.Fatalf("ReadIndex: %v", err)
	}
	if len(outs) != 2 {
		t.Fatalf("ReadIndex sent %d messages, want 2 heartbeats", len(outs))
	}
	for _, e := range outs {
		hb, ok := e.Msg.(Heartbeat)
		if !ok || hb.ReadSeq != seq {
			t.Fatalf("ReadIndex sent %+v, want Heartbeat with ReadSeq %d", e.Msg, seq)
		}
	}
	if evs := l.Events(); len(evs) != 0 {
		t.Fatalf("ReadReady emitted with only the leader's own ack: %+v", evs)
	}
	// An ack for an older read sequence does not count.
	l.Step(c.now, Envelope{From: peers[0], To: l.Self(), Msg: HeartbeatAck{Ballot: l.Ballot(), Promised: l.Ballot(), CommitIndex: l.CommitIndex(), ReadSeq: seq - 1}})
	if evs := l.Events(); len(evs) != 0 {
		t.Fatalf("ReadReady emitted for an ack with a lower ReadSeq: %+v", evs)
	}
	// An ack for a stale ballot does not count.
	stale := paxos.Ballot{Round: l.Ballot().Round - 1, Node: l.Self()}
	l.Step(c.now, Envelope{From: peers[0], To: l.Self(), Msg: HeartbeatAck{Ballot: stale, Promised: stale, ReadSeq: seq}})
	if evs := l.Events(); len(evs) != 0 {
		t.Fatalf("ReadReady emitted for an ack at a stale ballot: %+v", evs)
	}
	// One ack at the leader's ballot completes the quorum of 2.
	l.Step(c.now, Envelope{From: peers[0], To: l.Self(), Msg: HeartbeatAck{Ballot: l.Ballot(), Promised: l.Ballot(), CommitIndex: l.CommitIndex(), ReadSeq: seq}})
	evs := l.Events()
	if len(evs) != 1 {
		t.Fatalf("events after quorum ack = %+v, want one ReadReady", evs)
	}
	rr, ok := evs[0].(ReadReady)
	if !ok || rr.Seq != seq || rr.Index != l.CommitIndex() {
		t.Fatalf("event = %+v, want ReadReady{%d, %d}", evs[0], seq, l.CommitIndex())
	}
	// A second read: an ack reporting a higher promise deposes the leader
	// and fails the read.
	seq2, _, err := l.ReadIndex(c.now)
	if err != nil {
		t.Fatalf("second ReadIndex: %v", err)
	}
	higher := paxos.Ballot{Round: l.Ballot().Round + 1, Node: peers[1]}
	l.Step(c.now, Envelope{From: peers[1], To: l.Self(), Msg: HeartbeatAck{Ballot: l.Ballot(), Promised: higher, ReadSeq: seq2}})
	if l.Role() != Follower {
		t.Fatalf("role after ack with higher promise = %v, want Follower", l.Role())
	}
	evs = l.Events()
	var failed *ReadFailed
	var changed *LeaderChanged
	for _, ev := range evs {
		switch e := ev.(type) {
		case ReadFailed:
			failed = &e
		case LeaderChanged:
			changed = &e
		}
	}
	if failed == nil || failed.Seq != seq2 {
		t.Fatalf("no ReadFailed for seq %d in %+v", seq2, evs)
	}
	var nl NotLeaderError
	if !errors.As(failed.Err, &nl) || nl.Leader != peers[1] {
		t.Errorf("ReadFailed.Err = %v, want NotLeaderError{%d}", failed.Err, peers[1])
	}
	if changed == nil || changed.Self || changed.Leader != peers[1] || changed.Ballot != higher {
		t.Errorf("LeaderChanged = %+v, want {Leader %d, Ballot %v, Self false}", changed, peers[1], higher)
	}
	if l.MaxRound() < higher.Round {
		t.Errorf("MaxRound %d not raised to the higher promise round %d", l.MaxRound(), higher.Round)
	}
}

func TestRestartRestoresDurableState(t *testing.T) {
	c := newCluster(t, 3, nil)
	l := c.electLeader()
	for i := 0; i < 5; i++ {
		c.propose(l, paxos.Value(fmt.Sprintf("v%d", i)))
	}
	// Leave one Accept unlearned on a follower so that accepted and chosen
	// differ.
	var f *Node
	for _, id := range c.ids {
		if id != l.Self() {
			f = c.nodes[id]
			break
		}
	}
	c.drop = func(e Envelope) bool {
		_, isLearn := e.Msg.(Learn)
		return isLearn && e.To == f.Self()
	}
	c.propose(l, paxos.Value("unlearned"))
	c.drop = nil

	maxSlot := c.maxChosen() + 2
	type snapshot struct {
		promised paxos.Ballot
		maxRound uint64
		commit   paxos.Slot
		accepted map[paxos.Slot]paxos.PValue
		chosen   map[paxos.Slot]Entry
	}
	take := func(n *Node) snapshot {
		s := snapshot{promised: n.Promised(), maxRound: n.MaxRound(), commit: n.CommitIndex(),
			accepted: map[paxos.Slot]paxos.PValue{}, chosen: map[paxos.Slot]Entry{}}
		for slot := paxos.Slot(1); slot <= maxSlot; slot++ {
			if pv, ok := n.Accepted(slot); ok {
				s.accepted[slot] = pv
			}
			if e, ok := n.Chosen(slot); ok {
				s.chosen[slot] = e
			}
		}
		return s
	}
	before := take(f)
	if len(before.accepted) <= len(before.chosen) {
		t.Fatalf("test setup: accepted (%d) should exceed chosen (%d) on the follower", len(before.accepted), len(before.chosen))
	}
	id := f.Self()
	c.crash(id)
	f2 := c.restart(id)
	after := take(f2)
	if after.promised != before.promised {
		t.Errorf("promised after restart %v, want %v", after.promised, before.promised)
	}
	if after.maxRound != before.maxRound {
		t.Errorf("maxRound after restart %d, want %d", after.maxRound, before.maxRound)
	}
	if after.commit != before.commit {
		t.Errorf("commit index after restart %d, want %d", after.commit, before.commit)
	}
	if len(after.accepted) != len(before.accepted) {
		t.Errorf("accepted count after restart %d, want %d", len(after.accepted), len(before.accepted))
	}
	for s, pv := range before.accepted {
		got, ok := after.accepted[s]
		if !ok || got.Ballot != pv.Ballot || !paxos.ValueEqual(got.Value, pv.Value) {
			t.Errorf("accepted[%d] after restart = %+v ok=%t, want %+v", s, got, ok, pv)
		}
	}
	for s, e := range before.chosen {
		got, ok := after.chosen[s]
		if !ok || got.Ballot != e.Ballot || !paxos.ValueEqual(got.Value, e.Value) {
			t.Errorf("chosen[%d] after restart = %+v ok=%t, want %+v", s, got, ok, e)
		}
	}
	if f2.Role() != Follower {
		t.Errorf("role after restart %v, want Follower", f2.Role())
	}
	// The restarted node's next ballot is above everything it has seen.
	var now = c.now
	c.crash(id) // keep it out of the cluster while ticking it alone
	tickUntilCandidate(t, f2, &now)
	if f2.Ballot().Round <= before.maxRound {
		t.Errorf("first ballot after restart %v does not exceed saved maxRound %d", f2.Ballot(), before.maxRound)
	}
}

func TestStepDownOnHigherPromiseNack(t *testing.T) {
	c := newCluster(t, 3, nil)
	l := c.electLeader()
	l.Events()
	outs, err := l.Propose(c.now, paxos.Value("x"))
	if err != nil {
		t.Fatal(err)
	}
	var peer paxos.NodeID
	var slot paxos.Slot
	for _, e := range outs {
		if a, ok := e.Msg.(Accept); ok {
			peer, slot = e.To, a.Slot
		}
	}
	higher := paxos.Ballot{Round: l.Ballot().Round + 3, Node: peer}
	l.Step(c.now, Envelope{From: peer, To: l.Self(), Msg: Nack{Ballot: l.Ballot(), Promised: higher, Slot: slot}})
	if l.Role() != Follower {
		t.Fatalf("role after Nack with higher promise = %v, want Follower", l.Role())
	}
	inFlight, queued := l.Pending()
	if inFlight != 0 || queued != 0 {
		t.Errorf("pending after step-down = %d in flight, %d queued; want 0, 0", inFlight, queued)
	}
	_, err = l.Propose(c.now, paxos.Value("y"))
	var nl NotLeaderError
	if !errors.As(err, &nl) || nl.Leader != peer {
		t.Errorf("Propose after step-down: err = %v, want NotLeaderError{%d}", err, peer)
	}
	found := false
	for _, ev := range l.Events() {
		if lc, ok := ev.(LeaderChanged); ok && !lc.Self && lc.Leader == peer && lc.Ballot == higher {
			found = true
		}
	}
	if !found {
		t.Error("no LeaderChanged event naming the node with the higher promise")
	}
	// A stale Nack for another ballot is ignored by a follower.
	l.Step(c.now, Envelope{From: peer, To: l.Self(), Msg: Nack{Ballot: l.Ballot(), Promised: higher}})
}

func TestLeaderStepsDownWithoutMajorityContact(t *testing.T) {
	c := newCluster(t, 3, nil)
	l := c.electLeader()
	l.Events()
	// Cut the leader off from every reply.
	c.drop = func(e Envelope) bool { return e.To == l.Self() }
	stepped := c.runUntil(2*time.Second, func() bool { return l.Role() != Leader })
	if !stepped {
		t.Fatal("leader kept leading without hearing from a majority")
	}
	found := false
	for _, ev := range c.events[l.Self()] {
		if lc, ok := ev.(LeaderChanged); ok && !lc.Self && lc.Leader == 0 {
			found = true
		}
	}
	for _, ev := range l.Events() {
		if lc, ok := ev.(LeaderChanged); ok && !lc.Self && lc.Leader == 0 {
			found = true
		}
	}
	if !found {
		t.Error("no LeaderChanged{Leader: 0} event on loss of majority contact")
	}
}

func TestLearnRequestCatchUp(t *testing.T) {
	c := newCluster(t, 3, func(cfg *Config) { cfg.LearnBatch = 4 })
	l := c.electLeader()
	var late paxos.NodeID
	for _, id := range c.ids {
		if id != l.Self() {
			late = id
		}
	}
	c.drop = func(e Envelope) bool {
		_, isLearn := e.Msg.(Learn)
		return isLearn && e.To == late
	}
	for i := 0; i < 10; i++ {
		c.propose(l, paxos.Value(fmt.Sprintf("v%d", i)))
	}
	if c.nodes[late].CommitIndex() >= l.CommitIndex() {
		t.Fatalf("late node commit %d not behind leader %d", c.nodes[late].CommitIndex(), l.CommitIndex())
	}
	behind := c.nodes[late].CommitIndex()
	c.drop = nil
	// Heartbeats carry the leader's commit index; the late node requests
	// batches of LearnBatch until it has caught up.
	ok := c.runUntil(2*time.Second, func() bool { return c.nodes[late].CommitIndex() == l.CommitIndex() })
	if !ok {
		t.Fatalf("late node stuck at %d, leader at %d", c.nodes[late].CommitIndex(), l.CommitIndex())
	}
	for s := behind + 1; s <= l.CommitIndex(); s++ {
		want, _ := l.Chosen(s)
		got, ok := c.nodes[late].Chosen(s)
		if !ok || !paxos.ValueEqual(got.Value, want.Value) {
			t.Errorf("slot %d on late node = %+v ok=%t, want %+v", s, got, ok, want)
		}
	}
}

func TestProposeOnFollowerReportsLeader(t *testing.T) {
	c := newCluster(t, 3, nil)
	l := c.electLeader()
	for _, id := range c.ids {
		if id == l.Self() {
			continue
		}
		_, err := c.nodes[id].Propose(c.now, paxos.Value("x"))
		var nl NotLeaderError
		if !errors.As(err, &nl) {
			t.Fatalf("node %d: Propose err = %v, want NotLeaderError", id, err)
		}
		if nl.Leader != l.Self() {
			t.Errorf("node %d: leader hint %d, want %d", id, nl.Leader, l.Self())
		}
		leader, ballot, ok := c.nodes[id].Leader()
		if !ok || leader != l.Self() || ballot != l.Ballot() {
			t.Errorf("node %d: Leader() = %d %v %t, want %d %v true", id, leader, ballot, ok, l.Self(), l.Ballot())
		}
	}
	if leader, _, ok := l.Leader(); !ok || leader != l.Self() {
		t.Errorf("leader's Leader() = %d %t, want itself", leader, ok)
	}
}

func TestWindowQueuesExcessProposals(t *testing.T) {
	c := newCluster(t, 3, func(cfg *Config) { cfg.Window = 2 })
	l := c.electLeader()
	// Stop Accepted replies so slots stay in flight.
	c.drop = func(e Envelope) bool { _, ok := e.Msg.(Accepted); return ok }
	for i := 0; i < 5; i++ {
		c.propose(l, paxos.Value(fmt.Sprintf("v%d", i)))
	}
	inFlight, queued := l.Pending()
	if inFlight != 2 || queued != 3 {
		t.Fatalf("pending = %d in flight, %d queued; want 2, 3", inFlight, queued)
	}
	c.drop = nil
	ok := c.runUntil(2*time.Second, func() bool {
		inFlight, queued := l.Pending()
		return inFlight == 0 && queued == 0
	})
	if !ok {
		t.Fatal("queued proposals never drained")
	}
	for i := 0; i < 5; i++ {
		want := paxos.Value(fmt.Sprintf("v%d", i))
		found := false
		for s := paxos.Slot(1); s <= l.CommitIndex(); s++ {
			if e, ok := l.Chosen(s); ok && paxos.ValueEqual(e.Value, want) {
				found = true
			}
		}
		if !found {
			t.Errorf("%q never chosen", want)
		}
	}
}

// TestQueueLimitRefusesExcessProposals: once Window slots are in flight and
// QueueLimit values are queued, Propose refuses with ErrBusy instead of
// holding every value a stalled leader is given.
func TestQueueLimitRefusesExcessProposals(t *testing.T) {
	c := newCluster(t, 3, func(cfg *Config) { cfg.Window, cfg.QueueLimit = 2, 3 })
	l := c.electLeader()
	c.drop = func(e Envelope) bool { _, ok := e.Msg.(Accepted); return ok }
	for i := 0; i < 5; i++ {
		c.propose(l, paxos.Value(fmt.Sprintf("v%d", i)))
	}
	if _, err := l.Propose(c.now, paxos.Value("v5")); !errors.Is(err, ErrBusy) {
		t.Fatalf("Propose with a full queue = %v, want ErrBusy", err)
	}
	if inFlight, queued := l.Pending(); inFlight != 2 || queued != 3 {
		t.Fatalf("pending = %d in flight, %d queued; want 2, 3", inFlight, queued)
	}
	c.drop = nil
	if !c.runUntil(2*time.Second, func() bool { in, q := l.Pending(); return in == 0 && q == 0 }) {
		t.Fatal("queued proposals never drained")
	}
	if _, err := l.Propose(c.now, paxos.Value("v5")); err != nil {
		t.Fatalf("Propose after the queue drained: %v", err)
	}
}

// TestProposeRefusesOversizedValue: a value longer than MaxValueBytes is
// refused before it takes a slot, because no transport would carry its
// Accept.
func TestProposeRefusesOversizedValue(t *testing.T) {
	c := newCluster(t, 3, nil)
	l := c.electLeader()
	commit := l.CommitIndex()
	if _, err := l.Propose(c.now, make(paxos.Value, MaxValueBytes+1)); !errors.Is(err, ErrValueTooLarge) {
		t.Fatalf("Propose of %d bytes = %v, want ErrValueTooLarge", MaxValueBytes+1, err)
	}
	if in, q := l.Pending(); in != 0 || q != 0 {
		t.Fatalf("the refused value is pending: %d in flight, %d queued", in, q)
	}
	c.propose(l, make(paxos.Value, MaxValueBytes))
	if !c.runUntil(2*time.Second, func() bool { return l.CommitIndex() == commit+1 }) {
		t.Fatalf("a value of exactly MaxValueBytes was not chosen (commit %d)", l.CommitIndex())
	}
}

// failingStore fails the next Save call of the given kind.
type failingStore struct {
	*MemStore
	failAccepted bool
}

var errDisk = errors.New("disk full")

func (s *failingStore) SaveAccepted(pv paxos.PValue) error {
	if s.failAccepted {
		s.failAccepted = false
		return errDisk
	}
	return s.MemStore.SaveAccepted(pv)
}

func TestStoreFailureStopsNodeWithoutReplying(t *testing.T) {
	st := &failingStore{MemStore: NewMemStore(), failAccepted: true}
	n, err := New(DefaultConfig(1, threeNodes()), st, rand.New(rand.NewPCG(1, 0)))
	if err != nil {
		t.Fatal(err)
	}
	b := paxos.Ballot{Round: 1, Node: 2}
	outs := n.Step(0, Envelope{From: 2, To: 1, Msg: Accept{Ballot: b, Slot: 1, Value: paxos.Value("x")}})
	if len(outs) != 0 {
		t.Fatalf("a node whose store failed replied: %+v", outs)
	}
	if !errors.Is(n.Failed(), errDisk) {
		t.Fatalf("Failed() = %v, want wrapped %v", n.Failed(), errDisk)
	}
	if outs := n.Tick(time.Second); len(outs) != 0 {
		t.Errorf("failed node produced messages on Tick: %+v", outs)
	}
	if _, err := n.Propose(time.Second, paxos.Value("y")); !errors.Is(err, errDisk) {
		t.Errorf("Propose on failed node: %v, want the store error", err)
	}
	// The durable state does not contain the unsaved acceptance, and the
	// promise it did save survives.
	d, _ := st.Load()
	if len(d.Accepted) != 0 {
		t.Errorf("store holds %d accepted pvalues after a failed save, want 0", len(d.Accepted))
	}
	n2, err := New(DefaultConfig(1, threeNodes()), st, rand.New(rand.NewPCG(1, 0)))
	if err != nil {
		t.Fatal(err)
	}
	if n2.Promised() != d.Promised {
		t.Errorf("rebuilt node promise %v, want %v", n2.Promised(), d.Promised)
	}
}

func TestIgnoresForeignAndMisaddressedMessages(t *testing.T) {
	n, err := New(DefaultConfig(1, threeNodes()), NewMemStore(), rand.New(rand.NewPCG(1, 0)))
	if err != nil {
		t.Fatal(err)
	}
	b := paxos.Ballot{Round: 1, Node: 9}
	if outs := n.Step(0, Envelope{From: 9, To: 1, Msg: Prepare{Ballot: b, FromSlot: 1}}); len(outs) != 0 {
		t.Errorf("answered a node outside Peers: %+v", outs)
	}
	if outs := n.Step(0, Envelope{From: 2, To: 3, Msg: Prepare{Ballot: paxos.Ballot{Round: 1, Node: 2}, FromSlot: 1}}); len(outs) != 0 {
		t.Errorf("answered a message addressed to another node: %+v", outs)
	}
	if !n.Promised().IsZero() {
		t.Errorf("promise changed by an ignored message: %v", n.Promised())
	}
}

func TestSingleNodeClusterChoosesAlone(t *testing.T) {
	ids := []paxos.NodeID{1}
	n, err := New(DefaultConfig(1, ids), NewMemStore(), rand.New(rand.NewPCG(1, 0)))
	if err != nil {
		t.Fatal(err)
	}
	var now time.Duration
	for i := 0; i < 100 && n.Role() != Leader; i++ {
		now += tickStep
		if outs := n.Tick(now); len(outs) != 0 {
			t.Fatalf("single node sent messages: %+v", outs)
		}
	}
	if !n.Ready() {
		t.Fatalf("single node not a ready leader: role %v commit %d", n.Role(), n.CommitIndex())
	}
	if _, err := n.Propose(now, paxos.Value("x")); err != nil {
		t.Fatal(err)
	}
	if e, ok := n.Chosen(2); !ok || !paxos.ValueEqual(e.Value, paxos.Value("x")) {
		t.Errorf("slot 2 = %+v ok=%t, want x", e, ok)
	}
	seq, _, err := n.ReadIndex(now)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, ev := range n.Events() {
		if rr, ok := ev.(ReadReady); ok && rr.Seq == seq && rr.Index == 2 {
			found = true
		}
	}
	if !found {
		t.Error("single node did not complete its own read barrier")
	}
}

func TestEventsDrain(t *testing.T) {
	c := newCluster(t, 3, nil)
	l := c.electLeader()
	if evs := c.events[l.Self()]; len(evs) == 0 {
		t.Fatal("no events recorded for the leader during election")
	}
	if evs := l.Events(); len(evs) != 0 {
		t.Errorf("Events() returned already drained events: %+v", evs)
	}
}
