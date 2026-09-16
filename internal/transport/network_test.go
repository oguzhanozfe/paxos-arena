package transport

import (
	"math"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/oguzhanozfe/paxos-arena/internal/paxos"
	"github.com/oguzhanozfe/paxos-arena/internal/replog"
)

func env(from, to paxos.NodeID, slot paxos.Slot) replog.Envelope {
	return replog.Envelope{From: from, To: to, Msg: replog.Accepted{Ballot: paxos.Ballot{Round: 1, Node: from}, Slot: slot}}
}

func TestNetworkStatsMatchProbabilities(t *testing.T) {
	const sends = 100_000
	cases := []struct {
		name string
		f    Faults
	}{
		{"no faults", Faults{MinDelay: time.Millisecond, MaxDelay: time.Millisecond}},
		{"drop 0.1 dup 0.3", Faults{DropP: 0.1, DupP: 0.3, MinDelay: time.Millisecond, MaxDelay: 10 * time.Millisecond}},
		{"drop 0.5", Faults{DropP: 0.5, MinDelay: 0, MaxDelay: 0}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			n := NewNetwork(rand.New(rand.NewPCG(1, 0)), tc.f)
			for i := 0; i < sends; i++ {
				n.Send(0, env(1, 2, paxos.Slot(i+1)))
			}
			s := n.Stats()
			if s.Sent != sends {
				t.Fatalf("Sent = %d, want %d", s.Sent, sends)
			}
			tol := 0.01 * sends
			if got, want := float64(s.Dropped), tc.f.DropP*sends; math.Abs(got-want) > tol {
				t.Errorf("Dropped = %d, want about %.0f", s.Dropped, want)
			}
			survivors := float64(sends) - float64(s.Dropped)
			if got, want := float64(s.Duplicated), tc.f.DupP*survivors; math.Abs(got-want) > tol {
				t.Errorf("Duplicated = %d, want about %.0f", s.Duplicated, want)
			}
			if got, want := uint64(n.Pending()), sends-s.Dropped+s.Duplicated; got != want {
				t.Errorf("Pending = %d, want %d", got, want)
			}
			// Drain and check delivery order and delay bounds.
			var last time.Duration
			delivered := uint64(0)
			for {
				at, e, ok := n.Next()
				if !ok {
					break
				}
				delivered++
				if at < last {
					t.Fatalf("delivery out of time order: %v after %v", at, last)
				}
				last = at
				if at < tc.f.MinDelay || at > tc.f.MaxDelay {
					t.Fatalf("delay %v outside [%v, %v]", at, tc.f.MinDelay, tc.f.MaxDelay)
				}
				if e.To != 2 {
					t.Fatalf("envelope to %d, want 2", e.To)
				}
			}
			if delivered != n.Stats().Delivered || delivered != sends-s.Dropped+s.Duplicated {
				t.Errorf("Delivered = %d (stats %d), want %d", delivered, n.Stats().Delivered, sends-s.Dropped+s.Duplicated)
			}
		})
	}
}

func TestNetworkIsDeterministic(t *testing.T) {
	run := func() []time.Duration {
		n := NewNetwork(rand.New(rand.NewPCG(9, 0)), Faults{DropP: 0.2, DupP: 0.2, MinDelay: time.Millisecond, MaxDelay: 20 * time.Millisecond})
		for i := 0; i < 1000; i++ {
			n.Send(time.Duration(i)*time.Millisecond, env(1, 2, paxos.Slot(i)))
		}
		var out []time.Duration
		for {
			at, e, ok := n.Next()
			if !ok {
				return out
			}
			out = append(out, at, time.Duration(e.Msg.(replog.Accepted).Slot))
		}
	}
	a, b := run(), run()
	if len(a) != len(b) {
		t.Fatalf("two runs delivered %d and %d items", len(a), len(b))
	}
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("runs diverge at item %d: %v vs %v", i, a[i], b[i])
		}
	}
}

func TestNetworkTiesBreakBySendOrder(t *testing.T) {
	n := NewNetwork(rand.New(rand.NewPCG(1, 0)), Faults{})
	for i := 1; i <= 5; i++ {
		n.Send(0, env(1, 2, paxos.Slot(i)))
	}
	for i := 1; i <= 5; i++ {
		_, e, ok := n.Next()
		if !ok || e.Msg.(replog.Accepted).Slot != paxos.Slot(i) {
			t.Fatalf("item %d: got %+v ok=%t", i, e, ok)
		}
	}
}

func TestNetworkBlockSplitHeal(t *testing.T) {
	n := NewNetwork(rand.New(rand.NewPCG(1, 0)), Faults{})
	n.Block(1, 2, true)
	n.Send(0, env(1, 2, 1))
	n.Send(0, env(2, 1, 2))
	if n.Pending() != 1 || n.Stats().Blocked != 1 {
		t.Fatalf("directional block: pending %d blocked %d, want 1 and 1", n.Pending(), n.Stats().Blocked)
	}
	if !n.Blocked(1, 2) || n.Blocked(2, 1) {
		t.Error("Blocked() does not reflect the directional block")
	}
	n.Next()
	n.Block(1, 2, false)
	n.Send(0, env(1, 2, 3))
	if n.Pending() != 1 {
		t.Fatalf("after unblock: pending %d, want 1", n.Pending())
	}
	n.Next()

	n.Split([]paxos.NodeID{1, 2}, []paxos.NodeID{3, 4, 5})
	pairs := [][2]paxos.NodeID{{1, 2}, {2, 1}, {3, 4}, {5, 3}} // inside groups: delivered
	for _, p := range pairs {
		n.Send(0, env(p[0], p[1], 1))
	}
	if n.Pending() != len(pairs) {
		t.Fatalf("intra-group sends pending %d, want %d", n.Pending(), len(pairs))
	}
	for n.Pending() > 0 {
		n.Next()
	}
	across := [][2]paxos.NodeID{{1, 3}, {3, 1}, {2, 5}, {4, 2}}
	before := n.Stats().Blocked
	for _, p := range across {
		n.Send(0, env(p[0], p[1], 1))
	}
	if n.Pending() != 0 || n.Stats().Blocked-before != uint64(len(across)) {
		t.Fatalf("cross-group sends: pending %d blocked %d, want 0 and %d", n.Pending(), n.Stats().Blocked-before, len(across))
	}
	n.Heal()
	for _, p := range across {
		n.Send(0, env(p[0], p[1], 1))
	}
	if n.Pending() != len(across) {
		t.Fatalf("after Heal: pending %d, want %d", n.Pending(), len(across))
	}
}

func TestNetworkFilter(t *testing.T) {
	n := NewNetwork(rand.New(rand.NewPCG(1, 0)), Faults{})
	n.SetFilter(func(e replog.Envelope) bool {
		_, isLearn := e.Msg.(replog.Learn)
		return isLearn && e.To == 3
	})
	n.Send(0, replog.Envelope{From: 1, To: 3, Msg: replog.Learn{Slot: 1}})
	n.Send(0, replog.Envelope{From: 1, To: 2, Msg: replog.Learn{Slot: 1}})
	n.Send(0, env(1, 3, 1))
	if n.Pending() != 2 || n.Stats().Blocked != 1 {
		t.Fatalf("filter: pending %d blocked %d, want 2 and 1", n.Pending(), n.Stats().Blocked)
	}
	n.Heal()
	n.Send(0, replog.Envelope{From: 1, To: 3, Msg: replog.Learn{Slot: 2}})
	if n.Pending() != 3 {
		t.Fatalf("Heal did not remove the filter: pending %d, want 3", n.Pending())
	}
}

func TestNetworkPurge(t *testing.T) {
	n := NewNetwork(rand.New(rand.NewPCG(1, 0)), Faults{MinDelay: time.Millisecond, MaxDelay: 5 * time.Millisecond})
	for i := 0; i < 20; i++ {
		n.Send(0, env(1, paxos.NodeID(2+i%3), paxos.Slot(i)))
	}
	before := n.Stats()
	n.Purge(3)
	after := n.Stats()
	if after.Dropped-before.Dropped == 0 {
		t.Fatal("Purge dropped nothing")
	}
	var last time.Duration
	for {
		at, e, ok := n.Next()
		if !ok {
			break
		}
		if e.To == 3 {
			t.Fatalf("envelope to purged node 3 delivered: %+v", e)
		}
		if at < last {
			t.Fatalf("heap order broken after Purge: %v after %v", at, last)
		}
		last = at
	}
	if _, ok := n.PeekTime(); ok {
		t.Error("PeekTime reports pending after draining")
	}
}

func TestNetworkSetFaults(t *testing.T) {
	n := NewNetwork(rand.New(rand.NewPCG(1, 0)), Faults{DropP: 1})
	n.Send(0, env(1, 2, 1))
	if n.Pending() != 0 {
		t.Fatal("DropP=1 delivered a message")
	}
	n.SetFaults(Faults{MinDelay: 3 * time.Millisecond, MaxDelay: 3 * time.Millisecond})
	if n.Faults().DropP != 0 {
		t.Fatal("SetFaults did not replace the parameters")
	}
	n.Send(time.Second, env(1, 2, 2))
	at, ok := n.PeekTime()
	if !ok || at != time.Second+3*time.Millisecond {
		t.Fatalf("PeekTime = %v %t, want 1.003s true", at, ok)
	}
}
