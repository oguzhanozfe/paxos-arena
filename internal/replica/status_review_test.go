package replica

import (
	"context"
	"fmt"
	"math/rand/v2"
	"runtime"
	"testing"
	"time"

	"github.com/oguzhanozfe/paxos-arena/internal/paxos"
	"github.com/oguzhanozfe/paxos-arena/internal/replog"
)

// BenchmarkReviewStatusPerEvent measures Core.Status, which Runner.after
// calls on every inbound message, tick, submit and read. Its cost grows
// with the number of tournaments because Status copies State.Tournaments()
// only to take its length.
func BenchmarkReviewStatusPerEvent(b *testing.B) {
	for _, n := range []int{1, 1_000, 10_000, 100_000} {
		b.Run(fmt.Sprintf("tournaments=%d", n), func(b *testing.B) {
			core, err := NewCore(replog.DefaultConfig(1, []paxos.NodeID{1}), replog.NewMemStore(), rand.New(rand.NewPCG(1, 1)))
			if err != nil {
				b.Fatal(err)
			}
			st := core.State()
			for i := 0; i < n; i++ {
				cmd := createCmd(fmt.Sprintf("k%d", i), fmt.Sprintf("t%d", i))
				st.Apply(paxos.Slot(i+1), paxos.Ballot{Round: 1, Node: 1}, cmd)
			}
			if got := len(st.Tournaments()); got != n {
				b.Fatalf("%d tournaments, want %d", got, n)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = core.Status()
			}
		})
	}
}

// TestReviewAbandonedSubmitsAreRetained: while the leader keeps quorum
// contact but a slot cannot be chosen (here every Accept to the follower is
// lost, as when the HTTP transport refuses an oversized message), every
// Submit whose context ends used to leave a queued proposal in replog.Node
// and a waiter channel in Runner.waiters, with nothing bounding or removing
// either until the key was applied or leadership changed. The queue is now
// bounded by Config.QueueLimit and the runner forgets abandoned waiters at
// its next tick, so this runs by default as a regression test.
func TestReviewAbandonedSubmitsAreRetained(t *testing.T) {
	peers := []paxos.NodeID{1, 2}
	c := newCluster(t)
	defer c.stop()
	r1 := c.add(replog.DefaultConfig(1, peers), replog.NewMemStore())
	c.add(passive(2, peers), replog.NewMemStore())
	waitFor(t, ready(r1), "node 1 to be ready")
	c.bus.SetFilter(func(e replog.Envelope) bool {
		_, isAccept := e.Msg.(replog.Accept)
		return isAccept
	})
	heap := func() uint64 {
		runtime.GC()
		runtime.GC()
		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)
		return ms.HeapAlloc
	}
	base := heap()
	const n = 50_000
	for i := 0; i < n; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Microsecond)
		r1.Submit(ctx, createCmd(fmt.Sprintf("abandoned-%d", i), fmt.Sprintf("t%d", i)))
		cancel()
	}
	time.Sleep(500 * time.Millisecond)
	grown := heap()
	st := r1.Status()
	t.Logf("after %d abandoned submits: heap %d -> %d bytes (%.0f bytes each retained); node 1 role %s, commit %d",
		n, base, grown, float64(grown-base)/n, st.Role, st.CommitIndex)
	if st.Role == replog.Leader && grown > base+uint64(n)*200 {
		t.Fatalf("abandoned submits are retained: %d bytes for %d requests while the leader stays in office", grown-base, n)
	}
}
