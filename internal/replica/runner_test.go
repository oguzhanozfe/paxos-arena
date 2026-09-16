package replica

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/oguzhanozfe/paxos-arena/internal/paxos"
	"github.com/oguzhanozfe/paxos-arena/internal/replog"
	"github.com/oguzhanozfe/paxos-arena/internal/tournament"
	"github.com/oguzhanozfe/paxos-arena/internal/transport"
)

// cluster is a set of runners on one Local bus inside a synctest bubble.
type cluster struct {
	t       *testing.T
	bus     *transport.Local
	runners map[paxos.NodeID]*Runner
	cancel  map[paxos.NodeID]context.CancelFunc
	wg      sync.WaitGroup
	errs    map[paxos.NodeID]error
	mu      sync.Mutex
}

// passive returns a config for a node that never starts an election, so
// the other node leads deterministically.
func passive(self paxos.NodeID, peers []paxos.NodeID) replog.Config {
	cfg := replog.DefaultConfig(self, peers)
	cfg.ElectionTimeoutMin = 100 * time.Second
	cfg.ElectionTimeoutMax = 200 * time.Second
	return cfg
}

func newCluster(t *testing.T) *cluster {
	return &cluster{t: t, bus: transport.NewLocal(), runners: make(map[paxos.NodeID]*Runner),
		cancel: make(map[paxos.NodeID]context.CancelFunc), errs: make(map[paxos.NodeID]error)}
}

// add starts a runner for cfg on store.
func (c *cluster) add(cfg replog.Config, store replog.Store) *Runner {
	c.t.Helper()
	core, err := NewCore(cfg, store, rand.New(rand.NewPCG(uint64(cfg.Self), 0)))
	if err != nil {
		c.t.Fatal(err)
	}
	r := NewRunner(core, c.bus.Send, nil)
	c.bus.Register(cfg.Self, r.Deliver)
	ctx, cancel := context.WithCancel(context.Background())
	c.runners[cfg.Self] = r
	c.cancel[cfg.Self] = cancel
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		err := r.Run(ctx)
		c.mu.Lock()
		c.errs[cfg.Self] = err
		c.mu.Unlock()
	}()
	return r
}

// stop cancels every runner and waits for Run to return, so no goroutine
// outlives the bubble.
func (c *cluster) stop() {
	for _, cancel := range c.cancel {
		cancel()
	}
	c.wg.Wait()
	for id, err := range c.errs {
		if err != nil {
			c.t.Errorf("runner %d: %v", id, err)
		}
	}
}

// waitFor advances the fake clock until cond holds or 30 seconds pass.
func waitFor(t *testing.T, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func ready(r *Runner) func() bool {
	return func() bool { return r.Status().Ready }
}

// TestSubmitResolvesFromEarlierSlot: node 2 holds an accepted but unchosen
// value for slot 1 from an earlier leader; node 1 takes over, re-proposes
// it, and while its Accepts are blocked the client submits the same key.
// The waiter resolves when slot 1 is applied, not the slot the runner
// proposed, and the result carries slot 1.
func TestSubmitResolvesFromEarlierSlot(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		peers := []paxos.NodeID{1, 2}
		old := paxos.Ballot{Round: 1, Node: 1}
		cmd := createCmd("k-old", "t-old")
		v, _ := tournament.Encode(cmd)
		s1 := replog.NewMemStore()
		s1.SavePromised(old)
		s1.SaveMaxRound(1)
		s2 := replog.NewMemStore()
		s2.SavePromised(old)
		s2.SaveAccepted(paxos.PValue{Ballot: old, Slot: 1, Value: paxos.Value(v)})
		c := newCluster(t)
		defer c.stop()
		c.bus.SetFilter(func(e replog.Envelope) bool {
			_, isAccept := e.Msg.(replog.Accept)
			return isAccept && e.From == 1
		})
		r1 := c.add(replog.DefaultConfig(1, peers), s1)
		c.add(passive(2, peers), s2)
		waitFor(t, func() bool { return r1.Status().Role == replog.Leader }, "node 1 to lead")
		if r1.Status().Ready {
			t.Fatal("node 1 became ready although its Accepts are blocked")
		}
		var res tournament.Result
		var err error
		done := make(chan struct{})
		go func() {
			defer close(done)
			res, err = r1.Submit(context.Background(), cmd)
		}()
		synctest.Wait() // the submit is registered and pending
		c.bus.SetFilter(nil)
		<-done
		if err != nil {
			t.Fatalf("Submit: %v", err)
		}
		if res.Slot != 1 || res.Replayed || res.Code != tournament.OK {
			t.Fatalf("result %+v, want the result of slot 1 (the earlier leader's proposal)", res)
		}
		// The runner's own proposal of the key follows in slot 3.
		waitFor(t, func() bool {
			st := r1.Status()
			return st.CommitIndex >= 3 && st.Applied == st.CommitIndex
		}, "slot 3 to commit")
		// The runner's own proposal of the key sits in a later slot and was
		// applied as a replay, so a second Submit answers from the table.
		again, err := r1.Submit(context.Background(), cmd)
		if err != nil || !again.Replayed || again.Slot != 1 {
			t.Errorf("second Submit = %+v, %v", again, err)
		}
	})
}

// TestSubmitFailsWithLeadershipLost: the leader loses contact with the
// majority while a command is pending; the waiter ends with
// ErrLeadershipLost instead of hanging.
func TestSubmitFailsWithLeadershipLost(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		peers := []paxos.NodeID{1, 2}
		c := newCluster(t)
		defer c.stop()
		r1 := c.add(replog.DefaultConfig(1, peers), replog.NewMemStore())
		c.add(passive(2, peers), replog.NewMemStore())
		waitFor(t, ready(r1), "node 1 to be ready")
		c.bus.Block(2, 1, true)
		_, err := r1.Submit(context.Background(), createCmd("k", "t"))
		if !errors.Is(err, ErrLeadershipLost) {
			t.Fatalf("Submit = %v, want ErrLeadershipLost", err)
		}
		if r1.Status().Role == replog.Leader {
			t.Error("node 1 still leads after losing the majority")
		}
		// A consistent read now fails as not-leader; a stale read works.
		err = r1.Read(context.Background(), true, func(*tournament.State) error { return nil })
		var nl replog.NotLeaderError
		if !errors.As(err, &nl) {
			t.Errorf("consistent read on a deposed leader = %v", err)
		}
		if err := r1.Read(context.Background(), false, func(*tournament.State) error { return nil }); err != nil {
			t.Errorf("stale read = %v", err)
		}
	})
}

// TestReadConsistentSeesCompletedSubmit: a consistent read after a
// completed submit sees its effect; a follower refuses consistent reads
// and answers stale ones; a follower replays a recorded key without
// forwarding and refuses new keys with the leader's identity.
func TestReadConsistentSeesCompletedSubmit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		peers := []paxos.NodeID{1, 2}
		c := newCluster(t)
		defer c.stop()
		r1 := c.add(replog.DefaultConfig(1, peers), replog.NewMemStore())
		r2 := c.add(passive(2, peers), replog.NewMemStore())
		waitFor(t, ready(r1), "node 1 to be ready")
		cmd := createCmd("k1", "t1")
		res, err := r1.Submit(context.Background(), cmd)
		if err != nil || res.Code != tournament.OK || res.Replayed {
			t.Fatalf("Submit = %+v, %v", res, err)
		}
		var seen bool
		if err := r1.Read(context.Background(), true, func(s *tournament.State) error {
			_, seen = s.Tournament("t1")
			return nil
		}); err != nil {
			t.Fatalf("consistent read: %v", err)
		}
		if !seen {
			t.Error("consistent read did not see the completed submit")
		}
		want := errors.New("from fn")
		if err := r1.Read(context.Background(), true, func(*tournament.State) error { return want }); !errors.Is(err, want) {
			t.Errorf("Read did not return fn's error: %v", err)
		}
		err = r2.Read(context.Background(), true, func(*tournament.State) error { return nil })
		var nl replog.NotLeaderError
		if !errors.As(err, &nl) || nl.Leader != 1 {
			t.Errorf("consistent read on the follower = %v, want NotLeaderError{1}", err)
		}
		waitFor(t, func() bool {
			var ok bool
			r2.Read(context.Background(), false, func(s *tournament.State) error {
				_, ok = s.Tournament("t1")
				return nil
			})
			return ok
		}, "the follower to apply the command")
		replay, err := r2.Submit(context.Background(), cmd)
		if err != nil || !replay.Replayed || replay.Slot != res.Slot {
			t.Errorf("follower replay = %+v, %v", replay, err)
		}
		_, err = r2.Submit(context.Background(), createCmd("k2", "t2"))
		if !errors.As(err, &nl) || nl.Leader != 1 {
			t.Errorf("follower submit of a new key = %v, want NotLeaderError{1}", err)
		}
		mutated := cmd
		mutated.Op = tournament.Close{Tournament: "t1"}
		reused, err := r1.Submit(context.Background(), mutated)
		if err != nil || reused.Code != tournament.KeyReused {
			t.Errorf("mutated payload under a recorded key = %+v, %v", reused, err)
		}
		if st := r1.Status(); st.Applied != st.CommitIndex || st.Tournaments != 1 {
			t.Errorf("status: %+v", st)
		}
	})
}

// TestStopFailsPendingRequests: cancelling Run answers pending submits with
// ErrStopped and later calls return ErrStopped at once.
func TestStopFailsPendingRequests(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		peers := []paxos.NodeID{1, 2}
		c := newCluster(t)
		r1 := c.add(replog.DefaultConfig(1, peers), replog.NewMemStore())
		c.add(passive(2, peers), replog.NewMemStore())
		waitFor(t, ready(r1), "node 1 to be ready")
		c.bus.Block(2, 1, true)
		errc := make(chan error, 1)
		go func() {
			_, err := r1.Submit(context.Background(), createCmd("k", "t"))
			errc <- err
		}()
		synctest.Wait()
		c.stop()
		if err := <-errc; !errors.Is(err, ErrStopped) {
			t.Errorf("pending submit after stop = %v, want ErrStopped", err)
		}
		if _, err := r1.Submit(context.Background(), createCmd("k2", "t")); !errors.Is(err, ErrStopped) {
			t.Errorf("submit after stop = %v", err)
		}
		if err := r1.Read(context.Background(), false, func(*tournament.State) error { return nil }); !errors.Is(err, ErrStopped) {
			t.Errorf("read after stop = %v", err)
		}
	})
}

func TestSubmitHonoursContext(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		peers := []paxos.NodeID{1, 2}
		c := newCluster(t)
		defer c.stop()
		r1 := c.add(replog.DefaultConfig(1, peers), replog.NewMemStore())
		c.add(passive(2, peers), replog.NewMemStore())
		waitFor(t, ready(r1), "node 1 to be ready")
		c.bus.Block(2, 1, true)
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		_, err := r1.Submit(ctx, createCmd("k", "t"))
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("Submit with an expired context = %v", err)
		}
	})
}

func TestDeliverDropsWhenInboxIsFull(t *testing.T) {
	core, err := NewCore(replog.DefaultConfig(1, []paxos.NodeID{1}), replog.NewMemStore(), rand.New(rand.NewPCG(1, 1)))
	if err != nil {
		t.Fatal(err)
	}
	r := NewRunner(core, func(replog.Envelope) {}, nil)
	for i := 0; i < InboxSize+10; i++ {
		r.Deliver(replog.Envelope{From: 2, To: 1, Msg: replog.Heartbeat{}})
	}
	if got := r.Dropped(); got != 10 {
		t.Errorf("Dropped = %d, want 10", got)
	}
}

// TestAbandonedSubmitsAreForgotten: while no slot can be chosen, a Submit
// whose context ends is forgotten by the runner at its next tick, and once
// the leader's queue is full a further Submit fails at once with
// replog.ErrBusy instead of being held.
func TestAbandonedSubmitsAreForgotten(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		peers := []paxos.NodeID{1, 2}
		c := newCluster(t)
		defer c.stop()
		cfg := replog.DefaultConfig(1, peers)
		cfg.Window, cfg.QueueLimit = 2, 3
		r1 := c.add(cfg, replog.NewMemStore())
		c.add(passive(2, peers), replog.NewMemStore())
		waitFor(t, ready(r1), "node 1 to be ready")
		c.bus.SetFilter(func(e replog.Envelope) bool {
			_, isAccept := e.Msg.(replog.Accept)
			return isAccept
		})
		for i := 0; i < 5; i++ {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
			_, err := r1.Submit(ctx, createCmd(fmt.Sprintf("k%d", i), fmt.Sprintf("t%d", i)))
			cancel()
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("Submit %d = %v, want a timeout", i, err)
			}
		}
		_, err := r1.Submit(context.Background(), createCmd("k5", "t5"))
		if !errors.Is(err, replog.ErrBusy) {
			t.Fatalf("Submit with a full queue = %v, want replog.ErrBusy", err)
		}
		waitFor(t, func() bool { return r1.Held() == 0 }, "the abandoned submits to be forgotten")
		if st := r1.Status(); st.Role != replog.Leader {
			t.Fatalf("node 1 is %s; the test needs a leader that cannot choose", st.Role)
		}
		// The proposals are still in flight: when Accepts flow again they
		// are chosen, and a retry under the same key gets the recorded
		// result.
		c.bus.SetFilter(nil)
		waitFor(t, func() bool { return r1.Status().Applied >= 6 }, "the queued proposals to be applied")
		res, err := r1.Submit(context.Background(), createCmd("k4", "t4"))
		if err != nil || res.Code != tournament.OK || !res.Replayed {
			t.Fatalf("retry of an abandoned submit = %+v, %v; want the replayed ok", res, err)
		}
	})
}

// TestSlowReadDoesNotStallTheEventLoop: a read runs on its caller's
// goroutine, so a read that does not return keeps neither heartbeats nor
// the log from making progress. Before reads left the event loop, a read
// function that blocked stopped the leader's heartbeats and the followers
// elected a new leader within an election timeout. While the read runs the
// leader leaves a chosen command unapplied, a read that starts meanwhile
// waits for that apply, and both complete once the slow read returns.
func TestSlowReadDoesNotStallTheEventLoop(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		peers := []paxos.NodeID{1, 2, 3}
		c := newCluster(t)
		defer c.stop()
		rs := []*Runner{
			c.add(replog.DefaultConfig(1, peers), replog.NewMemStore()),
			c.add(replog.DefaultConfig(2, peers), replog.NewMemStore()),
			c.add(replog.DefaultConfig(3, peers), replog.NewMemStore()),
		}
		var leader *Runner
		waitFor(t, func() bool {
			for _, r := range rs {
				if r.Status().Ready {
					leader = r
					return true
				}
			}
			return false
		}, "a ready leader")
		ballot := leader.Status().Ballot

		release := make(chan struct{})
		slow := make(chan error, 1)
		entered := make(chan struct{})
		go func() {
			slow <- leader.Read(context.Background(), false, func(*tournament.State) error {
				close(entered)
				<-release
				return nil
			})
		}()
		<-entered

		submitted := make(chan error, 1)
		go func() {
			res, err := leader.Submit(context.Background(), createCmd("k-slow", "t-slow"))
			if err == nil && res.Code != tournament.OK {
				err = fmt.Errorf("result %s", res.Code)
			}
			submitted <- err
		}()
		waitFor(t, func() bool {
			st := leader.Status()
			return st.CommitIndex > st.Applied
		}, "the command to be chosen while the read runs")

		later := make(chan bool, 1)
		go func() {
			var seen bool
			leader.Read(context.Background(), false, func(s *tournament.State) error {
				_, seen = s.Tournament("t-slow")
				return nil
			})
			later <- seen
		}()

		// Ten election timeouts with the read still running.
		time.Sleep(3 * time.Second)
		synctest.Wait()
		for _, r := range rs {
			if st := r.Status(); st.Ballot != ballot || st.Leader != leader.Status().Self {
				t.Fatalf("node %d: ballot %s leader %d, want %s and node %d: the slow read stopped the heartbeats",
					st.Self, st.Ballot, st.Leader, ballot, leader.Status().Self)
			}
		}
		select {
		case err := <-submitted:
			t.Fatalf("Submit answered before its slot could be applied: %v", err)
		case <-later:
			t.Fatal("a read started while an apply was deferred ran before the apply")
		default:
		}

		close(release)
		if err := <-slow; err != nil {
			t.Fatalf("slow read = %v", err)
		}
		if err := <-submitted; err != nil {
			t.Fatalf("Submit after the read = %v", err)
		}
		if !<-later {
			t.Error("the read that waited for the deferred apply did not see the command")
		}
		if st := leader.Status(); st.Applied != st.CommitIndex {
			t.Errorf("after the read: applied %d, commit %d", st.Applied, st.CommitIndex)
		}
	})
}
