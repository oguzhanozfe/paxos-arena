package replica

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/oguzhanozfe/paxos-arena/internal/paxos"
	"github.com/oguzhanozfe/paxos-arena/internal/replog"
	"github.com/oguzhanozfe/paxos-arena/internal/tournament"
)

// TestWaitApplied: WaitApplied returns at once for a slot already applied,
// wakes when a follower applies a later command, honours its context, and
// returns ErrStopped after Run has returned; a read after it returns sees
// the slot.
func TestWaitApplied(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		peers := []paxos.NodeID{1, 2}
		c := newCluster(t)
		r1 := c.add(replog.DefaultConfig(1, peers), replog.NewMemStore())
		r2 := c.add(passive(2, peers), replog.NewMemStore())
		waitFor(t, ready(r1), "node 1 to be ready")
		now := r1.Status().Applied
		if got, err := r1.WaitApplied(context.Background(), now-1); err != nil || got != now {
			t.Fatalf("WaitApplied below the applied slot = %d, %v", got, err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		base := r2.Status().Applied
		if _, err := r2.WaitApplied(ctx, base+100); !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("WaitApplied past a deadline = %v", err)
		}
		cancel()
		type out struct {
			slot paxos.Slot
			err  error
		}
		woke := make(chan out, 1)
		after := r2.Status().Applied
		go func() {
			s, err := r2.WaitApplied(context.Background(), after)
			woke <- out{s, err}
		}()
		synctest.Wait()
		select {
		case o := <-woke:
			t.Fatalf("WaitApplied returned before anything was applied: %+v", o)
		default:
		}
		res, err := r1.Submit(context.Background(), createCmd("k1", "t1"))
		if err != nil || res.Code != tournament.OK {
			t.Fatalf("Submit = %+v, %v", res, err)
		}
		o := <-woke
		if o.err != nil || o.slot <= after {
			t.Fatalf("WaitApplied = %+v", o)
		}
		for o.slot < res.Slot {
			s, err := r2.WaitApplied(context.Background(), o.slot)
			if err != nil {
				t.Fatal(err)
			}
			o.slot = s
		}
		var seen bool
		r2.Read(context.Background(), false, func(s *tournament.State) error {
			_, seen = s.Tournament("t1")
			return nil
		})
		if !seen {
			t.Error("a read after WaitApplied returned the command's slot did not see it")
		}
		stopped := make(chan error, 1)
		go func() {
			_, err := r1.WaitApplied(context.Background(), 1<<40)
			stopped <- err
		}()
		synctest.Wait()
		c.stop()
		if err := <-stopped; !errors.Is(err, ErrStopped) {
			t.Errorf("WaitApplied across a stop = %v, want ErrStopped", err)
		}
	})
}
