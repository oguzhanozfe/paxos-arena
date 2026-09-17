package replica

import (
	"context"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/oguzhanozfe/paxos-arena/internal/paxos"
	"github.com/oguzhanozfe/paxos-arena/internal/replog"
)

// recorder is an Observer that keeps everything it is shown.
type recorder struct {
	mu       sync.Mutex
	sent     []replog.Envelope
	received []replog.Envelope
	changes  [][2]Status
	applied  []Applied
}

func (o *recorder) Sent(env replog.Envelope) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.sent = append(o.sent, env)
}

func (o *recorder) Received(env replog.Envelope) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.received = append(o.received, env)
}

func (o *recorder) Changed(prev, cur Status, applied []Applied) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.changes = append(o.changes, [2]Status{prev, cur})
	o.applied = append(o.applied, applied...)
}

// TestObserverSeesMessagesAndChanges: the observer of a leader sees its
// Prepare and the Promise answering it, the Accept, Accepted and Learn of a
// submitted command's slot, its own change to leader, and the slot applied,
// with every change reported against the snapshot before it.
func TestObserverSeesMessagesAndChanges(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		peers := []paxos.NodeID{1, 2}
		obs := &recorder{}
		c := newCluster(t)
		defer c.stop()
		r1 := c.addObserved(replog.DefaultConfig(1, peers), replog.NewMemStore(), obs)
		c.add(passive(2, peers), replog.NewMemStore())
		waitFor(t, ready(r1), "node 1 to lead")
		res, err := r1.Submit(context.Background(), createCmd("k-obs", "t-obs"))
		if err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		c.stop()

		obs.mu.Lock()
		defer obs.mu.Unlock()
		sent := func(pred func(replog.Envelope) bool) bool {
			for _, env := range obs.sent {
				if env.From == 1 && env.To == 2 && pred(env) {
					return true
				}
			}
			return false
		}
		received := func(pred func(replog.Envelope) bool) bool {
			for _, env := range obs.received {
				if env.From == 2 && env.To == 1 && pred(env) {
					return true
				}
			}
			return false
		}
		checks := []struct {
			what string
			ok   bool
		}{
			{"sent prepare", sent(func(e replog.Envelope) bool { _, ok := e.Msg.(replog.Prepare); return ok })},
			{"received promise", received(func(e replog.Envelope) bool { _, ok := e.Msg.(replog.Promise); return ok })},
			{"sent accept for the slot", sent(func(e replog.Envelope) bool { m, ok := e.Msg.(replog.Accept); return ok && m.Slot == res.Slot })},
			{"received accepted for the slot", received(func(e replog.Envelope) bool { m, ok := e.Msg.(replog.Accepted); return ok && m.Slot == res.Slot })},
			{"sent learn for the slot", sent(func(e replog.Envelope) bool { m, ok := e.Msg.(replog.Learn); return ok && m.Slot == res.Slot })},
		}
		for _, ch := range checks {
			if !ch.ok {
				t.Errorf("observer did not see: %s (sent %d, received %d)", ch.what, len(obs.sent), len(obs.received))
			}
		}
		if len(obs.changes) == 0 {
			t.Fatal("observer saw no change")
		}
		became := false
		for i, ch := range obs.changes {
			prev, cur := ch[0], ch[1]
			if prev == cur {
				t.Errorf("change %d reports an unchanged snapshot %+v", i, cur)
			}
			if i > 0 && prev != obs.changes[i-1][1] {
				t.Errorf("change %d starts from %+v, not from the previous change's %+v", i, prev, obs.changes[i-1][1])
			}
			if prev.Role != replog.Leader && cur.Role == replog.Leader {
				became = true
			}
		}
		if !became {
			t.Error("observer did not see node 1 become leader")
		}
		found := false
		for _, a := range obs.applied {
			if a.Slot == res.Slot && a.Key == "k-obs" {
				found = true
			}
		}
		if !found {
			t.Errorf("observer did not see slot %d applied: %+v", res.Slot, obs.applied)
		}
		if last := obs.changes[len(obs.changes)-1][1]; last != r1.Status() {
			t.Errorf("last change %+v differs from the final status %+v", last, r1.Status())
		}
	})
}
