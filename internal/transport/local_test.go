package transport

import (
	"sync"
	"testing"

	"github.com/oguzhanozfe/paxos-arena/internal/paxos"
	"github.com/oguzhanozfe/paxos-arena/internal/replog"
)

func TestLocalDeliversBlocksAndFilters(t *testing.T) {
	bus := NewLocal()
	var mu sync.Mutex
	got := map[paxos.NodeID][]replog.Envelope{}
	for _, id := range []paxos.NodeID{1, 2} {
		id := id
		bus.Register(id, func(e replog.Envelope) {
			mu.Lock()
			got[id] = append(got[id], e)
			mu.Unlock()
		})
	}
	hb := replog.Heartbeat{Ballot: paxos.Ballot{Round: 1, Node: 1}}
	bus.Send(replog.Envelope{From: 1, To: 2, Msg: hb})
	bus.Send(replog.Envelope{From: 1, To: 3, Msg: hb}) // unregistered
	bus.Block(1, 2, true)
	bus.Send(replog.Envelope{From: 1, To: 2, Msg: hb}) // blocked
	bus.Send(replog.Envelope{From: 2, To: 1, Msg: hb}) // other direction open
	bus.Block(1, 2, false)
	bus.SetFilter(func(e replog.Envelope) bool { _, ok := e.Msg.(replog.Learn); return ok })
	bus.Send(replog.Envelope{From: 1, To: 2, Msg: replog.Learn{Slot: 1}}) // filtered
	bus.Send(replog.Envelope{From: 1, To: 2, Msg: hb})
	bus.Isolate(2, true)
	bus.Send(replog.Envelope{From: 1, To: 2, Msg: hb})
	bus.Send(replog.Envelope{From: 2, To: 1, Msg: hb})
	bus.Heal()
	bus.Send(replog.Envelope{From: 1, To: 2, Msg: replog.Learn{Slot: 2}})
	bus.Unregister(1)
	bus.Send(replog.Envelope{From: 2, To: 1, Msg: hb})
	mu.Lock()
	defer mu.Unlock()
	if len(got[2]) != 3 {
		t.Errorf("node 2 received %d envelopes, want 3: %+v", len(got[2]), got[2])
	}
	if len(got[1]) != 1 {
		t.Errorf("node 1 received %d envelopes, want 1", len(got[1]))
	}
	st := bus.Stats()
	if st.Sent != 10 || st.Delivered != 4 || st.Blocked != 4 || st.Dropped != 2 {
		t.Errorf("Stats = %+v", st)
	}
}
