package transport

import (
	"sync"

	"github.com/oguzhanozfe/paxos-arena/internal/paxos"
	"github.com/oguzhanozfe/paxos-arena/internal/replog"
)

// Local connects replicas that live in one process. Send hands the envelope
// to the receiver's Deliver callback on the caller's goroutine; Deliver must
// not block, which replica.Runner.Deliver guarantees. Links can be blocked
// and a filter installed, so tests can stage partitions and message loss
// without a virtual clock. Safe for concurrent use.
type Local struct {
	mu      sync.RWMutex
	peers   map[paxos.NodeID]Deliver
	blocked map[[2]paxos.NodeID]bool
	filter  func(replog.Envelope) bool
	stats   Stats
}

// NewLocal returns an empty bus.
func NewLocal() *Local {
	return &Local{peers: make(map[paxos.NodeID]Deliver), blocked: make(map[[2]paxos.NodeID]bool)}
}

// Register attaches the receiver of node id. Registering an id again
// replaces the receiver.
func (l *Local) Register(id paxos.NodeID, deliver Deliver) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.peers[id] = deliver
}

// Unregister detaches node id; envelopes to it are dropped afterwards.
func (l *Local) Unregister(id paxos.NodeID) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.peers, id)
}

// Send delivers env to its receiver unless the link is blocked, the filter
// rejects it or the receiver is not registered.
func (l *Local) Send(env replog.Envelope) {
	l.mu.RLock()
	deliver, ok := l.peers[env.To]
	blocked := l.blocked[[2]paxos.NodeID{env.From, env.To}] || (l.filter != nil && l.filter(env))
	l.mu.RUnlock()
	l.mu.Lock()
	l.stats.Sent++
	switch {
	case blocked:
		l.stats.Blocked++
	case !ok:
		l.stats.Dropped++
	default:
		l.stats.Delivered++
	}
	l.mu.Unlock()
	if !blocked && ok {
		deliver(env)
	}
}

// Block sets or clears a directional partition from one node to another.
func (l *Local) Block(from, to paxos.NodeID, blocked bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	key := [2]paxos.NodeID{from, to}
	if blocked {
		l.blocked[key] = true
	} else {
		delete(l.blocked, key)
	}
}

// Isolate blocks every link into and out of node.
func (l *Local) Isolate(node paxos.NodeID, isolated bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for id := range l.peers {
		if id == node {
			continue
		}
		for _, key := range [][2]paxos.NodeID{{node, id}, {id, node}} {
			if isolated {
				l.blocked[key] = true
			} else {
				delete(l.blocked, key)
			}
		}
	}
}

// SetFilter installs a predicate that blocks every envelope for which it
// returns true, on top of the partition table. A nil filter blocks nothing.
func (l *Local) SetFilter(f func(replog.Envelope) bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.filter = f
}

// Heal removes every partition and the filter.
func (l *Local) Heal() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.blocked = make(map[[2]paxos.NodeID]bool)
	l.filter = nil
}

// Stats returns the counters so far.
func (l *Local) Stats() Stats {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.stats
}
