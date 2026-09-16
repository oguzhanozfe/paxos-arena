package replog

import (
	"math/rand/v2"
	"testing"
	"time"

	"github.com/oguzhanozfe/paxos-arena/internal/paxos"
)

// cluster is a minimal deterministic harness for unit tests: nodes share one
// seeded generator, messages are delivered in FIFO order with no delay, and
// a drop filter stands in for the network faults the simulator injects.
type cluster struct {
	t      *testing.T
	ids    []paxos.NodeID
	nodes  map[paxos.NodeID]*Node
	stores map[paxos.NodeID]*MemStore
	cfgs   map[paxos.NodeID]Config
	queue  []Envelope
	now    time.Duration
	rng    *rand.Rand
	drop   func(Envelope) bool
	events map[paxos.NodeID][]Event
}

const tickStep = 10 * time.Millisecond

func newCluster(t *testing.T, n int, mod func(*Config)) *cluster {
	t.Helper()
	c := &cluster{
		t:      t,
		nodes:  make(map[paxos.NodeID]*Node),
		stores: make(map[paxos.NodeID]*MemStore),
		cfgs:   make(map[paxos.NodeID]Config),
		rng:    rand.New(rand.NewPCG(1, 0)),
		events: make(map[paxos.NodeID][]Event),
	}
	for i := 1; i <= n; i++ {
		c.ids = append(c.ids, paxos.NodeID(i))
	}
	for _, id := range c.ids {
		cfg := DefaultConfig(id, c.ids)
		if mod != nil {
			mod(&cfg)
		}
		c.cfgs[id] = cfg
		c.stores[id] = NewMemStore()
		c.restart(id)
	}
	return c
}

// restart builds (or rebuilds) node id from its store.
func (c *cluster) restart(id paxos.NodeID) *Node {
	c.t.Helper()
	n, err := New(c.cfgs[id], c.stores[id], c.rng)
	if err != nil {
		c.t.Fatalf("New(%d): %v", id, err)
	}
	c.nodes[id] = n
	return n
}

// crash removes node id; messages to it are dropped until restart.
func (c *cluster) crash(id paxos.NodeID) {
	delete(c.nodes, id)
}

func (c *cluster) enqueue(id paxos.NodeID, outs []Envelope) {
	for _, e := range outs {
		if e.From != id {
			c.t.Fatalf("node %d returned an envelope from %d", id, e.From)
		}
		if e.To == id {
			c.t.Fatalf("node %d returned an envelope addressed to itself: %T", id, e.Msg)
		}
	}
	c.queue = append(c.queue, outs...)
	if n := c.nodes[id]; n != nil {
		c.events[id] = append(c.events[id], n.Events()...)
	}
}

// deliverAll delivers queued messages in order until the queue is empty.
func (c *cluster) deliverAll() {
	for len(c.queue) > 0 {
		env := c.queue[0]
		c.queue = c.queue[1:]
		if c.drop != nil && c.drop(env) {
			continue
		}
		n, ok := c.nodes[env.To]
		if !ok {
			continue
		}
		c.enqueue(env.To, n.Step(c.now, env))
	}
}

// tickAll advances the clock by one step and ticks every live node.
func (c *cluster) tickAll() {
	c.now += tickStep
	for _, id := range c.ids {
		if n, ok := c.nodes[id]; ok {
			c.enqueue(id, n.Tick(c.now))
		}
	}
}

// runUntil ticks and delivers until cond holds or limit elapses.
func (c *cluster) runUntil(limit time.Duration, cond func() bool) bool {
	deadline := c.now + limit
	for c.now < deadline {
		c.tickAll()
		c.deliverAll()
		if cond() {
			return true
		}
	}
	return cond()
}

// leader returns the live node whose role is Leader, if exactly one exists.
func (c *cluster) leader() (*Node, bool) {
	var l *Node
	for _, id := range c.ids {
		if n, ok := c.nodes[id]; ok && n.Role() == Leader {
			if l != nil {
				return nil, false
			}
			l = n
		}
	}
	return l, l != nil
}

// electLeader runs until one node is a Ready leader and returns it.
func (c *cluster) electLeader() *Node {
	c.t.Helper()
	var l *Node
	ok := c.runUntil(5*time.Second, func() bool {
		n, one := c.leader()
		if one && n.Ready() {
			l = n
			return true
		}
		return false
	})
	if !ok {
		c.t.Fatalf("no ready leader after 5s of simulated time")
	}
	return l
}

// propose submits v to the leader and delivers everything.
func (c *cluster) propose(l *Node, v paxos.Value) {
	c.t.Helper()
	outs, err := l.Propose(c.now, v)
	if err != nil {
		c.t.Fatalf("Propose(%q): %v", v, err)
	}
	c.enqueue(l.Self(), outs)
	c.deliverAll()
}

// maxChosen returns the highest chosen slot on any live node.
func (c *cluster) maxChosen() paxos.Slot {
	var m paxos.Slot
	for _, n := range c.nodes {
		for s := n.CommitIndex() + 1; ; s++ {
			if _, ok := n.Chosen(s); !ok {
				if s-1 > m {
					m = s - 1
				}
				break
			}
		}
		if n.CommitIndex() > m {
			m = n.CommitIndex()
		}
	}
	return m
}
