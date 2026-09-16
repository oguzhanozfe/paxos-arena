// Package transport carries protocol messages between replicas. Network is
// the in-memory fault-injecting transport for the deterministic simulation.
// The HTTP transport for running separate processes is a later milestone.
package transport

import (
	"container/heap"
	"math/rand/v2"
	"time"

	"github.com/oguzhanozfe/paxos-arena/internal/paxos"
	"github.com/oguzhanozfe/paxos-arena/internal/replog"
)

// Deliver is the callback a transport uses to hand an inbound message to a
// replica.
type Deliver func(replog.Envelope)

// Faults are the random faults Network applies to every Send: independent
// loss with probability DropP, duplication with probability DupP, and a
// delivery delay drawn uniformly from [MinDelay, MaxDelay]. Because every
// copy gets its own delay, reordering follows from delay.
type Faults struct {
	// DropP is the probability that a Send loses the envelope.
	DropP float64
	// DupP is the probability that a surviving envelope is queued twice.
	DupP float64
	// MinDelay is the lower bound of the uniform delivery delay.
	MinDelay time.Duration
	// MaxDelay is the upper bound of the uniform delivery delay.
	MaxDelay time.Duration
}

// Stats counts what happened to messages passed to Send.
type Stats struct {
	// Sent counts calls to Send.
	Sent uint64
	// Delivered counts envelopes returned by Next.
	Delivered uint64
	// Dropped counts random losses and purges.
	Dropped uint64
	// Duplicated counts extra copies queued.
	Duplicated uint64
	// Blocked counts envelopes discarded by a partition or a filter.
	Blocked uint64
}

type delivery struct {
	at  time.Duration
	seq uint64
	env replog.Envelope
}

type deliveryHeap []delivery

func (h deliveryHeap) Len() int { return len(h) }
func (h deliveryHeap) Less(i, j int) bool {
	if h[i].at != h[j].at {
		return h[i].at < h[j].at
	}
	return h[i].seq < h[j].seq
}
func (h deliveryHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *deliveryHeap) Push(x any)   { *h = append(*h, x.(delivery)) }
func (h *deliveryHeap) Pop() any {
	old := *h
	n := len(old)
	d := old[n-1]
	*h = old[:n-1]
	return d
}

// Network is the in-memory fault-injecting transport. It is single-threaded:
// the simulator owns the clock, calls Send with the current time and pops
// messages with Next when their delivery time comes. Ordering is by delivery
// time, then by send order, so two runs with the same seed and the same
// sequence of calls deliver identically.
type Network struct {
	rng     *rand.Rand
	faults  Faults
	heap    deliveryHeap
	seq     uint64
	blocked map[[2]paxos.NodeID]bool
	filter  func(replog.Envelope) bool
	stats   Stats
}

// NewNetwork returns an empty network driven by rng with faults f.
func NewNetwork(rng *rand.Rand, f Faults) *Network {
	return &Network{rng: rng, faults: f, blocked: make(map[[2]paxos.NodeID]bool)}
}

// SetFaults replaces the random fault parameters for subsequent sends.
func (n *Network) SetFaults(f Faults) { n.faults = f }

// Faults returns the current random fault parameters.
func (n *Network) Faults() Faults { return n.faults }

// SetFilter installs a predicate that blocks every envelope for which it
// returns true, on top of the partition table. A nil filter blocks nothing.
// Scripted scenarios use it to lose one message type on one link.
func (n *Network) SetFilter(f func(replog.Envelope) bool) { n.filter = f }

// Send applies the partition table, the filter, loss, duplication and delay
// to env and queues the surviving copies for delivery.
func (n *Network) Send(now time.Duration, env replog.Envelope) {
	n.stats.Sent++
	if n.blocked[[2]paxos.NodeID{env.From, env.To}] || (n.filter != nil && n.filter(env)) {
		n.stats.Blocked++
		return
	}
	if n.faults.DropP > 0 && n.rng.Float64() < n.faults.DropP {
		n.stats.Dropped++
		return
	}
	n.push(now, env)
	if n.faults.DupP > 0 && n.rng.Float64() < n.faults.DupP {
		n.stats.Duplicated++
		n.push(now, env)
	}
}

func (n *Network) push(now time.Duration, env replog.Envelope) {
	n.seq++
	heap.Push(&n.heap, delivery{at: now + n.delay(), seq: n.seq, env: env})
}

func (n *Network) delay() time.Duration {
	lo, hi := n.faults.MinDelay, n.faults.MaxDelay
	if lo < 0 {
		lo = 0
	}
	if hi <= lo {
		return lo
	}
	return lo + time.Duration(n.rng.Int64N(int64(hi-lo)+1))
}

// Next pops the earliest queued envelope. ok is false when nothing is
// queued.
func (n *Network) Next() (at time.Duration, env replog.Envelope, ok bool) {
	if len(n.heap) == 0 {
		return 0, replog.Envelope{}, false
	}
	d := heap.Pop(&n.heap).(delivery)
	n.stats.Delivered++
	return d.at, d.env, true
}

// PeekTime returns the delivery time of the earliest queued envelope.
func (n *Network) PeekTime() (time.Duration, bool) {
	if len(n.heap) == 0 {
		return 0, false
	}
	return n.heap[0].at, true
}

// Pending returns the number of queued envelopes.
func (n *Network) Pending() int { return len(n.heap) }

// Block sets or clears a directional partition from one node to another.
// Envelopes already queued are not affected.
func (n *Network) Block(from, to paxos.NodeID, blocked bool) {
	key := [2]paxos.NodeID{from, to}
	if blocked {
		n.blocked[key] = true
	} else {
		delete(n.blocked, key)
	}
}

// Blocked reports whether from -> to is currently partitioned.
func (n *Network) Blocked(from, to paxos.NodeID) bool {
	return n.blocked[[2]paxos.NodeID{from, to}]
}

// Split blocks every link between nodes in different groups, in both
// directions. Links inside a group are untouched.
func (n *Network) Split(groups ...[]paxos.NodeID) {
	for i, g := range groups {
		for j, h := range groups {
			if i == j {
				continue
			}
			for _, a := range g {
				for _, b := range h {
					n.Block(a, b, true)
				}
			}
		}
	}
}

// Heal removes every partition and the filter. Queued envelopes and the
// random fault parameters are unchanged.
func (n *Network) Heal() {
	n.blocked = make(map[[2]paxos.NodeID]bool)
	n.filter = nil
}

// Purge discards every queued envelope addressed to node, as a crash
// discards the messages in the node's socket buffers. They count as Dropped.
func (n *Network) Purge(node paxos.NodeID) {
	kept := n.heap[:0]
	for _, d := range n.heap {
		if d.env.To == node {
			n.stats.Dropped++
			continue
		}
		kept = append(kept, d)
	}
	n.heap = kept
	heap.Init(&n.heap)
}

// Stats returns the counters so far.
func (n *Network) Stats() Stats { return n.stats }
