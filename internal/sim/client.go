package sim

import (
	"errors"
	"fmt"
	"time"

	"github.com/oguzhanozfe/paxos-arena/internal/paxos"
	"github.com/oguzhanozfe/paxos-arena/internal/replog"
)

// client submits opaque commands one at a time to the node it believes is
// the leader, follows ErrNotLeader hints, and moves on when it observes the
// command chosen. In this milestone the observation is omniscient (the
// checker's record of chosen values); the tournament clients of the next
// milestone learn results from the state machine.
type client struct {
	id      int
	batch   int // commands per batch
	issued  int // commands generated so far
	todo    []paxos.Value
	cur     paxos.Value
	target  paxos.NodeID
	done    int
	submits int
}

// readKey identifies one pending read-index barrier. gen ties it to one
// incarnation of the node so a restart cannot complete an old read.
type readKey struct {
	node paxos.NodeID
	gen  uint64
	seq  uint64
}

// pendingRead records what the checker needs to verify S7 when the barrier
// completes: the highest commit index observed anywhere when the read was
// issued.
type pendingRead struct {
	client    int
	maxCommit paxos.Slot
	issued    time.Duration
}

func newClient(id, n int) *client {
	c := &client{id: id, batch: n}
	c.nextBatch()
	return c
}

// nextBatch queues one more batch of distinct commands.
func (c *client) nextBatch() {
	for i := 0; i < c.batch; i++ {
		c.todo = append(c.todo, paxos.Value(fmt.Sprintf("c%d/%d", c.id, c.issued)))
		c.issued++
	}
}

// clientTurn is one client action: advance past chosen commands, submit or
// re-submit the current one, and sometimes issue a consistent read. While
// faults are on, a client that finishes a batch starts another, so the fault
// phase stays loaded however long it runs; after Heal it finishes what it
// has and stops.
func (r *Run) clientTurn(c *client) {
	for attempt := 0; attempt < 2; attempt++ {
		if c.cur == nil && len(c.todo) == 0 && r.faultsOn {
			c.nextBatch()
		}
		if c.cur == nil && len(c.todo) > 0 {
			c.cur = c.todo[0]
			c.todo = c.todo[1:]
		}
		if c.cur == nil {
			break
		}
		if _, chosen := r.checker.chosenValues[string(c.cur)]; chosen {
			r.tracef("client %d: %q chosen", c.id, c.cur)
			c.done++
			c.cur = nil
			r.report.Completed++
			continue
		}
		r.submit(c)
		break
	}
	if r.rng.Float64() < readP {
		if nd := r.pickTarget(c); nd != nil {
			r.issueRead(nd, c.id)
		}
	}
}

// submit proposes the client's current command to its target node.
func (r *Run) submit(c *client) {
	nd := r.pickTarget(c)
	if nd == nil {
		return
	}
	v := c.cur
	var err error
	r.callNode(nd, nil, func(now time.Duration) []replog.Envelope {
		outs, e := nd.node.Propose(now, v)
		err = e
		return outs
	})
	c.submits++
	r.report.Submits++
	if err != nil {
		var nl replog.ErrNotLeader
		if errors.As(err, &nl) {
			c.target = nl.Leader
		} else {
			c.target = 0
		}
		r.tracef("client %d: propose %q to node %d: %v", c.id, v, nd.id, err)
		return
	}
	r.tracef("client %d: proposed %q to node %d", c.id, v, nd.id)
}

// pickTarget returns the client's current target if it is live and in the
// core, otherwise a random live core node, which becomes the target.
func (r *Run) pickTarget(c *client) *simNode {
	if c.target != 0 {
		if nd := r.nodeByID(c.target); nd != nil && nd.alive && r.inCore(nd) {
			return nd
		}
		c.target = 0
	}
	alive := r.aliveNodes()
	if len(alive) == 0 {
		return nil
	}
	nd := alive[r.rng.IntN(len(alive))]
	c.target = nd.id
	return nd
}

// issueRead starts a read-index barrier on nd for client (or -1 for the
// simulator's own read at a leadership change) and registers it so that
// ReadReady can be checked against the commit index reached before the call.
func (r *Run) issueRead(nd *simNode, clientID int) {
	if !nd.alive {
		return
	}
	maxCommit := r.checker.maxCommit()
	gen := nd.gen
	r.checker.beforeCall(nd, nil)
	seq, outs, err := nd.node.ReadIndex(nd.now(r.clock))
	if err == nil {
		r.report.Reads++
		r.reads[readKey{node: nd.id, gen: gen, seq: seq}] = pendingRead{client: clientID, maxCommit: maxCommit, issued: r.clock}
		r.tracef("client %d: read %d issued on node %d (commit %d)", clientID, seq, nd.id, maxCommit)
	} else {
		r.tracef("client %d: read on node %d refused: %v", clientID, nd.id, err)
	}
	r.afterCall(nd, nil, outs)
}

// dropReadsFor forgets every pending read on a node that crashed.
func (r *Run) dropReadsFor(id paxos.NodeID) {
	for k := range r.reads {
		if k.node == id {
			delete(r.reads, k)
		}
	}
}
