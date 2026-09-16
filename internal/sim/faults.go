package sim

import (
	"fmt"
	"time"

	"github.com/oguzhanozfe/paxos-arena/internal/paxos"
)

// randomFaults draws the per-tick fault probabilities.
func (r *Run) randomFaults() {
	p := r.p
	if p.PartitionP > 0 && r.rng.Float64() < p.PartitionP {
		r.randomPartition()
	}
	if p.HealP > 0 && r.partitioned && r.rng.Float64() < p.HealP {
		r.healPartitions()
	}
	if p.CrashP > 0 && r.rng.Float64() < p.CrashP {
		if alive := r.aliveNodes(); len(alive) > 0 {
			r.crashMaybeTorn(alive[r.rng.IntN(len(alive))])
		}
	}
}

// healPartitions removes every partition but keeps a scenario's filter.
func (r *Run) healPartitions() {
	r.net.Heal()
	r.net.SetFilter(r.filter)
	r.partitioned = false
	r.tracef("partitions healed")
}

// randomPartition installs either a random two-group split (70%) or a
// single directional block (30%), so that non-transitive connectivity is
// exercised as well.
func (r *Run) randomPartition() {
	ids := make([]paxos.NodeID, len(r.nodes))
	for i, nd := range r.nodes {
		ids[i] = nd.id
	}
	if len(ids) < 2 {
		return
	}
	r.rng.Shuffle(len(ids), func(i, j int) { ids[i], ids[j] = ids[j], ids[i] })
	if r.rng.Float64() < 0.7 {
		k := 1 + r.rng.IntN(len(ids)-1)
		r.net.Split(ids[:k], ids[k:])
		r.tracef("partition %v | %v", ids[:k], ids[k:])
	} else {
		r.net.Block(ids[0], ids[1], true)
		r.tracef("block %d->%d", ids[0], ids[1])
	}
	r.partitioned = true
	r.report.Partitions++
}

// crashMaybeTorn crashes nd now, or with probability TornWriteP arms a torn
// write: the node's next Save fails and the node crashes then, with a hard
// crash 50ms later in case it never saves.
func (r *Run) crashMaybeTorn(nd *simNode) {
	if r.p.TornWriteP > 0 && r.rng.Float64() < r.p.TornWriteP {
		nd.store.armed = true
		r.tracef("node %d: torn write armed", nd.id)
		r.schedule(event{at: r.clock + 50*time.Millisecond, kind: evHardCrash, node: r.index(nd), gen: nd.gen})
		return
	}
	r.crash(nd, "crash")
}

// crash discards the node's volatile state and in-flight inbound messages,
// keeps its store, and schedules a restart.
func (r *Run) crash(nd *simNode, reason string) {
	if !nd.alive {
		return
	}
	d, err := nd.store.Load()
	if err != nil {
		panic(fmt.Sprintf("sim: node %d store Load: %v", nd.id, err))
	}
	r.checker.observeCrash(nd, d)
	r.tracef("node %d crashed (%s): role %s commit %d applied %d", nd.id, reason, nd.node.Role(), nd.node.CommitIndex(), nd.applied)
	nd.alive = false
	nd.node = nil
	nd.core = nil
	nd.gen++
	nd.store.armed = false
	r.net.Purge(nd.id)
	r.report.Crashes++
	r.dropReadsFor(nd.id)
	for _, c := range r.clients {
		if c.target == nd.id {
			c.target = 0
		}
	}
	for _, c := range r.playClients {
		if c.target == nd.id {
			c.target = 0
		}
	}
	delay := r.p.RestartAfter[0] + r.jitter(r.p.RestartAfter[1]-r.p.RestartAfter[0])
	r.schedule(event{at: r.clock + delay, kind: evRestart, node: r.index(nd), gen: nd.gen})
}

// restart rebuilds the node from its store and replays the chosen log into a
// fresh state machine.
func (r *Run) restart(nd *simNode) {
	if nd.alive {
		return
	}
	if err := r.start(nd); err != nil {
		panic(err)
	}
	r.checker.observeRestart(r, nd)
	r.tracef("node %d restarted: promised %v commit %d", nd.id, nd.node.Promised(), nd.node.CommitIndex())
	r.applyCommitted(nd)
}

// Heal stops every fault: no more random faults or scripted steps, no loss
// or duplication, delays within healMaxDelay, partitions and filters
// removed, clock rates back to one, and every crashed node restarted.
func (r *Run) Heal() {
	r.healWith(nil)
}

// HealCore heals only the given majority of nodes and freezes every fault
// outside it: links between the core and the rest stay blocked, crashed
// nodes outside the core stay down, and clients use core nodes only. It is
// the liveness regime under which random healing cannot hide a stuck
// protocol.
func (r *Run) HealCore(core []paxos.NodeID) {
	set := make(map[paxos.NodeID]bool, len(core))
	for _, id := range core {
		set[id] = true
	}
	r.healWith(set)
}

func (r *Run) healWith(core map[paxos.NodeID]bool) {
	r.faultsOn = false
	r.healed = true
	r.core = core
	r.filter = nil
	r.net.Heal()
	r.partitioned = false
	f := r.p.Faults
	f.DropP, f.DupP = 0, 0
	if f.MaxDelay > healMaxDelay {
		f.MaxDelay = healMaxDelay
	}
	if f.MinDelay > f.MaxDelay {
		f.MinDelay = f.MaxDelay
	}
	r.net.SetFaults(f)
	if core != nil {
		for _, a := range r.nodes {
			for _, b := range r.nodes {
				if core[a.id] != core[b.id] {
					r.net.Block(a.id, b.id, true)
				}
			}
		}
	}
	for _, nd := range r.nodes {
		if core != nil && !core[nd.id] {
			nd.frozen = true
			continue
		}
		nd.store.armed = false
		if nd.rate != 1 {
			// Keep the node's clock continuous while returning to rate 1.
			nd.offset = nd.now(r.clock) - r.clock
			nd.rate = 1
		}
		if !nd.alive {
			r.restart(nd)
		}
	}
	for _, c := range r.clients {
		c.target = 0
	}
	for _, c := range r.playClients {
		c.target = 0
	}
	r.tracef("heal (core %v)", core)
}
