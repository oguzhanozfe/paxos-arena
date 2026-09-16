package sim

import (
	"time"

	"github.com/oguzhanozfe/paxos-arena/internal/paxos"
	"github.com/oguzhanozfe/paxos-arena/internal/replog"
)

// scenario is a named fault schedule layered on the random background
// faults. params adjusts Params before the cluster is built (it may read
// p.Seed to pick variants); setup schedules scripted events; onEvent runs
// after every event while the run lasts.
type scenario struct {
	name    string
	params  func(p *Params)
	setup   func(r *Run)
	onEvent func(r *Run)
}

// lateLearner is the state of the late_learner script.
type lateLearner struct {
	node   paxos.NodeID
	start  paxos.Slot
	target paxos.Slot
	active bool
}

// lateLearnerSlots is how many slots the late learner misses.
const lateLearnerSlots = 500

var scenarios = []*scenario{
	{
		name: "dueling_leaders",
		params: func(p *Params) {
			if p.Seed%2 == 0 {
				p.Nodes = 3
			} else {
				p.Nodes = 5
			}
			p.NoLease = (p.Seed/2)%2 == 1
			p.PartitionP, p.HealP, p.CrashP = 0, 0, 0
			p.Faults.DropP, p.Faults.DupP = 0.05, 0.02
			if p.Steps < 8000 {
				p.Steps = 8000
			}
		},
		setup: func(r *Run) { r.scheduleScript(400*time.Millisecond, duel) },
	},
	{
		name: "partition_and_heal",
		params: func(p *Params) {
			p.Nodes = 5
			p.PartitionP, p.HealP, p.CrashP = 0, 0, 0
			p.Faults.DropP, p.Faults.DupP = 0.05, 0.02
			if p.Steps < 8000 {
				p.Steps = 8000
			}
		},
		setup: func(r *Run) { r.scheduleScript(400*time.Millisecond, splitAndHeal) },
	},
	{
		name: "duplicated_and_reordered_messages",
		params: func(p *Params) {
			p.Faults.DropP = 0.1
			p.Faults.DupP = 0.3
			p.Faults.MinDelay = time.Millisecond
			p.Faults.MaxDelay = 20 * replog.DefaultHeartbeatInterval
			p.PartitionP, p.HealP, p.CrashP = 0, 0, 0
		},
	},
	{
		name: "crash_restart_storm",
		params: func(p *Params) {
			p.CrashP = 0.03
			p.TornWriteP = 0.2
			p.RestartAfter = [2]time.Duration{50 * time.Millisecond, 400 * time.Millisecond}
			p.PartitionP, p.HealP = 0.003, 0.03
			p.Faults.DropP = 0.05
		},
	},
	{
		name: "clock_skew",
		params: func(p *Params) {
			if p.Seed%2 == 0 {
				p.Nodes = 3
			} else {
				p.Nodes = 5
			}
			p.ClockSkewMax = 10 * replog.DefaultElectionTimeoutMax
			p.NoLease = false
			p.Faults.DropP = 0.05
			p.PartitionP, p.HealP, p.CrashP = 0.005, 0.03, 0.002
		},
	},
	{
		name: "late_learner",
		params: func(p *Params) {
			p.Nodes = 5
			p.Clients = 5
			p.Tournaments, p.Entrants = 10, 12
			p.Faults.DropP, p.Faults.DupP = 0.02, 0.02
			p.PartitionP, p.HealP, p.CrashP = 0, 0, 0
			if p.Steps < 16000 {
				p.Steps = 16000
			}
		},
		setup: func(r *Run) { r.scheduleScript(300*time.Millisecond, blockLateLearner) },
		onEvent: func(r *Run) {
			l := r.late
			if l == nil || !l.active {
				return
			}
			if mc := r.checker.maxCommit(); mc >= l.target {
				l.active = false
				r.filter = nil
				r.net.SetFilter(nil)
				r.report.Marks["late_learner.unblocked_at_commit"] = int(mc)
				r.tracef("late learner %d reconnected at commit %d", l.node, mc)
			}
		},
	},
}

// Scenarios lists the scripted scenario names.
func Scenarios() []string {
	out := make([]string, 0, len(scenarios))
	for _, s := range scenarios {
		out = append(out, s.name)
	}
	return out
}

func findScenario(name string) *scenario {
	for _, s := range scenarios {
		if s.name == name {
			return s
		}
	}
	return nil
}

// otherNodes returns the live nodes other than l in random order.
func (r *Run) otherNodes(l *simNode) []*simNode {
	var out []*simNode
	for _, nd := range r.nodes {
		if nd.alive && nd != l {
			out = append(out, nd)
		}
	}
	r.rng.Shuffle(len(out), func(i, j int) { out[i], out[j] = out[j], out[i] })
	return out
}

// duel blocks the leader's messages to two other nodes, so that their
// election fires while the leader still believes it leads and keeps taking
// client commands, then unblocks and repeats.
func duel(r *Run) {
	l := r.leaderNode()
	if l == nil {
		r.scheduleScript(r.clock+100*time.Millisecond, duel)
		return
	}
	others := r.otherNodes(l)
	if len(others) < 2 {
		r.scheduleScript(r.clock+100*time.Millisecond, duel)
		return
	}
	b, c := others[0], others[1]
	r.net.Block(l.id, b.id, true)
	r.net.Block(l.id, c.id, true)
	r.report.Partitions++
	r.tracef("duel: block %d->%d and %d->%d", l.id, b.id, l.id, c.id)
	hold := 300*time.Millisecond + r.jitter(400*time.Millisecond)
	r.scheduleScript(r.clock+hold, func(r *Run) {
		r.net.Block(l.id, b.id, false)
		r.net.Block(l.id, c.id, false)
		r.tracef("duel: unblock %d", l.id)
		r.scheduleScript(r.clock+300*time.Millisecond+r.jitter(300*time.Millisecond), duel)
	})
}

// splitAndHeal alternates a majority/minority split that isolates the
// leader with one follower and a non-transitive split in which two nodes
// cannot see each other while everyone else sees both, healing after a
// random interval.
func splitAndHeal(r *Run) {
	l := r.leaderNode()
	if l == nil {
		r.scheduleScript(r.clock+100*time.Millisecond, splitAndHeal)
		return
	}
	others := r.otherNodes(l)
	if len(others) < 3 {
		r.scheduleScript(r.clock+100*time.Millisecond, splitAndHeal)
		return
	}
	if r.rng.IntN(2) == 0 {
		minority := []paxos.NodeID{l.id, others[0].id}
		var majority []paxos.NodeID
		for _, nd := range others[1:] {
			majority = append(majority, nd.id)
		}
		r.net.Split(minority, majority)
		r.tracef("split: minority %v | majority %v", minority, majority)
	} else {
		b, c := others[0], others[1]
		r.net.Block(b.id, c.id, true)
		r.net.Block(c.id, b.id, true)
		r.tracef("split: non-transitive, %d and %d cannot see each other", b.id, c.id)
	}
	r.partitioned = true
	r.report.Partitions++
	hold := 300*time.Millisecond + r.jitter(1200*time.Millisecond)
	r.scheduleScript(r.clock+hold, func(r *Run) {
		r.healPartitions()
		r.scheduleScript(r.clock+200*time.Millisecond+r.jitter(400*time.Millisecond), splitAndHeal)
	})
}

// blockLateLearner picks one follower and drops every Learn addressed to it
// until lateLearnerSlots more slots are committed.
func blockLateLearner(r *Run) {
	l := r.leaderNode()
	if l == nil {
		r.scheduleScript(r.clock+100*time.Millisecond, blockLateLearner)
		return
	}
	others := r.otherNodes(l)
	if len(others) == 0 {
		r.scheduleScript(r.clock+100*time.Millisecond, blockLateLearner)
		return
	}
	x := others[0]
	start := r.checker.maxCommit()
	r.late = &lateLearner{node: x.id, start: start, target: start + lateLearnerSlots, active: true}
	r.filter = func(e replog.Envelope) bool {
		_, isLearn := e.Msg.(replog.Learn)
		return isLearn && e.To == x.id
	}
	r.net.SetFilter(r.filter)
	r.report.Marks["late_learner.node"] = int(x.id)
	r.report.Marks["late_learner.blocked_at_commit"] = int(start)
	r.tracef("late learner %d loses every Learn from commit %d", x.id, start)
}
