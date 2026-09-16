package sim

import (
	"time"

	"github.com/oguzhanozfe/paxos-arena/internal/paxos"
	"github.com/oguzhanozfe/paxos-arena/internal/replica"
	"github.com/oguzhanozfe/paxos-arena/internal/replog"
	"github.com/oguzhanozfe/paxos-arena/internal/tournament"
)

// scenario is a named fault schedule layered on the random background
// faults. params adjusts Params before the cluster is built (it may read
// p.Seed to pick variants); setup schedules scripted events; onEvent runs
// after every event while the run lasts; onSubmit runs just before a client
// submits a command to a node and afterSubmit after the Submit call
// succeeded; onCall runs after a node call's messages were sent and before
// its committed slots are applied; onApplied runs after each slot a node
// applies. Hooks may crash nodes.
type scenario struct {
	name        string
	params      func(p *Params)
	setup       func(r *Run)
	onEvent     func(r *Run)
	onSubmit    func(r *Run, c *client, nd *simNode, cmd tournament.Command)
	afterSubmit func(r *Run, c *client, nd *simNode, cmd tournament.Command)
	onCall      func(r *Run, nd *simNode, delivered *replog.Envelope, outs []replog.Envelope)
	onApplied   func(r *Run, nd *simNode, a replica.Applied)
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

// midSettlement is the state of the leader_crash_mid_settlement script: the
// crash point chosen by the seed, and the progress of the Settle command
// through the protocol.
type midSettlement struct {
	variant int
	leader  paxos.NodeID
	key     tournament.IdempotencyKey
	slot    paxos.Slot
	accepts map[paxos.NodeID]bool // peers that received the Accept
	learns  map[paxos.NodeID]bool // peers that received the Learn
	crashed bool
}

// midSettlementVariants is the number of crash points; the seed selects
// one. See crashPoint.
const midSettlementVariants = 12

// crashPoint names the crash point of a variant.
func crashPoint(v int) string {
	switch {
	case v == 0:
		return "after Submit returned"
	case v >= 1 && v <= 4:
		return "after the Accept reached " + string(rune('0'+v)) + " of 4 peers"
	case v == 5:
		return "after a quorum of Accepted, with the Learns lost"
	case v >= 6 && v <= 9:
		return "after the Learn reached " + string(rune('0'+v-5)) + " of 4 peers"
	case v == 10:
		return "after the leader applied the Settle, before the client saw it"
	}
	return "on the first Accept sent"
}

var scenarios = []*scenario{
	{
		name: "leader_crash_mid_settlement",
		params: func(p *Params) {
			p.Nodes = 5
			p.Clients = 1
			p.Tournaments = 1
			p.Entrants = 100
			p.skipScoreP = 0
			p.singleBatch = true
			p.RetryP = 0
			p.PartitionP, p.HealP, p.CrashP, p.TornWriteP = 0, 0, 0, 0
			p.Faults.DropP, p.Faults.DupP = 0, 0
			p.Faults.MinDelay, p.Faults.MaxDelay = time.Millisecond, 5*time.Millisecond
			p.RestartAfter = [2]time.Duration{200 * time.Millisecond, 400 * time.Millisecond}
			if p.Steps < 20000 {
				p.Steps = 20000
			}
		},
		setup: func(r *Run) {
			r.mid = &midSettlement{variant: int(r.p.Seed % midSettlementVariants), accepts: map[paxos.NodeID]bool{}, learns: map[paxos.NodeID]bool{}}
			r.report.Marks["mid_settlement.variant"] = r.mid.variant
			r.tracef("mid-settlement crash point: %s", crashPoint(r.mid.variant))
		},
		onSubmit:    midSettlementArm,
		afterSubmit: midSettlementSubmitted,
		onCall:      midSettlementCall,
		onApplied:   midSettlementApplied,
	},
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
		name: "client_retry_storm",
		params: func(p *Params) {
			p.RetryP = 0.8
			p.Faults.DropP = 0.2
			p.PartitionP, p.HealP, p.CrashP = 0.005, 0.03, 0.002
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
			p.Tournaments, p.Entrants = 4, 8
			p.Faults.DropP, p.Faults.DupP = 0.02, 0.02
			p.PartitionP, p.HealP, p.CrashP = 0, 0, 0
			if p.Steps < 24000 {
				p.Steps = 24000
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
	{
		name: "exclusion_change_at_settle",
		params: func(p *Params) {
			p.exclusionChange = true
			p.skipScoreP = 0
			if p.Entrants < 4 {
				p.Entrants = 4
			}
			p.Faults.DropP = 0.05
			p.PartitionP, p.HealP, p.CrashP = 0.005, 0.03, 0.003
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

// --- leader_crash_mid_settlement ---

// midSettlementArm notes the Settle command's key and the leader it is
// about to be submitted to, the first time the client submits it to a
// leader.
func midSettlementArm(r *Run, c *client, nd *simNode, cmd tournament.Command) {
	m := r.mid
	if m == nil || m.crashed || m.key != "" {
		return
	}
	if _, ok := cmd.Op.(tournament.Settle); !ok {
		return
	}
	if nd.node.Role() != replog.Leader {
		return
	}
	m.key = cmd.Key
	m.leader = nd.id
	r.tracef("mid-settlement: settle %s about to be submitted to leader %d", m.key, nd.id)
}

// midSettlementSubmitted crashes the leader right after Submit returned,
// for variant 0.
func midSettlementSubmitted(r *Run, c *client, nd *simNode, cmd tournament.Command) {
	m := r.mid
	if m == nil || m.crashed || m.variant != 0 || cmd.Key != m.key || nd.id != m.leader {
		return
	}
	r.midCrash(nd, crashPoint(0))
}

// matches reports whether an envelope is the Accept or Learn for the tracked
// Settle command, and records its slot on first sight. The value is decoded
// only while the slot is still unknown.
func (m *midSettlement) matches(r *Run, env replog.Envelope) (accept, learn bool) {
	var v paxos.Value
	var slot paxos.Slot
	switch x := env.Msg.(type) {
	case replog.Accept:
		v, slot, accept = x.Value, x.Slot, true
	case replog.Learn:
		v, slot, learn = x.Value, x.Slot, true
	default:
		return false, false
	}
	if m.slot != 0 {
		if slot == m.slot {
			return accept, learn
		}
		return false, false
	}
	if len(v) == 0 {
		return false, false
	}
	cmd, err := tournament.Decode([]byte(v))
	if err != nil || cmd.Key != m.key {
		return false, false
	}
	m.slot = slot
	r.report.Marks["mid_settlement.settle_slot"] = int(slot)
	r.tracef("mid-settlement: settle occupies slot %d", slot)
	return accept, learn
}

// midSettlementCall watches the leader's outgoing Accepts and Learns for
// the Settle slot and the peers' receipt of them, and crashes the leader at
// the chosen point.
func midSettlementCall(r *Run, nd *simNode, delivered *replog.Envelope, outs []replog.Envelope) {
	m := r.mid
	if m == nil || m.crashed || m.key == "" {
		return
	}
	leader := r.nodeByID(m.leader)
	if leader == nil || !leader.alive {
		return
	}
	if nd.id == m.leader {
		// Outgoing messages of the leader.
		for _, o := range outs {
			accept, learn := m.matches(r, o)
			if accept && m.variant == 11 {
				r.midCrash(nd, crashPoint(11))
				return
			}
			if learn && m.variant == 5 {
				// A quorum accepted: the leader chose the slot and has just
				// queued its Learns. Lose them all and crash the leader before
				// it applies the slot.
				slot := m.slot
				n := r.net.Discard(func(e replog.Envelope) bool {
					l, ok := e.Msg.(replog.Learn)
					return ok && l.Slot == slot
				})
				r.tracef("mid-settlement: discarded %d Learns for slot %d", n, slot)
				r.midCrash(nd, crashPoint(5))
				return
			}
		}
		return
	}
	// A peer received something from the leader.
	if delivered == nil || delivered.From != m.leader {
		return
	}
	accept, learn := m.matches(r, *delivered)
	if accept {
		m.accepts[nd.id] = true
		if k := m.variant; k >= 1 && k <= 4 && len(m.accepts) == k {
			r.midCrash(leader, crashPoint(k))
		}
	}
	if learn {
		m.learns[nd.id] = true
		if j := m.variant - 5; m.variant >= 6 && m.variant <= 9 && len(m.learns) == j {
			r.midCrash(leader, crashPoint(m.variant))
		}
	}
}

// midSettlementApplied crashes the leader right after it applied the Settle
// slot, before the client's next turn can observe the result.
func midSettlementApplied(r *Run, nd *simNode, a replica.Applied) {
	m := r.mid
	if m == nil || m.crashed || m.variant != 10 || nd.id != m.leader || m.slot == 0 || a.Slot != m.slot {
		return
	}
	r.midCrash(nd, crashPoint(10))
}

// midCrash crashes nd once for the scenario and records the point.
func (r *Run) midCrash(nd *simNode, point string) {
	m := r.mid
	if m.crashed {
		return
	}
	m.crashed = true
	r.report.Marks["mid_settlement.crashed"] = 1
	r.tracef("mid-settlement: crashing leader %d %s", nd.id, point)
	r.crash(nd, "scripted: "+point)
}
