package sim

import (
	"time"

	"github.com/oguzhanozfe/paxos-arena/internal/paxos"
	"github.com/oguzhanozfe/paxos-arena/internal/replog"
	"github.com/oguzhanozfe/paxos-arena/internal/tournament"
)

// The scenarios of the play API (docs/UNITY-INTEGRATION.md section 12).
// Each runs play clients only; the checker evaluates P1-P6 beside S1-S8
// and D1-D6.

// Bounds of the scripted crashes and partitions.
const (
	// playScriptEvents bounds the leader crashes or partitions one scenario
	// stages per run.
	playScriptEvents = 4
	// playScriptSpacing is the least simulated time between two of them.
	playScriptSpacing = 600 * time.Millisecond
)

// playParams sets what every play scenario shares: play clients only, no
// random crashes, partitions or torn writes unless the scenario adds them.
func playParams(p *Params, steps int) {
	p.Clients = 0
	p.PlayClients = 2
	p.PartitionP, p.HealP, p.CrashP, p.TornWriteP = 0, 0, 0, 0
	p.Faults.DropP, p.Faults.DupP = 0.02, 0.02
	p.RestartAfter = [2]time.Duration{200 * time.Millisecond, 500 * time.Millisecond}
	if p.Steps < steps {
		p.Steps = steps
	}
}

// scriptGate counts and spaces the scripted events of one scenario.
type scriptGate struct {
	seen  map[tournament.IdempotencyKey]bool
	count int
	next  time.Duration
}

func newScriptGate() *scriptGate { return &scriptGate{seen: make(map[tournament.IdempotencyKey]bool)} }

// first reports whether cmd reaches a ready leader for the first time and
// the gate allows another scripted event now; it then counts the event.
func (g *scriptGate) first(r *Run, nd *simNode, cmd tournament.Command) bool {
	if g.seen[cmd.Key] {
		return false
	}
	g.seen[cmd.Key] = true
	if g.count >= playScriptEvents || r.clock < g.next || !nd.alive || nd.node.Role() != replog.Leader || !nd.node.Ready() {
		return false
	}
	g.count++
	g.next = r.clock + playScriptSpacing
	return true
}

// crashLeaderAfterProposal crashes leader nd, which has just proposed a
// command. Half the time its queued messages are discarded first, so the
// proposal never leaves it; otherwise the crash comes up to 3 ms later,
// after some of its Accepts may have arrived.
func (r *Run) crashLeaderAfterProposal(nd *simNode, what, mark string) {
	r.report.Marks[mark]++
	if r.rng.IntN(2) == 0 {
		id := nd.id
		n := r.net.Discard(func(e replog.Envelope) bool { return e.From == id })
		r.report.Marks[mark+"_lost"]++
		r.tracef("%s: discarded %d messages of leader %d", what, n, id)
		r.crash(nd, "scripted: "+what+", its proposal lost")
		return
	}
	gen := nd.gen
	r.scheduleScript(r.clock+r.jitter(3*time.Millisecond), func(r *Run) {
		if nd.alive && nd.gen == gen {
			r.crash(nd, "scripted: "+what)
		}
	})
}

// --- duplicate_intents_after_leader_change ---

// dupIntents is the state of the duplicate_intents_after_leader_change
// script.
type dupIntents struct{ gate *scriptGate }

// dupSubmitted crashes the leader that just proposed a move or a claim.
// The client retries the key through the new leader, and resends completed
// deals, moves and claims with their keys; the old proposal may be chosen
// in a later slot beside the retry.
func dupSubmitted(r *Run, _ *client, nd *simNode, cmd tournament.Command) {
	switch cmd.Op.(type) {
	case tournament.PlayMove, tournament.ClaimPayout:
	default:
		return
	}
	if r.dup.gate.first(r, nd, cmd) {
		r.crashLeaderAfterProposal(nd, "leader crash with "+string(cmd.Key)+" in flight", "dup.leader_crashes")
	}
}

// --- deal_during_leader_change ---

// dealChange is the state of the deal_during_leader_change script.
type dealChange struct{ gate *scriptGate }

// dealSubmitted crashes the leader that just proposed a StartRound, before
// it applies it and before any view of the round exists.
func dealSubmitted(r *Run, _ *client, nd *simNode, cmd tournament.Command) {
	if _, ok := cmd.Op.(tournament.StartRound); !ok {
		return
	}
	if r.dealChange.gate.first(r, nd, cmd) {
		r.crashLeaderAfterProposal(nd, "leader crash with deal "+string(cmd.Key)+" proposed", "deal_change.leader_crashes")
	}
}

// --- partition_during_payout_claim ---

// claimPartition is the state of the partition_during_payout_claim script:
// while a split is active, the nodes cut off with the leader, the highest
// commit index any of them had reached at the split, and the slots above it
// that the minority can still commit: those whose value a quorum could hold
// at the leader's ballot counting the minority and the majority acceptors
// that had already accepted it.
type claimPartition struct {
	gate      *scriptGate
	active    bool
	minority  []paxos.NodeID
	limit     paxos.Slot
	decidable map[paxos.Slot]bool
}

// claimSubmitted cuts the leader that just proposed a claim, with one
// follower in a cluster of five, off from the majority, drops the messages
// already in flight across the cut, and heals after 300 to 1200 ms.
func claimSubmitted(r *Run, _ *client, nd *simNode, cmd tournament.Command) {
	cp := r.claimSplit
	if _, ok := cmd.Op.(tournament.ClaimPayout); !ok || cp.active || !cp.gate.first(r, nd, cmd) {
		return
	}
	minority := []paxos.NodeID{nd.id}
	if len(r.nodes) >= 5 {
		if other := r.otherLiveNode(nd); other != nil {
			minority = append(minority, other.id)
		}
	}
	in := make(map[paxos.NodeID]bool, len(minority))
	for _, id := range minority {
		in[id] = true
	}
	var majority []paxos.NodeID
	limit := paxos.Slot(0)
	for _, o := range r.nodes {
		if in[o.id] {
			if o.alive && o.node.CommitIndex() > limit {
				limit = o.node.CommitIndex()
			}
			continue
		}
		majority = append(majority, o.id)
	}
	r.net.Split(minority, majority)
	n := r.net.Discard(func(e replog.Envelope) bool { return in[e.From] != in[e.To] })
	// Accepts still in flight to the majority were dropped, so the leader
	// can only count acceptances the majority had already made.
	decidable := make(map[paxos.Slot]bool)
	b := nd.node.Ballot()
	for s := limit + 1; s <= limit+2*replog.DefaultWindow; s++ {
		held := len(minority)
		for _, id := range majority {
			if o := r.nodeByID(id); o.alive {
				if pv, ok := o.node.Accepted(s); ok && pv.Ballot == b {
					held++
				}
			}
		}
		decidable[s] = held >= r.quorum
	}
	r.partitioned = true
	r.report.Partitions++
	r.report.Marks["claim_partition.splits"]++
	*cp = claimPartition{gate: cp.gate, active: true, minority: minority, limit: limit, decidable: decidable}
	r.tracef("claim partition: minority %v (commit %d) | majority %v while %s is in flight; %d messages dropped", minority, limit, majority, cmd.Key, n)
	hold := 300*time.Millisecond + r.jitter(900*time.Millisecond)
	r.scheduleScript(r.clock+hold, func(r *Run) {
		r.healPartitions()
		r.claimSplit.active = false
	})
}

// claimPartitionEvent fails the run if a node cut off with the leader
// commits a slot that no quorum could have accepted: a proposal the
// minority leader made, or completed, after the split was chosen without a
// majority.
func claimPartitionEvent(r *Run) {
	cp := r.claimSplit
	if cp == nil || !cp.active {
		return
	}
	for _, id := range cp.minority {
		nd := r.nodeByID(id)
		if nd == nil || !nd.alive {
			continue
		}
		r.checker.count("S1")
		for s := cp.limit + 1; s <= nd.node.CommitIndex(); s++ {
			if !cp.decidable[s] {
				r.checker.fail(r, "S1", "node %d committed slot %d while cut off from the majority, which had not accepted it at the split (minority commit %d)", id, s, cp.limit)
				return
			}
		}
		if ci := nd.node.CommitIndex(); ci > cp.limit {
			r.report.Marks["claim_partition.prior_quorum_commits"]++
			cp.limit = ci
		}
	}
}

// --- stale_sequence_replay ---

// stalePartitions installs a random partition, heals it 200 to 800 ms
// later and repeats, so captured requests are replayed on both sides of
// partitions as well as under the random background ones.
func stalePartitions(r *Run) {
	r.randomPartition()
	r.scheduleScript(r.clock+200*time.Millisecond+r.jitter(600*time.Millisecond), func(r *Run) {
		r.healPartitions()
		r.scheduleScript(r.clock+300*time.Millisecond+r.jitter(900*time.Millisecond), stalePartitions)
	})
}

// --- token_expiry_mid_round ---

// tokenExpiry is the state of the token_expiry_mid_round script.
type tokenExpiry struct {
	crashes int
}

// expiryCrashLeader crashes the ready leader while a player's app is
// suspended, so the resumed intent reaches a new leader with an expired
// token.
func expiryCrashLeader(r *Run) {
	l := r.leaderNode()
	if l == nil || !l.node.Ready() {
		r.scheduleScript(r.clock+100*time.Millisecond, expiryCrashLeader)
		return
	}
	r.expiry.crashes++
	r.report.Marks["expiry.leader_crashes"]++
	r.crash(l, "scripted: leader crash while a player's app is suspended")
}

// playScenarios are appended to the scenario list.
var playScenarios = []*scenario{
	{
		name: "duplicate_intents_after_leader_change",
		params: func(p *Params) {
			playParams(p, 12000)
			p.Nodes = 3 + 2*int(p.Seed%2)
			p.playResendP = 0.3
			p.playClaimAgainP = 0.5
		},
		setup:       func(r *Run) { r.dup = &dupIntents{gate: newScriptGate()} },
		afterSubmit: dupSubmitted,
	},
	{
		name: "stale_sequence_replay",
		params: func(p *Params) {
			playParams(p, 10000)
			p.Nodes = 3 + 2*int(p.Seed%2)
			p.playAttackP = 0.3
			p.PartitionP, p.HealP, p.CrashP = 0.005, 0.03, 0.002
			p.Faults.DropP = 0.05
		},
		setup: func(r *Run) { r.scheduleScript(300*time.Millisecond, stalePartitions) },
	},
	{
		name: "partition_during_payout_claim",
		params: func(p *Params) {
			playParams(p, 12000)
			p.Nodes = 3 + 2*int(p.Seed%2)
			p.playRetryElsewhere = true
			p.playClaimAgainP = 0.5
		},
		setup:       func(r *Run) { r.claimSplit = &claimPartition{gate: newScriptGate()} },
		afterSubmit: claimSubmitted,
		onEvent:     claimPartitionEvent,
	},
	{
		name: "token_expiry_mid_round",
		params: func(p *Params) {
			playParams(p, 30000)
			p.Nodes = 3
			p.playEntrants = 2
			p.playSessionTTL = 2 * time.Second
			p.playSuspend = true
			p.singleBatch = true
		},
		setup: func(r *Run) { r.expiry = &tokenExpiry{} },
	},
	{
		name: "deal_during_leader_change",
		params: func(p *Params) {
			playParams(p, 10000)
			p.Nodes = 3 + 2*int(p.Seed%2)
		},
		setup:       func(r *Run) { r.dealChange = &dealChange{gate: newScriptGate()} },
		afterSubmit: dealSubmitted,
	},
}
