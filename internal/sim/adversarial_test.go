package sim

// Adversarial simulation sweeps. Each test runs the full checker under a
// fault schedule that the named scenarios do not use: no lease under heavy
// duplication and long delays, even cluster sizes, clock skew without the
// lease, duplication and reordering alone at extreme rates, and partitions
// that flap faster than an election. A safety violation fails the test. A
// liveness bound reached after Heal is reported as a failure too, because
// after Heal every node is up and every link is open, but it is labelled so
// that it is not mistaken for a safety finding.

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/oguzhanozfe/paxos-arena/internal/replog"
	"github.com/oguzhanozfe/paxos-arena/internal/tournament"
)

func attackSeeds() int {
	if testing.Short() {
		return 4
	}
	return 40
}

// runAttack runs the fault phase, heals and runs the liveness phase. It
// fails the test on a safety violation, reports a liveness failure with the
// parameters needed to reproduce it, and requires the checker to have
// evaluated agreement and the acceptor rules.
func runAttack(t *testing.T, p Params) Report {
	t.Helper()
	r, err := New(p, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	err = r.RunFaults()
	if err == nil {
		r.Heal()
		err = r.RunLiveness()
	}
	rep := r.Report()
	var v *Violation
	var le *LivenessError
	switch {
	case errors.As(err, &v):
		t.Fatalf("SAFETY: %v\nparams: %+v", v, p)
	case errors.As(err, &le):
		t.Errorf("liveness (not a safety finding): %v\nparams: %+v", le, p)
	case err != nil:
		t.Fatalf("%v\nparams: %+v", err, p)
	}
	for _, inv := range []string{"S1", "S3", "S4"} {
		if rep.Checks[inv] == 0 {
			t.Errorf("invariant %s was never evaluated", inv)
		}
	}
	if rep.Unexpected != 0 {
		t.Errorf("%d unexpected results for workflow steps", rep.Unexpected)
	}
	return rep
}

// TestAttackSimHeavyFaultsNoLease: five nodes, no lease, one message in
// five lost, two in five duplicated, delays up to thirty heartbeat
// intervals, frequent partitions, crashes and torn writes.
func TestAttackSimHeavyFaultsNoLease(t *testing.T) {
	for seed := 1; seed <= attackSeeds(); seed++ {
		seed := uint64(seed)
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			t.Parallel()
			p := DefaultParams(seed)
			p.Nodes = 5
			p.NoLease = true
			p.Faults.DropP = 0.2
			p.Faults.DupP = 0.4
			p.Faults.MinDelay = 0
			p.Faults.MaxDelay = 30 * replog.DefaultHeartbeatInterval
			p.PartitionP = 0.05
			p.HealP = 0.05
			p.CrashP = 0.02
			p.TornWriteP = 0.3
			p.RestartAfter = [2]time.Duration{10 * time.Millisecond, 800 * time.Millisecond}
			rep := runAttack(t, p)
			t.Logf("seed %d: %d elections, %d crashes (%d torn), %d partitions, %d dups, %d slots committed",
				seed, rep.Elections, rep.Crashes, rep.TornWrites, rep.Partitions, rep.Net.Duplicated, rep.Commit)
		})
	}
}

// TestAttackSimEvenClusterSizes runs the default schedule on two and four
// nodes, where the quorum is every node and three of four respectively.
func TestAttackSimEvenClusterSizes(t *testing.T) {
	for _, nodes := range []int{2, 4} {
		for seed := 1; seed <= attackSeeds(); seed++ {
			nodes, seed := nodes, uint64(seed)
			t.Run(fmt.Sprintf("nodes=%d/seed=%d", nodes, seed), func(t *testing.T) {
				t.Parallel()
				p := DefaultParams(seed)
				p.Nodes = nodes
				p.NoLease = seed%2 == 0
				rep := runAttack(t, p)
				t.Logf("nodes %d seed %d: %d elections, %d crashes, %d slots committed", nodes, seed, rep.Elections, rep.Crashes, rep.Commit)
			})
		}
	}
}

// TestAttackSimClockSkewNoLease gives every node its own clock rate and
// offset with the lease off, so election timers fire at unrelated moments
// and a node whose clock runs at twice the rate deposes leaders constantly.
func TestAttackSimClockSkewNoLease(t *testing.T) {
	for seed := 1; seed <= attackSeeds(); seed++ {
		seed := uint64(seed)
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			t.Parallel()
			p := DefaultParams(seed)
			p.Nodes = 5
			p.NoLease = true
			p.ClockSkewMax = 10 * replog.DefaultElectionTimeoutMax
			p.Faults.DropP = 0.1
			p.PartitionP = 0.01
			p.CrashP = 0.005
			rep := runAttack(t, p)
			t.Logf("seed %d: %d elections, %d leader changes, %d slots committed", seed, rep.Elections, rep.LeaderChanges, rep.Commit)
		})
	}
}

// TestAttackSimExtremeReorderingOnly duplicates nine messages in ten and
// delays each copy by up to fifty heartbeat intervals, with nothing lost,
// no partitions and no crashes: every stale Promise, Accept, Accepted,
// Learn and HeartbeatAck arrives long after newer ballots exist.
func TestAttackSimExtremeReorderingOnly(t *testing.T) {
	for seed := 1; seed <= attackSeeds(); seed++ {
		seed := uint64(seed)
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			t.Parallel()
			p := DefaultParams(seed)
			p.Nodes = 3 + 2*int(seed%2)
			p.NoLease = seed%4 < 2
			p.Faults.DropP = 0
			p.Faults.DupP = 0.9
			p.Faults.MinDelay = 0
			p.Faults.MaxDelay = 50 * replog.DefaultHeartbeatInterval
			p.PartitionP, p.HealP, p.CrashP, p.TornWriteP = 0, 0, 0, 0
			rep := runAttack(t, p)
			if rep.Net.Duplicated == 0 {
				t.Error("no message was duplicated")
			}
			t.Logf("seed %d: %d elections, %d dups over %d sent, %d slots committed", seed, rep.Elections, rep.Net.Duplicated, rep.Net.Sent, rep.Commit)
		})
	}
}

// TestAttackSimPartitionFlapping installs and removes random partitions,
// including one-directional blocks, faster than an election completes, so
// candidates win with quorums that dissolve at once and several nodes
// believe they lead within one election timeout.
func TestAttackSimPartitionFlapping(t *testing.T) {
	for seed := 1; seed <= attackSeeds(); seed++ {
		seed := uint64(seed)
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			t.Parallel()
			p := DefaultParams(seed)
			p.Nodes = 5
			p.NoLease = seed%2 == 0
			p.PartitionP = 0.3
			p.HealP = 0.3
			p.Faults.DropP = 0.05
			p.Faults.DupP = 0.1
			p.CrashP = 0.01
			rep := runAttack(t, p)
			if rep.Partitions == 0 {
				t.Error("no partition was installed")
			}
			t.Logf("seed %d: %d partitions, %d elections, %d slots committed", seed, rep.Partitions, rep.Elections, rep.Commit)
		})
	}
}

// TestAttackSimRetryStormUnderCrashes combines the retry storm (every
// completed command re-sent up to ten times, a third of them with a mutated
// payload under the same key) with crashes, torn writes, partitions, heavy
// duplication and no lease, so that retries land on leaders that are about
// to lose their slots and on replicas replaying the log from slot 1. S8 and
// D2 must hold: one non-replayed result per key per node, no second fee or
// prize posting.
func TestAttackSimRetryStormUnderCrashes(t *testing.T) {
	for seed := 1; seed <= attackSeeds(); seed++ {
		seed := uint64(seed)
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			t.Parallel()
			p := DefaultParams(seed)
			p.Nodes = 3 + 2*int(seed%2)
			p.NoLease = seed%3 == 0
			p.RetryP = 0.8
			p.Faults.DropP = 0.15
			p.Faults.DupP = 0.3
			p.Faults.MaxDelay = 20 * replog.DefaultHeartbeatInterval
			p.PartitionP = 0.02
			p.HealP = 0.05
			p.CrashP = 0.02
			p.TornWriteP = 0.3
			rep := runAttack(t, p)
			if rep.ExtraRetries == 0 || rep.Mutated == 0 {
				t.Errorf("the storm sent %d extra retries, %d mutated", rep.ExtraRetries, rep.Mutated)
			}
			t.Logf("seed %d: %d extra retries (%d mutated), %d replays, %d key_reused, %d crashes (%d torn)",
				seed, rep.ExtraRetries, rep.Mutated, rep.Replays, rep.KeyReused, rep.Crashes, rep.TornWrites)
		})
	}
}

// TestAttackSimCollidingIdentifiers runs two ordinary one-entrant
// tournaments through the full simulation, with faults, whose identifiers
// differ only around the ':' that separates identifiers in posting keys.
// Every command is replicated and applied exactly once, so S1-S8 hold; the
// invariants that must also hold are D1 and D2 on each tournament's own
// postings.
//
// The test originally used tournament "t" with player "x:p" and tournament
// "t:x" with player "p", which shared every posting key. Identifiers may no
// longer contain ':' (the state machine rejects them, see
// replica.TestAttackPostingKeysCollideAcrossTournaments), so a workflow built
// on them would be refused at create and join rather than exercise the
// ledger. The run keeps the same shape with the nearest legal identifiers,
// "t" with "x-p" and "t-x" with "p".
func TestAttackSimCollidingIdentifiers(t *testing.T) {
	p := DefaultParams(7)
	p.Clients = 2
	p.singleBatch = true
	r, err := New(p, nil)
	if err != nil {
		t.Fatal(err)
	}
	mk := func(tid, pid string) *workflow {
		return &workflow{
			tid: tournament.TournamentID(tid),
			rules: tournament.Rules{
				EntryFee: 100, RakeBps: 0, PrizeBps: []uint32{10000}, MinEntrants: 1, MaxEntrants: 1,
				MaxScore: 1000, MinAge: 18, TieBreak: tournament.EarliestSubmission,
				Exclusions: tournament.Exclusions{Version: listVersionAtCreate, Jurisdictions: []string{creationExcluded}},
			},
			players:  []tournament.Player{{ID: tournament.PlayerID(pid), Jurisdiction: "TR", Age: 30}},
			scores:   []int64{500},
			skip:     []bool{false},
			joined:   []bool{false},
			settleEx: tournament.Exclusions{Version: listVersionAtSettle, Jurisdictions: []string{creationExcluded}},
		}
	}
	r.clients[0].wf, r.clients[0].remaining = mk("t", "x-p"), 0
	r.clients[1].wf, r.clients[1].remaining = mk("t-x", "p"), 0
	err = r.RunFaults()
	if err == nil {
		r.Heal()
		err = r.RunLiveness()
	}
	var v *Violation
	if errors.As(err, &v) {
		t.Fatalf("SAFETY: %v", v)
	}
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if rep := r.Report(); rep.Settled != 2 {
		t.Fatalf("setup: %d tournaments settled, want 2", rep.Settled)
	}
}
