// Package sim runs a whole cluster of replicas in one goroutine under a
// virtual clock, a seeded fault schedule and an invariant checker.
//
// Everything is deterministic in the seed: the same Params produce the same
// events, the same messages and the same trace, so a failing seed is a
// reproducible test case. Each replica is a replica.Core: the replicated
// log feeding the tournament state machine. Clients run tournament
// workflows (create, join, submit scores, close, settle) against the
// cluster and learn results from the state machine, as the HTTP API does.
package sim

import (
	"errors"
	"fmt"
	"time"

	"github.com/oguzhanozfe/paxos-arena/internal/paxos"
	"github.com/oguzhanozfe/paxos-arena/internal/replog"
	"github.com/oguzhanozfe/paxos-arena/internal/transport"
)

// Scheduling constants of the simulator, in simulated time.
const (
	// tickInterval is how often each node's Tick runs.
	tickInterval = 10 * time.Millisecond
	// clientInterval is the base interval between a client's actions; a
	// random jitter up to itself is added each time.
	clientInterval = 20 * time.Millisecond
	// retryInterval is how long a client waits for a result before it
	// re-submits the same command with the same key.
	retryInterval = 100 * time.Millisecond
	// faultInterval is how often the random fault probabilities are drawn.
	faultInterval = 10 * time.Millisecond
	// applyBatch bounds how many chosen entries a node applies per event, so
	// that a crash can land between the commit index and the applied index.
	applyBatch = 16
	// readP is the probability that a client issues a consistent read on
	// one of its turns.
	readP = 0.5
	// healMaxDelay is the message delay bound in liveness mode.
	healMaxDelay = 10 * time.Millisecond
	// maxExtraRetries bounds the duplicates a client sends per command in
	// the retry storm.
	maxExtraRetries = 10
	// mutateP is the probability that an extra retry carries a mutated
	// payload under the same key.
	mutateP = 0.3
)

// Params configures one simulation run. Probabilities are per faultInterval
// for PartitionP, HealP and CrashP, and per Send for Faults.
type Params struct {
	// Seed drives every random choice of the run.
	Seed uint64
	// Nodes is the cluster size, 3 or 5 in the scenarios; any size >= 1
	// works.
	Nodes int
	// Steps is the number of events processed in fault mode.
	Steps int
	// LivenessSteps bounds the events processed after Heal before the run
	// is declared stuck.
	LivenessSteps int
	// Faults are the random message faults of the network.
	Faults transport.Faults
	// PartitionP is the per-tick probability of installing a random
	// partition.
	PartitionP float64
	// HealP is the per-tick probability of removing every partition.
	HealP float64
	// CrashP is the per-tick probability of crashing one random live node.
	CrashP float64
	// RestartAfter bounds the random delay before a crashed node restarts.
	RestartAfter [2]time.Duration
	// ClockSkewMax bounds the per-node offset added to the virtual clock.
	// Each node sees one consistent clock: the offset applies to every call
	// into the node, not only to Tick. When it is non-zero, each node's clock
	// also runs at a random rate between 0.5 and 2 in fault mode.
	ClockSkewMax time.Duration
	// TornWriteP is the probability that a crash is a torn write: the node's
	// next Save call fails, its volatile update is lost and the store keeps
	// the previous durable record.
	TornWriteP float64
	// Clients is the number of concurrent clients.
	Clients int
	// Tournaments is the number of tournaments each client runs per batch.
	// Clients start a new batch whenever they finish one while faults are
	// on, and finish the current batch after Heal.
	Tournaments int
	// Entrants is the number of entrants per tournament.
	Entrants int
	// RetryP is the probability that a client re-sends a completed command
	// with the same key, up to maxExtraRetries times at random intervals,
	// sometimes with a mutated payload.
	RetryP float64
	// Scenario names a scripted fault schedule from Scenarios, or "" for the
	// purely random schedule.
	Scenario string
	// NoLease disables the leader lease on every node.
	NoLease bool

	// skipScoreP is the probability that an entrant never submits a score,
	// so that some tournaments void at Close.
	skipScoreP float64
	// exclusionChange makes every workflow include one entrant on the
	// creation-time list (rejected at Join) and one whose jurisdiction is
	// added to the settlement-time list (withheld at Settle).
	exclusionChange bool
	// singleBatch stops every client after its first batch even while
	// faults are on, for scenarios that stage one tournament.
	singleBatch bool
	// unsafe switches protocol rules off; set only by tests in this package.
	unsafe *replog.UnsafeKnobs
}

// DefaultParams returns the random-mode parameters used by TestRandom: three
// nodes, moderate loss and duplication, occasional partitions and crashes,
// three clients running two tournaments of four entrants per batch.
func DefaultParams(seed uint64) Params {
	return Params{
		Seed:          seed,
		Nodes:         3,
		Steps:         6000,
		LivenessSteps: 60000,
		Faults: transport.Faults{
			DropP:    0.1,
			DupP:     0.05,
			MinDelay: time.Millisecond,
			MaxDelay: 10 * time.Millisecond,
		},
		PartitionP:   0.01,
		HealP:        0.03,
		CrashP:       0.005,
		RestartAfter: [2]time.Duration{100 * time.Millisecond, 500 * time.Millisecond},
		TornWriteP:   0.1,
		Clients:      3,
		Tournaments:  2,
		Entrants:     4,
		skipScoreP:   0.15,
	}
}

// Validate reports the first problem with p.
func (p Params) Validate() error {
	if p.Nodes < 1 {
		return errors.New("sim: Nodes must be at least 1")
	}
	if p.Steps < 0 || p.LivenessSteps < 0 {
		return errors.New("sim: Steps and LivenessSteps must not be negative")
	}
	// A slice, not a map, so that the first field reported is always the
	// same one.
	for _, f := range []struct {
		name string
		v    float64
	}{
		{"DropP", p.Faults.DropP}, {"DupP", p.Faults.DupP}, {"PartitionP", p.PartitionP},
		{"HealP", p.HealP}, {"CrashP", p.CrashP}, {"TornWriteP", p.TornWriteP}, {"RetryP", p.RetryP},
	} {
		if f.v < 0 || f.v > 1 {
			return fmt.Errorf("sim: %s = %v is not a probability", f.name, f.v)
		}
	}
	if p.Faults.MinDelay < 0 || p.Faults.MaxDelay < p.Faults.MinDelay {
		return errors.New("sim: Faults delays must satisfy 0 <= MinDelay <= MaxDelay")
	}
	if p.RestartAfter[0] < 0 || p.RestartAfter[1] < p.RestartAfter[0] {
		return errors.New("sim: RestartAfter must satisfy 0 <= min <= max")
	}
	if p.ClockSkewMax < 0 {
		return errors.New("sim: ClockSkewMax must not be negative")
	}
	if p.Clients < 0 || p.Tournaments < 0 || p.Entrants < 0 {
		return errors.New("sim: Clients, Tournaments and Entrants must not be negative")
	}
	if p.Scenario != "" && findScenario(p.Scenario) == nil {
		return fmt.Errorf("sim: unknown scenario %q (known: %v)", p.Scenario, Scenarios())
	}
	return nil
}

// tournamentsPerBatch is the number of tournaments a client runs per batch.
func (p Params) tournamentsPerBatch() int {
	if p.Tournaments < 1 {
		return 1
	}
	return p.Tournaments
}

// entrants is the number of entrants per tournament, at least one.
func (p Params) entrants() int {
	if p.Entrants < 1 {
		return 1
	}
	return p.Entrants
}

// commandsPerTournament is the number of commands a full workflow takes
// when every entrant scores: create, joins, scores, close, settle.
func (p Params) commandsPerTournament() int { return 2*p.entrants() + 3 }

// Report summarises a run.
type Report struct {
	// Steps is the number of events processed.
	Steps int
	// Elections counts nodes becoming leader.
	Elections int
	// LeaderChanges counts every other change of the leader a node knows
	// about.
	LeaderChanges int
	// Crashes counts crashes, including those a torn write caused.
	Crashes int
	// TornWrites counts Save calls that failed as torn writes.
	TornWrites int
	// Partitions counts partitions installed, random and scripted.
	Partitions int
	// Net holds the network counters.
	Net transport.Stats
	// Settled is the number of tournaments observed Settled.
	Settled int
	// Voided is the number of settled tournaments that voided at Close.
	Voided int
	// Issued counts the distinct client commands generated.
	Issued int
	// Completed counts client commands whose result the client observed.
	Completed int
	// Submits counts Submit calls by clients, retries included.
	Submits int
	// ExtraRetries counts duplicate submissions after a result was seen.
	ExtraRetries int
	// Mutated counts extra retries sent with a mutated payload.
	Mutated int
	// Unexpected counts results a client did not expect for its workflow
	// step; a correct system produces none.
	Unexpected int
	// Keys is the number of distinct idempotency keys applied anywhere.
	Keys int
	// Applies counts command applications on all nodes.
	Applies int
	// Replays counts applications answered from the results table.
	Replays int
	// KeyReused counts applications answered key_reused.
	KeyReused int
	// Rejections counts first-time rejections other than key_reused.
	Rejections int
	// Reads counts consistent reads started.
	Reads int
	// ReadsCompleted counts reads that reached ReadReady.
	ReadsCompleted int
	// ReadsFailed counts reads that ended in ReadFailed.
	ReadsFailed int
	// Applied is the highest applied slot on any node.
	Applied paxos.Slot
	// Commit is the highest commit index on any live node.
	Commit paxos.Slot
	// Participants is the number of core nodes that hold the final leader's
	// ballot and its commit index when the liveness phase completed.
	Participants int
	// Nodes is the cluster size after the scenario adjusted it.
	Nodes int
	// LeaseEnabled reports whether the lease was on.
	LeaseEnabled bool
	// SimTime is the virtual clock at the end.
	SimTime time.Duration
	// Checks counts, per invariant identifier, how many times the checker
	// evaluated it.
	Checks map[string]int
	// Marks holds scenario-specific counters, keyed by scenario name.
	Marks map[string]int
}

// Invariants lists the identifiers the checker evaluates, in the order of
// the design's tables, with a one-line description each.
var Invariants = []struct {
	ID   string
	Desc string
}{
	{"S1", "single chosen value per slot"},
	{"S2", "identical applied sequences and state hashes"},
	{"S3", "acceptor promise monotone; accepts at or above the promise"},
	{"S4", "ballots unique per node; one value per (ballot, slot)"},
	{"S5", "durable state survives a crash; torn writes keep the old record"},
	{"S6", "a slot chosen after a higher slot is a no-op"},
	{"S7", "consistent reads reflect every command completed before them"},
	{"S8", "at most one non-replayed result per idempotency key"},
	{"D1", "prize pool equals entry fees minus rake; pool account nets to zero"},
	{"D2", "every payout appears exactly once, sums to the pool"},
	{"D3", "a settled tournament never changes"},
	{"D4", "standings are a function of the scores and the tie-break rule"},
	{"D5", "eligibility checked and its list version recorded at entry and payout"},
	{"D6", "money never appears or disappears"},
}
