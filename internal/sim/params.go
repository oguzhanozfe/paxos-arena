// Package sim runs a whole cluster of replog nodes in one goroutine under a
// virtual clock, a seeded fault schedule and an invariant checker.
//
// Everything is deterministic in the seed: the same Params produce the same
// events, the same messages and the same trace, so a failing seed is a
// reproducible test case. In this milestone the state machine fed by the log
// is a stand-in that applies opaque byte values and hashes them; the
// tournament state machine replaces it in the next milestone.
package sim

import (
	"errors"
	"fmt"
	"time"

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
	// Tournaments and Entrants size each client's workload: one batch is
	// Tournaments*Entrants opaque commands, submitted one at a time until
	// each has been chosen. Clients start a new batch whenever they finish
	// one while faults are on.
	Tournaments int
	// Entrants is the second factor of the batch size.
	Entrants int
	// RetryP is reserved for the client-retry scenario of the next
	// milestone; it has no effect on the byte-value stand-in.
	RetryP float64
	// Scenario names a scripted fault schedule from Scenarios, or "" for the
	// purely random schedule.
	Scenario string
	// NoLease disables the leader lease on every node.
	NoLease bool

	// unsafe switches protocol rules off; set only by tests in this package.
	unsafe *replog.UnsafeKnobs
}

// DefaultParams returns the random-mode parameters used by TestRandom: three
// nodes, moderate loss and duplication, occasional partitions and crashes,
// three clients with 20 commands each.
func DefaultParams(seed uint64) Params {
	return Params{
		Seed:          seed,
		Nodes:         3,
		Steps:         6000,
		LivenessSteps: 40000,
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
		Tournaments:  4,
		Entrants:     5,
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
	for name, v := range map[string]float64{
		"DropP": p.Faults.DropP, "DupP": p.Faults.DupP, "PartitionP": p.PartitionP,
		"HealP": p.HealP, "CrashP": p.CrashP, "TornWriteP": p.TornWriteP, "RetryP": p.RetryP,
	} {
		if v < 0 || v > 1 {
			return fmt.Errorf("sim: %s = %v is not a probability", name, v)
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

// commandsPerClient is the number of opaque commands each client submits.
func (p Params) commandsPerClient() int {
	n := p.Tournaments * p.Entrants
	if n < 1 {
		n = 1
	}
	return n
}
