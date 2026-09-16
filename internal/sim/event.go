package sim

import (
	"fmt"
	"time"
)

// evKind is the kind of a scheduled simulator event. Message deliveries are
// not events on this heap; the network keeps them and the run loop merges
// the two by time.
type evKind uint8

const (
	evTick       evKind = iota // one node's Tick
	evClient                   // one client's turn
	evFault                    // draw the random fault probabilities
	evRestart                  // restart a crashed node
	evHardCrash                // crash a node whose armed torn write did not fire
	evScript                   // a scripted scenario step
	evPlayClient               // one play client's turn
)

func (k evKind) String() string {
	switch k {
	case evTick:
		return "tick"
	case evClient:
		return "client"
	case evFault:
		return "fault"
	case evRestart:
		return "restart"
	case evHardCrash:
		return "hardcrash"
	case evScript:
		return "script"
	case evPlayClient:
		return "play_client"
	}
	return fmt.Sprintf("event(%d)", uint8(k))
}

// event is one entry of the simulator's heap, ordered by time then by
// sequence number so equal times are processed in scheduling order.
type event struct {
	at     time.Duration
	seq    uint64
	kind   evKind
	node   int // index into Run.nodes for evTick, evRestart, evHardCrash
	gen    uint64
	client int
	fn     func(*Run)
}

type eventHeap []event

func (h eventHeap) Len() int { return len(h) }
func (h eventHeap) Less(i, j int) bool {
	if h[i].at != h[j].at {
		return h[i].at < h[j].at
	}
	return h[i].seq < h[j].seq
}
func (h eventHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *eventHeap) Push(x any)   { *h = append(*h, x.(event)) }
func (h *eventHeap) Pop() any {
	old := *h
	n := len(old)
	e := old[n-1]
	*h = old[:n-1]
	return e
}

// Violation is a safety invariant failure found by the Checker. Invariant is
// the identifier of design section 6 (S1 through S8, D1 through D6) or of
// docs/UNITY-INTEGRATION.md section 12 (P1 through P6). Its Error string
// carries the seed, step and scenario needed to replay the run.
type Violation struct {
	// Invariant is the identifier: S1 through S8, D1 through D6, P1 through
	// P6.
	Invariant string
	// Detail says which node, slot or ballot broke the rule.
	Detail string
	// Seed is the run's seed.
	Seed uint64
	// Step is the event count at the violation.
	Step int
	// Scenario is the run's scenario name, "" for the random schedule.
	Scenario string
	// At is the virtual time of the violating event.
	At time.Duration
}

// Error implements error.
func (v *Violation) Error() string {
	return fmt.Sprintf("invariant %s violated at t=%v: %s (seed=%d step=%d scenario=%q)",
		v.Invariant, v.At, v.Detail, v.Seed, v.Step, v.Scenario)
}

// LivenessError reports that a run did not complete its workload within the
// liveness bound after Heal. It is not a safety failure.
type LivenessError struct {
	// Detail describes the clients and nodes that had not finished.
	Detail string
	// Seed is the run's seed.
	Seed uint64
	// Step is the event count when the bound was reached.
	Step int
	// Scenario is the run's scenario name, "" for the random schedule.
	Scenario string
}

// Error implements error.
func (e *LivenessError) Error() string {
	return fmt.Sprintf("no progress within the liveness bound: %s (seed=%d step=%d scenario=%q)",
		e.Detail, e.Seed, e.Step, e.Scenario)
}
