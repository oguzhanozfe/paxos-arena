// Package replica joins the replicated log of package replog with the
// tournament state machine of package tournament. Core is the pure
// composition the simulator drives directly; Runner is the single event-loop
// goroutine that owns a Core in a running process.
package replica

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/oguzhanozfe/paxos-arena/internal/paxos"
	"github.com/oguzhanozfe/paxos-arena/internal/replog"
	"github.com/oguzhanozfe/paxos-arena/internal/tournament"
)

// Core is one replica's log node and state machine. It has no goroutines
// and no clock: time enters as a time.Duration and the caller serialises
// every call. Chosen slots are applied by ApplyCommitted or ApplyNext, in
// order, exactly once.
type Core struct {
	log   *replog.Node
	state *tournament.State
}

// NewCore builds the log node from cfg and store and an empty state
// machine. The state machine is not persisted: the caller replays the
// chosen prefix with ApplyCommitted after a restart.
func NewCore(cfg replog.Config, store replog.Store, rng *rand.Rand) (*Core, error) {
	n, err := replog.New(cfg, store, rng)
	if err != nil {
		return nil, err
	}
	return &Core{log: n, state: tournament.New()}, nil
}

// Applied is one slot the state machine consumed.
type Applied struct {
	// Slot is the log slot.
	Slot paxos.Slot
	// Key is the command's idempotency key; empty for a no-op.
	Key tournament.IdempotencyKey
	// Command is the decoded command; zero for a no-op.
	Command tournament.Command
	// Result is what applying the command produced; zero for a no-op.
	Result tournament.Result
	// NoOp is true for a gap fill, a leadership marker, or an entry that
	// did not decode (Err set), all of which the state machine skips.
	NoOp bool
	// Err is the decode error of an undecodable entry.
	Err error
}

// Step feeds one inbound message to the log node.
func (c *Core) Step(now time.Duration, env replog.Envelope) []replog.Envelope {
	return c.log.Step(now, env)
}

// Tick runs the log node's timers.
func (c *Core) Tick(now time.Duration) []replog.Envelope { return c.log.Tick(now) }

// Submit proposes cmd as the value of the next free slot. It returns
// replog.ErrNotLeader when this replica does not lead, and an error for a
// command that does not encode or whose key is malformed.
func (c *Core) Submit(now time.Duration, cmd tournament.Command) ([]replog.Envelope, error) {
	if err := tournament.ValidateKey(cmd.Key); err != nil {
		return nil, fmt.Errorf("replica: submit: %w", err)
	}
	v, err := tournament.Encode(cmd)
	if err != nil {
		return nil, fmt.Errorf("replica: submit: %w", err)
	}
	return c.log.Propose(now, paxos.Value(v))
}

// ReadIndex starts a read-index barrier on the leader; see
// replog.Node.ReadIndex.
func (c *Core) ReadIndex(now time.Duration) (uint64, []replog.Envelope, error) {
	return c.log.ReadIndex(now)
}

// ApplyNext applies the next chosen slot above the applied index if it is
// within the commit index, and reports what it applied. ok is false when
// the state machine has caught up with the commit index.
func (c *Core) ApplyNext() (Applied, bool) {
	next := c.state.Applied() + 1
	if next > c.log.CommitIndex() {
		return Applied{}, false
	}
	e, ok := c.log.Chosen(next)
	if !ok {
		// The commit index guarantees the slot is chosen; a missing entry
		// would be a log bug. Report it as an undecodable no-op rather than
		// panic, so the host can log it.
		c.state.Skip(next)
		return Applied{Slot: next, NoOp: true, Err: errors.New("replica: slot within the commit index is not chosen")}, true
	}
	if e.NoOp() {
		c.state.Skip(next)
		return Applied{Slot: next, NoOp: true}, true
	}
	cmd, err := tournament.Decode([]byte(e.Value))
	if err != nil {
		c.state.Skip(next)
		return Applied{Slot: next, NoOp: true, Err: err}, true
	}
	res := c.state.Apply(next, e.Ballot, cmd)
	return Applied{Slot: next, Key: cmd.Key, Command: cmd, Result: res}, true
}

// ApplyCommitted applies every chosen slot above the applied index up to
// the commit index, in order, and returns what it applied.
func (c *Core) ApplyCommitted() []Applied {
	var out []Applied
	for {
		a, ok := c.ApplyNext()
		if !ok {
			return out
		}
		out = append(out, a)
	}
}

// Events drains the log node's events.
func (c *Core) Events() []replog.Event { return c.log.Events() }

// Log returns the log node, for observation.
func (c *Core) Log() *replog.Node { return c.log }

// State returns the state machine, for observation and reads.
func (c *Core) State() *tournament.State { return c.state }

// Failed returns the store error that stopped the log node, or nil.
func (c *Core) Failed() error { return c.log.Failed() }

// Status is a snapshot of a replica for operators and tests.
type Status struct {
	// Self is this replica.
	Self paxos.NodeID `json:"self"`
	// Leader is the node believed to lead, 0 when unknown.
	Leader paxos.NodeID `json:"leader"`
	// Role is Follower, Candidate or Leader.
	Role replog.Role `json:"role"`
	// Ballot is the leader's ballot as this replica knows it.
	Ballot paxos.Ballot `json:"ballot"`
	// Ready is true on a leader whose leadership no-op is chosen.
	Ready bool `json:"ready"`
	// CommitIndex is the log's commit index.
	CommitIndex paxos.Slot `json:"commit_index"`
	// Applied is the state machine's applied slot.
	Applied paxos.Slot `json:"applied"`
	// StateHash identifies the applied prefix.
	StateHash [32]byte `json:"state_hash"`
	// Tournaments is the number of tournaments in the state.
	Tournaments int `json:"tournaments"`
}

// Status returns a snapshot of the core.
func (c *Core) Status() Status {
	leader, lb, _ := c.log.Leader()
	return Status{
		Self:        c.log.Self(),
		Leader:      leader,
		Role:        c.log.Role(),
		Ballot:      lb,
		Ready:       c.log.Ready(),
		CommitIndex: c.log.CommitIndex(),
		Applied:     c.state.Applied(),
		StateHash:   c.state.Hash(),
		Tournaments: len(c.state.Tournaments()),
	}
}
