// Package replog implements a Multi-Paxos replicated log with a stable
// leader, a leader lease used only for liveness, learners with catch-up, and
// a read-index barrier for consistent reads.
//
// One Node per replica. A Node is a pure state machine: it has no
// goroutines, never reads the clock and never calls the global random
// functions. Time enters every method as a time.Duration argument and
// randomness as an injected *rand.Rand, so the deterministic simulator in
// package sim can drive a whole cluster from one goroutine. A Node is not
// safe for concurrent use; the host serialises all calls.
package replog

import (
	"errors"
	"fmt"
	"time"

	"github.com/oguzhanozfe/paxos-arena/internal/paxos"
)

// Role is the leadership state of a Node.
type Role uint8

const (
	// Follower accepts and learns; it starts an election when it hears
	// nothing from a leader for an election timeout.
	Follower Role = iota
	// Candidate has sent Prepare for a new ballot and is collecting promises.
	Candidate
	// Leader has a quorum of promises and proposes values in Phase 2 only.
	Leader
)

// String returns the lower-case role name.
func (r Role) String() string {
	switch r {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	}
	return fmt.Sprintf("role(%d)", uint8(r))
}

// MarshalText renders the role by name, so status documents read
// "leader" rather than 2.
func (r Role) MarshalText() ([]byte, error) {
	if r > Leader {
		return nil, fmt.Errorf("replog: unknown role %d", uint8(r))
	}
	return []byte(r.String()), nil
}

// UnmarshalText parses a role name.
func (r *Role) UnmarshalText(b []byte) error {
	for _, role := range []Role{Follower, Candidate, Leader} {
		if role.String() == string(b) {
			*r = role
			return nil
		}
	}
	return fmt.Errorf("replog: unknown role %q", string(b))
}

// UnsafeKnobs switches individual protocol rules off so that the simulator's
// checker can be shown to catch the resulting violations. Every knob breaks
// safety. It is nil in production and set only by tests in package sim.
type UnsafeKnobs struct {
	// AcceptBelowPromise makes the acceptor ignore paxos.MayAccept, so it
	// accepts proposals below its promise (breaks S3).
	AcceptBelowPromise bool
	// IgnorePhase1Reports makes a new leader propose its own no-op for every
	// slot instead of the value paxos.Choose selects from the promises
	// (breaks S1).
	IgnorePhase1Reports bool
	// SkipLeadershipNoOp lets a leader serve read-index requests before its
	// leadership no-op is chosen (breaks S7).
	SkipLeadershipNoOp bool
	// ForgetAcceptedOnRestart drops every accepted pvalue when the durable
	// state is loaded (breaks S5).
	ForgetAcceptedOnRestart bool
}

// Default timing and sizing values, applied by DefaultConfig and by New for
// zero fields (except LeaseDuration, where zero means "no lease").
const (
	DefaultHeartbeatInterval  = 50 * time.Millisecond
	DefaultElectionTimeoutMin = 150 * time.Millisecond
	DefaultElectionTimeoutMax = 300 * time.Millisecond
	DefaultLeaseDuration      = 150 * time.Millisecond
	DefaultWindow             = 64
	DefaultLearnBatch         = 256
	DefaultQueueLimit         = 1024
)

// MaxValueBytes bounds the length of a value Propose accepts. Every value
// travels whole in one Accept and one Learn, so a transport must carry a
// message holding a value of this length in its own encoding; see
// transport.MaxMessageBytes. It is far above the HTTP API's default body
// limit of 64 KiB, so that a command decoded from any accepted body and
// re-encoded canonically (where JSON may escape a character as six bytes)
// still fits; the API checks the encoded length against it before
// proposing.
const MaxValueBytes = 1 << 20

// Config configures one Node.
type Config struct {
	// Self is this replica's identifier. It must be non-zero and appear in
	// Peers.
	Self paxos.NodeID
	// Peers lists every replica including Self. The quorum is
	// paxos.Quorum(len(Peers)). Membership is fixed for the life of the log.
	Peers []paxos.NodeID
	// HeartbeatInterval is how often a leader sends Heartbeat and re-sends
	// pending Accepts. Default 50ms.
	HeartbeatInterval time.Duration
	// ElectionTimeoutMin is the lower bound of the randomised time a
	// follower waits without hearing from a leader before it becomes a
	// candidate. Default 150ms.
	ElectionTimeoutMin time.Duration
	// ElectionTimeoutMax is the upper bound of that time, and also how long
	// a leader keeps leading without hearing from a majority. Default 300ms.
	ElectionTimeoutMax time.Duration
	// LeaseDuration is how long an acceptor refuses Prepare from any node
	// other than the leader it last heard from. Zero disables the lease. It
	// must not exceed ElectionTimeoutMin. DefaultConfig sets 150ms.
	LeaseDuration time.Duration
	// Window bounds the number of slots a leader may have in flight. Default
	// 64.
	Window int
	// LearnBatch bounds the number of entries in one reply to LearnRequest.
	// Default 256.
	LearnBatch int
	// QueueLimit bounds the number of values a leader holds back while
	// Window slots are in flight. Propose returns ErrBusy beyond it, so a
	// leader that cannot get slots chosen does not accumulate proposals
	// without bound. Default 1024.
	QueueLimit int
	// Unsafe is nil in production. See UnsafeKnobs.
	Unsafe *UnsafeKnobs
}

// DefaultConfig returns a Config for self among peers with every default
// applied, including the default lease.
func DefaultConfig(self paxos.NodeID, peers []paxos.NodeID) Config {
	return Config{
		Self:               self,
		Peers:              append([]paxos.NodeID(nil), peers...),
		HeartbeatInterval:  DefaultHeartbeatInterval,
		ElectionTimeoutMin: DefaultElectionTimeoutMin,
		ElectionTimeoutMax: DefaultElectionTimeoutMax,
		LeaseDuration:      DefaultLeaseDuration,
		Window:             DefaultWindow,
		LearnBatch:         DefaultLearnBatch,
		QueueLimit:         DefaultQueueLimit,
	}
}

// withDefaults fills zero fields with their defaults. LeaseDuration is left
// as given because zero is meaningful there.
func (c Config) withDefaults() Config {
	if c.HeartbeatInterval == 0 {
		c.HeartbeatInterval = DefaultHeartbeatInterval
	}
	if c.ElectionTimeoutMin == 0 {
		c.ElectionTimeoutMin = DefaultElectionTimeoutMin
	}
	if c.ElectionTimeoutMax == 0 {
		c.ElectionTimeoutMax = DefaultElectionTimeoutMax
	}
	if c.Window == 0 {
		c.Window = DefaultWindow
	}
	if c.LearnBatch == 0 {
		c.LearnBatch = DefaultLearnBatch
	}
	if c.QueueLimit == 0 {
		c.QueueLimit = DefaultQueueLimit
	}
	return c
}

// Validate reports the first problem with the configuration after defaults
// are applied to zero fields.
func (c Config) Validate() error {
	c = c.withDefaults()
	if c.Self == 0 {
		return errors.New("replog: Self must be non-zero")
	}
	if len(c.Peers) == 0 {
		return errors.New("replog: Peers must not be empty")
	}
	seen := make(map[paxos.NodeID]bool, len(c.Peers))
	selfSeen := false
	for _, p := range c.Peers {
		if p == 0 {
			return errors.New("replog: Peers contains the invalid node 0")
		}
		if seen[p] {
			return fmt.Errorf("replog: Peers contains node %d twice", p)
		}
		seen[p] = true
		if p == c.Self {
			selfSeen = true
		}
	}
	if !selfSeen {
		return fmt.Errorf("replog: Self %d is not in Peers", c.Self)
	}
	if c.HeartbeatInterval <= 0 {
		return errors.New("replog: HeartbeatInterval must be positive")
	}
	if c.ElectionTimeoutMin < c.HeartbeatInterval {
		return errors.New("replog: ElectionTimeoutMin must be at least HeartbeatInterval")
	}
	if c.ElectionTimeoutMax < c.ElectionTimeoutMin {
		return errors.New("replog: ElectionTimeoutMax must be at least ElectionTimeoutMin")
	}
	if c.LeaseDuration < 0 {
		return errors.New("replog: LeaseDuration must not be negative")
	}
	if c.LeaseDuration > c.ElectionTimeoutMin {
		return errors.New("replog: LeaseDuration must not exceed ElectionTimeoutMin")
	}
	if c.Window < 1 {
		return errors.New("replog: Window must be at least 1")
	}
	if c.LearnBatch < 1 {
		return errors.New("replog: LearnBatch must be at least 1")
	}
	if c.QueueLimit < 1 {
		return errors.New("replog: QueueLimit must be at least 1")
	}
	return nil
}
