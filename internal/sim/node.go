package sim

import (
	"errors"
	"time"

	"github.com/oguzhanozfe/paxos-arena/internal/paxos"
	"github.com/oguzhanozfe/paxos-arena/internal/replica"
	"github.com/oguzhanozfe/paxos-arena/internal/replog"
)

// errTornWrite is the store error a torn write produces.
var errTornWrite = errors.New("sim: torn write (crash during save)")

// faultStore wraps a MemStore and fails the next Save call while armed. The
// node that receives the error stops and the simulator crashes it, so its
// volatile update is lost while the store keeps the previous durable record.
type faultStore struct {
	mem   *replog.MemStore
	armed bool
	torn  int
}

func newFaultStore() *faultStore { return &faultStore{mem: replog.NewMemStore()} }

func (s *faultStore) fail() bool {
	if s.armed {
		s.armed = false
		s.torn++
		return true
	}
	return false
}

func (s *faultStore) Load() (replog.Durable, error) { return s.mem.Load() }

func (s *faultStore) SavePromised(b paxos.Ballot) error {
	if s.fail() {
		return errTornWrite
	}
	return s.mem.SavePromised(b)
}

func (s *faultStore) SaveMaxRound(r uint64) error {
	if s.fail() {
		return errTornWrite
	}
	return s.mem.SaveMaxRound(r)
}

func (s *faultStore) SaveAccepted(pv paxos.PValue) error {
	if s.fail() {
		return errTornWrite
	}
	return s.mem.SaveAccepted(pv)
}

func (s *faultStore) SaveChosen(e replog.Entry) error {
	if s.fail() {
		return errTornWrite
	}
	return s.mem.SaveChosen(e)
}

// simNode is one replica in the simulation: its configuration, its store
// (which survives crashes), the live core (nil while crashed) with its log
// node and tournament state machine, and its clock.
type simNode struct {
	id      paxos.NodeID
	cfg     replog.Config
	store   *faultStore
	core    *replica.Core
	node    *replog.Node // core.Log(), nil while crashed
	alive   bool
	frozen  bool   // outside the healed core: never restarted
	gen     uint64 // incremented on every crash; stale events carry an old gen
	applied paxos.Slot
	offset  time.Duration
	rate    float64
}

// now maps the simulator's clock to this node's clock.
func (nd *simNode) now(t time.Duration) time.Duration {
	if nd.rate == 1 {
		return t + nd.offset
	}
	return time.Duration(float64(t)*nd.rate) + nd.offset
}
