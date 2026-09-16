package sim

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"time"

	"github.com/oguzhanozfe/paxos-arena/internal/paxos"
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

// byteState is the stand-in state machine of this milestone: it applies
// chosen entries in slot order, skips no-ops and keeps a hash chain over the
// applied values. Two nodes with the same applied prefix have the same hash.
type byteState struct {
	applied paxos.Slot
	hash    [32]byte
	count   int
}

func newByteState() *byteState { return &byteState{} }

// apply consumes the entry for slot applied+1 and returns the new hash.
func (s *byteState) apply(e replog.Entry) [32]byte {
	if e.Slot != s.applied+1 {
		panic("sim: byteState.apply out of order")
	}
	s.applied = e.Slot
	if e.NoOp() {
		return s.hash
	}
	var slot [8]byte
	binary.BigEndian.PutUint64(slot[:], uint64(e.Slot))
	h := sha256.New()
	h.Write(s.hash[:])
	h.Write(slot[:])
	h.Write(e.Value)
	copy(s.hash[:], h.Sum(nil))
	s.count++
	return s.hash
}

// simNode is one replica in the simulation: its configuration, its store
// (which survives crashes), the live Node (nil while crashed), the stand-in
// state machine and its clock.
type simNode struct {
	id     paxos.NodeID
	cfg    replog.Config
	store  *faultStore
	node   *replog.Node
	sm     *byteState
	alive  bool
	frozen bool   // outside the healed core: never restarted
	gen    uint64 // incremented on every crash; stale events carry an old gen
	offset time.Duration
	rate   float64
}

// now maps the simulator's clock to this node's clock.
func (nd *simNode) now(t time.Duration) time.Duration {
	if nd.rate == 1 {
		return t + nd.offset
	}
	return time.Duration(float64(t)*nd.rate) + nd.offset
}
