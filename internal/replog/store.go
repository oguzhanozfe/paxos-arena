package replog

import (
	"sort"

	"github.com/oguzhanozfe/paxos-arena/internal/paxos"
)

// Durable is everything a node must keep across a crash: its promise, the
// highest round it has used as a proposer, every accepted pvalue (the most
// recent one per slot) and every chosen entry. Accepted and Chosen are in
// slot order.
type Durable struct {
	// Promised is the acceptor's promise.
	Promised paxos.Ballot
	// MaxRound is the highest round used as a proposer or seen promised.
	MaxRound uint64
	// Accepted holds the most recently accepted pvalue per slot, in slot
	// order.
	Accepted []paxos.PValue
	// Chosen holds every chosen entry, in slot order.
	Chosen []Entry
}

// Store persists Durable. Every Save call must be durable before it
// returns; a Node that receives an error from a Save call stops processing
// and reports the error from Failed, so that no message reflecting the
// unsaved change is ever sent. Values passed to Save are never modified by
// the node afterwards and may be retained without copying.
type Store interface {
	// Load returns the durable state. A fresh store returns the zero Durable.
	Load() (Durable, error)
	// SavePromised records the acceptor's promise.
	SavePromised(paxos.Ballot) error
	// SaveMaxRound records the highest round this node has used as a
	// proposer or seen promised.
	SaveMaxRound(uint64) error
	// SaveAccepted records the most recently accepted pvalue for its slot,
	// replacing any earlier pvalue for the same slot.
	SaveAccepted(paxos.PValue) error
	// SaveChosen records a chosen entry.
	SaveChosen(Entry) error
}

// MemStore is a Store that keeps Durable in memory. It has a single owner
// and no locking; the simulator keeps one MemStore per node across
// simulated crashes so that a restart recovers exactly what a disk would.
type MemStore struct {
	promised paxos.Ballot
	maxRound uint64
	accepted map[paxos.Slot]paxos.PValue
	chosen   map[paxos.Slot]Entry
}

// NewMemStore returns an empty store.
func NewMemStore() *MemStore {
	return &MemStore{
		accepted: make(map[paxos.Slot]paxos.PValue),
		chosen:   make(map[paxos.Slot]Entry),
	}
}

// Load implements Store.
func (s *MemStore) Load() (Durable, error) {
	d := Durable{Promised: s.promised, MaxRound: s.maxRound}
	d.Accepted = make([]paxos.PValue, 0, len(s.accepted))
	for _, pv := range s.accepted {
		d.Accepted = append(d.Accepted, pv)
	}
	sort.Slice(d.Accepted, func(i, j int) bool { return d.Accepted[i].Slot < d.Accepted[j].Slot })
	d.Chosen = make([]Entry, 0, len(s.chosen))
	for _, e := range s.chosen {
		d.Chosen = append(d.Chosen, e)
	}
	sort.Slice(d.Chosen, func(i, j int) bool { return d.Chosen[i].Slot < d.Chosen[j].Slot })
	return d, nil
}

// SavePromised implements Store.
func (s *MemStore) SavePromised(b paxos.Ballot) error {
	s.promised = b
	return nil
}

// SaveMaxRound implements Store.
func (s *MemStore) SaveMaxRound(r uint64) error {
	s.maxRound = r
	return nil
}

// SaveAccepted implements Store.
func (s *MemStore) SaveAccepted(pv paxos.PValue) error {
	s.accepted[pv.Slot] = pv
	return nil
}

// SaveChosen implements Store.
func (s *MemStore) SaveChosen(e Entry) error {
	s.chosen[e.Slot] = e
	return nil
}
