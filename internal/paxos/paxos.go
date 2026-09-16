// Package paxos implements single-decree Paxos: the identifiers and ballot
// type shared by every layer above it, the protocol rules stated once as
// pure functions, and a reference acceptor and proposer that run the
// two-phase protocol for one value.
//
// The package has no goroutines, no clock and no randomness. The replicated
// log in package replog does not embed Acceptor per slot, because in
// Multi-Paxos the promise is per acceptor and the accepted value is per slot;
// it calls the rule functions of this package instead.
package paxos

import "fmt"

// NodeID identifies one replica. The zero value is invalid and means "no
// node" wherever a NodeID is optional.
type NodeID uint32

// Slot numbers one position of the replicated log. Slots start at 1; 0 means
// "no slot".
type Slot uint64

// Value is the opaque payload agreed for one slot. A Value of length zero is
// the no-op that fills gaps and marks the start of a leadership.
type Value []byte

// Ballot is a proposal number. Ballots are totally ordered by Round, then by
// Node, so two nodes with distinct identifiers never use the same ballot.
type Ballot struct {
	// Round is the proposer's attempt counter; it dominates the order.
	Round uint64 `json:"round"`
	// Node is the proposer; it breaks ties between equal rounds.
	Node NodeID `json:"node"`
}

// Compare orders ballots by Round, then Node. It returns -1 when b is lower
// than o, 0 when they are equal and 1 when b is higher.
func (b Ballot) Compare(o Ballot) int {
	switch {
	case b.Round < o.Round:
		return -1
	case b.Round > o.Round:
		return 1
	case b.Node < o.Node:
		return -1
	case b.Node > o.Node:
		return 1
	}
	return 0
}

// Less reports whether b is strictly lower than o.
func (b Ballot) Less(o Ballot) bool { return b.Compare(o) < 0 }

// IsZero reports whether b is the zero ballot, which no proposer ever uses.
func (b Ballot) IsZero() bool { return b.Round == 0 && b.Node == 0 }

// String renders the ballot as "r<round>.n<node>".
func (b Ballot) String() string { return fmt.Sprintf("r%d.n%d", b.Round, b.Node) }

// PValue is one accepted (ballot, slot, value) triple, the unit an acceptor
// reports in a promise.
type PValue struct {
	// Ballot is the ballot at which the value was accepted.
	Ballot Ballot `json:"ballot"`
	// Slot is the log slot; 0 in the single-decree reference types.
	Slot Slot `json:"slot"`
	// Value is the accepted value.
	Value Value `json:"value"`
}

// Quorum returns the majority size for n participants: n/2 + 1.
func Quorum(n int) int { return n/2 + 1 }

// MayPromise reports whether an acceptor whose promise is promised may
// promise ballot b: only a strictly higher ballot.
func MayPromise(promised, b Ballot) bool { return promised.Less(b) }

// MayAccept reports whether an acceptor whose promise is promised may accept
// a proposal at ballot b: any ballot that is not lower than the promise.
func MayAccept(promised, b Ballot) bool { return !b.Less(promised) }

// Choose implements rule P2c of Paxos Made Simple: the value to propose is
// the value of the highest-ballot report, or fallback when reports is empty.
// Reports with a zero ballot are ignored, so a Promise that reports nothing
// accepted can be passed through unchanged.
func Choose(reports []PValue, fallback Value) Value {
	var best PValue
	found := false
	for _, r := range reports {
		if r.Ballot.IsZero() {
			continue
		}
		if !found || best.Ballot.Less(r.Ballot) {
			best = r
			found = true
		}
	}
	if !found {
		return fallback
	}
	return best.Value
}

// ValueEqual reports whether two values are byte-for-byte equal. A nil value
// and an empty value are equal: both are the no-op.
func ValueEqual(a, b Value) bool { return string(a) == string(b) }
