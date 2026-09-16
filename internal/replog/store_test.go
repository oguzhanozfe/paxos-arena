package replog

import (
	"math/rand/v2"
	"testing"

	"github.com/oguzhanozfe/paxos-arena/internal/paxos"
)

func newTestRNG() *rand.Rand { return rand.New(rand.NewPCG(42, 0)) }

func TestMemStoreLoadIsSortedAndReplaces(t *testing.T) {
	s := NewMemStore()
	d, err := s.Load()
	if err != nil || !d.Promised.IsZero() || d.MaxRound != 0 || len(d.Accepted) != 0 || len(d.Chosen) != 0 {
		t.Fatalf("fresh store Load = %+v, %v; want zero Durable", d, err)
	}
	b1 := paxos.Ballot{Round: 1, Node: 1}
	b2 := paxos.Ballot{Round: 2, Node: 2}
	if err := s.SavePromised(b1); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveMaxRound(5); err != nil {
		t.Fatal(err)
	}
	for _, pv := range []paxos.PValue{
		{Ballot: b1, Slot: 9, Value: paxos.Value("nine")},
		{Ballot: b1, Slot: 3, Value: paxos.Value("three")},
		{Ballot: b2, Slot: 9, Value: paxos.Value("nine again")},
	} {
		if err := s.SaveAccepted(pv); err != nil {
			t.Fatal(err)
		}
	}
	for _, e := range []Entry{
		{Slot: 4, Ballot: b1, Value: paxos.Value("four")},
		{Slot: 2, Ballot: b1, Value: nil},
	} {
		if err := s.SaveChosen(e); err != nil {
			t.Fatal(err)
		}
	}
	d, err = s.Load()
	if err != nil {
		t.Fatal(err)
	}
	if d.Promised != b1 || d.MaxRound != 5 {
		t.Errorf("promised %v maxRound %d, want %v 5", d.Promised, d.MaxRound, b1)
	}
	if len(d.Accepted) != 2 || d.Accepted[0].Slot != 3 || d.Accepted[1].Slot != 9 {
		t.Fatalf("accepted = %+v, want slots 3 and 9 in order", d.Accepted)
	}
	if d.Accepted[1].Ballot != b2 || string(d.Accepted[1].Value) != "nine again" {
		t.Errorf("slot 9 not replaced by the later save: %+v", d.Accepted[1])
	}
	if len(d.Chosen) != 2 || d.Chosen[0].Slot != 2 || d.Chosen[1].Slot != 4 {
		t.Fatalf("chosen = %+v, want slots 2 and 4 in order", d.Chosen)
	}
	// Load returns copies of the slices: mutating them does not affect the
	// store.
	d.Accepted[0].Slot = 100
	d2, _ := s.Load()
	if d2.Accepted[0].Slot != 3 {
		t.Error("Load returned a slice aliased with store state")
	}
}
