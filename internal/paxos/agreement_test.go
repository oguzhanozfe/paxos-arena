package paxos

import (
	"fmt"
	"math/rand/v2"
	"testing"
)

// kind is a message type in the single-decree harness.
type kind uint8

const (
	kPrepare kind = iota
	kPromise
	kAccept
	kAccepted
	kNack
)

// hmsg is one in-flight message of the harness. Proposers and acceptors are
// indexed separately; from/to refer to the index of the sending and receiving
// role for the message kind.
type hmsg struct {
	kind     kind
	from, to int
	b        Ballot
	v        Value
	promise  Promise
	nack     Nack
}

type hproposer struct {
	p       *Proposer
	want    Value
	round   uint64
	maxSeen uint64
	phase   int // 0 collecting promises, 1 collecting accepts
}

// agreementRun runs one Paxos instance for one seed and returns every value
// observed chosen (by a proposer counting a quorum of Accepted, and by
// counting acceptor state directly), plus whether any value was chosen.
func agreementRun(t *testing.T, seed uint64) (chosen []Value, any bool) {
	t.Helper()
	rng := rand.New(rand.NewPCG(seed, 0))
	nAcc := 3 + 2*rng.IntN(2) // 3 or 5
	nProp := 2 + rng.IntN(2)  // 2 or 3
	dropP := 0.05 + 0.15*rng.Float64()
	dupP := 0.1 * rng.Float64()
	quorum := Quorum(nAcc)

	accs := make([]Acceptor, nAcc)
	props := make([]*hproposer, nProp)
	var pending []hmsg

	broadcastPrepare := func(i int) {
		hp := props[i]
		hp.round = max(hp.round, hp.maxSeen) + 1
		b := hp.p.Start(hp.round)
		hp.phase = 0
		for j := range accs {
			pending = append(pending, hmsg{kind: kPrepare, from: i, to: j, b: b})
		}
	}
	for i := range props {
		props[i] = &hproposer{p: NewProposer(NodeID(i+1), nAcc), want: Value(fmt.Sprintf("v%d", i))}
		broadcastPrepare(i)
	}

	record := func(v Value) {
		chosen = append(chosen, v)
		any = true
	}
	// majorityValue derives "chosen" from acceptor state: any (ballot, value)
	// held by a quorum of acceptors.
	majorityValue := func() {
		counts := make(map[Ballot]int)
		for _, a := range accs {
			if !a.Accepted.IsZero() {
				counts[a.Accepted]++
			}
		}
		for b, c := range counts {
			if c >= quorum {
				for _, a := range accs {
					if a.Accepted == b {
						record(a.Value)
						break
					}
				}
			}
		}
	}

	const maxSteps = 3000
	for step := 0; step < maxSteps; step++ {
		if len(pending) == 0 || rng.Float64() < 0.02 {
			// A proposer times out and retries with a higher round.
			i := rng.IntN(nProp)
			props[i].maxSeen = max(props[i].maxSeen, props[i].round)
			broadcastPrepare(i)
			continue
		}
		k := rng.IntN(len(pending))
		m := pending[k]
		pending[k] = pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		if rng.Float64() < dropP {
			continue
		}
		if rng.Float64() < dupP {
			pending = append(pending, m)
		}
		switch m.kind {
		case kPrepare:
			a := &accs[m.to]
			before := a.Promised
			pr, nk, ok := a.OnPrepare(m.b)
			if a.Promised.Less(before) {
				t.Fatalf("seed %d: acceptor %d promise decreased %v -> %v", seed, m.to, before, a.Promised)
			}
			if ok {
				pending = append(pending, hmsg{kind: kPromise, from: m.to, to: m.from, b: m.b, promise: pr})
			} else {
				pending = append(pending, hmsg{kind: kNack, from: m.to, to: m.from, b: m.b, nack: nk})
			}
		case kPromise:
			hp := props[m.to]
			if hp.p.OnPromise(NodeID(m.from+1), m.promise) && hp.phase == 0 {
				b, v, err := hp.p.Propose(hp.want)
				if err != nil {
					t.Fatalf("seed %d: Propose after quorum: %v", seed, err)
				}
				hp.phase = 1
				for j := range accs {
					pending = append(pending, hmsg{kind: kAccept, from: m.to, to: j, b: b, v: v})
				}
			}
		case kAccept:
			a := &accs[m.to]
			before := a.Promised
			nk, ok := a.OnAccept(m.b, m.v)
			if ok {
				if a.Accepted.Less(before) {
					t.Fatalf("seed %d: acceptor %d accepted %v below promise %v", seed, m.to, a.Accepted, before)
				}
				pending = append(pending, hmsg{kind: kAccepted, from: m.to, to: m.from, b: m.b})
			} else {
				pending = append(pending, hmsg{kind: kNack, from: m.to, to: m.from, b: m.b, nack: nk})
			}
			majorityValue()
		case kAccepted:
			hp := props[m.to]
			if hp.p.OnAccepted(NodeID(m.from+1), m.b) {
				record(hp.p.Value())
			}
		case kNack:
			hp := props[m.to]
			if m.nack.Ballot != hp.p.Ballot() {
				continue
			}
			hp.p.OnNack(m.nack)
			hp.maxSeen = max(hp.maxSeen, m.nack.Promised.Round)
			broadcastPrepare(m.to)
		}
	}
	return chosen, any
}

// TestSingleDecreeAgreement runs one Paxos instance with two or three
// proposers and three or five acceptors under random interleaving, loss and
// duplication, for 1000 seeds, and asserts that every value observed chosen
// in a run is the same value (invariant S1 of the design).
func TestSingleDecreeAgreement(t *testing.T) {
	seeds := 1000
	if testing.Short() {
		seeds = 100
	}
	decided := 0
	for seed := uint64(1); seed <= uint64(seeds); seed++ {
		chosen, any := agreementRun(t, seed)
		if any {
			decided++
		}
		for i := 1; i < len(chosen); i++ {
			if !ValueEqual(chosen[0], chosen[i]) {
				t.Fatalf("seed %d: two different values chosen: %q and %q", seed, chosen[0], chosen[i])
			}
		}
	}
	if decided < seeds/2 {
		t.Fatalf("only %d of %d runs chose a value; the harness does not exercise Phase 2 enough", decided, seeds)
	}
	t.Logf("%d of %d runs chose a value", decided, seeds)
}
