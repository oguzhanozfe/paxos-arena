package paxos

import "testing"

// TestAttackProposerChangesValueWithinOneBallot drives the reference
// Acceptor and Proposer through their public methods only. Five acceptors;
// acceptor 4 accepted "y" at an earlier ballot. The proposer reaches a quorum
// of empty promises from acceptors 1-3, proposes "x", and "x" is accepted by
// that quorum and reported chosen. Acceptor 4's promise then arrives late;
// OnPromise still takes it, and the next Propose (a retransmission asking
// for the value to resend) returns "y" at the same ballot. Acceptors 3-5
// accept it, so a second value is held by a quorum after the first was
// chosen. A proposer must send at most one value per ballot (S4), and a
// chosen value must never be displaced (S1).
func TestAttackProposerChangesValueWithinOneBallot(t *testing.T) {
	const n = 5
	accs := make([]Acceptor, n+1) // 1-based
	old := Ballot{Round: 1, Node: 9}
	if _, _, ok := accs[4].OnPrepare(old); !ok {
		t.Fatal("setup: acceptor 4 refused the old prepare")
	}
	if _, ok := accs[4].OnAccept(old, Value("y")); !ok {
		t.Fatal("setup: acceptor 4 refused the old accept")
	}

	p := NewProposer(1, n)
	b := p.Start(2)
	for id := 1; id <= 3; id++ {
		pr, _, ok := accs[id].OnPrepare(b)
		if !ok {
			t.Fatalf("setup: acceptor %d refused prepare %v", id, b)
		}
		p.OnPromise(NodeID(id), pr)
	}
	b1, v1, err := p.Propose(Value("x"))
	if err != nil || b1 != b || string(v1) != "x" {
		t.Fatalf("setup: first Propose = %v %q %v", b1, v1, err)
	}
	chosen := false
	for id := 1; id <= 3; id++ {
		if _, ok := accs[id].OnAccept(b1, v1); !ok {
			t.Fatalf("setup: acceptor %d refused %v", id, b1)
		}
		chosen = p.OnAccepted(NodeID(id), b1)
	}
	if !chosen || string(p.Value()) != "x" {
		t.Fatalf("setup: x not reported chosen (chosen=%t value=%q)", chosen, p.Value())
	}

	// The late promise from acceptor 4 reports "y" at the old ballot.
	pr4, _, ok := accs[4].OnPrepare(b)
	if !ok {
		t.Fatal("setup: acceptor 4 refused prepare")
	}
	p.OnPromise(4, pr4)
	b2, v2, err := p.Propose(Value("x"))
	if err != nil {
		return // refusing a second Propose is safe
	}
	if b2 == b1 && string(v2) != string(v1) {
		// Show the consequence: the second value reaches a quorum too.
		for id := 3; id <= 5; id++ {
			accs[id].OnAccept(b2, v2)
		}
		held := map[string]int{}
		for id := 1; id <= n; id++ {
			if accs[id].Accepted == b2 {
				held[string(accs[id].Value)]++
			}
		}
		t.Fatalf("Propose returned %q at ballot %v after %q was proposed and chosen at the same ballot; acceptors now hold %v at %v (quorum %d): two values chosen",
			v2, b2, v1, held, b2, Quorum(n))
	}
}
