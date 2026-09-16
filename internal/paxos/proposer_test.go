package paxos

import (
	"errors"
	"testing"
)

func TestProposerStartAndBallot(t *testing.T) {
	p := NewProposer(3, 5)
	b := p.Start(7)
	if b != (Ballot{Round: 7, Node: 3}) {
		t.Fatalf("Start(7) = %v, want r7.n3", b)
	}
	if p.Ballot() != b {
		t.Errorf("Ballot() = %v, want %v", p.Ballot(), b)
	}
}

func TestProposerNeedsQuorumBeforePropose(t *testing.T) {
	cases := []struct {
		name      string
		n         int
		promisers []NodeID
		wantReady bool
	}{
		{"n=3 one promise", 3, []NodeID{1}, false},
		{"n=3 two promises", 3, []NodeID{1, 2}, true},
		{"n=3 duplicate promise counts once", 3, []NodeID{1, 1}, false},
		{"n=5 two promises", 5, []NodeID{1, 2}, false},
		{"n=5 three promises", 5, []NodeID{1, 2, 3}, true},
		{"n=1 self promise", 1, []NodeID{1}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := NewProposer(1, tc.n)
			b := p.Start(1)
			ready := false
			for _, from := range tc.promisers {
				ready = p.OnPromise(from, Promise{Ballot: b})
			}
			if ready != tc.wantReady {
				t.Errorf("ready = %t, want %t", ready, tc.wantReady)
			}
			_, _, err := p.Propose(Value("v"))
			if tc.wantReady && err != nil {
				t.Errorf("Propose error = %v, want nil", err)
			}
			if !tc.wantReady && !errors.Is(err, ErrNotReady) {
				t.Errorf("Propose error = %v, want ErrNotReady", err)
			}
		})
	}
}

func TestProposerIgnoresOtherBallots(t *testing.T) {
	p := NewProposer(1, 3)
	b := p.Start(2)
	stale := Ballot{Round: 1, Node: 1}
	if p.OnPromise(2, Promise{Ballot: stale}) {
		t.Fatal("stale promise counted toward quorum")
	}
	if p.OnPromise(3, Promise{Ballot: stale}) {
		t.Fatal("two stale promises counted toward quorum")
	}
	if p.OnPromise(1, Promise{Ballot: b}) {
		t.Fatal("one current promise must not reach quorum of 2")
	}
	if !p.OnPromise(2, Promise{Ballot: b}) {
		t.Fatal("own promise plus one current promise should reach quorum of 2")
	}
	if _, _, err := p.Propose(Value("v")); err != nil {
		t.Fatalf("Propose: %v", err)
	}
	if p.OnAccepted(2, stale) {
		t.Error("stale Accepted counted as chosen")
	}
	if p.OnAccepted(3, stale) {
		t.Error("two stale Accepted counted as chosen")
	}
	if p.OnAccepted(2, b) {
		t.Error("one current Accepted must not choose the value for n=3")
	}
	if !p.OnAccepted(3, b) {
		t.Error("two current Accepted (2 and 3) should choose the value for n=3")
	}
}

func TestProposerNackAbandonsAttempt(t *testing.T) {
	p := NewProposer(1, 3)
	b := p.Start(1)
	p.OnPromise(1, Promise{Ballot: b})
	p.OnNack(Nack{Ballot: Ballot{Round: 9, Node: 9}, Promised: Ballot{Round: 9, Node: 9}})
	if !p.OnPromise(2, Promise{Ballot: b}) {
		t.Fatal("a Nack for another ballot must not abandon the attempt")
	}
	p.OnNack(Nack{Ballot: b, Promised: Ballot{Round: 5, Node: 2}})
	if _, _, err := p.Propose(Value("v")); !errors.Is(err, ErrNotReady) {
		t.Fatalf("Propose after Nack: err = %v, want ErrNotReady", err)
	}
	if p.OnPromise(3, Promise{Ballot: b}) {
		t.Error("promises after abandonment must not make the proposer ready")
	}
	// A new attempt starts clean.
	b2 := p.Start(6)
	p.OnPromise(1, Promise{Ballot: b2})
	if !p.OnPromise(2, Promise{Ballot: b2}) {
		t.Error("new attempt did not reach quorum")
	}
}

func TestProposerChoosesHighestReportedValue(t *testing.T) {
	cases := []struct {
		name     string
		promises []Promise
		want     Value
	}{
		{"no values reported", []Promise{{}, {}}, Value("mine")},
		{"one value reported", []Promise{
			{Accepted: Ballot{1, 2}, Value: Value("theirs")}, {},
		}, Value("theirs")},
		{"highest ballot wins", []Promise{
			{Accepted: Ballot{1, 2}, Value: Value("old")},
			{Accepted: Ballot{2, 3}, Value: Value("new")},
		}, Value("new")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := NewProposer(1, 3)
			b := p.Start(5)
			for i, m := range tc.promises {
				m.Ballot = b
				p.OnPromise(NodeID(i+1), m)
			}
			gotB, gotV, err := p.Propose(Value("mine"))
			if err != nil {
				t.Fatalf("Propose: %v", err)
			}
			if gotB != b {
				t.Errorf("ballot = %v, want %v", gotB, b)
			}
			if !ValueEqual(gotV, tc.want) {
				t.Errorf("value = %q, want %q", gotV, tc.want)
			}
			if !ValueEqual(p.Value(), tc.want) {
				t.Errorf("Value() = %q, want %q", p.Value(), tc.want)
			}
		})
	}
}

func TestProposerAcceptedBeforeProposeIgnored(t *testing.T) {
	p := NewProposer(1, 3)
	b := p.Start(1)
	if p.OnAccepted(1, b) || p.OnAccepted(2, b) {
		t.Fatal("Accepted before Propose must not count")
	}
}
