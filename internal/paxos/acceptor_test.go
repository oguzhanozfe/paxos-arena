package paxos

import "testing"

// step is one message to the acceptor under test. prepare=true sends
// OnPrepare(b); otherwise OnAccept(b, v).
type step struct {
	prepare bool
	b       Ballot
	v       Value
	wantOK  bool
}

// TestAcceptorRules covers every ordering of two ballots (lower, higher,
// equal) across Prepare and Accept, and checks that the acceptor's promise
// never decreases and that an accepted proposal is never below the promise
// in force when it was accepted.
func TestAcceptorRules(t *testing.T) {
	lo, hi := Ballot{Round: 1, Node: 1}, Ballot{Round: 2, Node: 1}
	cases := []struct {
		name         string
		steps        []step
		wantPromised Ballot
		wantAccepted Ballot
		wantValue    Value
	}{
		{"prepare lo then hi", []step{
			{true, lo, nil, true}, {true, hi, nil, true},
		}, hi, Ballot{}, nil},
		{"prepare hi then lo", []step{
			{true, hi, nil, true}, {true, lo, nil, false},
		}, hi, Ballot{}, nil},
		{"prepare twice same ballot", []step{
			{true, lo, nil, true}, {true, lo, nil, false},
		}, lo, Ballot{}, nil},
		{"prepare lo then accept lo", []step{
			{true, lo, nil, true}, {false, lo, Value("a"), true},
		}, lo, lo, Value("a")},
		{"prepare hi then accept lo", []step{
			{true, hi, nil, true}, {false, lo, Value("a"), false},
		}, hi, Ballot{}, nil},
		{"prepare lo then accept hi raises promise", []step{
			{true, lo, nil, true}, {false, hi, Value("b"), true},
		}, hi, hi, Value("b")},
		{"accept without prepare", []step{
			{false, lo, Value("a"), true},
		}, lo, lo, Value("a")},
		{"accept lo then prepare hi reports lo", []step{
			{false, lo, Value("a"), true}, {true, hi, nil, true},
		}, hi, lo, Value("a")},
		{"accept hi then prepare lo", []step{
			{false, hi, Value("b"), true}, {true, lo, nil, false},
		}, hi, hi, Value("b")},
		{"accept then prepare same ballot", []step{
			{false, lo, Value("a"), true}, {true, lo, nil, false},
		}, lo, lo, Value("a")},
		{"accept same ballot twice", []step{
			{false, lo, Value("a"), true}, {false, lo, Value("a"), true},
		}, lo, lo, Value("a")},
		{"accept hi then accept lo", []step{
			{false, hi, Value("b"), true}, {false, lo, Value("a"), false},
		}, hi, hi, Value("b")},
		{"accept lo then accept hi", []step{
			{false, lo, Value("a"), true}, {false, hi, Value("b"), true},
		}, hi, hi, Value("b")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var a Acceptor
			for i, s := range tc.steps {
				before := a.Promised
				if s.prepare {
					p, n, ok := a.OnPrepare(s.b)
					if ok != s.wantOK {
						t.Fatalf("step %d: OnPrepare(%v) ok=%t, want %t", i, s.b, ok, s.wantOK)
					}
					if ok {
						if p.Ballot != s.b {
							t.Errorf("step %d: promise ballot %v, want %v", i, p.Ballot, s.b)
						}
						if p.Accepted != before && p.Accepted != a.Accepted {
							t.Errorf("step %d: promise reports accepted %v", i, p.Accepted)
						}
					} else {
						if n.Ballot != s.b || n.Promised != before {
							t.Errorf("step %d: nack %+v, want ballot %v promised %v", i, n, s.b, before)
						}
						if a.Promised != before {
							t.Errorf("step %d: refused prepare changed promise %v -> %v", i, before, a.Promised)
						}
					}
				} else {
					n, ok := a.OnAccept(s.b, s.v)
					if ok != s.wantOK {
						t.Fatalf("step %d: OnAccept(%v) ok=%t, want %t", i, s.b, ok, s.wantOK)
					}
					if ok {
						if a.Accepted.Less(before) {
							t.Errorf("step %d: accepted %v below promise in force %v", i, a.Accepted, before)
						}
					} else if n.Promised != before || a.Promised != before || a.Accepted.Less(Ballot{}) {
						t.Errorf("step %d: refused accept changed state or nack wrong: %+v", i, n)
					}
				}
				if a.Promised.Less(before) {
					t.Errorf("step %d: promise decreased %v -> %v", i, before, a.Promised)
				}
			}
			if a.Promised != tc.wantPromised {
				t.Errorf("promised = %v, want %v", a.Promised, tc.wantPromised)
			}
			if a.Accepted != tc.wantAccepted {
				t.Errorf("accepted = %v, want %v", a.Accepted, tc.wantAccepted)
			}
			if !ValueEqual(a.Value, tc.wantValue) {
				t.Errorf("value = %q, want %q", a.Value, tc.wantValue)
			}
		})
	}
}
