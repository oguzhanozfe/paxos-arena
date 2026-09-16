package paxos

import "testing"

func TestBallotCompare(t *testing.T) {
	cases := []struct {
		name string
		a, b Ballot
		want int
	}{
		{"equal", Ballot{1, 1}, Ballot{1, 1}, 0},
		{"zero equal", Ballot{}, Ballot{}, 0},
		{"lower round", Ballot{1, 9}, Ballot{2, 1}, -1},
		{"higher round", Ballot{3, 1}, Ballot{2, 9}, 1},
		{"same round lower node", Ballot{2, 1}, Ballot{2, 2}, -1},
		{"same round higher node", Ballot{2, 3}, Ballot{2, 2}, 1},
		{"zero below any", Ballot{}, Ballot{0, 1}, -1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.a.Compare(tc.b); got != tc.want {
				t.Errorf("Compare(%v, %v) = %d, want %d", tc.a, tc.b, got, tc.want)
			}
			if got := tc.a.Less(tc.b); got != (tc.want < 0) {
				t.Errorf("Less(%v, %v) = %t, want %t", tc.a, tc.b, got, tc.want < 0)
			}
			if got := tc.b.Compare(tc.a); got != -tc.want {
				t.Errorf("Compare(%v, %v) = %d, want %d (antisymmetry)", tc.b, tc.a, got, -tc.want)
			}
		})
	}
}

func TestBallotIsZeroAndString(t *testing.T) {
	cases := []struct {
		b      Ballot
		isZero bool
		str    string
	}{
		{Ballot{}, true, "r0.n0"},
		{Ballot{0, 1}, false, "r0.n1"},
		{Ballot{1, 0}, false, "r1.n0"},
		{Ballot{12, 3}, false, "r12.n3"},
	}
	for _, tc := range cases {
		t.Run(tc.str, func(t *testing.T) {
			if got := tc.b.IsZero(); got != tc.isZero {
				t.Errorf("IsZero(%v) = %t, want %t", tc.b, got, tc.isZero)
			}
			if got := tc.b.String(); got != tc.str {
				t.Errorf("String(%#v) = %q, want %q", tc.b, got, tc.str)
			}
		})
	}
}

func TestQuorum(t *testing.T) {
	cases := []struct{ n, want int }{
		{1, 1}, {2, 2}, {3, 2}, {4, 3}, {5, 3}, {6, 4}, {7, 4},
	}
	for _, tc := range cases {
		if got := Quorum(tc.n); got != tc.want {
			t.Errorf("Quorum(%d) = %d, want %d", tc.n, got, tc.want)
		}
		// Two quorums of n always intersect.
		if 2*Quorum(tc.n) <= tc.n {
			t.Errorf("Quorum(%d) = %d: two quorums would not intersect", tc.n, Quorum(tc.n))
		}
	}
}

func TestMayPromise(t *testing.T) {
	lo, hi := Ballot{1, 1}, Ballot{2, 1}
	cases := []struct {
		name        string
		promised, b Ballot
		want        bool
	}{
		{"nothing promised", Ballot{}, lo, true},
		{"higher ballot", lo, hi, true},
		{"equal ballot", lo, lo, false},
		{"lower ballot", hi, lo, false},
		{"same round higher node", Ballot{1, 1}, Ballot{1, 2}, true},
		{"same round lower node", Ballot{1, 2}, Ballot{1, 1}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := MayPromise(tc.promised, tc.b); got != tc.want {
				t.Errorf("MayPromise(%v, %v) = %t, want %t", tc.promised, tc.b, got, tc.want)
			}
		})
	}
}

func TestMayAccept(t *testing.T) {
	lo, hi := Ballot{1, 1}, Ballot{2, 1}
	cases := []struct {
		name        string
		promised, b Ballot
		want        bool
	}{
		{"nothing promised", Ballot{}, lo, true},
		{"higher ballot", lo, hi, true},
		{"equal ballot", lo, lo, true},
		{"lower ballot", hi, lo, false},
		{"same round lower node", Ballot{1, 2}, Ballot{1, 1}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := MayAccept(tc.promised, tc.b); got != tc.want {
				t.Errorf("MayAccept(%v, %v) = %t, want %t", tc.promised, tc.b, got, tc.want)
			}
		})
	}
}

func TestChoose(t *testing.T) {
	fallback := Value("fallback")
	cases := []struct {
		name    string
		reports []PValue
		want    Value
	}{
		{"no reports", nil, fallback},
		{"empty reports", []PValue{}, fallback},
		{"only zero-ballot reports", []PValue{{Ballot: Ballot{}, Value: Value("x")}}, fallback},
		{"single report", []PValue{{Ballot: Ballot{1, 1}, Value: Value("a")}}, Value("a")},
		{"highest round wins", []PValue{
			{Ballot: Ballot{1, 3}, Value: Value("a")},
			{Ballot: Ballot{3, 1}, Value: Value("b")},
			{Ballot: Ballot{2, 2}, Value: Value("c")},
		}, Value("b")},
		{"same round higher node wins", []PValue{
			{Ballot: Ballot{2, 1}, Value: Value("a")},
			{Ballot: Ballot{2, 2}, Value: Value("b")},
		}, Value("b")},
		{"highest report is a no-op", []PValue{
			{Ballot: Ballot{1, 1}, Value: Value("a")},
			{Ballot: Ballot{2, 1}, Value: nil},
		}, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := Choose(tc.reports, fallback)
			if !ValueEqual(got, tc.want) {
				t.Errorf("Choose(%v) = %q, want %q", tc.reports, got, tc.want)
			}
		})
	}
}

func TestValueEqual(t *testing.T) {
	cases := []struct {
		name string
		a, b Value
		want bool
	}{
		{"nil and empty", nil, Value{}, true},
		{"equal bytes", Value("ab"), Value("ab"), true},
		{"different", Value("ab"), Value("ac"), false},
		{"prefix", Value("a"), Value("ab"), false},
	}
	for _, tc := range cases {
		if got := ValueEqual(tc.a, tc.b); got != tc.want {
			t.Errorf("%s: ValueEqual(%q, %q) = %t, want %t", tc.name, tc.a, tc.b, got, tc.want)
		}
	}
}
