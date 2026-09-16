package tournament

import (
	"math/rand/v2"
	"testing"

	"github.com/oguzhanozfe/paxos-arena/internal/ledger"
)

// mk builds a tournament with the given rules and entries. scores maps a
// player index to its score in submission order; absent players are
// unscored.
func mk(rules Rules, players []Player, scores []int64) *Tournament {
	t := &Tournament{ID: "t", Seed: 7, Rules: rules, Status: Open}
	for i, p := range players {
		t.Entries = append(t.Entries, Entry{Player: p, JoinSeq: uint32(i + 1), ExclusionVersion: rules.Exclusions.Version})
	}
	seq := uint32(0)
	for i, sc := range scores {
		if sc < 0 {
			continue
		}
		seq++
		t.Entries[i].Scored = true
		t.Entries[i].Score = sc
		t.Entries[i].SubmitSeq = seq
	}
	return t
}

func players(n int) []Player {
	out := make([]Player, n)
	for i := range out {
		out[i] = Player{ID: PlayerID("p" + string(rune('a'+i))), Jurisdiction: "TR", Age: 30}
	}
	return out
}

func baseRules() Rules {
	return Rules{
		EntryFee: 500, RakeBps: 1000, PrizeBps: []uint32{5000, 3000, 2000},
		MinEntrants: 3, MaxEntrants: 100, MaxScore: 100000, MinAge: 18,
		TieBreak: EarliestSubmission, Exclusions: Exclusions{Version: 7, Jurisdictions: []string{"XX"}},
	}
}

func TestBps(t *testing.T) {
	cases := []struct {
		x    ledger.Money
		b    uint32
		want ledger.Money
	}{
		{0, 5000, 0},
		{10000, 5000, 5000},
		{1, 5000, 0},
		{3, 5000, 1},
		{12345, 1000, 1234},
		{12345, 9999, 12343},
		{MaxEntryFee * MaxEntrantsBound, 9999, MaxEntryFee * MaxEntrantsBound / 10000 * 9999},
		{999_999_999_999_999_999, 10000, 999_999_999_999_999_999},
	}
	for _, tc := range cases {
		if got := bps(tc.x, tc.b); got != tc.want {
			t.Errorf("bps(%d, %d) = %d, want %d", tc.x, tc.b, got, tc.want)
		}
	}
}

// TestComputePool covers the table of design D1: rake 0, rake 9999, one
// entrant, and the voided path.
func TestComputePool(t *testing.T) {
	cases := []struct {
		name             string
		fee              ledger.Money
		rakeBps          uint32
		entrants, scored int
		minEntrants      int
		fees, rake, pool ledger.Money
	}{
		{"rake 0", 500, 0, 4, 4, 3, 2000, 0, 2000},
		{"rake 10%", 500, 1000, 4, 4, 3, 2000, 200, 1800},
		{"rake 9999", 500, 9999, 4, 4, 3, 2000, 1999, 1},
		{"one entrant", 500, 1000, 1, 1, 1, 500, 50, 450},
		{"rounding down", 333, 1000, 3, 3, 3, 999, 99, 900},
		{"voided: too few scored", 500, 1000, 4, 2, 3, 2000, 0, 2000},
		{"voided: nobody scored", 500, 1000, 4, 0, 1, 2000, 0, 2000},
		{"no entrants", 500, 1000, 0, 0, 1, 0, 0, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := baseRules()
			r.EntryFee, r.RakeBps, r.MinEntrants = tc.fee, tc.rakeBps, tc.minEntrants
			r.PrizeBps = []uint32{10000}
			scores := make([]int64, tc.entrants)
			for i := range scores {
				if i < tc.scored {
					scores[i] = int64(i)
				} else {
					scores[i] = -1
				}
			}
			tr := mk(r, players(tc.entrants), scores)
			fees, rake, pool := ComputePool(tr)
			if fees != tc.fees || rake != tc.rake || pool != tc.pool {
				t.Errorf("ComputePool = %d, %d, %d; want %d, %d, %d", fees, rake, pool, tc.fees, tc.rake, tc.pool)
			}
			if pool != fees-rake {
				t.Errorf("pool %d != fees %d - rake %d", pool, fees, rake)
			}
		})
	}
}

// TestStandingsTable: ties, unscored entrants, a single entrant, zero
// scores, under both tie-break rules.
func TestStandingsTable(t *testing.T) {
	type row struct {
		place  int
		player PlayerID
		scored bool
	}
	cases := []struct {
		name   string
		tie    TieBreak
		n      int
		scores []int64
		want   []row
	}{
		{"single entrant", EarliestSubmission, 1, []int64{10}, []row{{1, "pa", true}}},
		{"distinct scores", EarliestSubmission, 3, []int64{5, 9, 7}, []row{{1, "pb", true}, {2, "pc", true}, {3, "pa", true}}},
		{"tie earliest", EarliestSubmission, 3, []int64{9, 9, 1}, []row{{1, "pa", true}, {2, "pb", true}, {3, "pc", true}}},
		{"tie split", Split, 3, []int64{9, 9, 1}, []row{{1, "pa", true}, {1, "pb", true}, {3, "pc", true}}},
		{"three-way tie split then next", Split, 4, []int64{2, 2, 2, 1}, []row{{1, "pa", true}, {1, "pb", true}, {1, "pc", true}, {4, "pd", true}}},
		{"zero scores split", Split, 2, []int64{0, 0}, []row{{1, "pa", true}, {1, "pb", true}}},
		{"unscored after scored", EarliestSubmission, 4, []int64{-1, 3, -1, 8}, []row{{1, "pd", true}, {2, "pb", true}, {3, "pa", false}, {4, "pc", false}}},
		{"unscored under split", Split, 4, []int64{-1, 3, 3, -1}, []row{{1, "pb", true}, {1, "pc", true}, {3, "pa", false}, {4, "pd", false}}},
		{"nobody scored", Split, 2, []int64{-1, -1}, []row{{1, "pa", false}, {2, "pb", false}}},
		{"no entrants", Split, 0, nil, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := baseRules()
			r.TieBreak = tc.tie
			tr := mk(r, players(tc.n), tc.scores)
			got := ComputeStandings(tr)
			if len(got) != len(tc.want) {
				t.Fatalf("got %d standings, want %d: %+v", len(got), len(tc.want), got)
			}
			for i, w := range tc.want {
				g := got[i]
				if g.Place != w.place || g.Player != w.player || g.Scored != w.scored {
					t.Errorf("standing %d = %+v, want place %d player %s scored %t", i, g, w.place, w.player, w.scored)
				}
				if g.Scored && g.SubmitSeq == 0 || !g.Scored && (g.SubmitSeq != 0 || g.Score != 0) {
					t.Errorf("standing %d has inconsistent Scored/SubmitSeq/Score: %+v", i, g)
				}
			}
		})
	}
}

func TestSplitTieBreak(t *testing.T) {
	r := baseRules()
	r.TieBreak = Split
	r.RakeBps = 0
	r.EntryFee = 100
	r.PrizeBps = []uint32{5000, 3000, 2000}
	// Six entrants: pool 600. pa and pb tie for first (shares 300+180=480,
	// 240 each); pc third (120); the rest nothing.
	tr := mk(r, players(6), []int64{50, 50, 40, 10, 10, 10})
	tr.Standings = ComputeStandings(tr)
	got := ComputePayouts(tr, Exclusions{Version: 8})
	if len(got) != 3 {
		t.Fatalf("got %d payouts: %+v", len(got), got)
	}
	want := []Payout{
		{Player: "pa", Place: 1, Amount: 240, ExclusionVersion: 8, Key: ledger.PrizeKey("t", "pa", 1)},
		{Player: "pb", Place: 1, Amount: 240, ExclusionVersion: 8, Key: ledger.PrizeKey("t", "pb", 1)},
		{Player: "pc", Place: 3, Amount: 120, ExclusionVersion: 8, Key: ledger.PrizeKey("t", "pc", 3)},
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("payout %d = %+v, want %+v", i, got[i], want[i])
		}
	}
	// A tie that extends past the last prize place is clipped: four tie
	// for first with three places; shares 300+180+120 = 600 split three
	// ways.
	tr = mk(r, players(6), []int64{50, 50, 50, 50, 10, 10})
	tr.Standings = ComputeStandings(tr)
	got = ComputePayouts(tr, Exclusions{})
	if len(got) != 3 || got[0].Amount != 200 || got[1].Amount != 200 || got[2].Amount != 200 || got[2].Player != "pc" {
		t.Errorf("clipped tie payouts = %+v", got)
	}
	// Remainder units go one each to the earliest submitters.
	r.EntryFee = 1
	r.PrizeBps = []uint32{5000, 3000, 2000}
	tr = mk(r, players(5), []int64{9, 9, 9, 0, 0})
	tr.Standings = ComputeStandings(tr)
	got = ComputePayouts(tr, Exclusions{})
	// pool 5: shares 2 (+1 remainder = 3), 1, 1 -> total 5 over three
	// members: 2, 2, 1 in submission order.
	if got[0].Amount != 2 || got[1].Amount != 2 || got[2].Amount != 1 {
		t.Errorf("remainder distribution = %+v, want 2, 2, 1", got)
	}
}

// TestPayoutRoundingSumsToPool is the property test of design D2: over
// 10 000 random pools, prize tables, tie-break rules and score patterns, the
// payouts sum exactly to the pool, every amount is non-negative, and under
// Split the members of a tie differ by at most one unit.
func TestPayoutRoundingSumsToPool(t *testing.T) {
	rng := rand.New(rand.NewPCG(2026, 9))
	for i := 0; i < 10000; i++ {
		r := baseRules()
		r.EntryFee = ledger.Money(1 + rng.Int64N(1000))
		if rng.IntN(10) == 0 {
			r.EntryFee = ledger.Money(1 + rng.Int64N(MaxEntryFee))
		}
		r.RakeBps = uint32(rng.IntN(10000))
		places := 1 + rng.IntN(5)
		r.PrizeBps = randomPrizeTable(rng, places)
		r.MinEntrants = places
		n := places + rng.IntN(8)
		r.MaxEntrants = n
		if rng.IntN(2) == 0 {
			r.TieBreak = Split
		}
		scores := make([]int64, n)
		distinct := 1 + rng.IntN(4) // few distinct scores force ties
		for j := range scores {
			scores[j] = int64(rng.IntN(distinct))
		}
		tr := mk(r, players(n), scores)
		tr.Standings = ComputeStandings(tr)
		fees, rake, pool := ComputePool(tr)
		if fees != r.EntryFee*ledger.Money(n) || pool != fees-rake || rake != bps(fees, r.RakeBps) {
			t.Fatalf("case %d: pool arithmetic fees=%d rake=%d pool=%d", i, fees, rake, pool)
		}
		got := ComputePayouts(tr, Exclusions{Version: 1})
		if len(got) != places {
			t.Fatalf("case %d: %d payouts for %d places", i, len(got), places)
		}
		var sum ledger.Money
		seen := make(map[ledger.PostingKey]bool)
		for _, p := range got {
			if p.Amount < 0 {
				t.Fatalf("case %d: negative payout %+v", i, p)
			}
			if seen[p.Key] {
				t.Fatalf("case %d: duplicate payout key %s", i, p.Key)
			}
			seen[p.Key] = true
			sum += p.Amount
		}
		if sum != pool {
			t.Fatalf("case %d: payouts sum to %d, pool is %d (rules %+v, scores %v)", i, sum, pool, r, scores)
		}
		if r.TieBreak == Split {
			for a := 0; a < len(got); a++ {
				for b := a + 1; b < len(got); b++ {
					if got[a].Place == got[b].Place {
						d := got[a].Amount - got[b].Amount
						if d < 0 || d > 1 {
							t.Fatalf("case %d: tied payouts differ by %d: %+v %+v", i, d, got[a], got[b])
						}
					}
				}
			}
		}
	}
}

func randomPrizeTable(rng *rand.Rand, places int) []uint32 {
	out := make([]uint32, places)
	remaining := uint32(10000)
	for i := 0; i < places-1; i++ {
		// Leave at least one unit per remaining place.
		maxHere := remaining - uint32(places-1-i)
		out[i] = 1 + uint32(rng.IntN(int(maxHere)))
		remaining -= out[i]
	}
	out[places-1] = remaining
	return out
}

func TestVoidRefunds(t *testing.T) {
	r := baseRules()
	r.MinEntrants = 3
	tr := mk(r, players(4), []int64{10, -1, 20, -1})
	if !tr.Voids() {
		t.Fatal("two scored of three required should void")
	}
	fees, rake, pool := ComputePool(tr)
	if fees != 2000 || rake != 0 || pool != 2000 {
		t.Errorf("ComputePool on a voided tournament = %d, %d, %d", fees, rake, pool)
	}
	got := ComputePayouts(tr, Exclusions{Version: 9, Jurisdictions: []string{"TR"}})
	if len(got) != 4 {
		t.Fatalf("got %d refunds, want 4", len(got))
	}
	var sum ledger.Money
	for i, p := range got {
		if p.Player != tr.Entries[i].Player.ID || p.Place != 0 || p.Amount != 500 || p.Withheld || p.ExclusionVersion != 9 || p.Key != ledger.RefundKey("t", string(p.Player)) {
			t.Errorf("refund %d = %+v", i, p)
		}
		sum += p.Amount
	}
	if sum != fees {
		t.Errorf("refunds sum to %d, want fees %d", sum, fees)
	}
}

func TestWithheldPayouts(t *testing.T) {
	r := baseRules()
	r.RakeBps = 0
	ps := players(3)
	ps[1].Jurisdiction = "YY"
	tr := mk(r, ps, []int64{5, 9, 1})
	tr.Standings = ComputeStandings(tr)
	got := ComputePayouts(tr, Exclusions{Version: 8, Jurisdictions: []string{"XX", "YY"}})
	if len(got) != 3 {
		t.Fatalf("payouts: %+v", got)
	}
	if !got[0].Withheld || got[0].Player != "pb" || got[0].Reason != string(JurisdictionExcluded) || got[0].Key != ledger.WithheldKey("t", "pb", 1) || got[0].Amount != 750 {
		t.Errorf("first payout should be withheld: %+v", got[0])
	}
	if got[1].Withheld || got[1].Key != ledger.PrizeKey("t", "pa", 2) {
		t.Errorf("second payout: %+v", got[1])
	}
	for _, p := range got {
		if p.ExclusionVersion != 8 {
			t.Errorf("payout carries version %d, want 8", p.ExclusionVersion)
		}
	}
}
