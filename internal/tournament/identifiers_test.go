package tournament

import (
	"strings"
	"testing"

	"github.com/oguzhanozfe/paxos-arena/internal/ledger"
)

// TestIdentifierShape checks the identifier rule on both identifiers a
// command carries. ':' is the separator of ledger account names and posting
// keys, and a non-UTF-8 byte encodes to U+FFFD in JSON, so either would let
// two different identifiers share a posting key or a fingerprint.
func TestIdentifierShape(t *testing.T) {
	cases := []struct {
		id string
		ok bool
	}{
		{"t1", true},
		{"Weekly-Cup_2026.09", true},
		{strings.Repeat("a", MaxIDLen), true},
		{"", false},
		{strings.Repeat("a", MaxIDLen+1), false},
		{"t:x", false},
		{"x:p", false},
		{"a/b", false},
		{"a b", false},
		{"a%2Fb", false},
		{"\xff", false},
		{"\xfe", false},
		{"café", false},
	}
	for _, tc := range cases {
		t.Run(tc.id, func(t *testing.T) {
			d := newDriver(t)
			r := d.apply(CreateTournament{ID: TournamentID(tc.id), Seed: 1, Rules: baseRules()})
			if want := map[bool]Code{true: OK, false: InvalidRules}[tc.ok]; r.Code != want {
				t.Fatalf("create %q = %s (%s), want %s", tc.id, r.Code, r.Detail, want)
			}
			d.mustOK(CreateTournament{ID: "t", Seed: 1, Rules: baseRules()})
			r = d.apply(Join{Tournament: "t", Player: player(tc.id, "TR", 30)})
			if want := map[bool]Code{true: OK, false: InvalidPlayer}[tc.ok]; r.Code != want {
				t.Fatalf("join as %q = %s (%s), want %s", tc.id, r.Code, r.Detail, want)
			}
		})
	}
}

// TestListBounds checks the length bounds on the two lists a command
// carries, which keep a command's encoding small enough to replicate.
func TestListBounds(t *testing.T) {
	d := newDriver(t)
	rules := baseRules()
	rules.PrizeBps = make([]uint32, MaxPrizePlaces+1)
	rules.MinEntrants = len(rules.PrizeBps)
	if r := d.apply(CreateTournament{ID: "places", Seed: 1, Rules: rules}); r.Code != InvalidRules {
		t.Errorf("create with %d prize places = %s, want %s", len(rules.PrizeBps), r.Code, InvalidRules)
	}
	many := Exclusions{Version: 1}
	for i := 0; i <= MaxJurisdictions; i++ {
		many.Jurisdictions = append(many.Jurisdictions, string([]byte{'A' + byte(i/26/26%26), 'A' + byte(i/26%26), 'A' + byte(i%26)}))
	}
	rules = baseRules()
	rules.Exclusions = many
	if r := d.apply(CreateTournament{ID: "list", Seed: 1, Rules: rules}); r.Code != InvalidRules {
		t.Errorf("create with %d jurisdictions = %s, want %s", len(many.Jurisdictions), r.Code, InvalidRules)
	}
	d.mustOK(CreateTournament{ID: "t", Seed: 1, Rules: baseRules()})
	d.mustOK(Close{Tournament: "t"})
	if r := d.apply(Settle{Tournament: "t", Exclusions: many}); r.Code != InvalidExclusions {
		t.Errorf("settle with %d jurisdictions = %s, want %s", len(many.Jurisdictions), r.Code, InvalidExclusions)
	}
}

// TestLedgerConflictIsARejection plants a posting under the key a command
// must create. The ledger would treat the command's posting as a retry and
// book nothing; the state machine must reject the command instead of
// recording an entry or a payout that moved no money.
func TestLedgerConflictIsARejection(t *testing.T) {
	plant := func(d *driver, key ledger.PostingKey) {
		d.t.Helper()
		_, ok, err := d.s.Ledger().Post(ledger.Posting{
			Key: key, Kind: ledger.EntryFee, Debit: "player:someone", Credit: "pool:elsewhere",
			Amount: 1, Tournament: "elsewhere", Player: "someone",
		})
		if err != nil || !ok {
			d.t.Fatalf("plant %q: ok=%t err=%v", key, ok, err)
		}
	}

	t.Run("join", func(t *testing.T) {
		d := newDriver(t)
		d.mustOK(CreateTournament{ID: "t", Seed: 1, Rules: baseRules()})
		plant(d, ledger.FeeKey("t", "pa"))
		before, muts := d.s.Ledger().Len(), d.s.Mutations()
		if r := d.apply(Join{Tournament: "t", Player: player("pa", "TR", 30)}); r.Code != LedgerConflict {
			t.Fatalf("join = %s (%s), want %s", r.Code, r.Detail, LedgerConflict)
		}
		tr, _ := d.s.Tournament("t")
		if len(tr.Entries) != 0 || d.s.Ledger().Len() != before || d.s.Mutations() != muts {
			t.Fatalf("rejected join changed state: entries %d, postings %d -> %d, mutations %d -> %d",
				len(tr.Entries), before, d.s.Ledger().Len(), muts, d.s.Mutations())
		}
	})

	t.Run("settle", func(t *testing.T) {
		d := newDriver(t)
		seed := d.setup(baseRules(), 3)
		d.scoreAll(seed, 30, 20, 10)
		d.mustOK(Close{Tournament: "t"})
		plant(d, ledger.PrizeKey("t", "pb", 2))
		before := d.s.Ledger().Len()
		if r := d.apply(Settle{Tournament: "t"}); r.Code != LedgerConflict {
			t.Fatalf("settle = %s (%s), want %s", r.Code, r.Detail, LedgerConflict)
		}
		tr, _ := d.s.Tournament("t")
		if tr.Status != Closed || len(tr.Payouts) != 0 || d.s.Ledger().Len() != before {
			t.Fatalf("rejected settle changed state: status %s, payouts %d, postings %d -> %d",
				tr.Status, len(tr.Payouts), before, d.s.Ledger().Len())
		}
		if bal := d.s.Ledger().Balance(ledger.PoolAccount("t")); bal != 1500 {
			t.Fatalf("pool:t = %d after the rejected settle, want the 1500 in fees", bal)
		}
	})
}
