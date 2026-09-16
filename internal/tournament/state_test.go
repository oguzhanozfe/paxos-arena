package tournament

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"strings"
	"testing"

	"github.com/oguzhanozfe/paxos-arena/internal/ledger"
	"github.com/oguzhanozfe/paxos-arena/internal/paxos"
)

// driver applies commands to a State with increasing slots and unique keys.
type driver struct {
	t    *testing.T
	s    *State
	slot paxos.Slot
	n    int
	b    paxos.Ballot
}

func newDriver(t *testing.T) *driver {
	return &driver{t: t, s: New(), b: paxos.Ballot{Round: 1, Node: 1}}
}

func (d *driver) key() IdempotencyKey {
	d.n++
	return IdempotencyKey(fmt.Sprintf("k%d", d.n))
}

// apply applies op under a fresh key and returns the result.
func (d *driver) apply(op Op) Result {
	return d.applyKey(d.key(), op)
}

func (d *driver) applyKey(k IdempotencyKey, op Op) Result {
	d.slot++
	return d.s.Apply(d.slot, d.b, Command{Key: k, ReceivedAt: 1, Op: op})
}

// mustOK applies op and fails the test on a rejection.
func (d *driver) mustOK(op Op) Result {
	d.t.Helper()
	r := d.apply(op)
	if r.Code != OK {
		d.t.Fatalf("%s: %s (%s)", OpName(op), r.Code, r.Detail)
	}
	return r
}

func player(id, jur string, age int) Player {
	return Player{ID: PlayerID(id), Jurisdiction: jur, Age: age}
}

// setup creates tournament "t" with baseRules and joins n players pa..;
// it returns the seed.
func (d *driver) setup(rules Rules, n int) uint64 {
	d.t.Helper()
	d.mustOK(CreateTournament{ID: "t", Seed: 4242, Rules: rules})
	var seed uint64
	for i := 0; i < n; i++ {
		r := d.mustOK(Join{Tournament: "t", Player: player("p"+string(rune('a'+i)), "TR", 30)})
		seed = r.Seed
	}
	return seed
}

func (d *driver) scoreAll(seed uint64, scores ...int64) {
	d.t.Helper()
	for i, sc := range scores {
		if sc < 0 {
			continue
		}
		d.mustOK(SubmitScore{Tournament: "t", Player: PlayerID("p" + string(rune('a'+i))), Score: sc, DealSeed: seed})
	}
}

func TestCreateValidation(t *testing.T) {
	cases := []struct {
		name string
		mod  func(*CreateTournament)
		want Code
	}{
		{"valid", func(*CreateTournament) {}, OK},
		{"empty id", func(c *CreateTournament) { c.ID = "" }, InvalidRules},
		{"long id", func(c *CreateTournament) { c.ID = TournamentID(strings.Repeat("x", 65)) }, InvalidRules},
		{"zero fee", func(c *CreateTournament) { c.Rules.EntryFee = 0 }, InvalidRules},
		{"huge fee", func(c *CreateTournament) { c.Rules.EntryFee = MaxEntryFee + 1 }, InvalidRules},
		{"rake 10000", func(c *CreateTournament) { c.Rules.RakeBps = 10000 }, InvalidRules},
		{"no prizes", func(c *CreateTournament) { c.Rules.PrizeBps = nil }, InvalidRules},
		{"zero prize share", func(c *CreateTournament) { c.Rules.PrizeBps = []uint32{10000, 0} }, InvalidRules},
		{"prizes not 10000", func(c *CreateTournament) { c.Rules.PrizeBps = []uint32{5000, 4000} }, InvalidRules},
		{"min below places", func(c *CreateTournament) { c.Rules.MinEntrants = 2 }, InvalidRules},
		{"max below min", func(c *CreateTournament) { c.Rules.MaxEntrants = 2 }, InvalidRules},
		{"max above bound", func(c *CreateTournament) { c.Rules.MaxEntrants = MaxEntrantsBound + 1 }, InvalidRules},
		{"negative max score", func(c *CreateTournament) { c.Rules.MaxScore = -1 }, InvalidRules},
		{"negative min age", func(c *CreateTournament) { c.Rules.MinAge = -1 }, InvalidRules},
		{"bad tie break", func(c *CreateTournament) { c.Rules.TieBreak = "coin" }, InvalidRules},
		{"bad jurisdiction", func(c *CreateTournament) { c.Rules.Exclusions.Jurisdictions = []string{"xx"} }, InvalidRules},
		{"split ok", func(c *CreateTournament) { c.Rules.TieBreak = Split }, OK},
		{"no exclusions ok", func(c *CreateTournament) { c.Rules.Exclusions = Exclusions{} }, OK},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := newDriver(t)
			c := CreateTournament{ID: "t", Seed: 1, Rules: baseRules()}
			tc.mod(&c)
			r := d.apply(c)
			if r.Code != tc.want {
				t.Fatalf("code %s (%s), want %s", r.Code, r.Detail, tc.want)
			}
			if _, ok := d.s.Tournament(c.ID); ok != (tc.want == OK) {
				t.Errorf("tournament exists = %t after %s", ok, r.Code)
			}
			if r.Slot != 1 {
				t.Errorf("Slot = %d, want 1", r.Slot)
			}
		})
	}
	d := newDriver(t)
	d.mustOK(CreateTournament{ID: "t", Seed: 1, Rules: baseRules()})
	if r := d.apply(CreateTournament{ID: "t", Seed: 2, Rules: baseRules()}); r.Code != TournamentExists {
		t.Errorf("second create = %s, want %s", r.Code, TournamentExists)
	}
	if got := d.s.Tournaments(); len(got) != 1 || got[0] != "t" {
		t.Errorf("Tournaments = %v", got)
	}
}

// TestJoinEligibility is the D5 unit test: the creation-time list and the
// minimum age are enforced at Join, and the version is recorded on the
// entry and on the fee posting.
func TestJoinEligibility(t *testing.T) {
	rules := baseRules()
	rules.MaxEntrants = 3
	cases := []struct {
		name string
		p    Player
		want Code
	}{
		{"ok", player("p1", "TR", 18), OK},
		{"excluded", player("p2", "XX", 30), JurisdictionExcluded},
		{"underage", player("p3", "TR", 17), Underage},
		{"already joined", player("p1", "DE", 40), AlreadyJoined},
		{"bad id", player("", "TR", 30), InvalidPlayer},
		{"bad jurisdiction", player("p4", "t", 30), InvalidPlayer},
		{"bad age", player("p4", "TR", 151), InvalidPlayer},
		{"ok two", player("p5", "DE", 99), OK},
		{"ok three", player("p6", "US", 18), OK},
		{"full", player("p7", "US", 18), TournamentFull},
	}
	d := newDriver(t)
	d.mustOK(CreateTournament{ID: "t", Seed: 99, Rules: rules})
	if r := d.apply(Join{Tournament: "nope", Player: player("p1", "TR", 30)}); r.Code != UnknownTournament {
		t.Errorf("unknown tournament = %s", r.Code)
	}
	for _, tc := range cases {
		r := d.apply(Join{Tournament: "t", Player: tc.p})
		if r.Code != tc.want {
			t.Errorf("%s: code %s (%s), want %s", tc.name, r.Code, r.Detail, tc.want)
		}
		if tc.want == OK && r.Seed != 99 {
			t.Errorf("%s: Seed = %d, want 99", tc.name, r.Seed)
		}
	}
	tr, _ := d.s.Tournament("t")
	if len(tr.Entries) != 3 {
		t.Fatalf("entries = %+v", tr.Entries)
	}
	for i, e := range tr.Entries {
		if e.JoinSeq != uint32(i+1) || e.ExclusionVersion != 7 || e.Scored {
			t.Errorf("entry %d = %+v", i, e)
		}
		post, ok := d.s.Ledger().Get(ledger.FeeKey("t", string(e.Player.ID)))
		if !ok || post.Amount != 500 || post.ExclusionVersion != 7 || post.Kind != ledger.EntryFee || post.Slot == 0 || post.Ballot != d.b {
			t.Errorf("fee posting for %s = %+v, %t", e.Player.ID, post, ok)
		}
	}
	if got := d.s.Ledger().Len(); got != 3 {
		t.Errorf("%d postings after 3 joins and rejections, want 3", got)
	}
	if got := d.s.Ledger().Balance(ledger.PoolAccount("t")); got != 1500 {
		t.Errorf("pool balance = %d, want 1500", got)
	}
	// Closing makes further joins not_open.
	for _, id := range []PlayerID{"p1", "p5", "p6"} {
		d.mustOK(SubmitScore{Tournament: "t", Player: id, Score: 1, DealSeed: 99})
	}
	d.mustOK(Close{Tournament: "t"})
	if r := d.apply(Join{Tournament: "t", Player: player("p9", "TR", 30)}); r.Code != NotOpen {
		t.Errorf("join after close = %s", r.Code)
	}
}

func TestSubmitScoreValidation(t *testing.T) {
	d := newDriver(t)
	rules := baseRules()
	rules.MaxScore = 100
	seed := d.setup(rules, 2)
	cases := []struct {
		name string
		op   SubmitScore
		want Code
	}{
		{"unknown", SubmitScore{Tournament: "x", Player: "pa", Score: 1, DealSeed: seed}, UnknownTournament},
		{"not joined", SubmitScore{Tournament: "t", Player: "pz", Score: 1, DealSeed: seed}, NotJoined},
		{"seed mismatch", SubmitScore{Tournament: "t", Player: "pa", Score: 1, DealSeed: seed + 1}, SeedMismatch},
		{"negative", SubmitScore{Tournament: "t", Player: "pa", Score: -1, DealSeed: seed}, ScoreOutOfRange},
		{"too high", SubmitScore{Tournament: "t", Player: "pa", Score: 101, DealSeed: seed}, ScoreOutOfRange},
		{"ok", SubmitScore{Tournament: "t", Player: "pa", Score: 100, DealSeed: seed, InputDigest: Digest{1, 2}}, OK},
		{"already scored", SubmitScore{Tournament: "t", Player: "pa", Score: 5, DealSeed: seed}, AlreadyScored},
		{"ok second", SubmitScore{Tournament: "t", Player: "pb", Score: 0, DealSeed: seed}, OK},
	}
	for _, tc := range cases {
		if r := d.apply(tc.op); r.Code != tc.want {
			t.Errorf("%s: code %s (%s), want %s", tc.name, r.Code, r.Detail, tc.want)
		}
	}
	tr, _ := d.s.Tournament("t")
	e, _ := tr.Entry("pa")
	if !e.Scored || e.Score != 100 || e.SubmitSeq != 1 || e.InputDigest != (Digest{1, 2}) {
		t.Errorf("entry pa = %+v", e)
	}
	e, _ = tr.Entry("pb")
	if !e.Scored || e.Score != 0 || e.SubmitSeq != 2 {
		t.Errorf("entry pb = %+v", e)
	}
	d.mustOK(Close{Tournament: "t"})
	if r := d.apply(SubmitScore{Tournament: "t", Player: "pb", Score: 1, DealSeed: seed}); r.Code != NotOpen {
		t.Errorf("score after close = %s", r.Code)
	}
}

// TestSettlePostings checks the Closed path end to end: the rake posting,
// one prize posting per payout in place order, the pool balance at zero,
// and slot and ballot on every posting.
func TestSettlePostings(t *testing.T) {
	d := newDriver(t)
	d.b = paxos.Ballot{Round: 3, Node: 2}
	seed := d.setup(baseRules(), 4) // fees 2000, rake 200, pool 1800
	d.scoreAll(seed, 10, 40, 30, 20)
	r := d.mustOK(Close{Tournament: "t"})
	tr, _ := d.s.Tournament("t")
	if tr.Status != Closed || tr.Fees != 2000 || tr.Rake != 200 || tr.Pool != 1800 || tr.ClosedAt != r.Slot {
		t.Fatalf("after close: %+v", tr)
	}
	if len(d.s.Ledger().ForTournament("t")) != 4 {
		t.Fatalf("close must post nothing; postings: %+v", d.s.Ledger().ForTournament("t"))
	}
	if got := d.apply(Settle{Tournament: "t", Exclusions: Exclusions{Version: 8, Jurisdictions: []string{"xx"}}}); got.Code != InvalidExclusions {
		t.Errorf("malformed settle list = %s", got.Code)
	}
	settleBallot := paxos.Ballot{Round: 9, Node: 5}
	d.b = settleBallot
	r = d.mustOK(Settle{Tournament: "t", Exclusions: Exclusions{Version: 8, Jurisdictions: []string{"XX"}}})
	tr, _ = d.s.Tournament("t")
	if tr.Status != Settled || tr.SettledAt != r.Slot || tr.Ballot != settleBallot {
		t.Fatalf("after settle: status %s settled_at %d ballot %v", tr.Status, tr.SettledAt, tr.Ballot)
	}
	wantPayouts := []Payout{
		{Player: "pb", Place: 1, Amount: 900, ExclusionVersion: 8, Key: ledger.PrizeKey("t", "pb", 1)},
		{Player: "pc", Place: 2, Amount: 540, ExclusionVersion: 8, Key: ledger.PrizeKey("t", "pc", 2)},
		{Player: "pd", Place: 3, Amount: 360, ExclusionVersion: 8, Key: ledger.PrizeKey("t", "pd", 3)},
	}
	if len(tr.Payouts) != 3 {
		t.Fatalf("payouts: %+v", tr.Payouts)
	}
	for i := range wantPayouts {
		if tr.Payouts[i] != wantPayouts[i] {
			t.Errorf("payout %d = %+v, want %+v", i, tr.Payouts[i], wantPayouts[i])
		}
	}
	posts := d.s.Ledger().ForTournament("t")
	if len(posts) != 8 {
		t.Fatalf("got %d postings, want 4 fees + rake + 3 prizes: %+v", len(posts), posts)
	}
	kinds := []ledger.Kind{ledger.EntryFee, ledger.EntryFee, ledger.EntryFee, ledger.EntryFee, ledger.Rake, ledger.Prize, ledger.Prize, ledger.Prize}
	for i, p := range posts {
		if p.Kind != kinds[i] {
			t.Errorf("posting %d kind %s, want %s", i, p.Kind, kinds[i])
		}
		if p.Slot == 0 || p.Ballot.IsZero() {
			t.Errorf("posting %d lacks slot or ballot: %+v", i, p)
		}
	}
	if posts[4].Amount != 200 || posts[4].Credit != ledger.RakeAccount("t") || posts[4].Slot != r.Slot || posts[4].Ballot != settleBallot {
		t.Errorf("rake posting = %+v", posts[4])
	}
	if posts[5].Key != ledger.PrizeKey("t", "pb", 1) || posts[5].Amount != 900 || posts[5].Place != 1 || posts[5].ExclusionVersion != 8 {
		t.Errorf("first prize posting = %+v", posts[5])
	}
	book := d.s.Ledger()
	if got := book.Balance(ledger.PoolAccount("t")); got != 0 {
		t.Errorf("pool balance after settle = %d, want 0", got)
	}
	if got := book.Balance(ledger.RakeAccount("t")); got != 200 {
		t.Errorf("rake balance = %d, want 200", got)
	}
	if got := book.Balance(ledger.PlayerAccount("pb")); got != 400 {
		t.Errorf("winner balance = %d, want 900 - 500", got)
	}
	if err := book.Check(); err != nil {
		t.Error(err)
	}
	if got := d.apply(Settle{Tournament: "t", Exclusions: Exclusions{Version: 8}}); got.Code != NotClosed {
		t.Errorf("second settle with a new key = %s, want %s", got.Code, NotClosed)
	}
}

// TestSettleWithholdsNewlyExcluded is the D5 test for the settlement-time
// list: a jurisdiction added between creation and settlement is withheld,
// with the new version recorded, and the totals do not change.
func TestSettleWithholdsNewlyExcluded(t *testing.T) {
	d := newDriver(t)
	rules := baseRules()
	rules.RakeBps = 0
	d.mustOK(CreateTournament{ID: "t", Seed: 5, Rules: rules})
	if r := d.apply(Join{Tournament: "t", Player: player("px", "XX", 30)}); r.Code != JurisdictionExcluded {
		t.Fatalf("version 7 entrant = %s", r.Code)
	}
	d.mustOK(Join{Tournament: "t", Player: player("pa", "TR", 30)})
	d.mustOK(Join{Tournament: "t", Player: player("pb", "YY", 30)})
	d.mustOK(Join{Tournament: "t", Player: player("pc", "TR", 30)})
	d.scoreAll(5, 10, 90, 50)
	d.mustOK(Close{Tournament: "t"})
	d.mustOK(Settle{Tournament: "t", Exclusions: Exclusions{Version: 8, Jurisdictions: []string{"YY", "XX", "YY"}}})
	tr, _ := d.s.Tournament("t")
	if len(tr.Payouts) != 3 {
		t.Fatalf("payouts: %+v", tr.Payouts)
	}
	w := tr.Payouts[0]
	if w.Player != "pb" || !w.Withheld || w.Reason != string(JurisdictionExcluded) || w.ExclusionVersion != 8 || w.Amount != 750 || w.Key != ledger.WithheldKey("t", "pb", 1) {
		t.Errorf("withheld payout = %+v", w)
	}
	for _, p := range tr.Payouts[1:] {
		if p.Withheld || p.ExclusionVersion != 8 {
			t.Errorf("payout %+v", p)
		}
	}
	book := d.s.Ledger()
	if got := book.Balance(ledger.WithheldAccount("t")); got != 750 {
		t.Errorf("withheld balance = %d, want 750", got)
	}
	if got := book.Balance(ledger.PlayerAccount("pb")); got != -500 {
		t.Errorf("excluded player's balance = %d, want -500 (fee only)", got)
	}
	if got := book.Balance(ledger.PoolAccount("t")); got != 0 {
		t.Errorf("pool balance = %d", got)
	}
	var sum ledger.Money
	for _, p := range tr.Payouts {
		sum += p.Amount
	}
	if sum != tr.Pool {
		t.Errorf("payouts sum %d != pool %d", sum, tr.Pool)
	}
	for _, e := range tr.Entries {
		if e.ExclusionVersion != 7 {
			t.Errorf("entry %s carries version %d, want 7", e.Player.ID, e.ExclusionVersion)
		}
	}
}

func TestVoidedSettleRefunds(t *testing.T) {
	d := newDriver(t)
	seed := d.setup(baseRules(), 4)
	d.scoreAll(seed, 10, -1, 30, -1)
	d.mustOK(Close{Tournament: "t"})
	tr, _ := d.s.Tournament("t")
	if tr.Status != Voided || tr.Rake != 0 || tr.Pool != 2000 || tr.Fees != 2000 {
		t.Fatalf("after close: %+v", tr)
	}
	d.mustOK(Settle{Tournament: "t", Exclusions: Exclusions{Version: 8, Jurisdictions: []string{"TR"}}})
	tr, _ = d.s.Tournament("t")
	if tr.Status != Settled || len(tr.Payouts) != 4 {
		t.Fatalf("after settle: %+v", tr)
	}
	posts := d.s.Ledger().ForTournament("t")
	if len(posts) != 8 {
		t.Fatalf("postings: %+v", posts)
	}
	for _, p := range posts[4:] {
		if p.Kind != ledger.Refund || p.Amount != 500 || p.Credit != ledger.PlayerAccount(p.Player) {
			t.Errorf("refund posting = %+v", p)
		}
	}
	for _, id := range []string{"pa", "pb", "pc", "pd"} {
		if got := d.s.Ledger().Balance(ledger.PlayerAccount(id)); got != 0 {
			t.Errorf("player %s balance = %d after refund, want 0", id, got)
		}
	}
	if got := d.s.Ledger().Balance(ledger.PoolAccount("t")); got != 0 {
		t.Errorf("pool balance = %d", got)
	}
	if d.s.Ledger().Has(ledger.RakeKey("t")) {
		t.Error("a voided tournament posted rake")
	}
}

// TestSettledTournamentRejectsAllCommands is the D3 unit test: after
// Settle, Join, SubmitScore, Close and Settle under new keys are rejected
// and the record and postings are unchanged.
func TestSettledTournamentRejectsAllCommands(t *testing.T) {
	d := newDriver(t)
	seed := d.setup(baseRules(), 3)
	d.scoreAll(seed, 1, 2, 3)
	d.mustOK(Close{Tournament: "t"})
	d.mustOK(Settle{Tournament: "t", Exclusions: Exclusions{Version: 8}})
	before, _ := d.s.Tournament("t")
	beforeEnc := EncodeTournament(before)
	beforePosts := d.s.Ledger().ForTournament("t")
	cases := []struct {
		op   Op
		want Code
	}{
		{Join{Tournament: "t", Player: player("pz", "TR", 30)}, NotOpen},
		{SubmitScore{Tournament: "t", Player: "pa", Score: 1, DealSeed: seed}, NotOpen},
		{Close{Tournament: "t"}, NotOpen},
		{Settle{Tournament: "t", Exclusions: Exclusions{Version: 9}}, NotClosed},
		{CreateTournament{ID: "t", Seed: 1, Rules: baseRules()}, TournamentExists},
	}
	for _, tc := range cases {
		if r := d.apply(tc.op); r.Code != tc.want {
			t.Errorf("%s after settle = %s (%s), want %s", OpName(tc.op), r.Code, r.Detail, tc.want)
		}
	}
	after, _ := d.s.Tournament("t")
	if !bytes.Equal(EncodeTournament(after), beforeEnc) {
		t.Error("the settled record changed")
	}
	afterPosts := d.s.Ledger().ForTournament("t")
	if len(afterPosts) != len(beforePosts) {
		t.Errorf("postings changed from %d to %d", len(beforePosts), len(afterPosts))
	}
}

// TestApplyReplaysRecordedResult is the S8 unit test: a second command
// with the same key and payload returns the first result with Replayed
// set, whether the first was a success or a rejection, and has no effect.
func TestApplyReplaysRecordedResult(t *testing.T) {
	d := newDriver(t)
	d.mustOK(CreateTournament{ID: "t", Seed: 3, Rules: baseRules()})
	join := Join{Tournament: "t", Player: player("pa", "TR", 30)}
	first := d.applyKey("join-a", join)
	if first.Code != OK || first.Replayed || first.Slot != 2 || first.Seed != 3 {
		t.Fatalf("first join = %+v", first)
	}
	again := d.applyKey("join-a", join)
	if again.Code != OK || !again.Replayed || again.Slot != 2 || again.Seed != 3 {
		t.Errorf("replayed join = %+v, want the first result with Replayed", again)
	}
	if got := d.s.Ledger().Len(); got != 1 {
		t.Errorf("%d fee postings after a replay, want 1", got)
	}
	tr, _ := d.s.Tournament("t")
	if len(tr.Entries) != 1 {
		t.Errorf("%d entries after a replay, want 1", len(tr.Entries))
	}
	// A rejection is recorded and replayed too, and a retry with a
	// different key is judged afresh.
	bad := Join{Tournament: "t", Player: player("px", "XX", 30)}
	r1 := d.applyKey("join-x", bad)
	r2 := d.applyKey("join-x", bad)
	if r1.Code != JurisdictionExcluded || r2.Code != JurisdictionExcluded || !r2.Replayed || r2.Slot != r1.Slot {
		t.Errorf("rejection replay: %+v then %+v", r1, r2)
	}
	if rec, ok := d.s.Result("join-x"); !ok || rec.Replayed || rec.Code != JurisdictionExcluded {
		t.Errorf("Result(join-x) = %+v, %t", rec, ok)
	}
	if _, ok := d.s.Result("never"); ok {
		t.Error("Result reported an unknown key")
	}
	// A Create retried through another leader carries a different seed
	// but the same fingerprint: replayed, first seed kept.
	c1 := d.applyKey("create-u", CreateTournament{ID: "u", Seed: 10, Rules: baseRules()})
	c2 := d.applyKey("create-u", CreateTournament{ID: "u", Seed: 11, Rules: baseRules()})
	if c1.Code != OK || c2.Code != OK || !c2.Replayed {
		t.Errorf("create retry: %+v then %+v", c1, c2)
	}
	if u, _ := d.s.Tournament("u"); u.Seed != 10 {
		t.Errorf("seed after a retried create = %d, want 10", u.Seed)
	}
}

// TestKeyReusedWithDifferentPayload: the same key with a different
// fingerprint is answered key_reused, changes nothing and is not recorded,
// so the original result is still replayed afterwards.
func TestKeyReusedWithDifferentPayload(t *testing.T) {
	d := newDriver(t)
	d.mustOK(CreateTournament{ID: "t", Seed: 3, Rules: baseRules()})
	join := Join{Tournament: "t", Player: player("pa", "TR", 30)}
	first := d.applyKey("k", join)
	mutated := join
	mutated.Player.Age = 31
	r := d.applyKey("k", mutated)
	if r.Code != KeyReused || r.Replayed {
		t.Fatalf("mutated retry = %+v, want %s", r, KeyReused)
	}
	if r.Slot != 3 {
		t.Errorf("key_reused Slot = %d, want the slot it was applied in (3)", r.Slot)
	}
	other := d.applyKey("k", SubmitScore{Tournament: "t", Player: "pa", Score: 1, DealSeed: 3})
	if other.Code != KeyReused {
		t.Errorf("different op under the key = %s", other.Code)
	}
	tr, _ := d.s.Tournament("t")
	if len(tr.Entries) != 1 || tr.Entries[0].Player.Age != 30 || tr.Entries[0].Scored {
		t.Errorf("state changed by a key_reused command: %+v", tr.Entries)
	}
	if d.s.Ledger().Len() != 1 {
		t.Errorf("postings = %d, want 1", d.s.Ledger().Len())
	}
	again := d.applyKey("k", join)
	if !again.Replayed || again.Slot != first.Slot {
		t.Errorf("original payload after key_reused = %+v, want a replay of %+v", again, first)
	}
	if got := d.s.Commands(); got != 5 {
		t.Errorf("Commands = %d, want 5 (every Apply counts, key_reused included)", got)
	}
}

func TestApplyKeyAndOrderGuards(t *testing.T) {
	s := New()
	b := paxos.Ballot{Round: 1, Node: 1}
	if r := s.Apply(1, b, Command{Key: "", Op: Close{Tournament: "t"}}); r.Code != InvalidKey {
		t.Errorf("empty key = %s", r.Code)
	}
	if r := s.Apply(2, b, Command{Key: "has space", Op: Close{Tournament: "t"}}); r.Code != InvalidKey {
		t.Errorf("key with a space = %s", r.Code)
	}
	if r := s.Apply(3, b, Command{Key: IdempotencyKey(strings.Repeat("k", 129)), Op: Close{Tournament: "t"}}); r.Code != InvalidKey {
		t.Errorf("long key = %s", r.Code)
	}
	if s.Applied() != 3 {
		t.Errorf("Applied = %d, want 3: invalid commands still consume their slot", s.Applied())
	}
	if r := s.Apply(3, b, Command{Key: "k", Op: Close{Tournament: "t"}}); r.Code != OutOfOrder {
		t.Errorf("slot 3 twice = %s", r.Code)
	}
	if r := s.Apply(2, b, Command{Key: "k", Op: Close{Tournament: "t"}}); r.Code != OutOfOrder {
		t.Errorf("slot 2 after 3 = %s", r.Code)
	}
	if _, ok := s.Result("k"); ok {
		t.Error("an out-of-order command was recorded")
	}
	if r := s.Apply(4, b, Command{Key: "k", Op: nil}); r.Code != InvalidOp {
		t.Errorf("nil op = %s", r.Code)
	}
	if _, ok := s.Result("k"); ok {
		t.Error("an invalid op was recorded")
	}
	h := s.Hash()
	s.Skip(4)
	if s.Hash() != h || s.Applied() != 4 {
		t.Error("Skip of an applied slot changed something")
	}
	s.Skip(10)
	if s.Hash() == h || s.Applied() != 10 {
		t.Error("Skip did not advance or mix the slot")
	}
	if r := s.Apply(10, b, Command{Key: "k", Op: Close{Tournament: "t"}}); r.Code != OutOfOrder {
		t.Errorf("apply at a skipped slot = %s", r.Code)
	}
}

func TestTournamentReturnsDeepCopy(t *testing.T) {
	d := newDriver(t)
	seed := d.setup(baseRules(), 2)
	d.scoreAll(seed, 1, 2)
	tr, ok := d.s.Tournament("t")
	if !ok {
		t.Fatal("missing")
	}
	tr.Entries[0].Score = 999
	tr.Rules.PrizeBps[0] = 1
	tr.Rules.Exclusions.Jurisdictions[0] = "ZZ"
	again, _ := d.s.Tournament("t")
	if again.Entries[0].Score == 999 || again.Rules.PrizeBps[0] == 1 || again.Rules.Exclusions.Jurisdictions[0] == "ZZ" {
		t.Error("Tournament returned shared slices")
	}
	if _, ok := d.s.Tournament("none"); ok {
		t.Error("unknown tournament reported present")
	}
	ids := d.s.Tournaments()
	ids[0] = "changed"
	if d.s.Tournaments()[0] != "t" {
		t.Error("Tournaments returned the internal slice")
	}
}

// workflow runs a random but seeded sequence of commands and returns the
// commands with the slots they were applied in.
func workflow(rng *rand.Rand, n int) []Command {
	var cmds []Command
	key := 0
	next := func(op Op) Command {
		key++
		return Command{Key: IdempotencyKey(fmt.Sprintf("w%d", key)), ReceivedAt: int64(key), Op: op}
	}
	for i := 0; i < n; i++ {
		tid := TournamentID(fmt.Sprintf("t%d", i))
		rules := baseRules()
		rules.RakeBps = uint32(rng.IntN(3000))
		if rng.IntN(2) == 0 {
			rules.TieBreak = Split
		}
		cmds = append(cmds, next(CreateTournament{ID: tid, Seed: rng.Uint64(), Rules: rules}))
		entrants := 2 + rng.IntN(6)
		for p := 0; p < entrants; p++ {
			jur := "TR"
			if rng.IntN(8) == 0 {
				jur = "XX"
			}
			cmds = append(cmds, next(Join{Tournament: tid, Player: player(fmt.Sprintf("p%d", p), jur, 16+rng.IntN(30))}))
		}
		for p := 0; p < entrants; p++ {
			if rng.IntN(5) == 0 {
				continue
			}
			cmds = append(cmds, next(SubmitScore{Tournament: tid, Player: PlayerID(fmt.Sprintf("p%d", p)), Score: int64(rng.IntN(4)), DealSeed: 0}))
		}
		cmds = append(cmds, next(Close{Tournament: tid}))
		cmds = append(cmds, next(Settle{Tournament: tid, Exclusions: Exclusions{Version: 8, Jurisdictions: []string{"XX", "DE"}}}))
		// Retries and reuse of earlier keys.
		if len(cmds) > 3 {
			cmds = append(cmds, cmds[rng.IntN(len(cmds))])
		}
	}
	return cmds
}

// TestReplayDeterministic is the S2 unit test: two states fed the same
// commands report equal hashes at every slot, a fresh state replaying the
// log reproduces the final hash, and different logs give different hashes.
func TestReplayDeterministic(t *testing.T) {
	rng := rand.New(rand.NewPCG(7, 7))
	cmds := workflow(rng, 6)
	// Scores need the seed: patch DealSeed to the tournament's seed.
	seeds := map[TournamentID]uint64{}
	for i, c := range cmds {
		switch op := c.Op.(type) {
		case CreateTournament:
			seeds[op.ID] = op.Seed
		case SubmitScore:
			op.DealSeed = seeds[op.Tournament]
			cmds[i].Op = op
		}
	}
	a, b := New(), New()
	ballot := paxos.Ballot{Round: 2, Node: 1}
	var hashes [][32]byte
	oks, rejects := 0, 0
	for i, c := range cmds {
		slot := paxos.Slot(i + 1)
		if slot%7 == 0 {
			a.Skip(slot)
			b.Skip(slot)
			hashes = append(hashes, a.Hash())
			continue
		}
		ra := a.Apply(slot, ballot, c)
		rb := b.Apply(slot, ballot, c)
		if ra != rb {
			t.Fatalf("slot %d: results differ: %+v vs %+v", slot, ra, rb)
		}
		if ra.Code == OK && !ra.Replayed {
			oks++
		} else if ra.Code != OK {
			rejects++
		}
		if a.Hash() != b.Hash() {
			t.Fatalf("slot %d: hashes differ", slot)
		}
		hashes = append(hashes, a.Hash())
		if err := a.Ledger().Check(); err != nil {
			t.Fatalf("slot %d: %v", slot, err)
		}
	}
	if oks == 0 || rejects == 0 {
		t.Fatalf("workflow exercised %d successes and %d rejections", oks, rejects)
	}
	// Replay from a fresh state through the encoded form.
	c := New()
	for i, cmd := range cmds {
		slot := paxos.Slot(i + 1)
		if slot%7 == 0 {
			c.Skip(slot)
		} else {
			enc, err := Encode(cmd)
			if err != nil {
				t.Fatal(err)
			}
			dec, err := Decode(enc)
			if err != nil {
				t.Fatal(err)
			}
			c.Apply(slot, ballot, dec)
		}
		if c.Hash() != hashes[i] {
			t.Fatalf("replay through the codec diverged at slot %d", slot)
		}
	}
	for _, id := range a.Tournaments() {
		ta, _ := a.Tournament(id)
		tc, _ := c.Tournament(id)
		if !bytes.Equal(EncodeTournament(ta), EncodeTournament(tc)) {
			t.Errorf("tournament %s differs after replay", id)
		}
	}
	if a.Ledger().Hash() != c.Ledger().Hash() {
		t.Error("ledger differs after replay")
	}
	// A different ballot for the same commands changes the postings'
	// audit fields and therefore the hash.
	d := New()
	for i, cmd := range cmds {
		slot := paxos.Slot(i + 1)
		if slot%7 == 0 {
			d.Skip(slot)
			continue
		}
		d.Apply(slot, paxos.Ballot{Round: 3, Node: 2}, cmd)
	}
	if d.Hash() == a.Hash() {
		t.Error("different ballots produced the same hash")
	}
}
