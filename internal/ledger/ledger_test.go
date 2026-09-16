package ledger

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/oguzhanozfe/paxos-arena/internal/paxos"
)

func fee(tid, pid string, amount Money) Posting {
	return Posting{
		Key: FeeKey(tid, pid), Kind: EntryFee,
		Debit: PlayerAccount(pid), Credit: PoolAccount(tid),
		Amount: amount, Tournament: tid, Player: pid,
		Slot: 3, Ballot: paxos.Ballot{Round: 1, Node: 2},
	}
}

func TestPostAssignsSeqAndMovesMoney(t *testing.T) {
	b := NewBook()
	p, ok, err := b.Post(fee("t1", "p1", 500))
	if err != nil || !ok {
		t.Fatalf("Post = ok %t, err %v", ok, err)
	}
	if p.Seq != 1 {
		t.Errorf("Seq = %d, want 1", p.Seq)
	}
	if got := b.Balance(PlayerAccount("p1")); got != -500 {
		t.Errorf("player balance = %d, want -500", got)
	}
	if got := b.Balance(PoolAccount("t1")); got != 500 {
		t.Errorf("pool balance = %d, want 500", got)
	}
	if got := b.Balance(Account("never")); got != 0 {
		t.Errorf("unknown account balance = %d, want 0", got)
	}
	if !b.Has(FeeKey("t1", "p1")) || b.Has(FeeKey("t1", "p2")) {
		t.Error("Has does not reflect the posting")
	}
	if b.Len() != 1 {
		t.Errorf("Len = %d, want 1", b.Len())
	}
}

// TestPostDuplicateKeyIsNoop: posting a key that exists changes nothing and
// returns the existing posting, even when the amount differs.
func TestPostDuplicateKeyIsNoop(t *testing.T) {
	b := NewBook()
	first, _, _ := b.Post(fee("t1", "p1", 500))
	again := fee("t1", "p1", 900)
	got, ok, err := b.Post(again)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Error("second Post with the same key reported ok=true")
	}
	if got != first {
		t.Errorf("second Post returned %+v, want the first posting %+v", got, first)
	}
	if b.Len() != 1 {
		t.Errorf("Len = %d after a duplicate, want 1", b.Len())
	}
	if got := b.Balance(PoolAccount("t1")); got != 500 {
		t.Errorf("pool balance = %d after a duplicate, want 500", got)
	}
}

func TestPostRejectsInvalidPostings(t *testing.T) {
	cases := []struct {
		name string
		mod  func(*Posting)
		want string
	}{
		{"empty key", func(p *Posting) { p.Key = "" }, "empty key"},
		{"zero amount", func(p *Posting) { p.Amount = 0 }, "non-positive"},
		{"negative amount", func(p *Posting) { p.Amount = -1 }, "non-positive"},
		{"same account", func(p *Posting) { p.Credit = p.Debit }, "same account"},
		{"missing account", func(p *Posting) { p.Debit = "" }, "debit and a credit"},
		{"unknown kind", func(p *Posting) { p.Kind = 99 }, "unknown kind"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := NewBook()
			p := fee("t1", "p1", 100)
			tc.mod(&p)
			_, ok, err := b.Post(p)
			if err == nil || ok {
				t.Fatalf("Post = ok %t, err %v, want an error", ok, err)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q does not mention %q", err, tc.want)
			}
			if b.Len() != 0 {
				t.Error("an invalid posting was appended")
			}
		})
	}
}

// TestBalancesSumZero: however many postings are made, the balances sum to
// zero, and Check passes.
func TestBalancesSumZero(t *testing.T) {
	b := NewBook()
	tid := "t9"
	for i, pid := range []string{"a", "b", "c", "d"} {
		if _, _, err := b.Post(fee(tid, pid, Money(100*(i+1)))); err != nil {
			t.Fatal(err)
		}
	}
	if _, _, err := b.Post(Posting{Key: RakeKey(tid), Kind: Rake, Debit: PoolAccount(tid), Credit: RakeAccount(tid), Amount: 100, Tournament: tid}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := b.Post(Posting{Key: PrizeKey(tid, "d", 1), Kind: Prize, Debit: PoolAccount(tid), Credit: PlayerAccount("d"), Amount: 600, Tournament: tid, Player: "d", Place: 1}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := b.Post(Posting{Key: WithheldKey(tid, "c", 2), Kind: Withheld, Debit: PoolAccount(tid), Credit: WithheldAccount(tid), Amount: 300, Tournament: tid, Player: "c", Place: 2}); err != nil {
		t.Fatal(err)
	}
	var sum Money
	for _, a := range []Account{PlayerAccount("a"), PlayerAccount("b"), PlayerAccount("c"), PlayerAccount("d"), PoolAccount(tid), RakeAccount(tid), WithheldAccount(tid)} {
		sum += b.Balance(a)
	}
	if sum != 0 {
		t.Errorf("balances sum to %d, want 0", sum)
	}
	if got := b.Balance(PoolAccount(tid)); got != 0 {
		t.Errorf("pool balance = %d after paying out everything, want 0", got)
	}
	if err := b.Check(); err != nil {
		t.Errorf("Check: %v", err)
	}
}

// TestCheck: Check accepts a consistent book and rejects one whose internal
// records have been corrupted.
func TestCheck(t *testing.T) {
	b := NewBook()
	if err := b.Check(); err != nil {
		t.Errorf("empty book: %v", err)
	}
	b.Post(fee("t1", "p1", 100))
	b.Post(fee("t1", "p2", 100))
	if err := b.Check(); err != nil {
		t.Errorf("two fees: %v", err)
	}
	corrupt := NewBook()
	corrupt.Post(fee("t1", "p1", 100))
	corrupt.balances[PoolAccount("t1")]++
	if err := corrupt.Check(); err == nil || !strings.Contains(err.Error(), "sum to") {
		t.Errorf("Check on unbalanced book = %v, want a sum error", err)
	}
	corrupt = NewBook()
	corrupt.Post(fee("t1", "p1", 100))
	corrupt.postings[0].Amount = 0
	if err := corrupt.Check(); err == nil {
		t.Error("Check accepted a zero amount")
	}
	corrupt = NewBook()
	corrupt.Post(fee("t1", "p1", 100))
	corrupt.Post(fee("t1", "p2", 100))
	corrupt.postings[1].Key = corrupt.postings[0].Key
	if err := corrupt.Check(); err == nil || !strings.Contains(err.Error(), "twice") {
		t.Errorf("Check on duplicate keys = %v, want a duplicate error", err)
	}
	// Balances that still sum to zero but disagree with the postings.
	corrupt = NewBook()
	corrupt.Post(fee("t1", "p1", 100))
	corrupt.balances[PoolAccount("t1")] -= 40
	corrupt.balances[RakeAccount("t1")] += 40
	if err := corrupt.Check(); err == nil || !strings.Contains(err.Error(), "postings give") {
		t.Errorf("Check on moved balances = %v, want a recomputation error", err)
	}
	corrupt = NewBook()
	corrupt.Post(fee("t1", "p1", 100))
	corrupt.postings[0].Amount = 60
	if err := corrupt.Check(); err == nil || !strings.Contains(err.Error(), "postings give") {
		t.Errorf("Check on an edited amount = %v, want a recomputation error", err)
	}
	corrupt = NewBook()
	corrupt.Post(fee("t1", "p1", 100))
	corrupt.postings[0].Seq = 7
	if err := corrupt.Check(); err == nil || !strings.Contains(err.Error(), "seq") {
		t.Errorf("Check on a wrong seq = %v, want a seq error", err)
	}
}

func TestPostingsAndForTournamentAreCopies(t *testing.T) {
	b := NewBook()
	b.Post(fee("t1", "p1", 100))
	b.Post(fee("t2", "p1", 100))
	b.Post(fee("t1", "p2", 100))
	all := b.Postings()
	if len(all) != 3 || all[0].Seq != 1 || all[2].Seq != 3 {
		t.Fatalf("Postings = %+v", all)
	}
	all[0].Amount = 1
	if b.postings[0].Amount != 100 {
		t.Error("Postings returned the internal slice")
	}
	t1 := b.ForTournament("t1")
	if len(t1) != 2 || t1[0].Player != "p1" || t1[1].Player != "p2" {
		t.Fatalf("ForTournament(t1) = %+v", t1)
	}
	if got := b.ForTournament("none"); len(got) != 0 {
		t.Errorf("ForTournament(none) = %+v, want empty", got)
	}
	if got, ok := b.Get(FeeKey("t2", "p1")); !ok || got.Seq != 2 {
		t.Errorf("Get = %+v, %t", got, ok)
	}
}

func TestHashDependsOnPostingsOnly(t *testing.T) {
	mk := func() *Book {
		b := NewBook()
		b.Post(fee("t1", "p1", 100))
		b.Post(fee("t1", "p2", 200))
		return b
	}
	a, b := mk(), mk()
	if a.Hash() != b.Hash() {
		t.Error("two books with the same postings hash differently")
	}
	if NewBook().Hash() == a.Hash() {
		t.Error("an empty book hashes like a non-empty one")
	}
	c := mk()
	c.Post(fee("t1", "p3", 300))
	if c.Hash() == a.Hash() {
		t.Error("an extra posting did not change the hash")
	}
	d := NewBook()
	d.Post(fee("t1", "p2", 200))
	d.Post(fee("t1", "p1", 100))
	if d.Hash() == a.Hash() {
		t.Error("posting order did not change the hash")
	}
}

func TestKindJSON(t *testing.T) {
	for k, name := range kindNames {
		enc, err := json.Marshal(k)
		if err != nil {
			t.Fatal(err)
		}
		if string(enc) != `"`+name+`"` {
			t.Errorf("Marshal(%v) = %s", k, enc)
		}
		var back Kind
		if err := json.Unmarshal(enc, &back); err != nil || back != k {
			t.Errorf("Unmarshal(%s) = %v, %v", enc, back, err)
		}
	}
	var k Kind
	if err := json.Unmarshal([]byte(`"bogus"`), &k); err == nil {
		t.Error("Unmarshal accepted an unknown kind")
	}
	if _, err := json.Marshal(Kind(42)); err == nil {
		t.Error("Marshal accepted an unknown kind")
	}
	if Kind(42).String() != "kind(42)" || Prize.String() != "prize" {
		t.Error("String rendering")
	}
}

func TestAccountAndKeyShapes(t *testing.T) {
	cases := map[string]string{
		string(PlayerAccount("p-17")):      "player:p-17",
		string(PoolAccount("t1")):          "pool:t1",
		string(RakeAccount("t1")):          "rake:t1",
		string(WithheldAccount("t1")):      "withheld:t1",
		string(FeeKey("t1", "p1")):         "fee:t1:p1",
		string(RakeKey("t1")):              "rake:t1",
		string(PrizeKey("t1", "p1", 2)):    "prize:t1:p1:2",
		string(WithheldKey("t1", "p1", 3)): "withheld:t1:p1:3",
		string(RefundKey("t1", "p1")):      "refund:t1:p1",
		string(ClaimsAccount("t1")):        "claims:t1",
		string(ClaimKey("t1", "p1")):       "claim:t1:p1",
		Claim.String():                     "claim",
	}
	for got, want := range cases {
		if got != want {
			t.Errorf("got %q, want %q", got, want)
		}
	}
}
