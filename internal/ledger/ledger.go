// Package ledger is an append-only double-entry book with idempotent
// postings. Every posting moves one positive amount from one account to
// another and carries a key; posting a key that already exists is a no-op
// that returns the existing posting. Balances are derived from postings, so
// the sum of all balances is always zero.
//
// The package has no goroutines, no clock and no randomness. A Book has a
// single owner; the tournament state machine owns one per replica.
package ledger

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"

	"github.com/oguzhanozfe/paxos-arena/internal/paxos"
)

// Money is an amount in minor units. Nothing in the module uses a float for
// money.
type Money int64

// Account names one balance. The shapes used by the tournament state
// machine are built by PlayerAccount, PoolAccount, RakeAccount,
// WithheldAccount and ClaimsAccount.
type Account string

// PostingKey is the idempotency key of one posting.
type PostingKey string

// Kind says why a posting was made.
type Kind uint8

const (
	// EntryFee moves a player's entry fee into a tournament's pool.
	EntryFee Kind = iota + 1
	// Rake moves the operator's share out of the pool.
	Rake
	// Prize moves a prize from the pool to a player.
	Prize
	// Withheld moves a prize the player may not receive into a holding
	// account.
	Withheld
	// Refund returns an entry fee from the pool to a player.
	Refund
	// Claim moves a player's settled payout from the player's account to
	// the tournament's claims account, once, when the player claims it.
	Claim
)

var kindNames = map[Kind]string{
	EntryFee: "entry_fee",
	Rake:     "rake",
	Prize:    "prize",
	Withheld: "withheld",
	Refund:   "refund",
	Claim:    "claim",
}

// String returns the lower-case kind name.
func (k Kind) String() string {
	if s, ok := kindNames[k]; ok {
		return s
	}
	return fmt.Sprintf("kind(%d)", uint8(k))
}

// MarshalText renders the kind by name.
func (k Kind) MarshalText() ([]byte, error) {
	s, ok := kindNames[k]
	if !ok {
		return nil, fmt.Errorf("ledger: unknown kind %d", uint8(k))
	}
	return []byte(s), nil
}

// UnmarshalText parses a kind name.
func (k *Kind) UnmarshalText(b []byte) error {
	for kind, name := range kindNames {
		if name == string(b) {
			*k = kind
			return nil
		}
	}
	return fmt.Errorf("ledger: unknown kind %q", string(b))
}

// Posting is one money movement. Seq is assigned by Post; every other field
// is supplied by the caller.
type Posting struct {
	// Seq is the 1-based append order, assigned by Post.
	Seq uint64 `json:"seq"`
	// Key is the idempotency key; a second Post with the same key is a
	// no-op.
	Key PostingKey `json:"key"`
	// Kind says why the posting was made.
	Kind Kind `json:"kind"`
	// Debit is the account the amount leaves.
	Debit Account `json:"debit"`
	// Credit is the account the amount enters.
	Credit Account `json:"credit"`
	// Amount is positive.
	Amount Money `json:"amount"`
	// Tournament is the tournament the posting belongs to.
	Tournament string `json:"tournament"`
	// Player is the player involved; empty for Rake.
	Player string `json:"player,omitempty"`
	// Place is the placement paid; 0 unless Kind is Prize or Withheld.
	Place int `json:"place,omitempty"`
	// ExclusionVersion is the exclusion list version checked for the
	// posting.
	ExclusionVersion uint64 `json:"exclusion_version"`
	// Slot is the log slot of the command that produced the posting.
	Slot paxos.Slot `json:"slot"`
	// Ballot is the ballot under which the replica that holds this book
	// learned the slot was chosen: audit metadata of that replica, which
	// may differ from another replica's when a chosen value was re-proposed
	// by a later leader. Book.Hash includes it; the tournament state
	// machine's hash does not.
	Ballot paxos.Ballot `json:"ballot"`
}

// Validate reports the first problem with a posting a caller is about to
// post: empty key, missing or equal accounts, non-positive amount, unknown
// kind.
func (p Posting) Validate() error {
	if p.Key == "" {
		return errors.New("ledger: posting has an empty key")
	}
	if p.Debit == "" || p.Credit == "" {
		return errors.New("ledger: posting needs a debit and a credit account")
	}
	if p.Debit == p.Credit {
		return fmt.Errorf("ledger: posting %q debits and credits the same account %q", p.Key, p.Debit)
	}
	if p.Amount <= 0 {
		return fmt.Errorf("ledger: posting %q has non-positive amount %d", p.Key, p.Amount)
	}
	if _, ok := kindNames[p.Kind]; !ok {
		return fmt.Errorf("ledger: posting %q has unknown kind %d", p.Key, uint8(p.Kind))
	}
	return nil
}

// Book is the append-only ledger. It is not safe for concurrent use.
type Book struct {
	postings []Posting
	byKey    map[PostingKey]int
	balances map[Account]Money
}

// NewBook returns an empty book.
func NewBook() *Book {
	return &Book{
		byKey:    make(map[PostingKey]int),
		balances: make(map[Account]Money),
	}
}

// Post appends p, assigning its Seq, and returns it with ok=true. When a
// posting with the same key exists, nothing changes and the existing
// posting is returned with ok=false. An invalid posting (Validate fails) is
// not appended and the error is returned.
func (b *Book) Post(p Posting) (Posting, bool, error) {
	if err := p.Validate(); err != nil {
		return Posting{}, false, err
	}
	if i, ok := b.byKey[p.Key]; ok {
		return b.postings[i], false, nil
	}
	p.Seq = uint64(len(b.postings)) + 1
	b.postings = append(b.postings, p)
	b.byKey[p.Key] = len(b.postings) - 1
	b.balances[p.Debit] -= p.Amount
	b.balances[p.Credit] += p.Amount
	return p, true, nil
}

// Has reports whether a posting with key k exists.
func (b *Book) Has(k PostingKey) bool {
	_, ok := b.byKey[k]
	return ok
}

// Get returns the posting with key k.
func (b *Book) Get(k PostingKey) (Posting, bool) {
	i, ok := b.byKey[k]
	if !ok {
		return Posting{}, false
	}
	return b.postings[i], true
}

// Balance returns the balance of account a: credits minus debits. An
// account that never appeared has balance 0.
func (b *Book) Balance(a Account) Money { return b.balances[a] }

// Len returns the number of postings.
func (b *Book) Len() int { return len(b.postings) }

// Postings returns a copy of every posting in Seq order.
func (b *Book) Postings() []Posting {
	return append([]Posting(nil), b.postings...)
}

// PostingsSince returns a copy of the postings after the first n, in Seq
// order: the postings appended since Len returned n.
func (b *Book) PostingsSince(n int) []Posting {
	if n < 0 {
		n = 0
	}
	if n >= len(b.postings) {
		return nil
	}
	return append([]Posting(nil), b.postings[n:]...)
}

// ForTournament returns a copy of the postings whose Tournament is id, in
// Seq order.
func (b *Book) ForTournament(id string) []Posting {
	var out []Posting
	for _, p := range b.postings {
		if p.Tournament == id {
			out = append(out, p)
		}
	}
	return out
}

// CheckBalances verifies only that the balances sum to zero. It is the
// cheap check a caller can afford after every posting; Check is the full
// one.
func (b *Book) CheckBalances() error {
	var sum Money
	for _, v := range b.balances {
		sum += v
	}
	if sum != 0 {
		return fmt.Errorf("ledger: balances sum to %d, want 0", sum)
	}
	return nil
}

// Check verifies the book's own consistency: every amount is positive,
// every posting's Seq equals its position, keys are unique and indexed at
// their posting, every balance equals the one recomputed from the postings,
// and the sum of all balances is zero. It returns the first problem found.
//
// Check proves that no money appeared or disappeared inside the book. It
// cannot tell whether the postings are the right ones: a posting that was
// never made (a command whose posting was skipped as a retry) leaves a
// consistent book. The tournament state machine and the simulator's domain
// invariants check that every entry and payout has its posting.
func (b *Book) Check() error {
	seen := make(map[PostingKey]bool, len(b.postings))
	derived := make(map[Account]Money)
	for i, p := range b.postings {
		if p.Seq != uint64(i)+1 {
			return fmt.Errorf("ledger: posting at position %d has seq %d", i+1, p.Seq)
		}
		if err := p.Validate(); err != nil {
			return err
		}
		if seen[p.Key] {
			return fmt.Errorf("ledger: key %q posted twice", p.Key)
		}
		seen[p.Key] = true
		if j, ok := b.byKey[p.Key]; !ok || j != i {
			return fmt.Errorf("ledger: key %q is not indexed at position %d", p.Key, i+1)
		}
		derived[p.Debit] -= p.Amount
		derived[p.Credit] += p.Amount
	}
	if len(b.byKey) != len(b.postings) {
		return fmt.Errorf("ledger: %d keys indexed for %d postings", len(b.byKey), len(b.postings))
	}
	if err := b.CheckBalances(); err != nil {
		return err
	}
	for _, a := range sortedAccounts(b.balances, derived) {
		if b.balances[a] != derived[a] {
			return fmt.Errorf("ledger: balance of %q is %d, postings give %d", a, b.balances[a], derived[a])
		}
	}
	return nil
}

// sortedAccounts returns the accounts of both maps in order, so that Check
// reports the same first problem on every run.
func sortedAccounts(ms ...map[Account]Money) []Account {
	seen := make(map[Account]bool)
	var out []Account
	for _, m := range ms {
		for a := range m {
			if !seen[a] {
				seen[a] = true
				out = append(out, a)
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// Hash returns a sha256 over the JSON encoding of every posting in Seq
// order. Two books with the same postings have the same hash.
func (b *Book) Hash() [32]byte {
	h := sha256.New()
	for _, p := range b.postings {
		enc, err := json.Marshal(p)
		if err != nil {
			// Posting has no field that json.Marshal can fail on.
			panic("ledger: marshal posting: " + err.Error())
		}
		h.Write(enc)
		h.Write([]byte{'\n'})
	}
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}

// PlayerAccount returns the account of player id: "player:<id>".
func PlayerAccount(id string) Account { return Account("player:" + id) }

// PoolAccount returns the prize pool account of tournament tid.
func PoolAccount(tid string) Account { return Account("pool:" + tid) }

// RakeAccount returns the rake account of tournament tid.
func RakeAccount(tid string) Account { return Account("rake:" + tid) }

// WithheldAccount returns the holding account for prizes of tournament tid
// that could not be paid to their winner.
func WithheldAccount(tid string) Account { return Account("withheld:" + tid) }

// ClaimsAccount returns the account that receives the claimed payouts of
// tournament tid: "claims:<tid>".
func ClaimsAccount(tid string) Account { return Account("claims:" + tid) }

// FeeKey returns the entry-fee posting key "fee:<tid>:<pid>".
func FeeKey(tid, pid string) PostingKey { return PostingKey("fee:" + tid + ":" + pid) }

// RakeKey returns the rake posting key "rake:<tid>".
func RakeKey(tid string) PostingKey { return PostingKey("rake:" + tid) }

// PrizeKey returns the prize posting key "prize:<tid>:<pid>:<place>".
func PrizeKey(tid, pid string, place int) PostingKey {
	return PostingKey("prize:" + tid + ":" + pid + ":" + strconv.Itoa(place))
}

// WithheldKey returns the withheld-prize posting key
// "withheld:<tid>:<pid>:<place>".
func WithheldKey(tid, pid string, place int) PostingKey {
	return PostingKey("withheld:" + tid + ":" + pid + ":" + strconv.Itoa(place))
}

// RefundKey returns the refund posting key "refund:<tid>:<pid>".
func RefundKey(tid, pid string) PostingKey { return PostingKey("refund:" + tid + ":" + pid) }

// ClaimKey returns the claim posting key "claim:<tid>:<pid>".
func ClaimKey(tid, pid string) PostingKey { return PostingKey("claim:" + tid + ":" + pid) }
