package tournament

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"github.com/oguzhanozfe/paxos-arena/internal/jsonx"
)

// Digest is a 32-byte digest rendered as 64 hex characters in JSON.
type Digest [32]byte

// MarshalText renders the digest as lower-case hex.
func (d Digest) MarshalText() ([]byte, error) {
	out := make([]byte, 64)
	hex.Encode(out, d[:])
	return out, nil
}

// UnmarshalText parses exactly 64 hex characters.
func (d *Digest) UnmarshalText(b []byte) error {
	if len(b) != 64 {
		return fmt.Errorf("tournament: digest must be 64 hex characters, got %d", len(b))
	}
	var tmp [32]byte
	if _, err := hex.Decode(tmp[:], b); err != nil {
		return fmt.Errorf("tournament: digest: %w", err)
	}
	*d = tmp
	return nil
}

// String renders the digest as hex.
func (d Digest) String() string { return hex.EncodeToString(d[:]) }

// Op names on the wire.
const (
	opCreate = "create_tournament"
	opJoin   = "join"
	opScore  = "submit_score"
	opClose  = "close"
	opSettle = "settle"
)

// OpName returns the wire name of a command's operation, or "" for an
// unknown type.
func OpName(op Op) string {
	switch op.(type) {
	case CreateTournament:
		return opCreate
	case Join:
		return opJoin
	case SubmitScore:
		return opScore
	case Close:
		return opClose
	case Settle:
		return opSettle
	}
	return ""
}

// TournamentOf returns the tournament a command addresses; for
// CreateTournament that is the new ID.
func TournamentOf(op Op) TournamentID {
	switch o := op.(type) {
	case CreateTournament:
		return o.ID
	case Join:
		return o.Tournament
	case SubmitScore:
		return o.Tournament
	case Close:
		return o.Tournament
	case Settle:
		return o.Tournament
	}
	return ""
}

// wireCommand is the JSON form of a Command:
// {"key":"k","received_at":0,"op":"join","body":{...}}.
type wireCommand struct {
	Key        IdempotencyKey  `json:"key"`
	ReceivedAt int64           `json:"received_at"`
	Op         string          `json:"op"`
	Body       json.RawMessage `json:"body"`
}

// Canonical returns a copy of op with every list whose order is not
// semantic sorted and deduplicated: the exclusion lists of CreateTournament
// and Settle. Encode applies it, so equal commands encode identically.
func Canonical(op Op) Op {
	switch o := op.(type) {
	case CreateTournament:
		o.Rules.PrizeBps = append([]uint32{}, o.Rules.PrizeBps...)
		o.Rules.Exclusions = canonicalExclusions(o.Rules.Exclusions)
		return o
	case Settle:
		o.Exclusions = canonicalExclusions(o.Exclusions)
		return o
	}
	return op
}

func canonicalExclusions(e Exclusions) Exclusions {
	out := make([]string, 0, len(e.Jurisdictions))
	seen := make(map[string]bool, len(e.Jurisdictions))
	for _, j := range e.Jurisdictions {
		if !seen[j] {
			seen[j] = true
			out = append(out, j)
		}
	}
	sort.Strings(out)
	return Exclusions{Version: e.Version, Jurisdictions: out}
}

// Encode renders a command in its canonical wire form. It fails only for a
// nil or unknown Op.
func Encode(c Command) ([]byte, error) {
	name := OpName(c.Op)
	if name == "" {
		return nil, fmt.Errorf("tournament: cannot encode op of type %T", c.Op)
	}
	body, err := json.Marshal(Canonical(c.Op))
	if err != nil {
		return nil, fmt.Errorf("tournament: encode %s body: %w", name, err)
	}
	return json.Marshal(wireCommand{Key: c.Key, ReceivedAt: c.ReceivedAt, Op: name, Body: body})
}

// Decode parses the wire form. It rejects unknown ops, unknown fields,
// trailing data and a malformed key, and never panics.
func Decode(b []byte) (Command, error) {
	var w wireCommand
	if err := strictUnmarshal(b, &w); err != nil {
		return Command{}, fmt.Errorf("tournament: decode command: %w", err)
	}
	if err := ValidateKey(w.Key); err != nil {
		return Command{}, fmt.Errorf("tournament: decode command: %w", err)
	}
	if len(w.Body) == 0 {
		return Command{}, errors.New("tournament: decode command: missing body")
	}
	c := Command{Key: w.Key, ReceivedAt: w.ReceivedAt}
	var err error
	switch w.Op {
	case opCreate:
		var o CreateTournament
		err = strictUnmarshal(w.Body, &o)
		c.Op = o
	case opJoin:
		var o Join
		err = strictUnmarshal(w.Body, &o)
		c.Op = o
	case opScore:
		var o SubmitScore
		err = strictUnmarshal(w.Body, &o)
		c.Op = o
	case opClose:
		var o Close
		err = strictUnmarshal(w.Body, &o)
		c.Op = o
	case opSettle:
		var o Settle
		err = strictUnmarshal(w.Body, &o)
		c.Op = o
	default:
		return Command{}, fmt.Errorf("tournament: decode command: unknown op %q", w.Op)
	}
	if err != nil {
		return Command{}, fmt.Errorf("tournament: decode %s body: %w", w.Op, err)
	}
	return c, nil
}

// DecodeOp parses one operation body of the named op, as the HTTP API
// receives it. It applies the same strictness as Decode.
func DecodeOp(name string, body []byte) (Op, error) {
	c, err := Decode(mustMarshal(wireCommand{Key: "k", Op: name, Body: body}))
	if err != nil {
		return nil, err
	}
	return c.Op, nil
}

func mustMarshal(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic("tournament: marshal: " + err.Error())
	}
	return b
}

// Fingerprint covers the client-supplied part of a command: the op name and
// the canonical payload, excluding Key, ReceivedAt and
// CreateTournament.Seed. Two commands with equal fingerprints are the same
// request from the state machine's point of view.
func Fingerprint(c Command) [32]byte {
	op := Canonical(c.Op)
	if o, ok := op.(CreateTournament); ok {
		o.Seed = 0
		op = o
	}
	h := sha256.New()
	h.Write([]byte(OpName(op)))
	h.Write([]byte{0})
	if op != nil {
		h.Write(mustMarshal(op))
	}
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}

// strictUnmarshal is jsonx.DecodeStrict: one JSON value, no unknown
// fields, nothing but whitespace after it.
func strictUnmarshal(b []byte, v any) error { return jsonx.DecodeStrict(b, v) }

// Shape bounds.
const (
	MaxKeyLen        = 128
	MaxIDLen         = 64
	MaxEntryFee      = 1_000_000_000_000 // 10^12 minor units
	MaxEntrantsBound = 1_000_000
	// MaxPrizePlaces bounds len(Rules.PrizeBps).
	MaxPrizePlaces = 1000
	// MaxJurisdictions bounds the codes of one exclusion list.
	MaxJurisdictions = 1000
)

// ValidateKey checks an idempotency key: 1 to MaxKeyLen bytes, each a
// printable ASCII character other than space.
func ValidateKey(k IdempotencyKey) error {
	if len(k) == 0 {
		return errors.New("idempotency key is empty")
	}
	if len(k) > MaxKeyLen {
		return fmt.Errorf("idempotency key is %d bytes, limit %d", len(k), MaxKeyLen)
	}
	for i := 0; i < len(k); i++ {
		if k[i] <= ' ' || k[i] > '~' {
			return fmt.Errorf("idempotency key has a byte 0x%02x at position %d that is not printable ASCII", k[i], i)
		}
	}
	return nil
}

// validID reports whether s is a well-formed tournament or player
// identifier: 1 to MaxIDLen bytes, each an ASCII letter, a digit, '.', '_'
// or '-'. The restriction is what makes the ledger's account names and
// posting keys unambiguous: they join identifiers with ':' ("fee:<tid>:<pid>"),
// so an identifier containing ':' could make two tournaments share a key.
// It also keeps identifiers valid UTF-8, so that two different identifiers
// never encode to the same JSON and share a fingerprint.
func validID(s string) bool {
	if len(s) < 1 || len(s) > MaxIDLen {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case 'a' <= c && c <= 'z', 'A' <= c && c <= 'Z', '0' <= c && c <= '9', c == '.', c == '_', c == '-':
		default:
			return false
		}
	}
	return true
}

// idRule describes validID for rejection details.
const idRule = "1 to 64 characters from A-Z, a-z, 0-9, '.', '_' and '-'"

// ValidateTournamentID checks the shape of a tournament identifier.
func ValidateTournamentID(id TournamentID) error {
	if !validID(string(id)) {
		return fmt.Errorf("tournament id must be %s", idRule)
	}
	return nil
}

func validJurisdiction(j string) bool {
	if len(j) < 2 || len(j) > 8 {
		return false
	}
	for i := 0; i < len(j); i++ {
		if j[i] < 'A' || j[i] > 'Z' {
			return false
		}
	}
	return true
}

// ValidateExclusions checks the shape of an exclusion list: at most
// MaxJurisdictions codes, each 2 to 8 upper-case letters.
func ValidateExclusions(e Exclusions) error {
	if len(e.Jurisdictions) > MaxJurisdictions {
		return fmt.Errorf("exclusion list has %d jurisdictions, limit %d", len(e.Jurisdictions), MaxJurisdictions)
	}
	for _, j := range e.Jurisdictions {
		if !validJurisdiction(j) {
			return fmt.Errorf("jurisdiction %q is not 2 to 8 upper-case letters", j)
		}
	}
	return nil
}

// ValidatePlayer checks the shape of a player claim.
func ValidatePlayer(p Player) error {
	if !validID(string(p.ID)) {
		return fmt.Errorf("player id must be %s", idRule)
	}
	if !validJurisdiction(p.Jurisdiction) {
		return fmt.Errorf("jurisdiction %q is not 2 to 8 upper-case letters", p.Jurisdiction)
	}
	if p.Age < 0 || p.Age > 150 {
		return fmt.Errorf("age %d is outside 0..150", p.Age)
	}
	return nil
}

// ValidateRules checks every Rules bound of the design.
func ValidateRules(r Rules) error {
	if r.EntryFee <= 0 {
		return errors.New("entry_fee must be positive")
	}
	if r.EntryFee > MaxEntryFee {
		return fmt.Errorf("entry_fee %d exceeds the limit %d", r.EntryFee, MaxEntryFee)
	}
	if r.RakeBps > 9999 {
		return fmt.Errorf("rake_bps %d exceeds 9999", r.RakeBps)
	}
	if len(r.PrizeBps) == 0 {
		return errors.New("prize_bps must have at least one place")
	}
	if len(r.PrizeBps) > MaxPrizePlaces {
		return fmt.Errorf("prize_bps has %d places, limit %d", len(r.PrizeBps), MaxPrizePlaces)
	}
	var sum uint64
	for i, b := range r.PrizeBps {
		if b == 0 {
			return fmt.Errorf("prize_bps[%d] is zero", i)
		}
		sum += uint64(b)
	}
	if sum != 10000 {
		return fmt.Errorf("prize_bps sum to %d, want 10000", sum)
	}
	if r.MinEntrants < len(r.PrizeBps) {
		return fmt.Errorf("min_entrants %d is below the %d prize places", r.MinEntrants, len(r.PrizeBps))
	}
	if r.MaxEntrants < r.MinEntrants {
		return fmt.Errorf("max_entrants %d is below min_entrants %d", r.MaxEntrants, r.MinEntrants)
	}
	if r.MaxEntrants > MaxEntrantsBound {
		return fmt.Errorf("max_entrants %d exceeds the limit %d", r.MaxEntrants, MaxEntrantsBound)
	}
	if r.MaxScore < 0 {
		return errors.New("max_score must not be negative")
	}
	if r.MinAge < 0 {
		return errors.New("min_age must not be negative")
	}
	if r.TieBreak != EarliestSubmission && r.TieBreak != Split {
		return fmt.Errorf("tie_break %q is not %q or %q", r.TieBreak, EarliestSubmission, Split)
	}
	return ValidateExclusions(r.Exclusions)
}
