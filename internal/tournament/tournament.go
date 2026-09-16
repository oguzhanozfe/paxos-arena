// Package tournament is the deterministic state machine fed with chosen log
// entries in slot order. It holds every tournament record, the results
// table that makes each idempotency key execute at most once, and the
// ledger of package ledger.
//
// Apply is total: it never panics on a decoded Command and never leaves the
// state half-changed. Two replicas that apply the same slots in the same
// order hold identical state and report identical hashes. The package has
// no goroutines, no clock and no randomness; the leader's API stamps the
// deal seed and the receipt time before a command enters the log.
package tournament

import (
	"fmt"

	"github.com/oguzhanozfe/paxos-arena/internal/ledger"
	"github.com/oguzhanozfe/paxos-arena/internal/paxos"
)

// TournamentID identifies a tournament: 1 to 64 bytes.
type TournamentID string

// PlayerID identifies a player: 1 to 64 bytes.
type PlayerID string

// IdempotencyKey identifies one client command: 1 to 128 bytes of printable
// ASCII without spaces. Two commands with the same key are the same command
// to the state machine.
type IdempotencyKey string

// Status is the life-cycle state of a tournament.
type Status uint8

const (
	// Open accepts joins and scores.
	Open Status = iota
	// Closed has standings and a prize pool and waits for Settle.
	Closed
	// Voided closed with too few scored entrants; Settle refunds every entry.
	Voided
	// Settled has paid out and never changes again.
	Settled
)

var statusNames = [...]string{"open", "closed", "voided", "settled"}

// String returns the lower-case status name.
func (s Status) String() string {
	if int(s) < len(statusNames) {
		return statusNames[s]
	}
	return fmt.Sprintf("status(%d)", uint8(s))
}

// MarshalText renders the status by name.
func (s Status) MarshalText() ([]byte, error) {
	if int(s) >= len(statusNames) {
		return nil, fmt.Errorf("tournament: unknown status %d", uint8(s))
	}
	return []byte(statusNames[s]), nil
}

// UnmarshalText parses a status name.
func (s *Status) UnmarshalText(b []byte) error {
	for i, name := range statusNames {
		if name == string(b) {
			*s = Status(i)
			return nil
		}
	}
	return fmt.Errorf("tournament: unknown status %q", string(b))
}

// TieBreak selects how equal scores are ranked and paid.
type TieBreak string

const (
	// EarliestSubmission ranks equal scores by submission order; every place
	// is distinct.
	EarliestSubmission TieBreak = "earliest_submission"
	// Split gives equal scores the same place and splits their combined
	// prize money.
	Split TieBreak = "split"
)

// Exclusions is a versioned list of jurisdictions that may not enter or be
// paid. Codes are upper-case letters; Encode sorts and deduplicates them.
type Exclusions struct {
	// Version identifies the list; it is recorded on entries and payouts.
	Version uint64 `json:"version"`
	// Jurisdictions holds the excluded codes.
	Jurisdictions []string `json:"jurisdictions"`
}

// Contains reports whether jurisdiction j is on the list.
func (e Exclusions) Contains(j string) bool {
	for _, x := range e.Jurisdictions {
		if x == j {
			return true
		}
	}
	return false
}

// Player is the eligibility claim a client makes for an entrant. Nothing
// here is verified beyond its shape.
type Player struct {
	// ID is 1 to 64 bytes.
	ID PlayerID `json:"id"`
	// Jurisdiction is 2 to 8 upper-case letters.
	Jurisdiction string `json:"jurisdiction"`
	// Age is 0 to 150.
	Age int `json:"age"`
}

// Rules are fixed at creation and never change.
type Rules struct {
	// EntryFee is positive.
	EntryFee ledger.Money `json:"entry_fee"`
	// RakeBps is the operator's share in basis points, 0 to 9999.
	RakeBps uint32 `json:"rake_bps"`
	// PrizeBps lists the prize shares by place; each is positive and they
	// sum to 10000.
	PrizeBps []uint32 `json:"prize_bps"`
	// MinEntrants is the minimum number of scored entrants at Close; at least
	// len(PrizeBps).
	MinEntrants int `json:"min_entrants"`
	// MaxEntrants bounds the entries; at least MinEntrants.
	MaxEntrants int `json:"max_entrants"`
	// MaxScore bounds an accepted score; scores are 0..MaxScore.
	MaxScore int64 `json:"max_score"`
	// MinAge is the minimum age to enter.
	MinAge int `json:"min_age"`
	// TieBreak is EarliestSubmission or Split.
	TieBreak TieBreak `json:"tie_break"`
	// Exclusions is the list checked at entry.
	Exclusions Exclusions `json:"exclusions"`
}

// Command is one client command with its idempotency key.
type Command struct {
	// Key is the client-supplied idempotency key.
	Key IdempotencyKey
	// ReceivedAt is the leader's receipt time in Unix milliseconds. It is
	// audit information and takes no part in validation or fingerprints.
	ReceivedAt int64
	// Op is one of CreateTournament, Join, SubmitScore, Close and Settle.
	Op Op
}

// Op is one of the five command payloads.
type Op interface{ op() }

// CreateTournament opens a tournament with rules and a deal seed.
type CreateTournament struct {
	// ID is the new tournament's identifier.
	ID TournamentID `json:"id"`
	// Seed is the deal every entrant plays, generated by the leader's API.
	// It is excluded from the fingerprint so that a retry through another
	// leader is still the same command.
	Seed uint64 `json:"seed"`
	// Rules are the tournament's rules.
	Rules Rules `json:"rules"`
}

// Join enters a player and charges the entry fee.
type Join struct {
	// Tournament is the tournament to enter.
	Tournament TournamentID `json:"tournament"`
	// Player is the entrant and their eligibility claims.
	Player Player `json:"player"`
}

// SubmitScore records an entrant's one accepted score.
type SubmitScore struct {
	// Tournament is the tournament played.
	Tournament TournamentID `json:"tournament"`
	// Player is the entrant.
	Player PlayerID `json:"player"`
	// Score is the validated score, 0..MaxScore.
	Score int64 `json:"score"`
	// DealSeed must equal the tournament's seed.
	DealSeed uint64 `json:"deal_seed"`
	// InputDigest is a digest of the client's input log. It is recorded,
	// not verified.
	InputDigest Digest `json:"input_digest"`
}

// Close ranks the entrants and fixes the prize pool.
type Close struct {
	// Tournament is the tournament to close.
	Tournament TournamentID `json:"tournament"`
}

// Settle pays prizes or refunds under the exclusion list current at
// settlement.
type Settle struct {
	// Tournament is the tournament to settle.
	Tournament TournamentID `json:"tournament"`
	// Exclusions is the list checked at payout.
	Exclusions Exclusions `json:"exclusions"`
}

func (CreateTournament) op() {}
func (Join) op()             {}
func (SubmitScore) op()      {}
func (Close) op()            {}
func (Settle) op()           {}

// Code is the outcome of applying a command: OK or one rejection code.
type Code string

// Result codes. Validation runs in the order the codes are listed for each
// command in the design; the first failing rule is the code.
const (
	// OK means the command took effect.
	OK Code = "ok"
	// KeyReused means the key was used before with a different command;
	// nothing happened and nothing was recorded.
	KeyReused Code = "key_reused"
	// InvalidKey means the key is malformed; nothing was recorded.
	InvalidKey Code = "invalid_key"
	// OutOfOrder means the slot is not above the applied slot; nothing
	// happened.
	OutOfOrder Code = "out_of_order"
	// InvalidOp means the command carries no known operation; nothing was
	// recorded. Decode never produces such a command.
	InvalidOp Code = "invalid_op"
	// InvalidRules means a Rules bound is violated or the ID is malformed.
	InvalidRules Code = "invalid_rules"
	// TournamentExists means the ID is taken.
	TournamentExists Code = "tournament_exists"
	// UnknownTournament means no tournament has the ID.
	UnknownTournament Code = "unknown_tournament"
	// NotOpen means the tournament no longer accepts the command.
	NotOpen Code = "not_open"
	// TournamentFull means MaxEntrants is reached.
	TournamentFull Code = "tournament_full"
	// AlreadyJoined means the player has an entry.
	AlreadyJoined Code = "already_joined"
	// JurisdictionExcluded means the player's jurisdiction is on the
	// creation-time list.
	JurisdictionExcluded Code = "jurisdiction_excluded"
	// Underage means the player is below MinAge.
	Underage Code = "underage"
	// InvalidPlayer means the player claim is malformed.
	InvalidPlayer Code = "invalid_player"
	// NotJoined means the player has no entry.
	NotJoined Code = "not_joined"
	// AlreadyScored means the entrant already has an accepted score.
	AlreadyScored Code = "already_scored"
	// SeedMismatch means the client did not play this tournament's deal.
	SeedMismatch Code = "seed_mismatch"
	// ScoreOutOfRange means the score is negative or above MaxScore.
	ScoreOutOfRange Code = "score_out_of_range"
	// NotClosed means the tournament is Open or already Settled.
	NotClosed Code = "not_closed"
	// InvalidExclusions means the Settle list is malformed.
	InvalidExclusions Code = "invalid_exclusions"
)

// Result is what applying a command produced. It is recorded under the
// command's key and replayed for every later command with that key.
type Result struct {
	// Code is OK or a rejection code.
	Code Code `json:"code"`
	// Replayed is true when the result came from the results table.
	Replayed bool `json:"replayed"`
	// Detail explains a rejection.
	Detail string `json:"detail,omitempty"`
	// Slot is the slot of the command that produced this result.
	Slot paxos.Slot `json:"slot"`
	// Seed is the deal to play; set for OK Join results.
	Seed uint64 `json:"seed,omitempty"`
}

// OK reports whether the command took effect (on first application) or was
// a replay of a command that did.
func (r Result) OK() bool { return r.Code == OK }

// Entry is one entrant of a tournament.
type Entry struct {
	// Player is the entrant's claim as made at Join.
	Player Player `json:"player"`
	// JoinSeq is the 1-based join order.
	JoinSeq uint32 `json:"join_seq"`
	// ExclusionVersion is the list version checked at Join.
	ExclusionVersion uint64 `json:"exclusion_version"`
	// Scored reports whether a score was accepted.
	Scored bool `json:"scored"`
	// Score is the accepted score; 0 until Scored.
	Score int64 `json:"score"`
	// SubmitSeq is the 1-based order among accepted scores; 0 until Scored.
	SubmitSeq uint32 `json:"submit_seq"`
	// InputDigest is the digest recorded with the score.
	InputDigest Digest `json:"input_digest"`
}

// Standing is one row of the ranking fixed at Close.
type Standing struct {
	// Place is 1-based; shared under Split for equal scores.
	Place int `json:"place"`
	// Player is the entrant.
	Player PlayerID `json:"player"`
	// Score is the accepted score, 0 when not Scored.
	Score int64 `json:"score"`
	// Scored reports whether the entrant submitted a score.
	Scored bool `json:"scored"`
	// SubmitSeq is the entrant's submission order, 0 when not Scored.
	SubmitSeq uint32 `json:"submit_seq"`
}

// Payout is one prize, withheld prize or refund fixed at Settle.
type Payout struct {
	// Player is the payee.
	Player PlayerID `json:"player"`
	// Place is the placement paid; 0 for a refund.
	Place int `json:"place"`
	// Amount is the amount; it may be 0 when the pool is too small to split.
	Amount ledger.Money `json:"amount"`
	// Withheld is true when the amount went to the holding account.
	Withheld bool `json:"withheld"`
	// Reason explains a withheld payout.
	Reason string `json:"reason,omitempty"`
	// ExclusionVersion is the list version checked at Settle.
	ExclusionVersion uint64 `json:"exclusion_version"`
	// Key is the posting key the payout was booked under.
	Key ledger.PostingKey `json:"key"`
}

// Tournament is the record of one tournament.
type Tournament struct {
	// ID is the identifier.
	ID TournamentID `json:"id"`
	// Seed is the deal every entrant plays.
	Seed uint64 `json:"seed"`
	// Rules are fixed at creation.
	Rules Rules `json:"rules"`
	// Status is the life-cycle state.
	Status Status `json:"status"`
	// Entries are in JoinSeq order.
	Entries []Entry `json:"entries"`
	// Fees is the sum of entry fees, set at Close.
	Fees ledger.Money `json:"fees"`
	// Rake is the operator's share, set at Close; 0 when Voided.
	Rake ledger.Money `json:"rake"`
	// Pool is Fees minus Rake, set at Close.
	Pool ledger.Money `json:"pool"`
	// Standings is the ranking, set at Close.
	Standings []Standing `json:"standings"`
	// Payouts are the prizes or refunds, set at Settle.
	Payouts []Payout `json:"payouts"`
	// CreatedAt is the slot of the CreateTournament command.
	CreatedAt paxos.Slot `json:"created_at"`
	// ClosedAt is the slot of the Close command; 0 until then.
	ClosedAt paxos.Slot `json:"closed_at"`
	// SettledAt is the slot of the Settle command; 0 until then.
	SettledAt paxos.Slot `json:"settled_at"`
	// Ballot is the ballot under which this replica learned the Settle slot
	// was chosen. It is audit metadata of this replica, not replicated
	// state: a value chosen under one ballot may be re-proposed and learned
	// under a later one on another replica. Hash and EncodeTournament
	// exclude it.
	Ballot paxos.Ballot `json:"ballot"`
}

// Entry returns the entry of player id.
func (t *Tournament) Entry(id PlayerID) (Entry, bool) {
	for _, e := range t.Entries {
		if e.Player.ID == id {
			return e, true
		}
	}
	return Entry{}, false
}

// ScoredCount returns the number of entries with an accepted score.
func (t *Tournament) ScoredCount() int {
	n := 0
	for _, e := range t.Entries {
		if e.Scored {
			n++
		}
	}
	return n
}

// Voids reports whether the tournament voids at Close: fewer scored
// entrants than MinEntrants. It is a function of the entries alone, so it
// gives the same answer before and after Close.
func (t *Tournament) Voids() bool { return t.ScoredCount() < t.Rules.MinEntrants }

// clone returns a deep copy.
func (t *Tournament) clone() Tournament {
	c := *t
	c.Rules.PrizeBps = append([]uint32(nil), t.Rules.PrizeBps...)
	c.Rules.Exclusions.Jurisdictions = append([]string(nil), t.Rules.Exclusions.Jurisdictions...)
	c.Entries = append([]Entry(nil), t.Entries...)
	c.Standings = append([]Standing(nil), t.Standings...)
	c.Payouts = append([]Payout(nil), t.Payouts...)
	return c
}
