// Package game is the rules engine of Ladder, the single-player card puzzle
// that server-authoritative tournaments play. docs/UNITY-INTEGRATION.md
// states the rules (section 4) and the deal (section 5). The tournament
// state machine uses this package to deal a round from the seed in the log,
// to validate every move against the authoritative board and to compute
// every score; the play API of package intent uses it to derive seeds and to
// build the views a client may see.
//
// The package is pure in the same sense as package tournament: no
// goroutines, no clock, no randomness beyond the seed it is given, and no
// imports beyond the standard library's hashing and encoding packages. A
// board is a function of its seed and its moves, so every replica computes
// the same board, and so can a client once the seed is revealed.
package game

import "errors"

// Name identifies a variant and its version in tournament rules. Changing
// any rule or constant of a variant makes a new Name, so the rules of a
// tournament never change while it runs.
type Name string

// LadderV1 is the variant this package implements.
const LadderV1 Name = "ladder-v1"

// Dimensions of a Ladder v1 round.
const (
	// DeckSize is the standard deck without jokers.
	DeckSize = 52
	// ColumnCount is the number of tableau columns.
	ColumnCount = 7
	// ColumnDepth is the number of cards dealt to each column.
	ColumnDepth = 5
	// TableauSize is the number of cards dealt to the tableau.
	TableauSize = ColumnCount * ColumnDepth
	// StockSize is the number of cards left to draw after the deal: the
	// deck minus the tableau minus the card that starts the waste.
	StockSize = DeckSize - TableauSize - 1
	// Rounds is the number of rounds of one tournament entry.
	Rounds = 3
	// RoundTimeLimitMs is how long a round accepts moves after its deal, in
	// milliseconds of the state machine's clock.
	RoundTimeLimitMs int64 = 300_000
)

// Scoring of a Ladder v1 round.
const (
	// PointsPerCard is scored for every card cleared from the tableau.
	PointsPerCard int64 = 100
	// ClearBonus is scored when the whole tableau is cleared.
	ClearBonus int64 = 500
	// StockBonus is scored, with ClearBonus, for every card still in the
	// stock when the tableau is cleared.
	StockBonus int64 = 50
	// MaxRoundScore is the score of a round that clears the tableau
	// without drawing.
	MaxRoundScore = TableauSize*PointsPerCard + ClearBonus + StockSize*StockBonus
	// MaxTotalScore is the best possible entry, and the max_score every
	// Ladder v1 tournament declares.
	MaxTotalScore = Rounds * MaxRoundScore
)

// Domain separation prefixes. Every hash input starts with one of them and
// a zero byte, so a value computed for one purpose is never valid for
// another.
const (
	// DealDomain prefixes the HMAC-SHA256 input that derives a round's seed.
	DealDomain = "paxos-arena/deal/v1"
	// CommitDomain prefixes the SHA-256 input of a seed's commitment.
	CommitDomain = "paxos-arena/commit/v1"
)

// Card is one card of the deck, 0 to 51. Its rank is Card/4 + 1 (ace 1 to
// king 13) and its suit Card%4 (clubs, diamonds, hearts, spades); suits
// play no part in the rules. On the wire a card is two characters, the rank
// from RankChars and the suit from SuitChars: card 0 is "Ac", card 51 "Ks".
type Card uint8

// Characters that spell a card on the wire.
const (
	// RankChars lists the rank characters, ace first.
	RankChars = "A23456789TJQK"
	// SuitChars lists the suit characters in suit order.
	SuitChars = "cdhs"
)

// Seed is the 32-byte deal seed of one round: HMAC-SHA256 under the deal
// secret over DealDomain, the tournament, the player and the round number.
// It is written to the replicated log before any card of the round is
// shown, and shown to the player only when the round is over. It renders as
// 64 lower-case hex characters.
type Seed [32]byte

// Commitment is SHA-256 over CommitDomain, a zero byte and a Seed. The
// player receives it with the deal and checks it against the seed revealed
// at the end of the round. It renders as 64 lower-case hex characters.
type Commitment [32]byte

// MoveKind names a move.
type MoveKind string

const (
	// Play moves the top card of a column onto the waste.
	Play MoveKind = "play"
	// Draw moves the next stock card onto the waste.
	Draw MoveKind = "draw"
)

// NoColumn is the column of a Draw.
const NoColumn = -1

// Move is one player move.
type Move struct {
	// Kind is Play or Draw.
	Kind MoveKind `json:"kind"`
	// Column is 0 to ColumnCount-1 for Play and NoColumn for Draw.
	Column int `json:"column"`
}

// FinishReason says why a round ended.
type FinishReason string

const (
	// Cleared means the last tableau card was played.
	Cleared FinishReason = "cleared"
	// Blocked means the stock is empty and no column's top card is adjacent
	// to the waste card.
	Blocked FinishReason = "blocked"
	// Resigned means the player finished the round before its deadline.
	Resigned FinishReason = "resigned"
	// Expired means the round was finished after its deadline.
	Expired FinishReason = "expired"
	// Closed means the tournament closed while the round was in play.
	Closed FinishReason = "closed"
)

// Board is the whole state of one round, hidden cards included. A client
// never receives a Board, only the view of it that the play API builds.
type Board struct {
	// Columns holds the tableau, each column bottom card first; the last
	// card of a column is its top card, the only one that can be played.
	Columns [ColumnCount][]Card
	// Waste holds the starting, played and drawn cards in the order they
	// arrived; the last one is the waste card.
	Waste []Card
	// Stock holds the cards not yet drawn, next card first.
	Stock []Card
	// Moves counts the accepted moves.
	Moves int
}

// Reasons a move is illegal. The state machine puts the message in the
// detail of an illegal_move rejection.
var (
	// ErrUnknownMove is a move whose kind is neither Play nor Draw.
	ErrUnknownMove = errors.New("game: the move kind is not play or draw")
	// ErrBadColumn is a play outside columns 0 to 6, or a draw whose column
	// is not -1.
	ErrBadColumn = errors.New("game: a play names a column from 0 to 6 and a draw names column -1")
	// ErrColumnEmpty is a play on a column that has no card left.
	ErrColumnEmpty = errors.New("game: the column is empty")
	// ErrNotAdjacent is a play whose card is not one rank above or below
	// the waste card, king and ace counting as adjacent.
	ErrNotAdjacent = errors.New("game: the column's top card is not one rank above or below the waste card")
	// ErrStockEmpty is a draw with no card left in the stock.
	ErrStockEmpty = errors.New("game: the stock is empty")
	// ErrRoundOver is any move on a board that is cleared or blocked.
	ErrRoundOver = errors.New("game: the round is over")
)
