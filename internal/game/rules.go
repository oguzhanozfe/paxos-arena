package game

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
)

// Rank returns the card's rank, 1 (ace) to 13 (king).
func (c Card) Rank() int { return int(c)/4 + 1 }

// Suit returns the card's suit, 0 (clubs) to 3 (spades).
func (c Card) Suit() int { return int(c) % 4 }

// Valid reports whether c is a card of the deck.
func (c Card) Valid() bool { return int(c) < DeckSize }

// String renders the card in its two-character wire form, for example
// "Ah"; a value outside the deck renders as "??".
func (c Card) String() string {
	if !c.Valid() {
		return "??"
	}
	return string([]byte{RankChars[c.Rank()-1], SuitChars[c.Suit()]})
}

// ParseCard parses the two-character wire form of a card.
func ParseCard(s string) (Card, error) {
	if len(s) != 2 {
		return 0, fmt.Errorf("game: card %q is not two characters", s)
	}
	rank, suit := -1, -1
	for i := 0; i < len(RankChars); i++ {
		if RankChars[i] == s[0] {
			rank = i
		}
	}
	for i := 0; i < len(SuitChars); i++ {
		if SuitChars[i] == s[1] {
			suit = i
		}
	}
	if rank < 0 || suit < 0 {
		return 0, fmt.Errorf("game: %q is not a card", s)
	}
	return Card(rank*4 + suit), nil
}

// Adjacent reports whether two cards' ranks differ by one, an ace and a
// king counting as adjacent: (rank a - rank b) mod 13 is 1 or 12.
func Adjacent(a, b Card) bool {
	d := ((a.Rank()-b.Rank())%13 + 13) % 13
	return d == 1 || d == 12
}

// MarshalText renders the seed as 64 lower-case hex characters.
func (s Seed) MarshalText() ([]byte, error) { return hexText(s[:]), nil }

// UnmarshalText parses exactly 64 lower-case hex characters.
func (s *Seed) UnmarshalText(b []byte) error { return parseHex32((*[32]byte)(s), b, "seed") }

// String renders the seed as hex.
func (s Seed) String() string { return hex.EncodeToString(s[:]) }

// MarshalText renders the commitment as 64 lower-case hex characters.
func (c Commitment) MarshalText() ([]byte, error) { return hexText(c[:]), nil }

// UnmarshalText parses exactly 64 lower-case hex characters.
func (c *Commitment) UnmarshalText(b []byte) error {
	return parseHex32((*[32]byte)(c), b, "commitment")
}

// String renders the commitment as hex.
func (c Commitment) String() string { return hex.EncodeToString(c[:]) }

func hexText(b []byte) []byte {
	out := make([]byte, hex.EncodedLen(len(b)))
	hex.Encode(out, b)
	return out
}

// parseHex32 decodes 64 lower-case hex characters into dst. Upper case is
// refused so that one value has one spelling.
func parseHex32(dst *[32]byte, b []byte, what string) error {
	if len(b) != 64 {
		return fmt.Errorf("game: %s must be 64 hex characters, got %d", what, len(b))
	}
	for _, c := range b {
		if !('0' <= c && c <= '9' || 'a' <= c && c <= 'f') {
			return fmt.Errorf("game: %s must be lower-case hex", what)
		}
	}
	var tmp [32]byte
	if _, err := hex.Decode(tmp[:], b); err != nil {
		return fmt.Errorf("game: %s: %w", what, err)
	}
	*dst = tmp
	return nil
}

// DeriveSeed returns the seed of one round: HMAC-SHA256 under secret over
// DealDomain, a zero byte, the tournament, a zero byte, the player, a zero
// byte and the round as four big-endian bytes.
func DeriveSeed(secret []byte, tournament, player string, round int) Seed {
	m := hmac.New(sha256.New, secret)
	m.Write([]byte(DealDomain))
	m.Write([]byte{0})
	m.Write([]byte(tournament))
	m.Write([]byte{0})
	m.Write([]byte(player))
	m.Write([]byte{0})
	var r [4]byte
	binary.BigEndian.PutUint32(r[:], uint32(round))
	m.Write(r[:])
	var s Seed
	copy(s[:], m.Sum(nil))
	return s
}

// Commit returns the commitment to s: SHA-256 over CommitDomain, a zero
// byte and the seed.
func Commit(s Seed) Commitment {
	h := sha256.New()
	h.Write([]byte(CommitDomain))
	h.Write([]byte{0})
	h.Write(s[:])
	var c Commitment
	copy(c[:], h.Sum(nil))
	return c
}

// stream is the byte stream expanded from a seed: block k is
// SHA-256(seed || k as eight big-endian bytes).
type stream struct {
	seed  Seed
	block uint64
	buf   [sha256.Size]byte
	off   int
}

func newStream(s Seed) *stream { return &stream{seed: s, off: sha256.Size} }

func (st *stream) next32() uint32 {
	var out [4]byte
	for i := range out {
		if st.off == sha256.Size {
			var k [8]byte
			binary.BigEndian.PutUint64(k[:], st.block)
			h := sha256.New()
			h.Write(st.seed[:])
			h.Write(k[:])
			h.Sum(st.buf[:0])
			st.block++
			st.off = 0
		}
		out[i] = st.buf[st.off]
		st.off++
	}
	return binary.BigEndian.Uint32(out[:])
}

// shuffleWith shuffles the canonical deck from the top with draws from
// next, rejecting draws that would bias the index.
func shuffleWith(next func() uint32) [DeckSize]Card {
	var deck [DeckSize]Card
	for i := range deck {
		deck[i] = Card(i)
	}
	for i := DeckSize - 1; i >= 1; i-- {
		n := uint64(i + 1)
		limit := (uint64(1) << 32) - (uint64(1)<<32)%n
		x := uint64(next())
		for x >= limit {
			x = uint64(next())
		}
		j := x % n
		deck[i], deck[j] = deck[j], deck[i]
	}
	return deck
}

// Shuffle returns the round's deck: the canonical order 0 to 51 shuffled
// with the stream expanded from s.
func Shuffle(s Seed) [DeckSize]Card { return shuffleWith(newStream(s).next32) }

// Deal lays out the round of seed s: column c holds deck positions 5c to
// 5c+4 bottom first, position 35 starts the waste, positions 36 to 51 are
// the stock in draw order.
func Deal(s Seed) Board { return layout(Shuffle(s)) }

func layout(deck [DeckSize]Card) Board {
	var b Board
	for c := 0; c < ColumnCount; c++ {
		col := make([]Card, ColumnDepth)
		copy(col, deck[c*ColumnDepth:(c+1)*ColumnDepth])
		b.Columns[c] = col
	}
	b.Waste = make([]Card, 1, DeckSize-TableauSize)
	b.Waste[0] = deck[TableauSize]
	b.Stock = make([]Card, StockSize)
	copy(b.Stock, deck[TableauSize+1:])
	return b
}

// Clone returns a deep copy of b. Copying a Board value shares its slices,
// and a move on one copy could then write into the other's waste.
func (b *Board) Clone() Board {
	c := Board{Moves: b.Moves}
	for i, col := range b.Columns {
		c.Columns[i] = append([]Card{}, col...)
	}
	c.Waste = append(make([]Card, 0, len(b.Waste)+len(b.Stock)), b.Waste...)
	c.Stock = append([]Card{}, b.Stock...)
	return c
}

// WasteCard returns the waste card.
func (b *Board) WasteCard() Card { return b.Waste[len(b.Waste)-1] }

// Cleared returns the number of cards played from the tableau.
func (b *Board) Cleared() int {
	left := 0
	for _, col := range b.Columns {
		left += len(col)
	}
	return TableauSize - left
}

// Score returns the score of the board under section 4.4 of the contract:
// PointsPerCard per cleared card, plus ClearBonus and StockBonus per card
// left in the stock when the tableau is cleared.
func (b *Board) Score() int64 {
	cleared := b.Cleared()
	score := PointsPerCard * int64(cleared)
	if cleared == TableauSize {
		score += ClearBonus + StockBonus*int64(len(b.Stock))
	}
	return score
}

// Playable returns, ascending, the columns whose top card is adjacent to
// the waste card. It is empty when the board is cleared.
func (b *Board) Playable() []int {
	out := []int{}
	w := b.WasteCard()
	for c, col := range b.Columns {
		if len(col) > 0 && Adjacent(col[len(col)-1], w) {
			out = append(out, c)
		}
	}
	return out
}

// Over reports whether the round has ended by itself: Cleared when the
// tableau is empty, Blocked when the stock is empty and no column's top
// card is adjacent to the waste card.
func (b *Board) Over() (FinishReason, bool) {
	if b.Cleared() == TableauSize {
		return Cleared, true
	}
	if len(b.Stock) == 0 && len(b.Playable()) == 0 {
		return Blocked, true
	}
	return "", false
}

// Check reports whether m is legal on b: nil, or one of the Err values.
// The move's shape is checked first, then whether the round is over, then
// the move against the cards.
func (b *Board) Check(m Move) error {
	switch m.Kind {
	case Play:
		if m.Column < 0 || m.Column >= ColumnCount {
			return ErrBadColumn
		}
	case Draw:
		if m.Column != NoColumn {
			return ErrBadColumn
		}
	default:
		return ErrUnknownMove
	}
	if _, over := b.Over(); over {
		return ErrRoundOver
	}
	if m.Kind == Draw {
		if len(b.Stock) == 0 {
			return ErrStockEmpty
		}
		return nil
	}
	col := b.Columns[m.Column]
	if len(col) == 0 {
		return ErrColumnEmpty
	}
	if !Adjacent(col[len(col)-1], b.WasteCard()) {
		return ErrNotAdjacent
	}
	return nil
}

// Apply checks m and, when it is legal, makes it: the card it moves becomes
// the waste card and Moves grows by one. b is unchanged on error.
func (b *Board) Apply(m Move) error {
	if err := b.Check(m); err != nil {
		return err
	}
	if m.Kind == Draw {
		b.Waste = append(b.Waste, b.Stock[0])
		b.Stock = b.Stock[1:]
	} else {
		col := b.Columns[m.Column]
		b.Waste = append(b.Waste, col[len(col)-1])
		b.Columns[m.Column] = col[:len(col)-1]
	}
	b.Moves++
	return nil
}

// Replay deals the round of s and applies moves in order. On an illegal
// move it returns the board before that move and an error wrapping the
// move's Err value with its index.
func Replay(s Seed, moves []Move) (Board, error) {
	b := Deal(s)
	for i, m := range moves {
		if err := b.Apply(m); err != nil {
			return b, fmt.Errorf("game: move %d: %w", i, err)
		}
	}
	return b, nil
}
