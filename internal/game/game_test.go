package game

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"math/rand/v2"
	"strings"
	"testing"
)

// Appendix A of docs/UNITY-INTEGRATION.md.
const (
	vectorSecret     = "202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f"
	vectorTournament = "daily-2026-09-17"
	vectorPlayer     = "p-mzxw6ytboi4dqnbrgm2wqzlmn4"
)

type roundVector struct {
	seed, commitment, deck string
	moves                  string
	cleared                int
	score                  int64
	waste                  string
}

var roundVectors = []roundVector{
	{
		seed:       "1898590d493e4a523bd5ff782956ceeba5cd1cbcf09d5158ee54a01dc4588377",
		commitment: "5e188f4cb340f1c0e94c8821b8f72279b339f5698c66e659b22d32b3f9b29065",
		deck: "Ks 8h 7s 3s Jd 2h 4c 5d 7h Qh 3c 8s Ah 8d Th 9d Td 3h Jh Qs Ts 7c 5s 6d 2d 5h " +
			"Tc Ad 9s 9h 2c 6s 6c 3d As 4h Kh Js Jc 5c Kd Kc 8c Ac 7d 4s 9c Qc 4d 2s 6h Qd",
		moves: "d p1 p0 p2 p5 p2 p1 d p3 p3 d d d p2 p4 p0 d p6 d p0 p0 p5 p2 d p0 p5 d p4 " +
			"p1 p1 p2 p1 p3 d p4 d p3 p3 p5 d d p5 d p6 d p4 p6 d",
		cleared: 32, score: 3200, waste: "Qd",
	},
	{
		seed:       "172b3ace53308aebc0a1219562d3f796f1f3e8a3bf443645a9d3439421be80c8",
		commitment: "e1662e3bf2f91bbfb5405a7198050114e24904d5bc77717c176c57cd4af16398",
		deck: "Qh Qs 6s 7c Ac 7h 8s Js 9h 3s Jc 5h 4c 4s 9c 3d Ah Jd 4d Ks 6c Kh Kd Qc Tc 2c " +
			"8d 3c 7s 2d 4h 6d 8h Jh 7d 5s Td 8c Ad 2s 5d Th Kc Qd 2h Ts 6h 5c 3h 9d 9s As",
		moves: "d p2 p4 d p6 d p3 p0 p5 p1 p2 d d p2 p2 p3 d p1 d p4 p1 d p2 d d p3 d p0 p0 p5 " +
			"p1 p1 d d d d d p4 p0 p4 p0 p6",
		cleared: 26, score: 2600, waste: "Jh",
	},
	{
		seed:       "995b407ebad8f37697d9db8c1b696976f821f3a4430b11d8e70eedb36750fcbc",
		commitment: "cd49d8cc8ab27eed2a5e78d5cf83f0173e96d74525faf9eb33f1d42767753578",
		deck: "Kc Ac 8s Ah Tc 5d 3h 4s 6c 8c Jd 4d Qc 5c 2h 6s 5h 8d 7s 2c 6d 7h 9h Qh 4c 6h " +
			"Kd 5s As 3s Jc Qd Ks 9c 3c 8h 9d Ad Th Js 7d Td 4h Jh 2d Kh 2s 9s 3d Ts 7c Qs",
		moves: "d p0 d p2 p0 p3 p5 p4 p2 d d p2 d p0 p3 p1 d d p6 p2 d p4 p2 d p0 p0 p5 d d d " +
			"p3 p4 d d p6 d p1 p3 p1 p1 d p6 p6 p6",
		cleared: 28, score: 2800, waste: "Jc",
	},
}

func vectorSecretBytes(t *testing.T) []byte {
	t.Helper()
	b, err := hex.DecodeString(vectorSecret)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func parseMoves(t *testing.T, s string) []Move {
	t.Helper()
	var out []Move
	for _, f := range strings.Fields(s) {
		switch {
		case f == "d":
			out = append(out, Move{Kind: Draw, Column: NoColumn})
		case len(f) == 2 && f[0] == 'p':
			out = append(out, Move{Kind: Play, Column: int(f[1] - '0')})
		default:
			t.Fatalf("bad move %q", f)
		}
	}
	return out
}

func cards(t *testing.T, s string) []Card {
	t.Helper()
	var out []Card
	for _, f := range strings.Fields(s) {
		c, err := ParseCard(f)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, c)
	}
	return out
}

func TestAppendixVectors(t *testing.T) {
	secret := vectorSecretBytes(t)
	total := int64(0)
	for i, v := range roundVectors {
		round := i + 1
		seed := DeriveSeed(secret, vectorTournament, vectorPlayer, round)
		if seed.String() != v.seed {
			t.Fatalf("round %d seed = %s, want %s", round, seed, v.seed)
		}
		if c := Commit(seed); c.String() != v.commitment {
			t.Errorf("round %d commitment = %s, want %s", round, c, v.commitment)
		}
		deck := Shuffle(seed)
		want := cards(t, v.deck)
		for j := range deck {
			if deck[j] != want[j] {
				t.Fatalf("round %d deck[%d] = %s, want %s", round, j, deck[j], want[j])
			}
		}
		b := Deal(seed)
		for c := 0; c < ColumnCount; c++ {
			for k := 0; k < ColumnDepth; k++ {
				if b.Columns[c][k] != want[c*ColumnDepth+k] {
					t.Fatalf("round %d column %d card %d = %s", round, c, k, b.Columns[c][k])
				}
			}
		}
		if b.WasteCard() != want[35] || len(b.Stock) != StockSize || b.Stock[0] != want[36] || b.Stock[15] != want[51] {
			t.Fatalf("round %d waste/stock wrong: waste %s stock %v", round, b.WasteCard(), b.Stock)
		}
		moves := parseMoves(t, v.moves)
		// The greedy player: always the lowest playable column, otherwise draw.
		g := Deal(seed)
		for n := 0; ; n++ {
			if _, over := g.Over(); over {
				break
			}
			m := Move{Kind: Draw, Column: NoColumn}
			if p := g.Playable(); len(p) > 0 {
				m = Move{Kind: Play, Column: p[0]}
			}
			if n >= len(moves) || moves[n] != m {
				t.Fatalf("round %d greedy move %d = %+v, vector has %d moves", round, n, m, len(moves))
			}
			if err := g.Apply(m); err != nil {
				t.Fatalf("round %d greedy move %d: %v", round, n, err)
			}
		}
		final, err := Replay(seed, moves)
		if err != nil {
			t.Fatalf("round %d replay: %v", round, err)
		}
		reason, over := final.Over()
		if !over || reason != Blocked || final.Cleared() != v.cleared || final.Score() != v.score || final.WasteCard().String() != v.waste || final.Moves != len(moves) {
			t.Errorf("round %d final: over %t %s cleared %d score %d waste %s moves %d", round, over, reason, final.Cleared(), final.Score(), final.WasteCard(), final.Moves)
		}
		total += final.Score()
	}
	if total != 8600 {
		t.Errorf("entry total = %d, want 8600", total)
	}
}

func TestBlockZeroPrefix(t *testing.T) {
	var seed Seed
	if err := seed.UnmarshalText([]byte(roundVectors[0].seed)); err != nil {
		t.Fatal(err)
	}
	st := newStream(seed)
	a, b := st.next32(), st.next32()
	if got := hex.EncodeToString([]byte{byte(a >> 24), byte(a >> 16), byte(a >> 8), byte(a), byte(b >> 24), byte(b >> 16), byte(b >> 8), byte(b)}); got != "969af5994aa697c0" {
		t.Errorf("block 0 prefix = %s", got)
	}
}

// TestShuffleRejectsBiasedDraws feeds the shuffle a draw at and above the
// limit for n = 52 and checks both are discarded.
func TestShuffleRejectsBiasedDraws(t *testing.T) {
	const limit52 = 4294967248
	base := []uint32{7, 11, 13}
	plain := func() func() uint32 {
		i := 0
		return func() uint32 { i++; return base[i%len(base)] * uint32(i) }
	}
	calls := 0
	rejected := func() func() uint32 {
		inner := plain()
		return func() uint32 {
			calls++
			switch calls {
			case 1:
				return limit52
			case 2:
				return 1<<32 - 1
			}
			return inner()
		}
	}
	if a, b := shuffleWith(plain()), shuffleWith(rejected()); a != b {
		t.Errorf("rejected draws changed the deck:\n%v\n%v", a, b)
	}
	if calls != DeckSize-1+2 {
		t.Errorf("draws = %d, want %d", calls, DeckSize+1)
	}
	// limit-1 is accepted.
	first := true
	d := shuffleWith(func() uint32 {
		if first {
			first = false
			return limit52 - 1
		}
		return 0
	})
	if d[51] != Card((limit52-1)%52) {
		t.Errorf("draw limit-1 not accepted: deck[51] = %d", d[51])
	}
}

func TestCards(t *testing.T) {
	for _, tc := range []struct {
		c    Card
		s    string
		rank int
	}{{0, "Ac", 1}, {13, "4d", 4}, {51, "Ks", 13}, {38, "Th", 10}, {40, "Jc", 11}} {
		if tc.c.String() != tc.s || tc.c.Rank() != tc.rank {
			t.Errorf("card %d = %s rank %d, want %s %d", tc.c, tc.c, tc.c.Rank(), tc.s, tc.rank)
		}
		if p, err := ParseCard(tc.s); err != nil || p != tc.c {
			t.Errorf("ParseCard(%q) = %d, %v", tc.s, p, err)
		}
	}
	for _, bad := range []string{"", "A", "Acc", "1c", "Ax", "ah", "AC"} {
		if _, err := ParseCard(bad); err == nil {
			t.Errorf("ParseCard(%q) accepted", bad)
		}
	}
	for i := 0; i < DeckSize; i++ {
		if p, _ := ParseCard(Card(i).String()); p != Card(i) {
			t.Errorf("round trip of %d", i)
		}
	}
	if Card(52).String() != "??" || Card(52).Valid() {
		t.Error("card 52 is not a card")
	}
}

func TestAdjacent(t *testing.T) {
	c := func(s string) Card { p, _ := ParseCard(s); return p }
	for _, tc := range []struct {
		a, b string
		want bool
	}{
		{"Ah", "2c", true}, {"2c", "Ah", true}, {"Ks", "Ad", true}, {"Ad", "Ks", true},
		{"Qh", "Kc", true}, {"Kc", "Qh", true}, {"9s", "Td", true},
		{"Ah", "Ac", false}, {"Ah", "3c", false}, {"Ks", "2d", false}, {"Qs", "Ad", false}, {"7h", "7h", false},
	} {
		if got := Adjacent(c(tc.a), c(tc.b)); got != tc.want {
			t.Errorf("Adjacent(%s, %s) = %t", tc.a, tc.b, got)
		}
	}
}

func TestSeedText(t *testing.T) {
	s := DeriveSeed([]byte("k"), "t", "p", 1)
	b, _ := json.Marshal(s)
	var back Seed
	if err := json.Unmarshal(b, &back); err != nil || back != s {
		t.Fatalf("seed JSON round trip: %s %v", b, err)
	}
	for _, bad := range []string{"", strings.Repeat("a", 63), strings.Repeat("A", 64), strings.Repeat("g", 64), strings.Repeat("a", 65)} {
		if err := back.UnmarshalText([]byte(bad)); err == nil {
			t.Errorf("seed %q accepted", bad)
		}
		var c Commitment
		if err := c.UnmarshalText([]byte(bad)); err == nil {
			t.Errorf("commitment %q accepted", bad)
		}
	}
	// Every input field separates seeds.
	if DeriveSeed([]byte("k"), "t", "p", 2) == s || DeriveSeed([]byte("k"), "t2", "p", 1) == s ||
		DeriveSeed([]byte("k"), "t", "p2", 1) == s || DeriveSeed([]byte("k2"), "t", "p", 1) == s ||
		DeriveSeed([]byte("k"), "tp", "", 1) == s {
		t.Error("seed does not depend on every input")
	}
}

// board builds a board from explicit columns, waste and stock.
func board(t *testing.T, cols [ColumnCount]string, waste, stock string) Board {
	t.Helper()
	var b Board
	for i, c := range cols {
		b.Columns[i] = append([]Card{}, cards(t, c)...)
	}
	b.Waste = cards(t, waste)
	b.Stock = append([]Card{}, cards(t, stock)...)
	return b
}

func TestMoveRules(t *testing.T) {
	cols := [ColumnCount]string{"Ks 5h", "2h 3c", "", "9d Qs", "Ts Ah", "5h 4c", "2c As"}
	play := func(c int) Move { return Move{Kind: Play, Column: c} }
	draw := Move{Kind: Draw, Column: NoColumn}
	cases := []struct {
		name  string
		b     func() Board
		m     Move
		want  error
		waste string
	}{
		{"play adjacent below", func() Board { return board(t, cols, "4h", "7d") }, play(0), nil, "5h"},
		{"play adjacent above", func() Board { return board(t, cols, "4h", "7d") }, play(1), nil, "3c"},
		{"play king on ace", func() Board { return board(t, cols, "Kd", "7d") }, play(6), nil, "As"},
		{"play ace on king", func() Board { return board(t, cols, "Ac", "7d") }, play(3), ErrNotAdjacent, ""},
		{"play queen on king", func() Board { return board(t, cols, "Kc", "7d") }, play(3), nil, "Qs"},
		{"play not adjacent", func() Board { return board(t, cols, "4h", "7d") }, play(3), ErrNotAdjacent, ""},
		{"play same rank", func() Board { return board(t, cols, "5c", "7d") }, play(0), ErrNotAdjacent, ""},
		{"play empty column", func() Board { return board(t, cols, "4h", "7d") }, play(2), ErrColumnEmpty, ""},
		{"play column -1", func() Board { return board(t, cols, "4h", "7d") }, play(-1), ErrBadColumn, ""},
		{"play column 7", func() Board { return board(t, cols, "4h", "7d") }, play(7), ErrBadColumn, ""},
		{"draw", func() Board { return board(t, cols, "4h", "7d 8d") }, draw, nil, "7d"},
		{"draw with column", func() Board { return board(t, cols, "4h", "7d") }, Move{Kind: Draw, Column: 0}, ErrBadColumn, ""},
		{"draw empty stock", func() Board { return board(t, cols, "4h", "") }, draw, ErrStockEmpty, ""},
		{"unknown kind", func() Board { return board(t, cols, "4h", "7d") }, Move{Kind: "undo", Column: 0}, ErrUnknownMove, ""},
		{"empty kind", func() Board { return board(t, cols, "4h", "7d") }, Move{}, ErrUnknownMove, ""},
		{"move on a blocked round", func() Board {
			return board(t, [ColumnCount]string{"9d", "", "", "", "", "", ""}, "4h", "")
		}, draw, ErrRoundOver, ""},
		{"move on a cleared round", func() Board { return board(t, [ColumnCount]string{}, "4h", "7d") }, draw, ErrRoundOver, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := tc.b()
			before := b.Clone()
			if err := b.Check(tc.m); !errors.Is(err, tc.want) || (tc.want == nil && err != nil) {
				t.Fatalf("Check = %v, want %v", err, tc.want)
			}
			err := b.Apply(tc.m)
			if tc.want != nil {
				if err != tc.want {
					t.Fatalf("Apply = %v, want %v", err, tc.want)
				}
				if !sameBoard(&b, &before) {
					t.Fatal("a refused move changed the board")
				}
				return
			}
			if err != nil {
				t.Fatalf("Apply = %v", err)
			}
			if b.WasteCard().String() != tc.waste || b.Moves != 1 || len(b.Waste) != len(before.Waste)+1 {
				t.Fatalf("after move: waste %s moves %d", b.WasteCard(), b.Moves)
			}
		})
	}
}

func sameBoard(a, b *Board) bool {
	x, _ := json.Marshal(a)
	y, _ := json.Marshal(b)
	return string(x) == string(y)
}

func TestFinishAndScore(t *testing.T) {
	// Clearing the last card with two cards in the stock.
	b := board(t, [ColumnCount]string{"5h"}, "4h", "7d 8d")
	if _, over := b.Over(); over {
		t.Fatal("over before the last play")
	}
	if b.Score() != 3400 {
		t.Fatalf("score before = %d", b.Score())
	}
	if err := b.Apply(Move{Kind: Play, Column: 0}); err != nil {
		t.Fatal(err)
	}
	if r, over := b.Over(); !over || r != Cleared {
		t.Fatalf("Over = %s %t, want cleared", r, over)
	}
	if b.Score() != 3500+500+2*50 {
		t.Errorf("cleared score = %d", b.Score())
	}
	if len(b.Playable()) != 0 {
		t.Errorf("playable on a cleared board: %v", b.Playable())
	}
	// Blocked: the stock empties onto a card nothing fits.
	b = board(t, [ColumnCount]string{"9d", "Jc"}, "4h", "2s")
	if _, over := b.Over(); over {
		t.Fatal("over with a card to draw")
	}
	if err := b.Apply(Move{Kind: Draw, Column: NoColumn}); err != nil {
		t.Fatal(err)
	}
	if r, over := b.Over(); !over || r != Blocked {
		t.Fatalf("Over = %s %t, want blocked", r, over)
	}
	// Not blocked while a column fits, even with an empty stock.
	b = board(t, [ColumnCount]string{"9d", "3c"}, "4h", "")
	if _, over := b.Over(); over || len(b.Playable()) != 1 || b.Playable()[0] != 1 {
		t.Fatalf("playable = %v", b.Playable())
	}
	if MaxRoundScore != 4800 || MaxTotalScore != 14400 {
		t.Errorf("max scores = %d %d", MaxRoundScore, MaxTotalScore)
	}
}

// TestReplayMatchesApply plays random legal games from random seeds and
// checks that Replay of the accepted moves reproduces the incremental
// board, that illegal moves change nothing, and that every score stays
// within the bound.
func TestReplayMatchesApply(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	for game := 0; game < 300; game++ {
		var seed Seed
		for i := range seed {
			seed[i] = byte(rng.Uint32())
		}
		b := Deal(seed)
		var moves []Move
		for step := 0; step < 200; step++ {
			m := Move{Kind: Draw, Column: NoColumn}
			switch rng.IntN(4) {
			case 0, 1:
				m = Move{Kind: Play, Column: rng.IntN(ColumnCount+2) - 1}
			case 2:
				if p := b.Playable(); len(p) > 0 {
					m = Move{Kind: Play, Column: p[rng.IntN(len(p))]}
				}
			}
			before := b.Clone()
			err := b.Apply(m)
			if err != nil {
				if !sameBoard(&b, &before) {
					t.Fatalf("game %d: refused move %+v changed the board", game, m)
				}
				continue
			}
			moves = append(moves, m)
			if s := b.Score(); s < 0 || s > MaxRoundScore {
				t.Fatalf("game %d: score %d out of bounds", game, s)
			}
			if _, over := b.Over(); over {
				break
			}
		}
		r, err := Replay(seed, moves)
		if err != nil {
			t.Fatalf("game %d: replay: %v", game, err)
		}
		if !sameBoard(&r, &b) {
			t.Fatalf("game %d: replay differs from the incremental board", game)
		}
		if TableauSize-r.Cleared()+len(r.Waste)+len(r.Stock) != DeckSize {
			t.Fatalf("game %d: cards lost", game)
		}
	}
	// An illegal move in a replay names its index and leaves the board
	// before it.
	seed := DeriveSeed([]byte("x"), "t", "p", 1)
	b, err := Replay(seed, []Move{{Kind: Draw, Column: NoColumn}, {Kind: Play, Column: 9}})
	if !errors.Is(err, ErrBadColumn) || !strings.Contains(err.Error(), "move 1") || b.Moves != 1 {
		t.Errorf("Replay illegal = %v, moves %d", err, b.Moves)
	}
}
