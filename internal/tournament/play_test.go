package tournament

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/oguzhanozfe/paxos-arena/internal/game"
	"github.com/oguzhanozfe/paxos-arena/internal/ledger"
	"github.com/oguzhanozfe/paxos-arena/internal/paxos"
)

// playDriver applies commands with increasing slots, a settable clock and
// fresh keys.
type playDriver struct {
	t    *testing.T
	s    *State
	slot paxos.Slot
	now  int64
	n    int
}

const startMs = 1_789_200_000_000

func newPlayDriver(t *testing.T) *playDriver {
	return &playDriver{t: t, s: NewState(), now: startMs}
}

func (d *playDriver) applyKey(key IdempotencyKey, op Op) Result {
	d.slot++
	return d.s.Apply(d.slot, paxos.Ballot{Round: 1, Node: 1}, Command{Key: key, ReceivedAt: d.now, Op: op})
}

func (d *playDriver) do(op Op) Result {
	d.n++
	return d.applyKey(IdempotencyKey(fmt.Sprintf("k%d", d.n)), op)
}

func (d *playDriver) ok(op Op) Result {
	d.t.Helper()
	r := d.do(op)
	if r.Code != OK {
		d.t.Fatalf("%s: %s (%s)", OpName(op), r.Code, r.Detail)
	}
	return r
}

func ladderRules() Rules {
	r := baseRules()
	r.MaxScore = game.MaxTotalScore
	r.Game = game.LadderV1
	return r
}

func device(i int) DeviceID { return DeviceID(fmt.Sprintf("%032x", i+1)) }

func verifier(i int) Digest { return Digest{byte(i + 1), 0xee} }

func pid(i int) PlayerID { return PlayerID(fmt.Sprintf("p-%d", i)) }

// bind opens a session for player i with jurisdiction TR and age 30.
func (d *playDriver) bind(i int) {
	d.t.Helper()
	d.ok(OpenSession{Device: device(i), Verifier: verifier(i), Player: pid(i), Jurisdiction: "TR", Age: 30})
}

// seqOf returns the next sequence number of player i.
func (d *playDriver) seqOf(i int) uint64 {
	rec, _ := d.s.Player(pid(i))
	return rec.LastSeq + 1
}

var blockedSecret, _ = hex.DecodeString("202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f")

// seed returns the Appendix A seed of the round (the greedy game ends
// blocked) for any tournament and player.
func vectorSeed(round int) game.Seed {
	return game.DeriveSeed(blockedSecret, "daily-2026-09-17", "p-mzxw6ytboi4dqnbrgm2wqzlmn4", round)
}

// clearingSeed is a seed whose greedy game clears the tableau with one card
// left in the stock.
var clearingSeed = game.DeriveSeed([]byte("scan"), "t", "p1", 1)

func (d *playDriver) deal(i, round int, seed game.Seed) Result {
	return d.do(StartRound{Tournament: "t", Player: pid(i), Seq: d.seqOf(i), Round: round, Seed: seed})
}

func (d *playDriver) move(i, round, index int, m game.Move) Result {
	return d.do(PlayMove{Tournament: "t", Player: pid(i), Seq: d.seqOf(i), Round: round, MoveIndex: index, Move: m})
}

func (d *playDriver) finish(i, round int) Result {
	return d.do(FinishRound{Tournament: "t", Player: pid(i), Seq: d.seqOf(i), Round: round})
}

// greedy plays round of player i to its end with the greedy strategy and
// returns the last result.
func (d *playDriver) greedy(i, round int) Result {
	d.t.Helper()
	for {
		rec, _ := d.s.Round("t", pid(i), round)
		if rec.Status == RoundDone {
			d.t.Fatalf("greedy on a finished round")
		}
		b, err := game.Replay(rec.Seed, rec.Moves)
		if err != nil {
			d.t.Fatal(err)
		}
		m := game.Move{Kind: game.Draw, Column: game.NoColumn}
		if p := b.Playable(); len(p) > 0 {
			m = game.Move{Kind: game.Play, Column: p[0]}
		}
		r := d.move(i, round, len(rec.Moves), m)
		if r.Code != OK {
			d.t.Fatalf("greedy move: %s %s", r.Code, r.Detail)
		}
		if after, _ := d.s.Round("t", pid(i), round); after.Status == RoundDone {
			return r
		}
	}
}

// setupPlay creates tournament "t" with rules, binds players 0..n-1 and
// enters them.
func (d *playDriver) setupPlay(rules Rules, n int) {
	d.t.Helper()
	d.ok(CreateTournament{ID: "t", Seed: 1, Rules: rules})
	for i := 0; i < n; i++ {
		d.bind(i)
		d.ok(Enter{Tournament: "t", Player: pid(i), Seq: d.seqOf(i)})
	}
}

func TestValidateRulesGame(t *testing.T) {
	r := ladderRules()
	if err := ValidateRules(r); err != nil {
		t.Fatalf("ladder rules refused: %v", err)
	}
	r.MaxScore = 14399
	if err := ValidateRules(r); err == nil || !strings.Contains(err.Error(), "14400") {
		t.Errorf("wrong max_score accepted: %v", err)
	}
	r = ladderRules()
	r.Game = "ladder-v2"
	if err := ValidateRules(r); err == nil {
		t.Error("unknown game accepted")
	}
	d := newPlayDriver(t)
	if res := d.do(CreateTournament{ID: "t", Rules: r}); res.Code != InvalidRules {
		t.Errorf("create with an unknown game = %s", res.Code)
	}
}

func TestOpenSession(t *testing.T) {
	d := newPlayDriver(t)
	d.now = startMs + 5
	res := d.do(OpenSession{Device: device(0), Verifier: verifier(0), Player: pid(0), Jurisdiction: "TR", Age: 30})
	want := PlayOutcome{Player: pid(0), NewPlayer: true, IssuedAtMs: startMs + 5, NextSeq: 1}
	if res.Code != OK || res.Play != want {
		t.Fatalf("first session = %+v", res)
	}
	b, ok := d.s.Binding(device(0))
	if !ok || b.Player != pid(0) || b.BoundAt != d.slot || b.Jurisdiction != "TR" || b.Age != 30 {
		t.Fatalf("binding = %+v", b)
	}
	// A later session of the device ignores the drawn id and the claims.
	d.now = startMs + 9
	res = d.do(OpenSession{Device: device(0), Verifier: verifier(0), Player: pid(7), Jurisdiction: "DE", Age: 50})
	want = PlayOutcome{Player: pid(0), IssuedAtMs: startMs + 9, NextSeq: 1}
	if res.Code != OK || res.Play != want {
		t.Fatalf("second session = %+v", res)
	}
	if b2, _ := d.s.Binding(device(0)); b2 != b {
		t.Errorf("binding changed: %+v", b2)
	}
	if _, ok := d.s.Player(pid(7)); ok {
		t.Error("the unused drawn id was bound")
	}
	cases := []struct {
		name string
		op   OpenSession
		want Code
	}{
		{"short device", OpenSession{Device: "abc", Verifier: verifier(1), Player: pid(1), Jurisdiction: "TR", Age: 30}, InvalidDevice},
		{"upper-case device", OpenSession{Device: DeviceID(strings.Repeat("A", 32)), Verifier: verifier(1), Player: pid(1), Jurisdiction: "TR", Age: 30}, InvalidDevice},
		{"device with letters beyond f", OpenSession{Device: "g" + device(1)[1:], Verifier: verifier(1), Player: pid(1), Jurisdiction: "TR", Age: 30}, InvalidDevice},
		{"zero verifier", OpenSession{Device: device(1), Player: pid(1), Jurisdiction: "TR", Age: 30}, InvalidDevice},
		{"wrong secret", OpenSession{Device: device(0), Verifier: verifier(1), Player: pid(1), Jurisdiction: "TR", Age: 30}, DeviceMismatch},
		{"bad player id", OpenSession{Device: device(1), Verifier: verifier(1), Player: "p:1", Jurisdiction: "TR", Age: 30}, InvalidPlayer},
		{"bad jurisdiction", OpenSession{Device: device(1), Verifier: verifier(1), Player: pid(1), Jurisdiction: "tr", Age: 30}, InvalidPlayer},
		{"bad age", OpenSession{Device: device(1), Verifier: verifier(1), Player: pid(1), Jurisdiction: "TR", Age: 151}, InvalidPlayer},
		{"player taken", OpenSession{Device: device(1), Verifier: verifier(1), Player: pid(0), Jurisdiction: "TR", Age: 30}, PlayerExists},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			before := d.s.Mutations()
			res := d.do(tc.op)
			if res.Code != tc.want {
				t.Fatalf("code = %s (%s), want %s", res.Code, res.Detail, tc.want)
			}
			if d.s.Mutations() != before {
				t.Error("a rejection counted as a mutation")
			}
			if _, bound := d.s.Binding(device(1)); bound {
				t.Error("a rejected session bound device 1")
			}
			if res.Play != (PlayOutcome{}) {
				t.Errorf("a rejected session carries an outcome: %+v", res.Play)
			}
		})
	}
}

func TestSequenceNumbers(t *testing.T) {
	d := newPlayDriver(t)
	d.ok(CreateTournament{ID: "t", Seed: 1, Rules: ladderRules()})
	d.bind(0)
	if res := d.do(Enter{Tournament: "t", Player: "p-nobody", Seq: 1}); res.Code != UnknownPlayer || res.Play.Player != "p-nobody" || res.Play.NextSeq != 0 {
		t.Fatalf("unknown player = %+v", res)
	}
	for _, seq := range []uint64{0, 2, 9} {
		res := d.do(Enter{Tournament: "t", Player: pid(0), Seq: seq})
		want := SeqGap
		if seq == 0 {
			want = StaleSeq
		}
		if res.Code != want || res.Play.NextSeq != 1 {
			t.Fatalf("seq %d = %+v, want %s with next 1", seq, res, want)
		}
	}
	if rec, _ := d.s.Player(pid(0)); rec.LastSeq != 0 {
		t.Fatalf("stale and gap consumed numbers: last %d", rec.LastSeq)
	}
	// A rule rejection consumes its number.
	res := d.do(Enter{Tournament: "nope", Player: pid(0), Seq: 1})
	if res.Code != UnknownTournament || res.Play.NextSeq != 2 {
		t.Fatalf("unknown tournament = %+v", res)
	}
	if res := d.do(Enter{Tournament: "t", Player: pid(0), Seq: 1}); res.Code != StaleSeq {
		t.Fatalf("reused number = %s", res.Code)
	}
	res = d.ok(Enter{Tournament: "t", Player: pid(0), Seq: 2})
	if res.Play.NextSeq != 3 || res.Play.JoinSeq != 1 {
		t.Fatalf("enter = %+v", res.Play)
	}
	// A replay of a stale rejection is the recorded rejection.
	d.applyKey("stale-key", Enter{Tournament: "t", Player: pid(0), Seq: 1})
	again := d.applyKey("stale-key", Enter{Tournament: "t", Player: pid(0), Seq: 1})
	if again.Code != StaleSeq || !again.Replayed || again.Play.NextSeq != 3 {
		t.Fatalf("replayed stale = %+v", again)
	}
	if rec, _ := d.s.Player(pid(0)); rec.LastSeq != 2 {
		t.Fatalf("last seq = %d, want 2", rec.LastSeq)
	}
}

func TestEnterValidationOrder(t *testing.T) {
	build := func(t *testing.T) *playDriver {
		d := newPlayDriver(t)
		r := ladderRules()
		r.MaxEntrants = 3
		r.MinAge = 21
		d.ok(CreateTournament{ID: "t", Seed: 1, Rules: r})
		d.ok(CreateTournament{ID: "legacy", Seed: 1, Rules: baseRules()})
		closed := ladderRules()
		d.ok(CreateTournament{ID: "closed", Seed: 1, Rules: closed})
		d.ok(Close{Tournament: "closed"})
		// Player 0: TR 30; player 1: excluded XX, 18 years old.
		d.bind(0)
		d.ok(OpenSession{Device: device(1), Verifier: verifier(1), Player: pid(1), Jurisdiction: "XX", Age: 18})
		d.ok(OpenSession{Device: device(2), Verifier: verifier(2), Player: pid(2), Jurisdiction: "TR", Age: 18})
		return d
	}
	cases := []struct {
		name string
		prep func(d *playDriver)
		op   func(d *playDriver) Enter
		want Code
	}{
		{"unknown_player", nil, func(d *playDriver) Enter { return Enter{Tournament: "nope", Player: "ghost", Seq: 0} }, UnknownPlayer},
		{"stale_seq", nil, func(d *playDriver) Enter { return Enter{Tournament: "nope", Player: pid(0), Seq: 0} }, StaleSeq},
		{"seq_gap", nil, func(d *playDriver) Enter { return Enter{Tournament: "nope", Player: pid(0), Seq: 2} }, SeqGap},
		{"unknown_tournament", nil, func(d *playDriver) Enter { return Enter{Tournament: "nope", Player: pid(1), Seq: 1} }, UnknownTournament},
		{"not_play_tournament", nil, func(d *playDriver) Enter { return Enter{Tournament: "legacy", Player: pid(1), Seq: 1} }, NotPlayTournament},
		{"not_open", nil, func(d *playDriver) Enter { return Enter{Tournament: "closed", Player: pid(1), Seq: 1} }, NotOpen},
		{"tournament_full", func(d *playDriver) {
			for i := 3; i < 6; i++ {
				d.bind(i)
				d.ok(Enter{Tournament: "t", Player: pid(i), Seq: 1})
			}
		}, func(d *playDriver) Enter { return Enter{Tournament: "t", Player: pid(1), Seq: 1} }, TournamentFull},
		{"already_joined", func(d *playDriver) {
			d.ok(Enter{Tournament: "t", Player: pid(0), Seq: 1})
		}, func(d *playDriver) Enter { return Enter{Tournament: "t", Player: pid(0), Seq: 2} }, AlreadyJoined},
		{"jurisdiction_excluded", nil, func(d *playDriver) Enter { return Enter{Tournament: "t", Player: pid(1), Seq: 1} }, JurisdictionExcluded},
		{"underage", nil, func(d *playDriver) Enter { return Enter{Tournament: "t", Player: pid(2), Seq: 1} }, Underage},
		{"ledger_conflict", func(d *playDriver) {
			d.s.book.Post(ledger.Posting{Key: ledger.FeeKey("t", string(pid(0))), Kind: ledger.EntryFee, Debit: "x", Credit: "y", Amount: 1})
		}, func(d *playDriver) Enter { return Enter{Tournament: "t", Player: pid(0), Seq: 1} }, LedgerConflict},
		{"ok", nil, func(d *playDriver) Enter { return Enter{Tournament: "t", Player: pid(0), Seq: 1} }, OK},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := build(t)
			if tc.prep != nil {
				tc.prep(d)
			}
			op := tc.op(d)
			before := d.s.Ledger().Len()
			tr, _ := d.s.Tournament(op.Tournament)
			res := d.do(op)
			if res.Code != tc.want {
				t.Fatalf("code = %s (%s), want %s", res.Code, res.Detail, tc.want)
			}
			after, _ := d.s.Tournament(op.Tournament)
			if tc.want != OK {
				if d.s.Ledger().Len() != before || len(after.Entries) != len(tr.Entries) {
					t.Error("a rejected enter changed the entries or the ledger")
				}
				return
			}
			if len(after.Entries) != 1 || after.Entries[0].Player != (Player{ID: pid(0), Jurisdiction: "TR", Age: 30}) || res.Play.JoinSeq != 1 {
				t.Fatalf("entry = %+v, outcome %+v", after.Entries, res.Play)
			}
			if p, ok := d.s.Ledger().Get(ledger.FeeKey("t", string(pid(0)))); !ok || p.Amount != 500 {
				t.Errorf("fee posting = %+v", p)
			}
		})
	}
}

func TestLegacyCommandsOnPlayTournament(t *testing.T) {
	d := newPlayDriver(t)
	d.setupPlay(ladderRules(), 1)
	if res := d.do(Join{Tournament: "t", Player: player("pz", "TR", 30)}); res.Code != PlayIntentRequired {
		t.Errorf("join = %s", res.Code)
	}
	if res := d.do(Join{Tournament: "t", Player: player("p:z", "TR", 30)}); res.Code != InvalidPlayer {
		t.Errorf("malformed join = %s, want invalid_player first", res.Code)
	}
	if res := d.do(Join{Tournament: "nope", Player: player("pz", "TR", 30)}); res.Code != UnknownTournament {
		t.Errorf("join unknown = %s", res.Code)
	}
	if res := d.do(SubmitScore{Tournament: "t", Player: pid(0), Score: 14400, DealSeed: 1}); res.Code != PlayIntentRequired {
		t.Errorf("submit = %s", res.Code)
	}
	if res := d.do(SubmitScore{Tournament: "nope", Player: pid(0), Score: 1}); res.Code != UnknownTournament {
		t.Errorf("submit unknown = %s", res.Code)
	}
}

func TestStartRound(t *testing.T) {
	build := func(t *testing.T) *playDriver {
		d := newPlayDriver(t)
		d.setupPlay(ladderRules(), 2)
		d.bind(5) // not entered
		d.ok(CreateTournament{ID: "closed", Seed: 1, Rules: ladderRules()})
		d.ok(Enter{Tournament: "closed", Player: pid(0), Seq: d.seqOf(0)})
		d.ok(Close{Tournament: "closed"})
		return d
	}
	cases := []struct {
		name  string
		prep  func(d *playDriver)
		op    func(d *playDriver) StartRound
		want  Code
		round int
	}{
		{"not_open", nil, func(d *playDriver) StartRound {
			return StartRound{Tournament: "closed", Player: pid(5), Seq: d.seqOf(5), Round: 9}
		}, NotOpen, 9},
		{"not_joined", nil, func(d *playDriver) StartRound {
			return StartRound{Tournament: "t", Player: pid(5), Seq: d.seqOf(5), Round: 9}
		}, NotJoined, 9},
		{"invalid_round 0", nil, func(d *playDriver) StartRound {
			return StartRound{Tournament: "t", Player: pid(0), Seq: d.seqOf(0), Round: 0}
		}, InvalidRound, 0},
		{"invalid_round 4", nil, func(d *playDriver) StartRound {
			return StartRound{Tournament: "t", Player: pid(0), Seq: d.seqOf(0), Round: 4}
		}, InvalidRound, 4},
		{"round_already_started", func(d *playDriver) { d.deal(0, 1, vectorSeed(1)) }, func(d *playDriver) StartRound {
			return StartRound{Tournament: "t", Player: pid(0), Seq: d.seqOf(0), Round: 1}
		}, RoundAlreadyStarted, 1},
		{"previous_round_unfinished in play", func(d *playDriver) { d.deal(0, 1, vectorSeed(1)) }, func(d *playDriver) StartRound {
			return StartRound{Tournament: "t", Player: pid(0), Seq: d.seqOf(0), Round: 2}
		}, PreviousRoundUnfinished, 2},
		{"previous_round_unfinished never dealt", nil, func(d *playDriver) StartRound {
			return StartRound{Tournament: "t", Player: pid(0), Seq: d.seqOf(0), Round: 2}
		}, PreviousRoundUnfinished, 2},
		{"ok", nil, func(d *playDriver) StartRound {
			return StartRound{Tournament: "t", Player: pid(0), Seq: d.seqOf(0), Round: 1, Seed: vectorSeed(1)}
		}, OK, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := build(t)
			if tc.prep != nil {
				tc.prep(d)
			}
			op := tc.op(d)
			events := d.s.EventCount()
			d.now += 1234
			res := d.do(op)
			if res.Code != tc.want || res.Play.Round != tc.round || res.Play.NextSeq != op.Seq+1 {
				t.Fatalf("result = %+v, want %s", res, tc.want)
			}
			if tc.want != OK {
				if d.s.EventCount() != events {
					t.Error("a rejected deal recorded an event")
				}
				return
			}
			rec, ok := d.s.Round("t", pid(0), 1)
			want := RoundRecord{Tournament: "t", Player: pid(0), Round: 1, Seed: vectorSeed(1), Moves: []game.Move{},
				Status: RoundInPlay, StartedAt: d.slot, StartedAtMs: d.now, DeadlineMs: d.now + 300_000, UpdatedAt: d.slot}
			if !ok || !reflect.DeepEqual(rec, want) {
				t.Fatalf("record = %+v\nwant %+v", rec, want)
			}
			evs := d.s.Events(d.slot-1, "", pid(0), 0)
			if len(evs) != 1 || evs[0] != (EventRecord{Slot: d.slot, Type: EventRoundStarted, Tournament: "t", Player: pid(0), Round: 1, Status: "playing", TimeMs: d.now}) {
				t.Fatalf("events = %+v", evs)
			}
		})
	}
}

func TestPlayMove(t *testing.T) {
	draw := game.Move{Kind: game.Draw, Column: game.NoColumn}
	build := func(t *testing.T) *playDriver {
		d := newPlayDriver(t)
		d.setupPlay(ladderRules(), 2)
		d.bind(5)
		d.deal(0, 1, vectorSeed(1))
		return d
	}
	cases := []struct {
		name string
		prep func(d *playDriver)
		op   func(d *playDriver) PlayMove
		want Code
	}{
		{"not_open", func(d *playDriver) { d.ok(Close{Tournament: "t"}) }, func(d *playDriver) PlayMove {
			return PlayMove{Tournament: "t", Player: pid(0), Seq: d.seqOf(0), Round: 1, MoveIndex: 0, Move: draw}
		}, NotOpen},
		{"not_joined", nil, func(d *playDriver) PlayMove {
			return PlayMove{Tournament: "t", Player: pid(5), Seq: d.seqOf(5), Round: 7, MoveIndex: 3, Move: draw}
		}, NotJoined},
		{"invalid_round", nil, func(d *playDriver) PlayMove {
			return PlayMove{Tournament: "t", Player: pid(0), Seq: d.seqOf(0), Round: 7, MoveIndex: 3, Move: draw}
		}, InvalidRound},
		{"round_not_started", nil, func(d *playDriver) PlayMove {
			return PlayMove{Tournament: "t", Player: pid(1), Seq: d.seqOf(1), Round: 1, MoveIndex: 3, Move: draw}
		}, RoundNotStarted},
		{"round_finished", func(d *playDriver) { d.finish(0, 1) }, func(d *playDriver) PlayMove {
			return PlayMove{Tournament: "t", Player: pid(0), Seq: d.seqOf(0), Round: 1, MoveIndex: 3, Move: draw}
		}, RoundFinished},
		{"round_expired", func(d *playDriver) { d.now += 300_001 }, func(d *playDriver) PlayMove {
			return PlayMove{Tournament: "t", Player: pid(0), Seq: d.seqOf(0), Round: 1, MoveIndex: 3, Move: draw}
		}, RoundExpired},
		{"at the deadline", func(d *playDriver) { d.now += 300_000 }, func(d *playDriver) PlayMove {
			return PlayMove{Tournament: "t", Player: pid(0), Seq: d.seqOf(0), Round: 1, MoveIndex: 0, Move: draw}
		}, OK},
		{"move_index_mismatch", nil, func(d *playDriver) PlayMove {
			return PlayMove{Tournament: "t", Player: pid(0), Seq: d.seqOf(0), Round: 1, MoveIndex: 1, Move: game.Move{Kind: "undo"}}
		}, MoveIndexMismatch},
		{"illegal_move not adjacent", nil, func(d *playDriver) PlayMove {
			return PlayMove{Tournament: "t", Player: pid(0), Seq: d.seqOf(0), Round: 1, MoveIndex: 0, Move: game.Move{Kind: game.Play, Column: 0}}
		}, IllegalMove},
		{"illegal_move bad column", nil, func(d *playDriver) PlayMove {
			return PlayMove{Tournament: "t", Player: pid(0), Seq: d.seqOf(0), Round: 1, MoveIndex: 0, Move: game.Move{Kind: game.Play, Column: 7}}
		}, IllegalMove},
		{"illegal_move unknown kind", nil, func(d *playDriver) PlayMove {
			return PlayMove{Tournament: "t", Player: pid(0), Seq: d.seqOf(0), Round: 1, MoveIndex: 0, Move: game.Move{Kind: "undo"}}
		}, IllegalMove},
		{"ok", nil, func(d *playDriver) PlayMove {
			return PlayMove{Tournament: "t", Player: pid(0), Seq: d.seqOf(0), Round: 1, MoveIndex: 0, Move: draw}
		}, OK},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := build(t)
			if tc.prep != nil {
				tc.prep(d)
			}
			op := tc.op(d)
			before, _ := d.s.Round("t", op.Player, op.Round)
			res := d.do(op)
			if res.Code != tc.want || res.Play.NextSeq != op.Seq+1 || res.Play.Round != op.Round {
				t.Fatalf("result = %+v, want %s", res, tc.want)
			}
			after, _ := d.s.Round("t", op.Player, op.Round)
			if tc.want != OK {
				if !reflect.DeepEqual(before, after) {
					t.Error("a rejected move changed the round")
				}
				if tc.want == IllegalMove && !strings.HasPrefix(res.Detail, "game: ") {
					t.Errorf("illegal_move detail %q is not the game error", res.Detail)
				}
				return
			}
			if len(after.Moves) != 1 || after.UpdatedAt != d.slot || res.Play.MoveIndex != 1 || after.Status != RoundInPlay {
				t.Fatalf("after = %+v outcome %+v", after, res.Play)
			}
		})
	}
	if msg := (func() string {
		d := build(t)
		return d.do(PlayMove{Tournament: "t", Player: pid(0), Seq: d.seqOf(0), Round: 1, Move: game.Move{Kind: game.Play, Column: 0}}).Detail
	})(); msg != game.ErrNotAdjacent.Error() {
		t.Errorf("detail = %q, want the game error message", msg)
	}
}

func TestRoundsFinishByThemselves(t *testing.T) {
	d := newPlayDriver(t)
	d.setupPlay(ladderRules(), 2)
	// Player 0 plays the Appendix A greedy games: every round blocked.
	scores := []int64{3200, 2600, 2800}
	for r := 1; r <= 3; r++ {
		if res := d.deal(0, r, vectorSeed(r)); res.Code != OK {
			t.Fatalf("deal %d: %s %s", r, res.Code, res.Detail)
		}
		last := d.greedy(0, r)
		rec, _ := d.s.Round("t", pid(0), r)
		if rec.Status != RoundDone || rec.Reason != game.Blocked || rec.Score != scores[r-1] || rec.FinishedAt != d.slot || last.Play.MoveIndex != len(rec.Moves) {
			t.Fatalf("round %d = %+v", r, rec)
		}
	}
	tr, _ := d.s.Tournament("t")
	e, _ := tr.Entry(pid(0))
	if !e.Scored || e.Score != 8600 || e.SubmitSeq != 1 {
		t.Fatalf("entry = %+v", e)
	}
	evs := d.s.Events(0, "t", pid(0), 0)
	var types []string
	for _, ev := range evs {
		types = append(types, string(ev.Type))
	}
	want := "round_started round_finished leaderboard_changed round_started round_finished leaderboard_changed round_started round_finished leaderboard_changed entry_scored"
	if strings.Join(types, " ") != want {
		t.Fatalf("events = %s", strings.Join(types, " "))
	}
	if ev := evs[len(evs)-1]; ev.TotalScore != 8600 || ev.RoundsFinished != 3 {
		t.Errorf("entry_scored = %+v", ev)
	}
	if ev := evs[4]; ev.Score != 2600 || ev.TotalScore != 5800 || ev.RoundsFinished != 2 {
		t.Errorf("round 2 finished = %+v", ev)
	}
	// Player 1 clears the tableau.
	d.deal(1, 1, clearingSeed)
	d.greedy(1, 1)
	rec, _ := d.s.Round("t", pid(1), 1)
	if rec.Reason != game.Cleared || rec.Cleared != 35 || rec.Score != 3500+500+50 {
		t.Fatalf("cleared round = %+v", rec)
	}
	// Player 1's events do not include player 0's own events.
	for _, ev := range d.s.Events(0, "", pid(1), 0) {
		if ev.Player != "" && ev.Player != pid(1) {
			t.Fatalf("player 1 sees %+v", ev)
		}
	}
}

func TestFinishRound(t *testing.T) {
	d := newPlayDriver(t)
	d.setupPlay(ladderRules(), 1)
	d.bind(5)
	if res := d.finish(5, 1); res.Code != NotJoined {
		t.Errorf("not joined = %s", res.Code)
	}
	if res := d.finish(0, 4); res.Code != InvalidRound {
		t.Errorf("invalid round = %s", res.Code)
	}
	if res := d.finish(0, 1); res.Code != RoundNotStarted {
		t.Errorf("not started = %s", res.Code)
	}
	d.deal(0, 1, vectorSeed(1))
	d.move(0, 1, 0, game.Move{Kind: game.Draw, Column: game.NoColumn})
	d.move(0, 1, 1, game.Move{Kind: game.Play, Column: 1})
	res := d.finish(0, 1)
	rec, _ := d.s.Round("t", pid(0), 1)
	if res.Code != OK || res.Play.MoveIndex != 2 || rec.Reason != game.Resigned || rec.Score != 100 || rec.Cleared != 1 {
		t.Fatalf("resign = %+v, record %+v", res, rec)
	}
	// Finishing again succeeds, consumes the number and changes nothing else.
	events, seq := d.s.EventCount(), d.seqOf(0)
	res = d.finish(0, 1)
	again, _ := d.s.Round("t", pid(0), 1)
	if res.Code != OK || !reflect.DeepEqual(again, rec) || d.s.EventCount() != events || d.seqOf(0) != seq+1 {
		t.Fatalf("second finish = %+v, record %+v", res, again)
	}
	// Past the deadline the reason is expired.
	d.deal(0, 2, vectorSeed(2))
	d.now += 300_001
	d.finish(0, 2)
	if rec, _ := d.s.Round("t", pid(0), 2); rec.Reason != game.Expired || rec.Score != 0 {
		t.Fatalf("expired = %+v", rec)
	}
}

func TestCloseFinishesRoundsAndScoresEntries(t *testing.T) {
	d := newPlayDriver(t)
	r := ladderRules()
	r.MinEntrants = 3
	d.setupPlay(r, 5)
	// Player 0: three rounds played, scored before close.
	for round := 1; round <= 3; round++ {
		d.deal(0, round, vectorSeed(round))
		d.greedy(0, round)
	}
	// Player 1: round 1 finished, round 2 in play and past its deadline.
	d.deal(1, 1, vectorSeed(1))
	d.greedy(1, 1)
	d.deal(1, 2, vectorSeed(2))
	d.move(1, 2, 0, game.Move{Kind: game.Draw, Column: game.NoColumn})
	d.now += 300_001
	// Player 2: round 1 in play, dealt after the clock moved, not expired.
	d.deal(2, 1, vectorSeed(1))
	d.move(2, 1, 0, game.Move{Kind: game.Draw, Column: game.NoColumn})
	d.move(2, 1, 1, game.Move{Kind: game.Play, Column: 1})
	// Player 3: round 3 in play.
	for round := 1; round <= 2; round++ {
		d.deal(3, round, vectorSeed(round))
		d.greedy(3, round)
	}
	d.deal(3, 3, vectorSeed(3))
	// Player 4 never deals.
	eventsBefore := d.slot
	d.ok(Close{Tournament: "t"})
	tr, _ := d.s.Tournament("t")
	check := func(p int, round int, reason game.FinishReason) {
		t.Helper()
		rec, _ := d.s.Round("t", pid(p), round)
		if rec.Status != RoundDone || rec.Reason != reason || rec.FinishedAt != d.slot {
			t.Errorf("player %d round %d = %+v", p, round, rec)
		}
	}
	check(1, 2, game.Expired)
	check(2, 1, game.Closed)
	check(3, 3, game.Closed)
	wantEntries := []struct {
		scored bool
		score  int64
		seq    uint32
	}{{true, 8600, 1}, {true, 3200, 3}, {true, 100, 4}, {true, 5800, 2}, {false, 0, 0}}
	for i, w := range wantEntries {
		e := tr.Entries[i]
		if e.Scored != w.scored || e.Score != w.score || e.SubmitSeq != w.seq {
			t.Errorf("entry %d = %+v, want %+v", i, e, w)
		}
	}
	if tr.Status != Closed || len(tr.Standings) != 5 || tr.Standings[0].Player != pid(0) || tr.Standings[1].Player != pid(3) || tr.Standings[4].Player != pid(4) {
		t.Fatalf("status %s standings %+v", tr.Status, tr.Standings)
	}
	var got []string
	for _, ev := range d.s.play.events {
		if ev.Slot > eventsBefore {
			got = append(got, fmt.Sprintf("%s:%s", ev.Type, ev.Player))
		}
	}
	want := "round_finished:p-1 round_finished:p-2 round_finished:p-3 entry_scored:p-3 entry_scored:p-1 entry_scored:p-2 tournament_status: leaderboard_changed:"
	if strings.Join(got, " ") != want {
		t.Fatalf("close events:\n%s\nwant\n%s", strings.Join(got, " "), want)
	}
	// Moves, deals and finishes on a closed tournament.
	if res := d.move(2, 1, 2, game.Move{Kind: game.Draw, Column: game.NoColumn}); res.Code != NotOpen {
		t.Errorf("move after close = %s", res.Code)
	}
	if res := d.finish(2, 1); res.Code != OK {
		t.Errorf("finish of a closed round = %s", res.Code)
	}
	if res := d.deal(2, 2, vectorSeed(2)); res.Code != NotOpen {
		t.Errorf("deal after close = %s", res.Code)
	}
}

func TestClaimPayout(t *testing.T) {
	d := newPlayDriver(t)
	r := ladderRules()
	r.MinEntrants = 3
	r.RakeBps = 0
	d.ok(CreateTournament{ID: "t", Seed: 1, Rules: r})
	for i := 0; i < 4; i++ {
		jur := "TR"
		if i == 2 {
			jur = "YY"
		}
		d.ok(OpenSession{Device: device(i), Verifier: verifier(i), Player: pid(i), Jurisdiction: jur, Age: 30})
		d.ok(Enter{Tournament: "t", Player: pid(i), Seq: 1})
	}
	d.bind(9)
	claim := func(i int) Result {
		return d.do(ClaimPayout{Tournament: "t", Player: pid(i), Seq: d.seqOf(i)})
	}
	if res := claim(9); res.Code != NotJoined {
		t.Errorf("not joined = %s", res.Code)
	}
	if res := claim(0); res.Code != NotSettled {
		t.Errorf("open = %s", res.Code)
	}
	// Players 0 to 2 finish round 1 with different scores; player 3 never
	// deals. Player 2's jurisdiction is excluded at settlement.
	for i, seed := range []game.Seed{vectorSeed(1), vectorSeed(2), vectorSeed(3)} {
		d.deal(i, 1, seed)
		d.greedy(i, 1)
	}
	d.ok(Close{Tournament: "t"})
	if res := claim(0); res.Code != NotSettled {
		t.Errorf("closed = %s", res.Code)
	}
	d.ok(Settle{Tournament: "t", Exclusions: Exclusions{Version: 8, Jurisdictions: []string{"XX", "YY"}}})
	tr, _ := d.s.Tournament("t")
	if tr.Status != Settled || len(tr.Payouts) != 3 {
		t.Fatalf("settled = %+v", tr)
	}
	// Player 0 won first place: 3200 > 2800 > 2600.
	evs := d.s.Events(d.slot-1, "t", pid(0), 0)
	if len(evs) != 3 || evs[0].Type != EventTournamentStatus || evs[0].Status != "settled" || evs[1].Type != EventLeaderboardChanged ||
		evs[2].Type != EventPayoutAvailable || evs[2].Amount != 1000 {
		t.Fatalf("settle events for player 0 = %+v", evs)
	}
	if res := claim(3); res.Code != NoPayout {
		t.Errorf("no payout = %s", res.Code)
	}
	key := IdempotencyKey("claim-0")
	res := d.applyKey(key, ClaimPayout{Tournament: "t", Player: pid(0), Seq: d.seqOf(0)})
	if res.Code != OK || res.Play.Amount != 1000 {
		t.Fatalf("claim = %+v", res)
	}
	post, ok := d.s.Ledger().Get(ledger.ClaimKey("t", string(pid(0))))
	if !ok || post.Kind != ledger.Claim || post.Amount != 1000 || post.Debit != ledger.PlayerAccount(string(pid(0))) || post.Credit != ledger.ClaimsAccount("t") || post.Slot != d.slot {
		t.Fatalf("claim posting = %+v", post)
	}
	if bal := d.s.Ledger().Balance(ledger.PlayerAccount(string(pid(0)))); bal != -500+1000-1000 {
		t.Errorf("player balance = %d", bal)
	}
	evs = d.s.Events(d.slot-1, "t", pid(0), 0)
	if len(evs) != 1 || evs[0].Type != EventPayoutClaimed || evs[0].Amount != 1000 {
		t.Errorf("claim events = %+v", evs)
	}
	postings := d.s.Ledger().Len()
	replay := d.applyKey(key, ClaimPayout{Tournament: "t", Player: pid(0), Seq: res.Play.NextSeq - 1})
	if replay.Code != OK || !replay.Replayed || replay.Play != res.Play {
		t.Errorf("replay = %+v", replay)
	}
	if res := claim(0); res.Code != AlreadyClaimed {
		t.Errorf("second claim = %s", res.Code)
	}
	if d.s.Ledger().Len() != postings {
		t.Error("a replay or a second claim posted")
	}
	// Player 2 is withheld: nothing to claim.
	if res := claim(2); res.Code != NoPayout {
		t.Errorf("withheld claim = %s", res.Code)
	}
	if err := d.s.Ledger().Check(); err != nil {
		t.Fatal(err)
	}

	// A voided tournament's refunds are claimable.
	v := newPlayDriver(t)
	v.setupPlay(ladderRules(), 2)
	v.ok(Close{Tournament: "t"})
	v.ok(Settle{Tournament: "t"})
	if res := v.do(ClaimPayout{Tournament: "t", Player: pid(1), Seq: v.seqOf(1)}); res.Code != OK || res.Play.Amount != 500 {
		t.Errorf("refund claim = %+v", res)
	}
}

func TestServerDrawnFieldsAndReplays(t *testing.T) {
	d := newPlayDriver(t)
	d.ok(CreateTournament{ID: "t", Seed: 1, Rules: ladderRules()})
	first := d.applyKey("d.dev.s1", OpenSession{Device: device(0), Verifier: verifier(0), Player: pid(0), Jurisdiction: "TR", Age: 30})
	d.now += 50
	again := d.applyKey("d.dev.s1", OpenSession{Device: device(0), Verifier: verifier(0), Player: pid(8), Jurisdiction: "TR", Age: 30})
	if !again.Replayed || again.Play != first.Play || again.Slot != first.Slot {
		t.Fatalf("session retry with a new drawn id = %+v, first %+v", again, first)
	}
	if res := d.applyKey("d.dev.s1", OpenSession{Device: device(0), Verifier: verifier(0), Player: pid(0), Jurisdiction: "DE", Age: 30}); res.Code != KeyReused {
		t.Errorf("changed claims under the key = %s", res.Code)
	}
	d.ok(Enter{Tournament: "t", Player: pid(0), Seq: 1})
	dealt := d.applyKey("u.p.deal", StartRound{Tournament: "t", Player: pid(0), Seq: 2, Round: 1, Seed: vectorSeed(1)})
	retry := d.applyKey("u.p.deal", StartRound{Tournament: "t", Player: pid(0), Seq: 2, Round: 1, Seed: vectorSeed(3)})
	if dealt.Code != OK || !retry.Replayed || retry.Play != dealt.Play {
		t.Fatalf("deal retry with another seed = %+v", retry)
	}
	if rec, _ := d.s.Round("t", pid(0), 1); rec.Seed != vectorSeed(1) {
		t.Error("the retry's seed replaced the committed one")
	}
	if res := d.applyKey("u.p.deal", StartRound{Tournament: "t", Player: pid(0), Seq: 2, Round: 2, Seed: vectorSeed(1)}); res.Code != KeyReused {
		t.Errorf("another round under the key = %s", res.Code)
	}
	if res := d.applyKey("u.p.deal", PlayMove{Tournament: "t", Player: pid(0), Seq: 2, Round: 1}); res.Code != KeyReused {
		t.Errorf("another op under the key = %s", res.Code)
	}
}

func TestEventsPaging(t *testing.T) {
	d := newPlayDriver(t)
	r := ladderRules()
	d.setupPlay(r, 2)
	d.ok(CreateTournament{ID: "u", Seed: 1, Rules: r})
	d.ok(Enter{Tournament: "u", Player: pid(0), Seq: d.seqOf(0)})
	// Rounds in both tournaments for player 0, one for player 1.
	d.deal(0, 1, vectorSeed(1))
	d.greedy(0, 1)
	d.do(StartRound{Tournament: "u", Player: pid(0), Seq: d.seqOf(0), Round: 1, Seed: vectorSeed(2)})
	d.deal(1, 1, vectorSeed(3))
	d.greedy(1, 1)
	all := d.s.Events(0, "", pid(0), 0)
	// round_started(t), round_finished(t)+leaderboard(t) in one slot,
	// round_started(u), leaderboard(t) from player 1's last move.
	var sum []string
	for _, ev := range all {
		sum = append(sum, fmt.Sprintf("%s/%s", ev.Type, ev.Tournament))
	}
	if strings.Join(sum, " ") != "round_started/t round_finished/t leaderboard_changed/t round_started/u leaderboard_changed/t" {
		t.Fatalf("all = %s", strings.Join(sum, " "))
	}
	if only := d.s.Events(0, "u", pid(0), 0); len(only) != 1 || only[0].Tournament != "u" {
		t.Errorf("filtered = %+v", only)
	}
	// max 2 never splits the slot of round_finished and its leaderboard.
	page := d.s.Events(0, "", pid(0), 2)
	if len(page) != 3 || page[1].Slot != page[2].Slot {
		t.Fatalf("page = %+v", page)
	}
	next := d.s.Events(page[len(page)-1].Slot, "", pid(0), 2)
	if len(next) != 2 || next[0].Tournament != "u" {
		t.Fatalf("next page = %+v", next)
	}
	if rest := d.s.Events(next[1].Slot, "", pid(0), 2); len(rest) != 0 {
		t.Errorf("after the last = %+v", rest)
	}
	if d.s.Events(0, "", "ghost", 0) != nil {
		t.Error("unknown player sees events")
	}
	// HasEvents answers what Events with max 1 would, for every cursor,
	// filter and player.
	last := all[len(all)-1].Slot
	for after := paxos.Slot(0); after <= last+1; after++ {
		for _, filter := range []TournamentID{"", "t", "u", "v"} {
			for _, p := range []PlayerID{pid(0), pid(1), "ghost"} {
				if got, want := d.s.HasEvents(after, filter, p), len(d.s.Events(after, filter, p, 1)) > 0; got != want {
					t.Errorf("HasEvents(%d, %q, %s) = %v, Events says %v", after, filter, p, got, want)
				}
			}
		}
	}
}

// playCorpus is one command per play op, for codec round trips and fuzz
// seeds.
func playCorpus() []Command {
	return []Command{
		{Key: "d.0123.k", ReceivedAt: 10, Op: OpenSession{Device: device(3), Verifier: verifier(3), Player: pid(3), Jurisdiction: "TR", Age: 31}},
		{Key: "u.p-3.k1", ReceivedAt: 11, Op: Enter{Tournament: "t1", Player: pid(3), Seq: 1}},
		{Key: "u.p-3.k2", ReceivedAt: 12, Op: StartRound{Tournament: "t1", Player: pid(3), Seq: 2, Round: 1, Seed: vectorSeed(1)}},
		{Key: "u.p-3.k3", ReceivedAt: 13, Op: PlayMove{Tournament: "t1", Player: pid(3), Seq: 3, Round: 1, MoveIndex: 0, Move: game.Move{Kind: game.Draw, Column: -1}}},
		{Key: "u.p-3.k4", ReceivedAt: 14, Op: FinishRound{Tournament: "t1", Player: pid(3), Seq: 4, Round: 1}},
		{Key: "u.p-3.k5", ReceivedAt: 15, Op: ClaimPayout{Tournament: "t1", Player: pid(3), Seq: 5}},
	}
}

func TestPlayCodec(t *testing.T) {
	names := map[string]bool{}
	for _, c := range playCorpus() {
		enc, err := Encode(c)
		if err != nil {
			t.Fatal(err)
		}
		dec, err := Decode(enc)
		if err != nil {
			t.Fatalf("Decode(%s): %v", enc, err)
		}
		if !reflect.DeepEqual(dec, c) {
			t.Errorf("round trip:\n got %+v\nwant %+v", dec, c)
		}
		names[OpName(c.Op)] = true
		if _, isSession := c.Op.(OpenSession); TournamentOf(c.Op) == "" != isSession {
			t.Errorf("TournamentOf(%s) = %q", OpName(c.Op), TournamentOf(c.Op))
		}
	}
	for _, n := range []string{"open_session", "enter", "start_round", "play_move", "finish_round", "claim_payout"} {
		if !names[n] {
			t.Errorf("op %s missing", n)
		}
	}
	if _, err := Decode([]byte(`{"key":"k","received_at":0,"op":"start_round","body":{"tournament":"t","player":"p","seq":1,"round":1,"seed":"` + strings.Repeat("A", 64) + `"}}`)); err == nil {
		t.Error("an upper-case seed decoded")
	}
	// Result encodings of legacy commands carry no play field.
	if b := mustMarshal(Result{Code: OK, Slot: 3}); bytes.Contains(b, []byte(`"play"`)) {
		t.Errorf("legacy result encodes %s", b)
	}
}
