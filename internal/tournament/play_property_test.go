package tournament

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/oguzhanozfe/paxos-arena/internal/game"
	"github.com/oguzhanozfe/paxos-arena/internal/ledger"
	"github.com/oguzhanozfe/paxos-arena/internal/paxos"
)

// logEntry is one slot of a generated log: a command, or a skipped slot.
type logEntry struct {
	skip bool
	cmd  Command
}

// playGen builds a random log of play intents the way clients and an
// operator would send them, reading a reference state to choose mostly
// sensible intents: sessions, entries, deals, legal and illegal moves,
// finishes, claims, closes and settlements, mixed with exact resends,
// resends with a new server-drawn field, key reuse with another payload,
// stale and skipped sequence numbers, skipped slots, a receipt clock that
// sometimes runs backwards, and jumps past round deadlines.
type playGen struct {
	t    *testing.T
	rng  *rand.Rand
	ref  *State
	log  []logEntry
	now  int64
	keys int
	sent []Command
}

const genPlayers = 5

var genTournaments = []TournamentID{"t0", "t1", "t2"}

func (g *playGen) key(prefix string) IdempotencyKey {
	g.keys++
	return IdempotencyKey(fmt.Sprintf("%s.%d", prefix, g.keys))
}

func (g *playGen) emit(cmd Command) Result {
	g.log = append(g.log, logEntry{cmd: cmd})
	g.sent = append(g.sent, cmd)
	return g.ref.Apply(paxos.Slot(len(g.log)), paxos.Ballot{Round: 1, Node: 1}, cmd)
}

func (g *playGen) tick() int64 {
	switch n := g.rng.IntN(400); {
	case n < 20:
		g.now -= g.rng.Int64N(5000) // a leader whose clock is behind
	case n == 20:
		g.now += 250_000 + g.rng.Int64N(100_000)
	default:
		g.now += g.rng.Int64N(3000)
	}
	return g.now
}

func (g *playGen) intent(p int) Op {
	rng := g.rng
	id := pid(p)
	rec, bound := g.ref.Player(id)
	if !bound {
		age := 18 + 3*p
		if p == genPlayers-1 {
			age = 16 // never enters
		}
		return OpenSession{Device: device(p), Verifier: verifier(p), Player: id, Jurisdiction: []string{"TR", "DE", "YY"}[p%3], Age: age}
	}
	seq := rec.LastSeq + 1
	tid := genTournaments[rng.IntN(g.ref.TournamentCount())]
	if rng.IntN(50) == 0 {
		tid = genTournaments[rng.IntN(len(genTournaments))]
	}
	tr, ok := g.ref.Tournament(tid)
	if !ok {
		return Enter{Tournament: tid, Player: id, Seq: seq}
	}
	if _, joined := tr.Entry(id); !joined {
		if p == genPlayers-1 && rng.IntN(10) > 0 {
			return FinishRound{Tournament: tid, Player: id, Seq: seq, Round: 1}
		}
		return Enter{Tournament: tid, Player: id, Seq: seq}
	}
	if tr.Status == Settled || rng.IntN(30) == 0 {
		return ClaimPayout{Tournament: tid, Player: id, Seq: seq}
	}
	prog := g.ref.Progress(tid, id)
	if prog.InPlay == 0 && prog.Next == 0 && rng.IntN(10) > 0 {
		return ClaimPayout{Tournament: tid, Player: id, Seq: seq}
	}
	if prog.InPlay == 0 {
		round := prog.Next
		if round == 0 || rng.IntN(15) == 0 {
			round = 1 + rng.IntN(4)
		}
		var seed game.Seed
		seed[0], seed[1] = byte(p), byte(rng.IntN(256)) // any seed deals a legal round
		return StartRound{Tournament: tid, Player: id, Seq: seq, Round: round, Seed: seed}
	}
	round, _ := g.ref.Round(tid, id, prog.InPlay)
	if rng.IntN(25) == 0 || (g.ref.Clock() > round.DeadlineMs && rng.IntN(3) > 0) {
		return FinishRound{Tournament: tid, Player: id, Seq: seq, Round: prog.InPlay}
	}
	b, err := game.Replay(round.Seed, round.Moves)
	if err != nil {
		g.t.Fatalf("recorded moves do not replay: %v", err)
	}
	m := game.Move{Kind: game.Draw, Column: game.NoColumn}
	switch n := rng.IntN(10); {
	case n < 6:
		if pl := b.Playable(); len(pl) > 0 {
			m = game.Move{Kind: game.Play, Column: pl[rng.IntN(len(pl))]}
		}
	case n == 6:
		m = game.Move{Kind: game.Play, Column: rng.IntN(9) - 1}
	}
	idx := len(round.Moves)
	if rng.IntN(20) == 0 {
		idx += rng.IntN(3) - 1
	}
	return PlayMove{Tournament: tid, Player: id, Seq: seq, Round: prog.InPlay, MoveIndex: idx, Move: m}
}

// withSeq returns op with its sequence number replaced.
func withSeq(op Op, seq uint64) Op {
	switch o := op.(type) {
	case Enter:
		o.Seq = seq
		return o
	case StartRound:
		o.Seq = seq
		return o
	case PlayMove:
		o.Seq = seq
		return o
	case FinishRound:
		o.Seq = seq
		return o
	case ClaimPayout:
		o.Seq = seq
		return o
	}
	return op
}

func genPlayLog(t *testing.T, seed uint64, steps int) *playGen {
	g := &playGen{t: t, rng: rand.New(rand.NewPCG(seed, 99)), ref: NewState(), now: startMs}
	rules := ladderRules()
	rules.MinEntrants = 3
	rules.Exclusions = Exclusions{Version: 1, Jurisdictions: []string{"XX"}}
	for _, tid := range genTournaments[:2] {
		g.emit(Command{Key: g.key("op"), ReceivedAt: g.tick(), Op: CreateTournament{ID: tid, Seed: 7, Rules: rules}})
	}
	for step := 0; step < steps; step++ {
		rng := g.rng
		switch step {
		case steps * 2 / 3:
			g.emit(Command{Key: g.key("op"), ReceivedAt: g.tick(), Op: Close{Tournament: "t0"}})
		case steps * 3 / 4:
			g.emit(Command{Key: g.key("op"), ReceivedAt: g.tick(), Op: Settle{Tournament: "t0", Exclusions: Exclusions{Version: 2, Jurisdictions: []string{"YY"}}}})
		}
		switch n := rng.IntN(100); {
		case n < 4:
			g.log = append(g.log, logEntry{skip: true})
			g.ref.Skip(paxos.Slot(len(g.log)))
		case n < 10 && len(g.sent) > 0:
			// A resend under the same key, with the receipt time and the
			// server-drawn fields a different leader would stamp.
			c := g.sent[rng.IntN(len(g.sent))]
			c.ReceivedAt = g.tick()
			switch o := c.Op.(type) {
			case OpenSession:
				o.Player = PlayerID(fmt.Sprintf("p-drawn-%d", rng.IntN(1000)))
				c.Op = o
			case StartRound:
				o.Seed[5] = byte(rng.IntN(256))
				c.Op = o
			}
			g.emit(c)
		case n < 13 && len(g.sent) > 0:
			c := g.sent[rng.IntN(len(g.sent))]
			if op, ok := c.Op.(PlayMove); ok {
				op.MoveIndex++
				c.Op = op
			} else {
				c.Op = withSeq(c.Op, 1_000_000)
			}
			c.ReceivedAt = g.tick()
			g.emit(c) // key_reused unless the op has no seq to change
		case n < 18:
			p := rng.IntN(genPlayers)
			op := g.intent(p)
			if rec, ok := g.ref.Player(pid(p)); ok {
				op = withSeq(op, []uint64{0, rec.LastSeq, rec.LastSeq + 2, rec.LastSeq + 9}[rng.IntN(4)])
			}
			g.emit(Command{Key: g.key("u." + string(pid(p))), ReceivedAt: g.tick(), Op: op})
		case n < 20 && step > steps/3 && rng.IntN(8) == 0:
			tid := genTournaments[rng.IntN(len(genTournaments))]
			if rng.IntN(2) == 0 {
				g.emit(Command{Key: g.key("op"), ReceivedAt: g.tick(), Op: Close{Tournament: tid}})
			} else {
				g.emit(Command{Key: g.key("op"), ReceivedAt: g.tick(), Op: Settle{Tournament: tid, Exclusions: Exclusions{Version: 2, Jurisdictions: []string{"YY"}}}})
			}
		case n < 21 && step > steps/2:
			g.emit(Command{Key: g.key("op"), ReceivedAt: g.tick(), Op: CreateTournament{ID: "t2", Seed: 8, Rules: rules}})
		default:
			p := rng.IntN(genPlayers)
			g.emit(Command{Key: g.key("u." + string(pid(p))), ReceivedAt: g.tick(), Op: g.intent(p)})
		}
	}
	return g
}

// replicate applies the log as a replica does: every value through the
// codec, under the replica's own ballot.
func replicate(t *testing.T, log []logEntry, node paxos.NodeID) *State {
	st := NewState()
	for i, e := range log {
		slot := paxos.Slot(i + 1)
		if e.skip {
			st.Skip(slot)
			continue
		}
		enc, err := Encode(e.cmd)
		if err != nil {
			t.Fatal(err)
		}
		cmd, err := Decode(enc)
		if err != nil {
			t.Fatal(err)
		}
		st.Apply(slot, paxos.Ballot{Round: uint64(node) * 3, Node: node}, cmd)
	}
	return st
}

// playFingerprint renders everything a client or an auditor could read of
// the play state.
func playFingerprint(st *State, keys []IdempotencyKey) []byte {
	var b bytes.Buffer
	fmt.Fprintf(&b, "clock %d applied %d mutations %d\n", st.Clock(), st.Applied(), st.Mutations())
	b.Write(EncodePostings(st.Ledger().Postings()))
	for _, tid := range st.Tournaments() {
		t, _ := st.Tournament(tid)
		b.Write(EncodeTournament(t))
		for p := 0; p < genPlayers; p++ {
			for r := 1; r <= game.Rounds; r++ {
				if rec, ok := st.Round(tid, pid(p), r); ok {
					b.Write(mustMarshal(rec))
				}
			}
		}
	}
	for p := 0; p < genPlayers; p++ {
		rec, _ := st.Player(pid(p))
		b.Write(mustMarshal(rec))
		b.Write(mustMarshal(st.Events(0, "", pid(p), 0)))
	}
	for _, k := range keys {
		r, _ := st.Result(k)
		b.Write(mustMarshal(r))
	}
	return b.Bytes()
}

// TestPlayLogReplaysIdentically: replaying the same log of play intents on
// any replica, under any ballots, yields the same state hash, the same
// ledger postings, the same results and the same events, and the log keeps
// the play invariants P1, P2, P4 and P6.
func TestPlayLogReplaysIdentically(t *testing.T) {
	seeds := 16
	if testing.Short() {
		seeds = 4
	}
	for seed := uint64(1); seed <= uint64(seeds); seed++ {
		g := genPlayLog(t, seed, 1500)
		var keys []IdempotencyKey
		for _, e := range g.log {
			if !e.skip {
				keys = append(keys, e.cmd.Key)
			}
		}
		want := playFingerprint(g.ref, keys)
		wantLedger := ledgerDigest(g.ref)
		for node := paxos.NodeID(1); node <= 3; node++ {
			st := replicate(t, g.log, node)
			if st.Hash() != g.ref.Hash() {
				t.Fatalf("seed %d node %d: state hash differs", seed, node)
			}
			if ledgerDigest(st) != wantLedger {
				t.Fatalf("seed %d node %d: ledger differs", seed, node)
			}
			if got := playFingerprint(st, keys); !bytes.Equal(got, want) {
				t.Fatalf("seed %d node %d: play state differs", seed, node)
			}
		}
		checkPlayInvariants(t, seed, g)
	}
}

func ledgerDigest(st *State) string { return string(EncodePostings(st.Ledger().Postings())) }

// checkPlayInvariants replays the log once more, checking after every slot.
func checkPlayInvariants(t *testing.T, seed uint64, g *playGen) {
	st := NewState()
	counts := map[Code]int{}
	lastClock := int64(0)
	for i, e := range g.log {
		slot := paxos.Slot(i + 1)
		if e.skip {
			st.Skip(slot)
			continue
		}
		pl, seq, sequenced := seqOf(e.cmd.Op)
		before, _ := st.Player(pl)
		res := st.Apply(slot, paxos.Ballot{}, e.cmd)
		counts[res.Code]++
		if res.Replayed {
			counts["replayed"]++
		}
		after, _ := st.Player(pl)
		// P6: the clock never decreases.
		if st.Clock() < lastClock {
			t.Fatalf("seed %d slot %d: clock went back", seed, slot)
		}
		lastClock = st.Clock()
		if !sequenced {
			continue
		}
		// P2: a command consumes exactly the next number or nothing, and a
		// stale, skipped, reused or replayed one consumes nothing.
		consumed := after.LastSeq != before.LastSeq
		switch {
		case consumed && (after.LastSeq != before.LastSeq+1 || after.LastSeq != seq):
			t.Fatalf("seed %d slot %d: seq moved from %d to %d on seq %d", seed, slot, before.LastSeq, after.LastSeq, seq)
		case consumed && (res.Replayed || res.Code == StaleSeq || res.Code == SeqGap || res.Code == KeyReused || res.Code == UnknownPlayer):
			t.Fatalf("seed %d slot %d: %s (replayed %t) consumed a number", seed, slot, res.Code, res.Replayed)
		case !consumed && !res.Replayed && res.Code != StaleSeq && res.Code != SeqGap && res.Code != KeyReused && res.Code != UnknownPlayer:
			t.Fatalf("seed %d slot %d: %s did not consume seq %d", seed, slot, res.Code, seq)
		}
		// P6: no move accepted past the deadline.
		if m, ok := e.cmd.Op.(PlayMove); ok && res.Code == OK && !res.Replayed {
			rec, _ := st.Round(m.Tournament, m.Player, m.Round)
			if st.Clock() > rec.DeadlineMs {
				t.Fatalf("seed %d slot %d: move accepted past the deadline", seed, slot)
			}
		}
	}
	for _, tid := range st.Tournaments() {
		tr, _ := st.Tournament(tid)
		for _, en := range tr.Entries {
			var sum int64
			for r := 1; r <= game.Rounds; r++ {
				rec, ok := st.Round(tid, en.Player.ID, r)
				if !ok || rec.Status != RoundDone {
					continue
				}
				// P4: the recorded score is the score of the replayed moves.
				b, err := game.Replay(rec.Seed, rec.Moves)
				if err != nil || b.Score() != rec.Score || b.Cleared() != rec.Cleared {
					t.Fatalf("seed %d: round %s/%s/%d score %d, replay %d (%v)", seed, tid, en.Player.ID, r, rec.Score, b.Score(), err)
				}
				sum += rec.Score
			}
			if en.Scored && en.Score != sum {
				t.Fatalf("seed %d: entry %s/%s score %d, rounds sum to %d", seed, tid, en.Player.ID, en.Score, sum)
			}
			// P1: one claim at most, of the claimable amount, after settle.
			if post, ok := st.Ledger().Get(ledger.ClaimKey(string(tid), string(en.Player.ID))); ok {
				if tr.Status != Settled || post.Amount != ClaimableAmount(&tr, en.Player.ID) {
					t.Fatalf("seed %d: claim %+v on %s tournament", seed, post, tr.Status)
				}
			}
		}
	}
	if err := st.Ledger().Check(); err != nil {
		t.Fatalf("seed %d: %v", seed, err)
	}
	if seed == 1 {
		t.Logf("seed 1 results: %v", counts)
		for _, c := range []Code{OK, StaleSeq, SeqGap, KeyReused, IllegalMove, MoveIndexMismatch, RoundExpired, NotSettled, NotJoined, AlreadyClaimed} {
			if counts[c] == 0 {
				t.Errorf("seed 1 never produced %s", c)
			}
		}
	}
}

func seqOf(op Op) (PlayerID, uint64, bool) {
	switch o := op.(type) {
	case Enter:
		return o.Player, o.Seq, true
	case StartRound:
		return o.Player, o.Seq, true
	case PlayMove:
		return o.Player, o.Seq, true
	case FinishRound:
		return o.Player, o.Seq, true
	case ClaimPayout:
		return o.Player, o.Seq, true
	}
	return "", 0, false
}
