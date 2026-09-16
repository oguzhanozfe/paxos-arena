package sim

import (
	"bytes"
	"testing"
	"time"

	"github.com/oguzhanozfe/paxos-arena/internal/game"
	"github.com/oguzhanozfe/paxos-arena/internal/ledger"
	"github.com/oguzhanozfe/paxos-arena/internal/paxos"
	"github.com/oguzhanozfe/paxos-arena/internal/replog"
	"github.com/oguzhanozfe/paxos-arena/internal/tournament"
)

// playSim drives play commands into a simulated cluster directly, as the
// leader's play API would, with the simulator's checker watching every
// applied slot.
type playSim struct {
	t *testing.T
	r *Run
}

// submitTo proposes cmd on nd and reports the log's answer.
func (ps *playSim) submitTo(nd *simNode, cmd tournament.Command) error {
	var err error
	ps.r.callNode(nd, nil, func(now time.Duration) []replog.Envelope {
		outs, e := nd.core.Submit(now, cmd)
		err = e
		return outs
	})
	return err
}

func (ps *playSim) step(cond func() bool, what string) {
	ps.t.Helper()
	for i := 0; i < 50_000; i++ {
		if cond() {
			return
		}
		if err := ps.r.Step(); err != nil {
			ps.t.Fatalf("seed %d: %s: %v", ps.r.p.Seed, what, err)
		}
	}
	ps.t.Fatalf("seed %d: never %s", ps.r.p.Seed, what)
}

func (ps *playSim) leader() *simNode {
	ps.t.Helper()
	var l *simNode
	ps.step(func() bool {
		l = ps.r.leaderNode()
		return l != nil && l.node.Ready()
	}, "elected a ready leader")
	return l
}

// applied reports whether every live node has applied key.
func (ps *playSim) applied(key tournament.IdempotencyKey) func() bool {
	return func() bool {
		for _, nd := range ps.r.nodes {
			if !nd.alive {
				continue
			}
			if _, ok := nd.core.State().Result(key); !ok {
				return false
			}
		}
		return true
	}
}

// do submits cmd to the leader, resubmitting to whichever node leads until
// every live node has applied its key, and returns the result.
func (ps *playSim) do(key string, op tournament.Op) tournament.Result {
	ps.t.Helper()
	cmd := tournament.Command{Key: tournament.IdempotencyKey(key), ReceivedAt: int64(ps.r.clock / time.Millisecond), Op: op}
	for attempt := 0; attempt < 20; attempt++ {
		l := ps.leader()
		ps.submitTo(l, cmd)
		done := ps.applied(cmd.Key)
		for i := 0; i < 5000 && !done(); i++ {
			if err := ps.r.Step(); err != nil {
				ps.t.Fatalf("seed %d: %s: %v", ps.r.p.Seed, key, err)
			}
		}
		if done() {
			res, _ := l.core.State().Result(cmd.Key)
			return res
		}
	}
	ps.t.Fatalf("seed %d: %s never applied", ps.r.p.Seed, key)
	return tournament.Result{}
}

func (ps *playSim) ok(key string, op tournament.Op) tournament.Result {
	ps.t.Helper()
	res := ps.do(key, op)
	if res.Code != tournament.OK {
		ps.t.Fatalf("seed %d: %s = %s (%s)", ps.r.p.Seed, key, res.Code, res.Detail)
	}
	return res
}

var simDealSecret = []byte("sim-deal-secret-of-at-least-32-bytes!!")

// TestPlayIntentsIdempotentAcrossLeaderChange: the leader proposes a move
// and a claim and crashes before the client learns their outcome; the
// client resends both keys to the new leader, whose Phase 1 may also
// re-propose the old values in later slots; a captured request replayed
// under a new key follows. Every node applies each key once, consumes each
// sequence number once, holds one claim posting, and ends with identical
// state, while the checker's S1-S8 and D1-D6 hold after every event.
func TestPlayIntentsIdempotentAcrossLeaderChange(t *testing.T) {
	seeds := 16
	if testing.Short() {
		seeds = 4
	}
	replays, reproposed := 0, 0
	for seed := uint64(1); seed <= uint64(seeds); seed++ {
		p := DefaultParams(seed)
		p.Clients = 0
		p.PartitionP, p.HealP, p.CrashP, p.TornWriteP = 0, 0, 0, 0
		p.Faults.DropP, p.Faults.DupP = 0, 0
		p.RestartAfter = [2]time.Duration{300 * time.Millisecond, 600 * time.Millisecond}
		r, err := New(p, nil)
		if err != nil {
			t.Fatal(err)
		}
		ps := &playSim{t: t, r: r}
		rules := tournament.Rules{EntryFee: 500, RakeBps: 1000, PrizeBps: []uint32{10000}, MinEntrants: 1, MaxEntrants: 10,
			MaxScore: game.MaxTotalScore, MinAge: 18, TieBreak: tournament.EarliestSubmission, Game: game.LadderV1}
		ps.ok("create-t1", tournament.CreateTournament{ID: "t1", Seed: 1, Rules: rules})
		ps.ok("create-t2", tournament.CreateTournament{ID: "t2", Seed: 2, Rules: rules})
		const player = tournament.PlayerID("p-sim")
		ps.ok("d.dev.session", tournament.OpenSession{Device: "0123456789abcdef0123456789abcdef", Verifier: tournament.Digest{9}, Player: player, Jurisdiction: "TR", Age: 30})
		seq := uint64(0)
		next := func() uint64 { seq++; return seq }
		ps.ok("u.enter-t1", tournament.Enter{Tournament: "t1", Player: player, Seq: next()})
		ps.ok("u.enter-t2", tournament.Enter{Tournament: "t2", Player: player, Seq: next()})
		seed1 := game.DeriveSeed(simDealSecret, "t1", string(player), 1)
		ps.ok("u.deal-t1", tournament.StartRound{Tournament: "t1", Player: player, Seq: next(), Round: 1, Seed: seed1})
		ps.ok("u.deal-t2", tournament.StartRound{Tournament: "t2", Player: player, Seq: next(), Round: 1, Seed: game.DeriveSeed(simDealSecret, "t2", string(player), 1)})
		ps.ok("close-t2", tournament.Close{Tournament: "t2"})
		ps.ok("settle-t2", tournament.Settle{Tournament: "t2"})

		// The move and the claim, proposed on the leader, which then crashes
		// after a seed-chosen number of events.
		board := game.Deal(seed1)
		move := game.Move{Kind: game.Draw, Column: game.NoColumn}
		if pl := board.Playable(); len(pl) > 0 {
			move = game.Move{Kind: game.Play, Column: pl[0]}
		}
		moveCmd := tournament.Command{Key: "u.move-1", ReceivedAt: int64(r.clock / time.Millisecond),
			Op: tournament.PlayMove{Tournament: "t1", Player: player, Seq: next(), Round: 1, MoveIndex: 0, Move: move}}
		claimCmd := tournament.Command{Key: "u.claim-t2", ReceivedAt: int64(r.clock / time.Millisecond),
			Op: tournament.ClaimPayout{Tournament: "t2", Player: player, Seq: next()}}
		old := ps.leader()
		lose := seed%3 == 0 // the old leader's Accepts never leave it
		if lose {
			for _, nd := range r.nodes {
				if nd != old {
					r.net.Block(old.id, nd.id, true)
				}
			}
		}
		if err := ps.submitTo(old, moveCmd); err != nil {
			t.Fatalf("seed %d: submit move: %v", seed, err)
		}
		if err := ps.submitTo(old, claimCmd); err != nil {
			t.Fatalf("seed %d: submit claim: %v", seed, err)
		}
		for i := 0; i < int(seed%5); i++ {
			if err := r.Step(); err != nil {
				t.Fatal(err)
			}
		}
		r.crash(old, "play test: leader crash with a move and a claim in flight")
		if lose {
			for _, nd := range r.nodes {
				r.net.Block(old.id, nd.id, false)
			}
		}

		// The client resends both keys, unchanged, to the new leader.
		var fresh *simNode
		ps.step(func() bool {
			fresh = r.leaderNode()
			return fresh != nil && fresh != old && fresh.node.Ready()
		}, "elected a new leader")
		for _, cmd := range []tournament.Command{moveCmd, claimCmd} {
			cmd.ReceivedAt = int64(r.clock / time.Millisecond)
			for attempt := 0; ; attempt++ {
				l := ps.leader()
				ps.submitTo(l, cmd)
				done := ps.applied(cmd.Key)
				for i := 0; i < 5000 && !done(); i++ {
					if err := r.Step(); err != nil {
						t.Fatal(err)
					}
				}
				if done() {
					break
				}
				if attempt > 20 {
					t.Fatalf("seed %d: resend of %s never applied", seed, cmd.Key)
				}
			}
		}
		// A captured claim under a new key carries a used number: stale.
		stale := ps.do("u.claim-t2-captured", tournament.ClaimPayout{Tournament: "t2", Player: player, Seq: seq})
		if stale.Code != tournament.StaleSeq {
			t.Fatalf("seed %d: captured claim = %s", seed, stale.Code)
		}
		// The same number skipped ahead is a gap; a new claim is refused.
		if gap := ps.do("u.claim-gap", tournament.ClaimPayout{Tournament: "t2", Player: player, Seq: seq + 5}); gap.Code != tournament.SeqGap {
			t.Fatalf("seed %d: gap = %s", seed, gap.Code)
		}
		if again := ps.do("u.claim-again", tournament.ClaimPayout{Tournament: "t2", Player: player, Seq: next()}); again.Code != tournament.AlreadyClaimed {
			t.Fatalf("seed %d: second claim = %s", seed, again.Code)
		}

		// Heal, let the crashed leader rejoin, and compare every node.
		r.Heal()
		ps.step(r.complete, "completed after heal")
		if err := r.Check(); err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
		var hash [32]byte
		var postings []byte
		for i, nd := range r.nodes {
			st := nd.core.State()
			rec, _ := st.Player(player)
			round, _ := st.Round("t1", player, 1)
			mv, _ := st.Result(moveCmd.Key)
			cl, _ := st.Result(claimCmd.Key)
			claims := 0
			for _, po := range st.Ledger().ForTournament("t2") {
				if po.Kind == ledger.Claim {
					claims++
				}
			}
			switch {
			case rec.LastSeq != seq:
				t.Fatalf("seed %d node %d: last seq %d, want %d", seed, nd.id, rec.LastSeq, seq)
			case len(round.Moves) != 1 || mv.Code != tournament.OK || mv.Play.MoveIndex != 1:
				t.Fatalf("seed %d node %d: moves %d, move result %+v", seed, nd.id, len(round.Moves), mv)
			case claims != 1 || cl.Code != tournament.OK || cl.Play.Amount != 450:
				t.Fatalf("seed %d node %d: %d claims, claim result %+v", seed, nd.id, claims, cl)
			}
			enc := tournament.EncodePostings(st.Ledger().Postings())
			if i == 0 {
				hash, postings = st.Hash(), enc
			} else if st.Hash() != hash || !bytes.Equal(enc, postings) {
				t.Fatalf("seed %d: node %d state differs from node 1", seed, nd.id)
			}
		}
		rep := r.Report()
		if rep.Crashes != 1 || rep.Checks["S8"] == 0 || rep.Checks["D3"] == 0 {
			t.Fatalf("seed %d: crashes %d, checks %v", seed, rep.Crashes, rep.Checks)
		}
		replays += rep.Replays
		extra := countKey(r, moveCmd.Key) + countKey(r, claimCmd.Key) - 2
		reproposed += extra
		if lose && extra != 0 {
			t.Fatalf("seed %d: the blocked leader's proposals were chosen", seed)
		}
	}
	// Across the seeds, old proposals were chosen beside the resends, and
	// the later of the two slots replayed the first.
	if replays == 0 || reproposed == 0 {
		t.Errorf("replays %d, extra slots for the in-flight keys %d: the leader change was not exercised", replays, reproposed)
	}
	t.Logf("replays %d, extra slots holding an in-flight key %d", replays, reproposed)
}

// countKey counts the chosen slots whose command carries key.
func countKey(r *Run, key tournament.IdempotencyKey) int {
	n := 0
	for s := paxos.Slot(1); ; s++ {
		v, ok := r.checker.chosen[s]
		if !ok {
			return n
		}
		if cmd, err := tournament.Decode([]byte(v)); err == nil && cmd.Key == key {
			n++
		}
	}
}
