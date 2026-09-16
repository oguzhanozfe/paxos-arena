package sim

import (
	"errors"
	"testing"

	"github.com/oguzhanozfe/paxos-arena/internal/game"
	"github.com/oguzhanozfe/paxos-arena/internal/intent"
	"github.com/oguzhanozfe/paxos-arena/internal/ledger"
	"github.com/oguzhanozfe/paxos-arena/internal/paxos"
	"github.com/oguzhanozfe/paxos-arena/internal/replica"
	"github.com/oguzhanozfe/paxos-arena/internal/tournament"
)

// TestPlayInvariantsCatchPlantedFaults runs a fault-free play workload
// until a payout was claimed, then plants one fault at a time and requires
// the checker to name the invariant it breaks: views that show an undrawn
// stock card, hide the seed of a finished round or predate the deal (P3), a
// command applied with a used sequence number (P2), a second claim posting
// (P1), a deal seed the secret does not derive (P5) and a clock that went
// back (P6). A checker that misses one is a checker bug.
func TestPlayInvariantsCatchPlantedFaults(t *testing.T) {
	p := DefaultParams(5)
	p.Clients, p.PlayClients = 0, 1
	p.PartitionP, p.HealP, p.CrashP, p.TornWriteP = 0, 0, 0, 0
	p.Faults.DropP, p.Faults.DupP = 0, 0
	r, err := New(p, nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; r.report.Marks["play.claims"] == 0; i++ {
		if i > 200_000 {
			t.Fatal("no claim within 200000 steps")
		}
		if err := r.Step(); err != nil {
			t.Fatal(err)
		}
	}
	nd := r.leaderNode()
	if nd == nil {
		t.Fatal("no leader")
	}
	st := nd.core.State()
	var (
		tid tournament.TournamentID
		pid tournament.PlayerID
	)
	for _, po := range st.Ledger().Postings() {
		if po.Kind == ledger.Claim {
			tid, pid = tournament.TournamentID(po.Tournament), tournament.PlayerID(po.Player)
		}
	}
	if tid == "" {
		t.Fatal("the leader holds no claim posting")
	}
	rec, ok := st.Round(tid, pid, playRound)
	if !ok || rec.Status != tournament.RoundDone {
		t.Fatalf("round of the claiming player: %+v, %t", rec, ok)
	}

	expect := func(name, invariant string, plant func()) {
		t.Helper()
		r.checker.err = nil
		plant()
		var v *Violation
		if !errors.As(r.checker.err, &v) || v.Invariant != invariant {
			t.Errorf("%s: checker reported %v, want a %s violation", name, r.checker.err, invariant)
			return
		}
		t.Logf("%s: %s", name, v.Detail)
	}
	view := func() intent.RoundView {
		v, ok := intent.BuildRoundView(st, tid, pid, playRound, 0)
		if !ok {
			t.Fatal("no view of the round")
		}
		return v
	}

	r.checker.err = nil
	r.checker.observeView(r, nd, st, tid, pid, playRound, view(), 0)
	if r.checker.err != nil {
		t.Fatalf("the untouched view fails: %v", r.checker.err)
	}
	expect("undrawn stock card as the waste card", "P3", func() {
		v := view()
		deck := game.Shuffle(rec.Seed)
		v.WasteTop = deck[game.DeckSize-1].String() // drawn only by the 16th draw; a round here has at most 8 moves
		r.checker.observeView(r, nd, st, tid, pid, playRound, v, 0)
	})
	expect("finished round without its seed", "P3", func() {
		v := view()
		v.Seed = ""
		r.checker.observeView(r, nd, st, tid, pid, playRound, v, 0)
	})
	expect("view as of the slot before the deal", "P3", func() {
		r.checker.observeView(r, nd, st, tid, pid, playRound, view(), rec.StartedAt-1)
	})
	expect("seed shown as of a slot before the round finished", "P3", func() {
		r.checker.observeView(r, nd, st, tid, pid, playRound, view(), rec.FinishedAt-1)
	})
	expect("move applied with a used sequence number", "P2", func() {
		snap := r.checker.beforeApply(nd)
		a := replica.Applied{Slot: st.Applied() + 1, Key: "planted", Result: tournament.Result{Code: tournament.OK},
			Command: tournament.Command{Key: "planted", Op: tournament.PlayMove{Tournament: tid, Player: pid, Seq: 1, Round: playRound}}}
		r.checker.checkPlayApply(r, nd, st, a, snap)
	})
	expect("clock went back", "P6", func() {
		snap := r.checker.beforeApply(nd)
		snap.clock = st.Clock() + 1
		a := replica.Applied{Slot: st.Applied() + 1, Key: "planted", Result: tournament.Result{Code: tournament.OK},
			Command: tournament.Command{Key: "planted", Op: tournament.OpenSession{}}}
		r.checker.checkPlayApply(r, nd, st, a, snap)
	})
	expect("deal seed from another secret", "P5", func() {
		other := tournament.NewState()
		slot := paxos.Slot(0)
		apply := func(key string, op tournament.Op) {
			slot++
			if res := other.Apply(slot, paxos.Ballot{}, tournament.Command{Key: tournament.IdempotencyKey(key), Op: op}); res.Code != tournament.OK {
				t.Fatalf("%s: %s %s", key, res.Code, res.Detail)
			}
		}
		rules := tournament.Rules{EntryFee: 100, PrizeBps: []uint32{10000}, MinEntrants: 1, MaxEntrants: 2, MaxScore: game.MaxTotalScore,
			TieBreak: tournament.EarliestSubmission, Game: game.LadderV1}
		apply("create", tournament.CreateTournament{ID: "planted", Rules: rules})
		apply("session", tournament.OpenSession{Device: "0123456789abcdef0123456789abcdef", Verifier: tournament.Digest{1}, Player: "p-planted", Jurisdiction: "TR", Age: 30})
		apply("enter", tournament.Enter{Tournament: "planted", Player: "p-planted", Seq: 1})
		apply("deal", tournament.StartRound{Tournament: "planted", Player: "p-planted", Seq: 2, Round: 1,
			Seed: game.DeriveSeed([]byte("a deal secret the simulation does not use"), "planted", "p-planted", 1)})
		r.checker.checkRound(r, nd, other, playRoundKey{"planted", "p-planted", 1})
	})
	expect("second claim posting", "P1", func() {
		if _, posted, err := st.Ledger().Post(ledger.Posting{Key: "claim-again:" + ledger.PostingKey(tid), Kind: ledger.Claim,
			Debit: ledger.PlayerAccount(string(pid)), Credit: ledger.ClaimsAccount(string(tid)), Amount: 1,
			Tournament: string(tid), Player: string(pid), Slot: st.Applied()}); err != nil || !posted {
			t.Fatalf("plant a posting: %v", err)
		}
		r.checker.checkClaims(r, nd, st, tid)
	})
}
