package replica

// Adversarial tests for the layer that joins the log to the state machine:
// what a replica applies, what the ledger books for it, and what a waiting
// Submit is told. Each test states the guarantee it attacks; a failure is a
// bug, not a flaky schedule.

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/oguzhanozfe/paxos-arena/internal/ledger"
	"github.com/oguzhanozfe/paxos-arena/internal/paxos"
	"github.com/oguzhanozfe/paxos-arena/internal/replog"
	"github.com/oguzhanozfe/paxos-arena/internal/tournament"
)

// advSubmitAll proposes every command on a single-node core and applies the
// result, failing the test on a rejection it did not expect.
func advSubmitAll(t *testing.T, c *Core, now time.Duration, cmds ...tournament.Command) []Applied {
	t.Helper()
	for _, cmd := range cmds {
		if _, err := c.Submit(now, cmd); err != nil {
			t.Fatalf("Submit(%s): %v", cmd.Key, err)
		}
	}
	var out []Applied
	for _, a := range c.ApplyCommitted() {
		if a.NoOp {
			continue
		}
		if a.Result.Code != tournament.OK {
			t.Fatalf("slot %d %s: %s (%s)", a.Slot, a.Key, a.Result.Code, a.Result.Detail)
		}
		out = append(out, a)
	}
	return out
}

// TestAttackPostingKeysCollideAcrossTournaments: posting keys are built by
// joining the tournament and player identifiers with ':' ("fee:<tid>:<pid>",
// "prize:<tid>:<pid>:<place>"), and identifiers used to be allowed to contain
// ':'. Tournament "t" with player "x:p" and tournament "t:x" with player "p"
// therefore shared every posting key. The ledger treats a repeated key as an
// idempotent retry and silently books nothing, so the second tournament's
// entry fee was never charged and the first tournament's prize was never
// paid, while both records said they were. Invariants D1 and D2 (every entry
// has its fee posting, every payout its posting, a settled pool nets to
// zero) must hold for both tournaments.
//
// The test first encoded the colliding pair as a legitimate setup. With the
// fix, identifiers are limited to letters, digits, '.', '_' and '-', so that
// setup is now expected to be refused: the ':' identifiers are rejected with
// invalid_rules and invalid_player and book nothing. The D1/D2 checks then
// run, unchanged, on the nearest legal pair: tournament "t" with player
// "x-p" and tournament "t-x" with player "p".
func TestAttackPostingKeysCollideAcrossTournaments(t *testing.T) {
	c := singleNode(t, replog.NewMemStore())
	r := tournament.Rules{
		EntryFee: 100, RakeBps: 0, PrizeBps: []uint32{10000}, MinEntrants: 1, MaxEntrants: 10,
		MaxScore: 1000, MinAge: 18, TieBreak: tournament.EarliestSubmission,
	}
	const seedA, seedB = 11, 22
	cmd := func(key string, op tournament.Op) tournament.Command {
		return tournament.Command{Key: tournament.IdempotencyKey(key), ReceivedAt: 1, Op: op}
	}
	now := time.Second

	// The colliding identifiers are refused before anything is booked.
	for _, bad := range []struct {
		cmd  tournament.Command
		want tournament.Code
	}{
		{cmd("colon-create", tournament.CreateTournament{ID: "t:x", Seed: seedB, Rules: r}), tournament.InvalidRules},
		{cmd("colon-host", tournament.CreateTournament{ID: "t", Seed: seedA, Rules: r}), tournament.OK},
		{cmd("colon-join", tournament.Join{Tournament: "t", Player: tournament.Player{ID: "x:p", Jurisdiction: "TR", Age: 30}}), tournament.InvalidPlayer},
	} {
		if _, err := c.Submit(now, bad.cmd); err != nil {
			t.Fatalf("Submit(%s): %v", bad.cmd.Key, err)
		}
		for _, a := range c.ApplyCommitted() {
			if !a.NoOp && a.Result.Code != bad.want {
				t.Fatalf("%s = %s (%s), want %s", a.Key, a.Result.Code, a.Result.Detail, bad.want)
			}
		}
	}
	if n := c.State().Ledger().Len(); n != 0 {
		t.Fatalf("the refused identifiers booked %d postings", n)
	}

	advSubmitAll(t, c, now,
		cmd("create-b", tournament.CreateTournament{ID: "t-x", Seed: seedB, Rules: r}),
		cmd("join-a", tournament.Join{Tournament: "t", Player: tournament.Player{ID: "x-p", Jurisdiction: "TR", Age: 30}}),
		cmd("join-b", tournament.Join{Tournament: "t-x", Player: tournament.Player{ID: "p", Jurisdiction: "TR", Age: 30}}),
		cmd("score-a", tournament.SubmitScore{Tournament: "t", Player: "x-p", Score: 10, DealSeed: seedA}),
		cmd("score-b", tournament.SubmitScore{Tournament: "t-x", Player: "p", Score: 10, DealSeed: seedB}),
		cmd("close-a", tournament.Close{Tournament: "t"}),
		cmd("close-b", tournament.Close{Tournament: "t-x"}),
		cmd("settle-b", tournament.Settle{Tournament: "t-x"}),
		cmd("settle-a", tournament.Settle{Tournament: "t"}),
	)
	st := c.State()
	book := st.Ledger()
	var problems []string
	for _, tc := range []struct {
		tid tournament.TournamentID
		pid string
	}{{"t", "x-p"}, {"t-x", "p"}} {
		rec, ok := st.Tournament(tc.tid)
		if !ok || rec.Status != tournament.Settled || len(rec.Entries) != 1 || len(rec.Payouts) != 1 {
			t.Fatalf("setup: tournament %q = %+v ok=%t", tc.tid, rec, ok)
		}
		fees := 0
		have := map[ledger.PostingKey]bool{}
		for _, p := range book.ForTournament(string(tc.tid)) {
			have[p.Key] = true
			if p.Kind == ledger.EntryFee {
				fees++
			}
		}
		if fees != len(rec.Entries) {
			problems = append(problems, fmt.Sprintf("tournament %q: %d entries but %d fee postings of its own", tc.tid, len(rec.Entries), fees))
		}
		if !have[rec.Payouts[0].Key] {
			problems = append(problems, fmt.Sprintf("tournament %q: payout %s has no posting of its own", tc.tid, rec.Payouts[0].Key))
		}
		if bal := book.Balance(ledger.PoolAccount(string(tc.tid))); bal != 0 {
			problems = append(problems, fmt.Sprintf("tournament %q: settled pool holds %d, want 0", tc.tid, bal))
		}
		// One entrant, no rake: the player paid the fee and won it back.
		if bal := book.Balance(ledger.PlayerAccount(tc.pid)); bal != 0 {
			problems = append(problems, fmt.Sprintf("player %q: net %d after paying 100 and winning 100, want 0", tc.pid, bal))
		}
	}
	if err := book.Check(); err != nil {
		problems = append(problems, err.Error())
	}
	if len(problems) > 0 {
		for _, p := range problems {
			t.Error(p)
		}
		t.Fatalf("the ledger merged postings of two tournaments; %d invariant breaches", len(problems))
	}
}

// TestAttackWaiterTakesResultOfAnotherPayload: two submissions under one
// idempotency key with different payloads are pending on the leader at the
// same time (a client that timed out and retried with an edited body, or two
// clients that picked the same key). The state machine applies the first and
// answers key_reused for the second, but Runner wakes every waiter on the key
// with the first application's result. The second caller is told "ok" for a
// command that never took effect. ADR 0003 and the API contract say a reused
// key with a different command answers key_reused.
func TestAttackWaiterTakesResultOfAnotherPayload(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		peers := []paxos.NodeID{1, 2}
		c := newCluster(t)
		defer c.stop()
		r1 := c.add(replog.DefaultConfig(1, peers), replog.NewMemStore())
		c.add(passive(2, peers), replog.NewMemStore())
		waitFor(t, ready(r1), "node 1 to be ready")
		// Hold every Accepted reply so both proposals stay pending.
		c.bus.SetFilter(func(e replog.Envelope) bool {
			_, ok := e.Msg.(replog.Accepted)
			return ok
		})
		first := createCmd("shared-key", "t-first")
		second := createCmd("shared-key", "t-second")
		var wg sync.WaitGroup
		var resFirst, resSecond tournament.Result
		var errFirst, errSecond error
		wg.Add(1)
		go func() {
			defer wg.Done()
			resFirst, errFirst = r1.Submit(context.Background(), first)
		}()
		synctest.Wait()
		wg.Add(1)
		go func() {
			defer wg.Done()
			resSecond, errSecond = r1.Submit(context.Background(), second)
		}()
		synctest.Wait()
		if r1.Status().Role != replog.Leader {
			t.Fatal("setup: node 1 lost leadership while the submissions were pending")
		}
		c.bus.SetFilter(nil)
		wg.Wait()
		if errFirst != nil || resFirst.Code != tournament.OK || resFirst.Replayed {
			t.Fatalf("first submission = %+v, %v; want a fresh ok", resFirst, errFirst)
		}
		waitFor(t, func() bool {
			st := r1.Status()
			return st.Applied == st.CommitIndex && st.CommitIndex >= resFirst.Slot+1
		}, "both proposals to be applied")
		var secondExists bool
		r1.Read(context.Background(), false, func(s *tournament.State) error {
			_, secondExists = s.Tournament("t-second")
			return nil
		})
		if secondExists {
			t.Fatal("setup: the second payload took effect; the state machine did not reject the reused key")
		}
		if errSecond != nil || resSecond.Code != tournament.KeyReused {
			t.Fatalf("second submission (same key, different payload, never applied) = %+v, %v; want key_reused", resSecond, errSecond)
		}
	})
}
