package intent

// Adversarial availability review of the play API (Milestone 4).
//
// Root cause shared by both tests: a play-API read runs its closure inline
// on the replica's single consensus event loop. handleRead in
// internal/replica/runner.go serves a stale read (the only kind the play
// API uses) as
//
//	req.reply <- req.fn(r.core.State())
//
// on the same goroutine that steps Paxos and fires the leader's heartbeat
// tick. Nothing bounds how much work one read does, and every open
// /v1/events long-poll re-runs a read on that goroutine each time the
// applied slot advances. An authenticated client (one session = one log
// slot) can therefore multiply the leader's per-slot work, or hand it an
// O(entrants) leaderboard sort, from cheap requests.

import (
	"context"
	"fmt"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/oguzhanozfe/paxos-arena/internal/game"
	"github.com/oguzhanozfe/paxos-arena/internal/paxos"
	"github.com/oguzhanozfe/paxos-arena/internal/session"
	"github.com/oguzhanozfe/paxos-arena/internal/tournament"
)

// countingBackend counts the reads the play API issues to the backend. In a
// real replica each of these runs on the consensus event loop.
type countingBackend struct {
	Backend
	mu    sync.Mutex
	reads int
}

func (c *countingBackend) Read(ctx context.Context, consistent bool, fn func(*tournament.State) error) error {
	c.mu.Lock()
	c.reads++
	c.mu.Unlock()
	return c.Backend.Read(ctx, consistent, fn)
}

func (c *countingBackend) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.reads
}

// TestReviewEventsPollFanOutMultipliesEventLoopReads reproduces the
// long-poll amplification the milestone task asks about ("exhausting ...
// long-polls"). With P open /v1/events long-polls, a single applied slot
// wakes all P of them and each issues a fresh backend read: one slot => P
// event-loop reads. An attacker who opens many polls (each poll needs only
// its own player, and a session is one cheap log slot) makes every applied
// slot cost O(P) reads on the goroutine that also runs consensus.
func TestReviewEventsPollFanOutMultipliesEventLoopReads(t *testing.T) {
	const players = 300

	now := time.UnixMilli(testStartMs)
	fake := newFake()
	cb := &countingBackend{Backend: fake}
	keys := testKeyring(t)
	s, err := New(Config{
		Self: 1, PublicURLs: map[paxos.NodeID]string{1: testSelfURL},
		Keys: keys, DealSecret: dealSecret(), Limits: NoLimits,
		Now: func() time.Time { return now },
	}, cb, nil)
	if err != nil {
		t.Fatal(err)
	}
	h := s.Handler()

	// Advance one slot so applied > 0 and open polls block waiting for the
	// next advance rather than returning at once.
	fake.skip()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	for i := 0; i < players; i++ {
		tok, err := keys.Sign(session.Claims{
			Player: fmt.Sprintf("p-%026d", i), Device: fmt.Sprintf("%032x", i),
			IssuedAtMs: now.UnixMilli(), ExpiresAtMs: now.Add(time.Hour).UnixMilli(),
		})
		if err != nil {
			t.Fatal(err)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			req := httptest.NewRequestWithContext(ctx, "GET", "/v1/events?wait_ms=25000&cursor=0", nil)
			req.Header.Set(HeaderAuthorization, "Bearer "+tok)
			h.ServeHTTP(httptest.NewRecorder(), req)
		}()
	}

	// Wait until the read count reaches at least want (or time out). Each
	// poll does exactly one read, then blocks in WaitApplied, so the count
	// climbs to players as they arm and by players again after each advance.
	waitFor := func(want int) int {
		for i := 0; i < 500; i++ {
			if cb.count() >= want {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		return cb.count()
	}
	armed := waitFor(players)
	if armed < players {
		t.Fatalf("only %d of %d polls armed", armed, players)
	}

	// One applied slot. It must wake every open poll, each re-reading state.
	before := cb.count()
	fake.skip()
	after := waitFor(before + players)
	fanout := after - before

	// A design that did not re-scan the whole world on the event loop for
	// every waiter on every slot would do O(1) here. The play API does O(P):
	// this assertion documents the amplification and fails if it is ever
	// bounded.
	if fanout < players {
		t.Fatalf("one applied slot triggered %d event-loop reads for %d open polls; "+
			"expected the amplification of >= %d", fanout, players, players)
	}
	t.Logf("one applied slot triggered %d event-loop reads across %d open long-polls "+
		"(each read runs on the single consensus goroutine)", fanout, players)

	cancel()
	wg.Wait()
}

// TestReviewLeaderboardReadCostGrowsWithEntrants reproduces the second half
// of the same weakness: a single GET /v1/tournaments/{id}/leaderboard sorts
// and materialises every entry, and that O(n log n) work runs inline on the
// consensus event loop (internal/replica/runner.go handleRead). Rules allow
// up to tournament.MaxEntrantsBound (1_000_000) entrants, and a daily mobile
// tournament with a few thousand is ordinary. A burst of these cheap-to-send
// reads keeps the leader's event loop busy well past its 150 ms election
// timeout, so it stops heart-beating and loses leadership.
func TestReviewLeaderboardReadCostGrowsWithEntrants(t *testing.T) {
	build := func(n int) (*tournament.State, tournament.Tournament) {
		st := tournament.NewState()
		slot := paxos.Slot(0)
		apply := func(key string, op tournament.Op) tournament.Result {
			slot++
			r := st.Apply(slot, paxos.Ballot{Round: 1, Node: 1},
				tournament.Command{Key: tournament.IdempotencyKey(key), ReceivedAt: testStartMs, Op: op})
			if r.Code != tournament.OK {
				t.Fatalf("%s: %s %s", key, r.Code, r.Detail)
			}
			return r
		}
		rules := tournament.Rules{
			EntryFee: 500, RakeBps: 1000, PrizeBps: []uint32{10000}, MinEntrants: 1, MaxEntrants: n,
			MaxScore: game.MaxTotalScore, TieBreak: tournament.EarliestSubmission, Game: game.LadderV1,
		}
		apply("create", tournament.CreateTournament{ID: "big", Rules: rules})
		for i := 0; i < n; i++ {
			dev := fmt.Sprintf("%032x", i+1)
			pid := tournament.PlayerID(fmt.Sprintf("p-%026d", i+1))
			var v tournament.Digest
			v[0] = 1
			apply("s"+dev, tournament.OpenSession{Device: tournament.DeviceID(dev), Verifier: v, Player: pid, Jurisdiction: "TR", Age: 30})
			apply("e"+dev, tournament.Enter{Tournament: "big", Player: pid, Seq: 1})
		}
		tr, _ := st.Tournament("big")
		return st, tr
	}

	measure := func(n, iters int) time.Duration {
		st, tr := build(n)
		start := time.Now()
		for i := 0; i < iters; i++ {
			leaderboard(st, tr, "p-00000000000000000000000001")
		}
		return time.Since(start) / time.Duration(iters)
	}

	small := measure(200, 50)
	large := measure(3000, 20)
	t.Logf("leaderboard build: 200 entrants %v/read, 3000 entrants %v/read (each runs on the consensus event loop)", small, large)

	// The per-read cost is not bounded: it rises with the entrant count. A
	// read that a client triggers for free should not scale with tournament
	// size on the goroutine that runs consensus.
	if large < 8*small {
		t.Fatalf("expected the leaderboard read to scale with entrants; 200:%v 3000:%v", small, large)
	}
	// The single-read cost already sits in the millisecond range at a few
	// thousand entrants; rules permit up to tournament.MaxEntrantsBound
	// (1_000_000). A handful of concurrent such reads exceed
	// replog.DefaultElectionTimeoutMin (150 ms) of event-loop time.
	if large < 2*time.Millisecond {
		t.Logf("note: 3000-entrant read is only %v here; the concern grows with entrant count", large)
	}
	_ = strings.TrimSpace
}
