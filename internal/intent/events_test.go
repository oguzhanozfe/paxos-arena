package intent

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/oguzhanozfe/paxos-arena/internal/game"
	"github.com/oguzhanozfe/paxos-arena/internal/paxos"
	"github.com/oguzhanozfe/paxos-arena/internal/session"
	"github.com/oguzhanozfe/paxos-arena/internal/tournament"
)

// eventsHarness has player 0 (the Appendix A player) and player 1 in
// tournaments t0..t(n-1), each dealt round 1 by both players.
func eventsHarness(t *testing.T, n int) (*harness, string, string) {
	hs := newHarness(t, nil)
	s0, s1 := hs.session(0), hs.session(1)
	now := hs.clock.Now().UnixMilli()
	for i := 0; i < n; i++ {
		tid := fmt.Sprintf("t%d", i)
		hs.createLadder(tid, 1)
		for _, p := range []tournament.PlayerID{testPlayer, tournament.PlayerID(s1.PlayerID)} {
			rec, _ := hs.f.state.Player(p)
			hs.f.apply(t, fmt.Sprintf("e-%s-%s", tid, p), tournament.Enter{Tournament: tournament.TournamentID(tid), Player: p, Seq: rec.LastSeq + 1}, now)
		}
	}
	return hs, s0.SessionToken, s1.SessionToken
}

func (hs *harness) events(tok, query string) EventsResponse {
	hs.t.Helper()
	r := hs.do("GET", "/v1/events?"+query, "", bearer(tok)...)
	if r.status != http.StatusOK {
		hs.t.Fatalf("events %s = %d %s", query, r.status, r.body)
	}
	checkWireShape(hs.t, r.body, EventsResponse{})
	var out EventsResponse
	r.decode(hs.t, &out)
	return out
}

func (hs *harness) deal(p tournament.PlayerID, tid string, round int) {
	hs.t.Helper()
	rec, _ := hs.f.state.Player(p)
	hs.f.apply(hs.t, fmt.Sprintf("d-%s-%s-%d", tid, p, round), tournament.StartRound{Tournament: tournament.TournamentID(tid), Player: p,
		Seq: rec.LastSeq + 1, Round: round, Seed: game.DeriveSeed(dealSecret(), tid, string(p), round)}, hs.clock.Now().UnixMilli())
}

func (hs *harness) finish(p tournament.PlayerID, tid string, round int) {
	hs.t.Helper()
	rec, _ := hs.f.state.Player(p)
	hs.f.apply(hs.t, fmt.Sprintf("f-%s-%s-%d", tid, p, round), tournament.FinishRound{Tournament: tournament.TournamentID(tid), Player: p,
		Seq: rec.LastSeq + 1, Round: round}, hs.clock.Now().UnixMilli())
}

func TestEventsCursorRules(t *testing.T) {
	hs, tok0, tok1 := eventsHarness(t, 2)
	p1 := tournament.PlayerID("p-2")
	hs.deal(testPlayer, "t0", 1)
	hs.deal(p1, "t0", 1)
	hs.finish(testPlayer, "t0", 1)
	hs.finish(p1, "t0", 1)
	hs.deal(testPlayer, "t1", 1)

	// Rule 1: only the player's own events and shared ones.
	all := hs.events(tok0, "wait_ms=0")
	var types []string
	for _, e := range all.Events {
		if e.PlayerID != "" && e.PlayerID != testPlayer {
			t.Fatalf("player 0 sees %+v", e)
		}
		types = append(types, e.Type+"/"+e.TournamentID)
	}
	// Rule 3: two leaderboard_changed of t0 (one per finishing slot) are
	// reduced to the last one.
	if strings.Join(types, " ") != "round_started/t0 round_finished/t0 leaderboard_changed/t0 round_started/t1" {
		t.Fatalf("events = %s", strings.Join(types, " "))
	}
	if all.Events[2].Slot <= all.Events[1].Slot {
		t.Errorf("the kept leaderboard_changed is not the last: %+v", all.Events)
	}
	// Rule 4: without more events the cursor is the applied slot.
	applied := int64(hs.f.Status().Applied)
	if all.HasMore || all.Cursor != applied || all.AppliedSlot != applied {
		t.Fatalf("cursor %d has_more %t applied %d", all.Cursor, all.HasMore, applied)
	}
	// A tournament filter, and nothing after the cursor.
	only := hs.events(tok1, "wait_ms=0&tournament_id=t0")
	if len(only.Events) != 3 || only.Events[0].PlayerID != "p-2" {
		t.Fatalf("player 1 t0 = %+v", only.Events)
	}
	if none := hs.events(tok0, fmt.Sprintf("wait_ms=0&cursor=%d", all.Cursor)); len(none.Events) != 0 || none.Cursor != applied {
		t.Fatalf("after the cursor = %+v", none)
	}
	for q, want := range map[string]string{
		"tournament_id=nope":   string(tournament.UnknownTournament),
		"tournament_id=t:1":    CodeMalformed,
		"cursor=-1":            CodeMalformed,
		"wait_ms=25001":        CodeMalformed,
		"cursor=1&wait_ms=abc": CodeMalformed,
	} {
		r := hs.do("GET", "/v1/events?"+q, "", bearer(tok0)...)
		if r.errorCode(t) != want {
			t.Errorf("%s = %d %s", q, r.status, r.body)
		}
	}
	hs.createLadder("t9", 1)
	if r := hs.do("GET", "/v1/events?tournament_id=t9", "", bearer(tok0)...); r.status != 404 || r.errorCode(t) != string(tournament.NotJoined) {
		t.Errorf("not joined = %d %s", r.status, r.body)
	}

	// Rule 2: a poll with nothing to return waits and answers when an event
	// is applied.
	cursor := int64(hs.f.Status().Applied)
	done := make(chan EventsResponse, 1)
	go func() { done <- hs.events(tok0, fmt.Sprintf("cursor=%d&wait_ms=5000", cursor)) }()
	time.Sleep(20 * time.Millisecond)
	hs.f.skip() // an applied slot with no visible event keeps it waiting
	time.Sleep(20 * time.Millisecond)
	select {
	case r := <-done:
		t.Fatalf("poll answered without events: %+v", r)
	default:
	}
	hs.finish(testPlayer, "t1", 1)
	got := <-done
	if len(got.Events) != 2 || got.Events[0].Type != "round_finished" || got.Cursor != int64(hs.f.Status().Applied) {
		t.Fatalf("woken poll = %+v", got)
	}
	// A wait that ends answers no events with the applied slot as cursor.
	start := time.Now()
	idle := hs.events(tok0, fmt.Sprintf("cursor=%d&wait_ms=60", got.Cursor))
	if len(idle.Events) != 0 || idle.Cursor != got.Cursor || time.Since(start) < 50*time.Millisecond {
		t.Fatalf("idle poll = %+v after %v", idle, time.Since(start))
	}
	// Rule 5: a cursor ahead of this replica is kept.
	ahead := hs.events(tok0, fmt.Sprintf("cursor=%d&wait_ms=30", got.Cursor+50))
	if ahead.Cursor != got.Cursor+50 || len(ahead.Events) != 0 {
		t.Fatalf("ahead = %+v", ahead)
	}
	// One open poll per player: a second request ends the first at once.
	first := make(chan EventsResponse, 1)
	go func() { first <- hs.events(tok0, fmt.Sprintf("cursor=%d&wait_ms=20000", got.Cursor)) }()
	time.Sleep(30 * time.Millisecond)
	second := make(chan EventsResponse, 1)
	go func() { second <- hs.events(tok0, fmt.Sprintf("cursor=%d&wait_ms=50", got.Cursor)) }()
	select {
	case r := <-first:
		if len(r.Events) != 0 || r.Cursor != got.Cursor {
			t.Errorf("superseded poll = %+v", r)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the first poll was not ended by the second")
	}
	<-second
}

// TestEventsPaging: more than 100 visible events come in pages that never
// split a slot, with has_more and the cursor at the last event returned.
func TestEventsPaging(t *testing.T) {
	hs, tok0, _ := eventsHarness(t, 40)
	for i := 0; i < 40; i++ {
		tid := fmt.Sprintf("t%d", i)
		hs.deal(testPlayer, tid, 1)
		hs.finish(testPlayer, tid, 1) // round_finished and leaderboard_changed in one slot
	}
	var got []EventItem
	cursor := int64(0)
	pages := 0
	for {
		page := hs.events(tok0, fmt.Sprintf("cursor=%d&wait_ms=0", cursor))
		pages++
		if len(page.Events) > MaxEventsPerResponse {
			t.Fatalf("page of %d events", len(page.Events))
		}
		got = append(got, page.Events...)
		if !page.HasMore {
			break
		}
		last := page.Events[len(page.Events)-1]
		if page.Cursor != last.Slot {
			t.Fatalf("has_more cursor %d, last slot %d", page.Cursor, last.Slot)
		}
		cursor = page.Cursor
	}
	if pages < 2 || len(got) != 120 {
		t.Fatalf("%d events in %d pages", len(got), pages)
	}
	for i := 1; i < len(got); i++ {
		if got[i].Slot < got[i-1].Slot || (got[i].Slot == got[i-1].Slot && got[i].Type == "round_started") {
			t.Fatalf("event %d out of order: %+v after %+v", i, got[i], got[i-1])
		}
		if got[i].Type == "leaderboard_changed" && got[i-1].Type != "round_finished" {
			t.Fatalf("a slot was split at event %d", i)
		}
	}
}

// TestRateLimits runs the buckets of section 7.1.6 on the synctest clock.
func TestRateLimits(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newFake()
		keys := testKeyring(t)
		s, err := New(Config{Self: 1, PublicURLs: map[paxos.NodeID]string{1: testSelfURL}, Keys: keys, DealSecret: dealSecret()}, f, nil)
		if err != nil {
			t.Fatal(err)
		}
		h := s.Handler()
		now := time.Now().UnixMilli()
		tok, _ := keys.Sign(sessionClaims(testPlayer, now))
		do := func(method, path, body string, hdr ...string) *httptest.ResponseRecorder {
			req := httptest.NewRequest(method, path, strings.NewReader(body))
			req.RemoteAddr = "192.0.2.7:4000"
			for i := 0; i+1 < len(hdr); i += 2 {
				req.Header.Set(hdr[i], hdr[i+1])
			}
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			return rec
		}
		burst := func(name string, n int, send func(i int) *httptest.ResponseRecorder, retry string) {
			t.Helper()
			for i := 0; i < n; i++ {
				if rec := send(i); rec.Code == http.StatusTooManyRequests {
					t.Fatalf("%s: request %d of the burst limited", name, i)
				}
			}
			rec := send(n)
			if rec.Code != http.StatusTooManyRequests || rec.Header().Get(HeaderRetryAfter) != retry || !strings.Contains(rec.Body.String(), CodeRateLimited) {
				t.Fatalf("%s: request %d = %d %s (Retry-After %q)", name, n, rec.Code, rec.Body, rec.Header().Get(HeaderRetryAfter))
			}
		}
		// Intents: 20 at once, then one per 100 ms.
		join := func(i int) *httptest.ResponseRecorder {
			return do("POST", "/v1/tournaments/t/join", `{"seq":1}`, keyed(fmt.Sprintf("join-key-%08d", i), tok)...)
		}
		burst("intents", 20, join, "1")
		time.Sleep(100 * time.Millisecond)
		if rec := join(99); rec.Code == http.StatusTooManyRequests {
			t.Fatal("intents: no token after 100 ms")
		}
		if rec := join(98); rec.Code != http.StatusTooManyRequests {
			t.Fatal("intents: two tokens after 100 ms")
		}
		// Reads and polls have their own buckets.
		burst("reads", 10, func(int) *httptest.ResponseRecorder { return do("GET", "/v1/tournaments", "", bearer(tok)...) }, "1")
		burst("polls", 4, func(int) *httptest.ResponseRecorder { return do("GET", "/v1/events?wait_ms=0", "", bearer(tok)...) }, "1")
		// Sessions: 6 per device, then one per 10 s; 30 per address.
		sess := func(dev int) func(i int) *httptest.ResponseRecorder {
			return func(i int) *httptest.ResponseRecorder {
				body := fmt.Sprintf(`{"device_id":"%032x","device_secret":"%064x","jurisdiction":"TR","age":30}`, dev, dev)
				return do("POST", "/v1/session", body, HeaderIdempotencyKey, fmt.Sprintf("session-%d-%08d", dev, i))
			}
		}
		burst("session-device", 6, sess(1), "10")
		time.Sleep(10 * time.Second) // the address bucket refills too
		for d := 2; d < 32; d++ {
			if rec := sess(d)(0); rec.Code == http.StatusTooManyRequests {
				t.Fatalf("session-address: device %d limited", d)
			}
		}
		if rec := sess(40)(0); rec.Code != http.StatusTooManyRequests {
			t.Fatalf("session-address: the 31st session = %d", rec.Code)
		}
		// Idle buckets are forgotten when the table grows.
		l := newLimiter(Rate{Every: time.Millisecond, Burst: 1})
		for i := 0; i < limiterSweepAt; i++ {
			l.allow(fmt.Sprint(i), time.Now())
		}
		time.Sleep(time.Second)
		l.allow("fresh", time.Now())
		if l.size() != 1 {
			t.Errorf("limiter holds %d keys after a sweep", l.size())
		}
	})
}

func sessionClaims(p string, now int64) session.Claims {
	return session.Claims{Player: p, Device: testDevice, IssuedAtMs: now, ExpiresAtMs: now + time.Hour.Milliseconds()}
}
