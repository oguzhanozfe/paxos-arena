package intent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/oguzhanozfe/paxos-arena/internal/game"
	"github.com/oguzhanozfe/paxos-arena/internal/paxos"
	"github.com/oguzhanozfe/paxos-arena/internal/replica"
	"github.com/oguzhanozfe/paxos-arena/internal/replog"
	"github.com/oguzhanozfe/paxos-arena/internal/session"
	"github.com/oguzhanozfe/paxos-arena/internal/tournament"
)

func TestNewRefusesBadConfig(t *testing.T) {
	good := func() Config {
		return Config{Self: 1, PublicURLs: map[paxos.NodeID]string{1: "http://a"}, Keys: testKeyring(t), DealSecret: dealSecret()}
	}
	cases := map[string]func(*Config){
		"no keys":        func(c *Config) { c.Keys = session.Keyring{} },
		"short key":      func(c *Config) { c.Keys = session.Keyring{Keys: []session.Key{{ID: "k1", Secret: make([]byte, 16)}}} },
		"short secret":   func(c *Config) { c.DealSecret = make([]byte, 31) },
		"no own URL":     func(c *Config) { c.PublicURLs = map[paxos.NodeID]string{2: "http://b"} },
		"not http":       func(c *Config) { c.PublicURLs[1] = "ftp://a" },
		"ttl above 24 h": func(c *Config) { c.SessionTTL = 25 * time.Hour },
	}
	for name, mod := range cases {
		cfg := good()
		mod(&cfg)
		if _, err := New(cfg, newFake(), nil); err == nil {
			t.Errorf("%s: New accepted the config", name)
		}
	}
	if _, err := New(good(), newFake(), nil); err != nil {
		t.Errorf("good config refused: %v", err)
	}
	if id := RandomPlayerID(); len(id) != 28 || !strings.HasPrefix(string(id), "p-") || tournament.ValidateTournamentID(tournament.TournamentID(id)) != nil {
		t.Errorf("RandomPlayerID = %q", id)
	}
}

func TestSessionRoute(t *testing.T) {
	hs := newHarness(t, nil)
	body := `{"device_id":"` + testDevice + `","device_secret":"` + testSecret + `","jurisdiction":"TR","age":31}`
	first := hs.do("POST", "/v1/session", body, HeaderIdempotencyKey, "session-key-000001")
	if first.status != 200 {
		t.Fatalf("session = %d %s", first.status, first.body)
	}
	checkWireShape(t, first.body, SessionResponse{})
	var s SessionResponse
	first.decode(t, &s)
	if !s.NewPlayer || s.PlayerID != testPlayer || s.NextSeq != 1 || s.IssuedAtMs != testStartMs || s.ExpiresAtMs != testStartMs+3_600_000 ||
		s.Jurisdiction != "TR" || s.Age != 31 || s.Replayed {
		t.Fatalf("session = %+v", s)
	}
	if first.header.Get(HeaderSlot) != strconv.FormatInt(s.Slot, 10) || first.header.Get(HeaderNode) != "1" || first.header.Get(HeaderLeader) != testSelfURL ||
		first.header.Get(HeaderServerTime) != strconv.FormatInt(testStartMs, 10) || first.header.Get("Content-Type") != "application/json" {
		t.Errorf("headers = %v", first.header)
	}
	// The token is the Appendix A token: same key, claims and times.
	const appendixToken = "v1.k1.eyJwaWQiOiJwLW16eHc2eXRib2k0ZHFuYnJnbTJ3cXpsbW40IiwiZGlkIjoiOWY4NmQwODE4ODRjN2Q2NTlhMmZlYWEwYzU1YWQwMTUiLCJpYXQiOjE3ODkyMDAwMDAwMDAsImV4cCI6MTc4OTIwMzYwMDAwMH0.5CWAjViqDUMjFQmhATnN_JiC085QYywGbSPGTGBVjiE"
	if s.SessionToken != appendixToken {
		t.Errorf("token = %s", s.SessionToken)
	}
	if b, ok := hs.f.state.Binding(testDevice); !ok || b.Verifier.String() != "eefcb1443d2b7d069bf765df3584178eb2b4c9b5125242a75bf53c0c7e3da2b8" {
		t.Errorf("binding verifier = %+v", b)
	}
	// The same key replays byte for byte but for replayed.
	hs.clock.add(time.Minute)
	again := hs.do("POST", "/v1/session", body, HeaderIdempotencyKey, "session-key-000001")
	if want := bytes.Replace(first.body, []byte(`"replayed":false`), []byte(`"replayed":true`), 1); again.status != 200 || !bytes.Equal(again.body, want) {
		t.Errorf("replay = %d %s", again.status, again.body)
	}
	// Another key: the same player, a new issue time.
	second := hs.do("POST", "/v1/session", body, HeaderIdempotencyKey, "session-key-000002")
	var s2 SessionResponse
	second.decode(t, &s2)
	if s2.NewPlayer || s2.PlayerID != testPlayer || s2.IssuedAtMs != testStartMs+60_000 {
		t.Errorf("second session = %+v", s2)
	}
	// The same key with other claims.
	other := strings.Replace(body, `"TR"`, `"DE"`, 1)
	if r := hs.do("POST", "/v1/session", other, HeaderIdempotencyKey, "session-key-000001"); r.status != 422 || r.errorCode(t) != CodeKeyReused {
		t.Errorf("key reuse = %d %s", r.status, r.body)
	}
	// A wrong secret is refused before the log.
	submits := hs.f.submits
	wrong := strings.Replace(body, testSecret, strings.Repeat("ab", 32), 1)
	r := hs.do("POST", "/v1/session", wrong, HeaderIdempotencyKey, "session-key-000003")
	if r.status != 403 || r.errorCode(t) != string(tournament.DeviceMismatch) || r.header.Get(HeaderSlot) != "" || hs.f.submits != submits {
		t.Errorf("wrong secret = %d %s (submits %d -> %d)", r.status, r.body, submits, hs.f.submits)
	}
	for name, bad := range map[string]string{
		"short device":   strings.Replace(body, testDevice, testDevice[:31], 1),
		"upper secret":   strings.Replace(body, testSecret, strings.ToUpper(testSecret), 1),
		"jurisdiction":   strings.Replace(body, `"TR"`, `"T"`, 1),
		"age":            strings.Replace(body, `31`, `151`, 1),
		"unknown field":  strings.Replace(body, `}`, `,"admin":true}`, 1),
		"trailing":       body + "{}",
		"not an object":  `[1]`,
		"age as string":  strings.Replace(body, `31`, `"31"`, 1),
		"empty":          ``,
		"oversized body": `{"device_id":"` + strings.Repeat("a", DefaultMaxBody) + `"}`,
	} {
		r := hs.do("POST", "/v1/session", bad, HeaderIdempotencyKey, "session-key-000009")
		want := 400
		if name == "oversized body" {
			want = 413
		}
		if r.status != want {
			t.Errorf("%s: %d %s", name, r.status, r.body)
		}
	}
}

// TestOrderOfChecks follows the order of section 7.1.6 with requests that
// fail several checks at once.
func TestOrderOfChecks(t *testing.T) {
	type step struct {
		name   string
		setup  func(hs *harness)
		method string
		path   string
		body   string
		hdr    func(hs *harness) []string
		status int
		code   string
	}
	tok := func(hs *harness) string { return hs.token(testPlayer, testDevice) }
	big := `{"seq":1,"pad":"` + strings.Repeat("x", DefaultMaxBody) + `"}`
	follower := func(leader paxos.NodeID) func(hs *harness) {
		return func(hs *harness) {
			hs.f.setStatus(func(s *replica.Status) { s.Role, s.Leader = replog.Follower, leader })
		}
	}
	steps := []step{
		{name: "unknown route", method: "POST", path: "/v1/nope", status: 404, code: CodeNotFound},
		{name: "method before leader", setup: follower(2), method: "GET", path: "/v1/tournaments/t/join", status: 405, code: CodeMethodNotAllowed},
		{name: "follower redirects before key and token", setup: follower(2), method: "POST", path: "/v1/tournaments/t/rounds/9/moves?x=1", body: big, status: 307, code: CodeNotLeader},
		{name: "follower without leader", setup: follower(0), method: "POST", path: "/v1/tournaments/t/join", status: 503, code: CodeNoLeader},
		{name: "leader not ready", setup: func(hs *harness) { hs.f.setStatus(func(s *replica.Status) { s.Ready = false }) }, method: "POST", path: "/v1/session", status: 503, code: CodeUnavailable},
		{name: "key before token", method: "POST", path: "/v1/tournaments/t/join", body: big, status: 400, code: CodeMissingKey},
		{name: "key shape", method: "POST", path: "/v1/tournaments/t/join", body: big, hdr: func(*harness) []string { return []string{HeaderIdempotencyKey, "short"} }, status: 400, code: CodeInvalidKey},
		{name: "key characters", method: "POST", path: "/v1/tournaments/t/join", hdr: func(*harness) []string { return []string{HeaderIdempotencyKey, "has.a.dot.in.the.key"} }, status: 400, code: CodeInvalidKey},
		{name: "token before body", method: "POST", path: "/v1/tournaments/t/join", body: big, hdr: func(*harness) []string { return []string{HeaderIdempotencyKey, "join-key-00000001"} }, status: 401, code: CodeSessionMissing},
		{name: "not bearer", method: "POST", path: "/v1/tournaments/t/join", hdr: func(*harness) []string {
			return []string{HeaderIdempotencyKey, "join-key-00000001", HeaderAuthorization, "Basic abc"}
		}, status: 401, code: CodeSessionMissing},
		{name: "invalid token", method: "POST", path: "/v1/tournaments/t/join", hdr: func(*harness) []string { return keyed("join-key-00000001", "v1.k1.x.y") }, status: 401, code: CodeSessionInvalid},
		{name: "expired token", method: "POST", path: "/v1/tournaments/t/join", hdr: func(hs *harness) []string {
			tk := tok(hs)
			hs.clock.add(time.Hour + time.Minute)
			return keyed("join-key-00000001", tk)
		}, status: 401, code: CodeSessionExpired},
		{name: "rate limit before body", setup: func(hs *harness) {
			hs.s.intentLimit = newLimiter(Rate{Every: time.Hour, Burst: 1})
			hs.s.intentLimit.allow(testPlayer, hs.clock.Now())
		},
			method: "POST", path: "/v1/tournaments/t/join", body: big, hdr: func(hs *harness) []string { return keyed("join-key-00000001", tok(hs)) }, status: 429, code: CodeRateLimited},
		{name: "size before path", method: "POST", path: "/v1/tournaments/t:x/join", body: big, hdr: func(hs *harness) []string { return keyed("join-key-00000001", tok(hs)) }, status: 413, code: CodeBodyTooLarge},
		{name: "body shape", method: "POST", path: "/v1/tournaments/t/join", body: `{"seq":0}`, hdr: func(hs *harness) []string { return keyed("join-key-00000001", tok(hs)) }, status: 400, code: CodeMalformed},
		{name: "path", method: "POST", path: "/v1/tournaments/t/rounds/4/deal", body: `{"seq":1}`, hdr: func(hs *harness) []string { return keyed("deal-key-00000001", tok(hs)) }, status: 400, code: CodeMalformed},
		{name: "move shape", method: "POST", path: "/v1/tournaments/t/rounds/1/moves", body: `{"seq":1,"move_index":0,"kind":"draw","column":0}`, hdr: func(hs *harness) []string { return keyed("move-key-00000001", tok(hs)) }, status: 400, code: CodeMalformed},
		{name: "recorded rejection", method: "POST", path: "/v1/tournaments/t/join", body: `{"seq":1}`, hdr: func(hs *harness) []string { return keyed("join-key-00000001", tok(hs)) }, status: 409, code: string(tournament.UnknownPlayer)},
		{name: "GET token before query", method: "GET", path: "/v1/tournaments?limit=0", status: 401, code: CodeSessionMissing},
		{name: "GET rate limit before query", setup: func(hs *harness) {
			hs.s.readLimit = newLimiter(Rate{Every: time.Hour, Burst: 1})
			hs.s.readLimit.allow(testPlayer, hs.clock.Now())
		},
			method: "GET", path: "/v1/tournaments?limit=0", hdr: func(hs *harness) []string { return bearer(tok(hs)) }, status: 429, code: CodeRateLimited},
		{name: "GET query", method: "GET", path: "/v1/tournaments?limit=0", hdr: func(hs *harness) []string { return bearer(tok(hs)) }, status: 400, code: CodeMalformed},
		{name: "GET min_slot on a follower", setup: follower(2), method: "GET", path: "/v1/tournaments/t/rounds/1?min_slot=99", hdr: func(hs *harness) []string { return bearer(tok(hs)) }, status: 307, code: CodeNotLeader},
		{name: "GET min_slot on the leader", method: "GET", path: "/v1/tournaments/t/rounds/1?min_slot=99", hdr: func(hs *harness) []string { return bearer(tok(hs)) }, status: 503, code: CodeReplicaBehind},
		{name: "GET read", method: "GET", path: "/v1/tournaments/t/rounds/1", hdr: func(hs *harness) []string { return bearer(tok(hs)) }, status: 404, code: string(tournament.UnknownTournament)},
		{name: "GET on a follower is served", setup: follower(2), method: "GET", path: "/v1/tournaments", hdr: func(hs *harness) []string { return bearer(tok(hs)) }, status: 200},
	}
	for _, st := range steps {
		t.Run(st.name, func(t *testing.T) {
			hs := newHarness(t, nil)
			if st.setup != nil {
				st.setup(hs)
			}
			var hdr []string
			if st.hdr != nil {
				hdr = st.hdr(hs)
			}
			r := hs.do(st.method, st.path, st.body, hdr...)
			if r.status != st.status {
				t.Fatalf("status %d, want %d: %s", r.status, st.status, r.body)
			}
			if st.code != "" {
				var e ErrorBody
				r.decode(t, &e)
				checkWireShape(t, r.body, ErrorBody{})
				if e.Code != st.code {
					t.Fatalf("code %s, want %s", e.Code, st.code)
				}
				wantRetry := map[int]bool{307: true, 401: true, 409: false, 429: true, 503: true}[st.status]
				if st.code == CodeInFlight {
					wantRetry = true
				}
				if e.Retryable != wantRetry {
					t.Errorf("retryable = %t", e.Retryable)
				}
			}
			switch st.status {
			case 307:
				if loc := r.header.Get(HeaderLocation); !strings.HasPrefix(loc, testOtherURL+"/v1/") || r.header.Get(HeaderLeader) != testOtherURL {
					t.Errorf("Location %q Leader %q", loc, r.header.Get(HeaderLeader))
				}
				if st.method == "POST" && r.header.Get(HeaderLocation) != testOtherURL+st.path {
					t.Errorf("Location %q lost the path or query", r.header.Get(HeaderLocation))
				}
			case 429, 503:
				if r.header.Get(HeaderRetryAfter) == "" {
					t.Error("no Retry-After")
				}
			case 405:
				if r.header.Get("Allow") != "POST" {
					t.Errorf("Allow = %q", r.header.Get("Allow"))
				}
			case 409:
				if r.header.Get(HeaderSlot) == "" || r.header.Get(HeaderAppliedSlot) == "" {
					t.Errorf("recorded rejection headers %v", r.header)
				}
			}
			if r.header.Get(HeaderNode) != "1" || r.header.Get(HeaderServerTime) == "" || r.header.Get("Content-Type") != "application/json" {
				t.Errorf("common headers %v", r.header)
			}
		})
	}
}

func TestInFlightAndSubmitErrors(t *testing.T) {
	hs := newHarness(t, nil)
	hs.createLadder(testTID, 1)
	s := hs.session(0)
	hs.f.mu.Lock()
	hs.f.hold, hs.f.holding = make(chan struct{}), make(chan struct{}, 1)
	hs.f.mu.Unlock()
	hdr := keyed("join-key-00000001", s.SessionToken)
	done := make(chan response, 1)
	go func() { done <- hs.do("POST", "/v1/tournaments/"+testTID+"/join", `{"seq":1}`, hdr...) }()
	<-hs.f.holding
	if r := hs.do("POST", "/v1/tournaments/"+testTID+"/join", `{"seq":1}`, hdr...); r.status != 409 || r.errorCode(t) != CodeInFlight || r.header.Get(HeaderRetryAfter) != "1" {
		t.Errorf("concurrent retry = %d %s", r.status, r.body)
	}
	hs.f.mu.Lock()
	close(hs.f.hold)
	hs.f.hold = nil
	hs.f.mu.Unlock()
	if r := <-done; r.status != 201 {
		t.Fatalf("held join = %d %s", r.status, r.body)
	}
	for _, tc := range []struct {
		err    error
		status int
		code   string
	}{
		{replog.NotLeaderError{Leader: 2}, 307, CodeNotLeader},
		{replog.NotLeaderError{}, 503, CodeNoLeader},
		{replica.ErrLeadershipLost, 503, CodeNoLeader},
		{replog.ErrBusy, 503, CodeUnavailable},
		{replica.ErrStopped, 503, CodeUnavailable},
		{fmt.Errorf("wrapped: %w", context.DeadlineExceeded), 504, CodeOutcomeUnknown},
		{fmt.Errorf("disk on fire"), 500, CodeInternal},
	} {
		hs.f.mu.Lock()
		hs.f.submitErr = tc.err
		hs.f.mu.Unlock()
		r := hs.do("POST", "/v1/tournaments/"+testTID+"/rounds/1/deal", `{"seq":2}`, keyed("deal-key-00000001", s.SessionToken)...)
		if r.status != tc.status || r.errorCode(t) != tc.code {
			t.Errorf("%v: %d %s", tc.err, r.status, r.body)
		}
	}
}

// TestRoundThroughHandlers plays the Appendix A round 1 through the
// handlers: join, deal, every greedy move, replays, stale and skipped
// sequence numbers, reads, finish, and the audit of the revealed seed.
func TestRoundThroughHandlers(t *testing.T) {
	hs := newHarness(t, nil)
	hs.createLadder(testTID, 1)
	s := hs.session(0)
	tok := s.SessionToken
	base := "/v1/tournaments/" + testTID
	seq := int64(0)
	post := func(path, key, body string, want int) response {
		t.Helper()
		r := hs.do("POST", base+path, body, keyed(key, tok)...)
		if r.status != want {
			t.Fatalf("POST %s = %d %s", path, r.status, r.body)
		}
		return r
	}
	seq++
	jr := post("/join", "join-key-00000001", fmt.Sprintf(`{"seq":%d}`, seq), 201)
	checkWireShape(t, jr.body, JoinResponse{})
	var join JoinResponse
	jr.decode(t, &join)
	if join.JoinSeq != 1 || join.EntryFee != 500 || join.Rounds != 3 || join.NextSeq != 2 || jr.header.Get(HeaderNextSeq) != "2" {
		t.Fatalf("join = %+v", join)
	}
	hs.clock.add(12 * time.Second)
	seq++
	dr := post("/rounds/1/deal", "deal-key-00000001", fmt.Sprintf(`{"seq":%d}`, seq), 201)
	checkWireShape(t, dr.body, RoundResponse{})
	var deal RoundResponse
	dr.decode(t, &deal)
	v := deal.Round
	want := [][]string{{"Ks", "8h", "7s", "3s", "Jd"}, {"2h", "4c", "5d", "7h", "Qh"}, {"3c", "8s", "Ah", "8d", "Th"}, {"9d", "Td", "3h", "Jh", "Qs"},
		{"Ts", "7c", "5s", "6d", "2d"}, {"5h", "Tc", "Ad", "9s", "9h"}, {"2c", "6s", "6c", "3d", "As"}}
	for c := range want {
		if strings.Join(v.Columns[c].Cards, " ") != strings.Join(want[c], " ") {
			t.Fatalf("column %d = %v", c, v.Columns[c].Cards)
		}
	}
	if v.WasteTop != "4h" || v.WasteCount != 1 || v.StockCount != 16 || len(v.PlayableColumns) != 0 || !v.CanDraw || v.Seed != "" ||
		v.Commitment != "5e188f4cb340f1c0e94c8821b8f72279b339f5698c66e659b22d32b3f9b29065" || v.Status != "playing" ||
		v.StartedAtMs != testStartMs+12_000 || v.DeadlineMs != testStartMs+312_000 || v.LastMove != (MoveView{Column: -1}) {
		t.Fatalf("deal view = %+v", v)
	}
	if strings.Contains(string(dr.body), "Kh") {
		t.Fatal("the deal response shows a stock card")
	}
	move := func(key string, idx int, kind string, col int, wantStatus int) (RoundResponse, response) {
		t.Helper()
		seq++
		r := post("/rounds/1/moves", key, fmt.Sprintf(`{"seq":%d,"move_index":%d,"kind":%q,"column":%d}`, seq, idx, kind, col), wantStatus)
		var out RoundResponse
		if wantStatus == 200 {
			checkWireShape(t, r.body, RoundResponse{})
			r.decode(t, &out)
		}
		return out, r
	}
	m1, r1 := move("move-key-00000000", 0, "draw", -1, 200)
	if m1.Round.WasteTop != "Kh" || m1.Round.WasteCount != 2 || m1.Round.StockCount != 15 || fmt.Sprint(m1.Round.PlayableColumns) != "[1 3 6]" ||
		m1.Round.LastMove != (MoveView{Kind: "draw", Column: -1, Card: "Kh"}) || m1.Round.MoveIndex != 1 || m1.NextSeq != 4 {
		t.Fatalf("after draw = %+v", m1.Round)
	}
	// A lost response: the same key and body replay the recorded response.
	seq--
	again, ar := move("move-key-00000000", 0, "draw", -1, 200)
	if !again.Replayed || !bytes.Equal(ar.body, bytes.Replace(r1.body, []byte(`"replayed":false`), []byte(`"replayed":true`), 1)) {
		t.Fatalf("replay differs:\n%s\n%s", r1.body, ar.body)
	}
	if ar.header.Get(HeaderSlot) != r1.header.Get(HeaderSlot) || ar.header.Get(HeaderNextSeq) != "4" {
		t.Errorf("replay headers %v", ar.header)
	}
	// A captured request replayed under a new key: stale_seq, nothing changes.
	seq--
	_, stale := move("move-key-stale-001", 0, "draw", -1, 409)
	if stale.errorCode(t) != string(tournament.StaleSeq) || stale.header.Get(HeaderNextSeq) != "4" {
		t.Fatalf("stale = %s %v", stale.body, stale.header)
	}
	seq++
	_, gap := move("move-key-gap-00001", 1, "play", 1, 409)
	if gap.errorCode(t) != string(tournament.SeqGap) || gap.header.Get(HeaderNextSeq) != "4" {
		t.Fatalf("gap = %s", gap.body)
	}
	seq -= 2
	m2, _ := move("move-key-00000001", 1, "play", 1, 200)
	if strings.Join(m2.Round.Columns[1].Cards, " ") != "2h 4c 5d 7h" || m2.Round.WasteTop != "Qh" || m2.Round.Score != 100 || fmt.Sprint(m2.Round.PlayableColumns) != "[0]" {
		t.Fatalf("after play = %+v", m2.Round)
	}
	// A rule rejection consumes the number and answers the view's owner
	// nothing but the code.
	_, illegal := move("move-key-illegal01", 2, "play", 3, 409)
	if illegal.errorCode(t) != string(tournament.IllegalMove) || illegal.header.Get(HeaderNextSeq) != strconv.FormatInt(seq+1, 10) {
		t.Fatalf("illegal = %s %v", illegal.body, illegal.header)
	}
	// A read with min_slot of the last intent shows the latest board.
	rr := hs.do("GET", base+"/rounds/1?min_slot="+m2Slot(t, hs), "", bearer(tok)...)
	var read RoundResponse
	rr.decode(t, &read)
	if rr.status != 200 || read.Round.MoveIndex != 2 || read.NextSeq != seq+1 || read.Replayed {
		t.Fatalf("read = %d %+v", rr.status, read)
	}
	// Play greedily to the end.
	last := m2
	for n := 2; last.Round.Status == "playing"; n++ {
		kind, col := "draw", -1
		if len(last.Round.PlayableColumns) > 0 {
			kind, col = "play", int(last.Round.PlayableColumns[0])
		}
		last, _ = move(fmt.Sprintf("move-key-%08d", n), n, kind, col, 200)
		if last.Round.Status == "playing" && last.Round.Seed != "" {
			t.Fatal("seed shown while playing")
		}
	}
	fv := last.Round
	if fv.MoveIndex != 48 || fv.FinishReason != "blocked" || fv.Cleared != 32 || fv.Score != 3200 || fv.WasteTop != "Qd" || fv.WasteCount != 49 ||
		fv.StockCount != 0 || fmt.Sprint(fv.Columns[4].Cards) != "[Ts]" || fmt.Sprint(fv.Columns[6].Cards) != "[2c 6s]" || fv.CanDraw || len(fv.PlayableColumns) != 0 {
		t.Fatalf("final view = %+v", fv)
	}
	// Audit: the revealed seed matches the commitment and replays the deal.
	var seed game.Seed
	if err := seed.UnmarshalText([]byte(fv.Seed)); err != nil || game.Commit(seed).String() != deal.Round.Commitment {
		t.Fatalf("seed %q does not match the commitment", fv.Seed)
	}
	if b := game.Deal(seed); b.Columns[0][4].String() != "Jd" {
		t.Error("the revealed seed does not deal the first view")
	}
	// Finish is idempotent on a finished round and returns the final view.
	seq++
	fr := post("/rounds/1/finish", "finish-key-000001", fmt.Sprintf(`{"seq":%d}`, seq), 200)
	var fin RoundResponse
	fr.decode(t, &fin)
	if fin.Round.Seed != fv.Seed || fin.Round.Status != "finished" || fin.Round.Score != 3200 {
		t.Fatalf("finish = %+v", fin.Round)
	}
	// Replaying the deal key long after returns the deal view, not the
	// final one: no seed, 16 cards to draw.
	seq = 1
	dealAgain := hs.do("POST", base+"/rounds/1/deal", `{"seq":2}`, keyed("deal-key-00000001", tok)...)
	if !bytes.Equal(dealAgain.body, bytes.Replace(dr.body, []byte(`"replayed":false`), []byte(`"replayed":true`), 1)) {
		t.Fatalf("deal replay differs:\n%s\n%s", dr.body, dealAgain.body)
	}
	// The list, the leaderboard and the events.
	lr := hs.do("GET", "/v1/tournaments", "", bearer(tok)...)
	checkWireShape(t, lr.body, TournamentListResponse{})
	var list TournamentListResponse
	lr.decode(t, &list)
	if list.Total != 1 || len(list.Tournaments) != 1 || !list.Tournaments[0].Joined || list.Tournaments[0].Eligible ||
		list.Tournaments[0].RoundsFinished != 1 || list.Tournaments[0].NextRound != 2 || list.Tournaments[0].ProjectedPool != 450 {
		t.Fatalf("list = %+v", list)
	}
	lb := hs.do("GET", base+"/leaderboard", "", bearer(tok)...)
	checkWireShape(t, lb.body, LeaderboardResponse{})
	var board LeaderboardResponse
	lb.decode(t, &board)
	if board.Final || board.Entrants != 1 || len(board.Rows) != 1 || board.Me.Place != 1 || board.Me.TotalScore != 3200 || board.Me.RoundsFinished != 1 {
		t.Fatalf("leaderboard = %+v", board)
	}
	er := hs.do("GET", "/v1/events?wait_ms=0", "", bearer(tok)...)
	checkWireShape(t, er.body, EventsResponse{})
	var evs EventsResponse
	er.decode(t, &evs)
	if len(evs.Events) != 3 || evs.Events[0].Type != "round_started" || evs.Events[1].Type != "round_finished" || evs.Events[1].Score != 3200 ||
		evs.Events[2].Type != "leaderboard_changed" || evs.HasMore || evs.Cursor != int64(hs.f.Status().Applied) {
		t.Fatalf("events = %+v", evs)
	}
	for _, e := range evs.Events {
		b := mustJSON(t, e)
		checkWireShape(t, b, EventItem{})
	}
}

func m2Slot(t *testing.T, hs *harness) string {
	return strconv.FormatUint(uint64(hs.f.Status().Applied), 10)
}

func TestClaimRoute(t *testing.T) {
	hs := newHarness(t, nil)
	hs.createLadder(testTID, 1)
	s := hs.session(0)
	tok := s.SessionToken
	base := "/v1/tournaments/" + testTID
	if r := hs.do("POST", base+"/join", `{"seq":1}`, keyed("join-key-00000001", tok)...); r.status != 201 {
		t.Fatal(r.status)
	}
	if r := hs.do("POST", base+"/payout/claim", `{"seq":2}`, keyed("claim-key-0000001", tok)...); r.status != 409 || r.errorCode(t) != string(tournament.NotSettled) {
		t.Fatalf("early claim = %d %s", r.status, r.body)
	}
	if r := hs.do("POST", base+"/rounds/1/deal", `{"seq":3}`, keyed("deal-key-00000001", tok)...); r.status != 201 {
		t.Fatal(r.status)
	}
	now := hs.clock.Now().UnixMilli()
	hs.f.apply(t, "close", tournament.Close{Tournament: testTID}, now)
	hs.f.apply(t, "settle", tournament.Settle{Tournament: testTID}, now)
	first := hs.do("POST", base+"/payout/claim", `{"seq":4}`, keyed("claim-key-0000002", tok)...)
	checkWireShape(t, first.body, ClaimResponse{})
	var c ClaimResponse
	first.decode(t, &c)
	if first.status != 200 || c.Amount != 450 || c.PostingKey != "claim:"+testTID+":"+testPlayer || c.NextSeq != 5 {
		t.Fatalf("claim = %d %+v", first.status, c)
	}
	again := hs.do("POST", base+"/payout/claim", `{"seq":4}`, keyed("claim-key-0000002", tok)...)
	if !bytes.Equal(again.body, bytes.Replace(first.body, []byte(`"replayed":false`), []byte(`"replayed":true`), 1)) {
		t.Errorf("claim replay = %s", again.body)
	}
	if r := hs.do("POST", base+"/payout/claim", `{"seq":5}`, keyed("claim-key-0000003", tok)...); r.status != 409 || r.errorCode(t) != string(tournament.AlreadyClaimed) {
		t.Errorf("second claim = %d %s", r.status, r.body)
	}
	// The round the close finished shows its reason and seed.
	rr := hs.do("GET", base+"/rounds/1", "", bearer(tok)...)
	var read RoundResponse
	rr.decode(t, &read)
	if read.Round.FinishReason != "closed" || read.Round.Seed == "" {
		t.Errorf("closed round = %+v", read.Round)
	}
	lb := hs.do("GET", base+"/leaderboard", "", bearer(tok)...)
	var board LeaderboardResponse
	lb.decode(t, &board)
	if !board.Final || board.Status != "settled" || board.Me.Amount != 450 || !board.Me.Claimed || !board.Me.Scored {
		t.Errorf("settled leaderboard = %+v", board)
	}
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
