package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/oguzhanozfe/paxos-arena/internal/paxos"
	"github.com/oguzhanozfe/paxos-arena/internal/replica"
	"github.com/oguzhanozfe/paxos-arena/internal/replog"
	"github.com/oguzhanozfe/paxos-arena/internal/tournament"
)

// fakeBackend applies commands to a real tournament.State on a single
// goroutine, so the handler tests exercise real results. submitErr, when
// set, is returned instead; block, when set, makes Submit wait until it is
// closed.
type fakeBackend struct {
	mu        sync.Mutex
	state     *tournament.State
	slot      paxos.Slot
	status    replica.Status
	submitErr error
	block     chan struct{}
	entered   chan struct{}
	reads     []bool
}

func newFake() *fakeBackend {
	return &fakeBackend{state: tournament.New(), status: replica.Status{Self: 1, Leader: 1, Role: replog.Leader, Ready: true}}
}

func (f *fakeBackend) Submit(ctx context.Context, cmd tournament.Command) (tournament.Result, error) {
	if f.block != nil {
		if f.entered != nil {
			select {
			case f.entered <- struct{}{}:
			default:
			}
		}
		select {
		case <-f.block:
		case <-ctx.Done():
			return tournament.Result{}, ctx.Err()
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.submitErr != nil {
		return tournament.Result{}, f.submitErr
	}
	if res, ok := f.state.Replay(cmd); ok {
		return res, nil
	}
	f.slot++
	return f.state.Apply(f.slot, paxos.Ballot{Round: 1, Node: 1}, cmd), nil
}

func (f *fakeBackend) Read(ctx context.Context, consistent bool, fn func(*tournament.State) error) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reads = append(f.reads, consistent)
	if consistent && f.status.Role != replog.Leader {
		return replog.ErrNotLeader{Leader: f.status.Leader}
	}
	return fn(f.state)
}

func (f *fakeBackend) Status() replica.Status {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.status
}

func (f *fakeBackend) setStatus(st replica.Status) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.status = st
}

const createBody = `{"id":"t1","rules":{"entry_fee":500,"rake_bps":1000,"prize_bps":[5000,3000,2000],"min_entrants":3,"max_entrants":100,"max_score":100000,"min_age":18,"tie_break":"earliest_submission","exclusions":{"version":7,"jurisdictions":["XX"]}}}`

func newServer(t *testing.T, f *fakeBackend, mod func(*Config)) http.Handler {
	t.Helper()
	cfg := Config{Self: 1, Seed: func() uint64 { return 4242 }, Now: func() time.Time { return time.UnixMilli(1000) }}
	if mod != nil {
		mod(&cfg)
	}
	return New(cfg, f, nil).Handler()
}

// do sends one request and returns the recorder.
func do(h http.Handler, method, path, key, body string, headers ...string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if key != "" {
		req.Header.Set(HeaderIdempotencyKey, key)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func problemOf(t *testing.T, rec *httptest.ResponseRecorder) Problem {
	t.Helper()
	if ct := rec.Header().Get("Content-Type"); ct != "application/problem+json" {
		t.Errorf("Content-Type = %q, want application/problem+json (body %s)", ct, rec.Body.String())
	}
	var p Problem
	if err := json.Unmarshal(rec.Body.Bytes(), &p); err != nil {
		t.Fatalf("problem body %q: %v", rec.Body.String(), err)
	}
	if p.Status != rec.Code {
		t.Errorf("problem status %d != response status %d", p.Status, rec.Code)
	}
	return p
}

func responseOf(t *testing.T, rec *httptest.ResponseRecorder) CommandResponse {
	t.Helper()
	var r CommandResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &r); err != nil {
		t.Fatalf("response body %q: %v", rec.Body.String(), err)
	}
	return r
}

// TestCommandTable is the handler table of design 6.5 for the request
// shape and the state-machine outcomes.
func TestCommandTable(t *testing.T) {
	f := newFake()
	h := newServer(t, f, func(c *Config) { c.MaxBody = 512 })
	if rec := do(h, "POST", "/v1/tournaments", "k1", createBody); rec.Code != http.StatusCreated {
		t.Fatalf("create: %d %s", rec.Code, rec.Body.String())
	}
	cases := []struct {
		name       string
		method     string
		path       string
		key        string
		body       string
		wantStatus int
		wantCode   string
	}{
		{"missing key", "POST", "/v1/tournaments", "", createBody, 400, CodeMissingKey},
		{"invalid key", "POST", "/v1/tournaments", "has space", createBody, 400, CodeInvalidKey},
		{"malformed body", "POST", "/v1/tournaments", "k2", `{"id":`, 400, CodeMalformedBody},
		{"unknown field", "POST", "/v1/tournaments", "k3", `{"id":"t2","rules":{},"seed":1}`, 400, CodeMalformedBody},
		{"trailing data", "POST", "/v1/tournaments/t1/close", "k4", `{} {}`, 400, CodeMalformedBody},
		{"oversized body", "POST", "/v1/tournaments", "k5", `{"id":"` + strings.Repeat("x", 600) + `"}`, 413, CodeBodyTooLarge},
		{"invalid rules", "POST", "/v1/tournaments", "k6", `{"id":"t2","rules":{"entry_fee":0}}`, 409, string(tournament.InvalidRules)},
		{"tournament exists", "POST", "/v1/tournaments", "k7", createBody, 409, string(tournament.TournamentExists)},
		{"key reused", "POST", "/v1/tournaments", "k1", `{"id":"other","rules":{"entry_fee":1,"prize_bps":[10000],"min_entrants":1,"max_entrants":1,"tie_break":"split"}}`, 422, string(tournament.KeyReused)},
		{"join unknown", "POST", "/v1/tournaments/nope/entries", "k8", `{"player":{"id":"p1","jurisdiction":"TR","age":30}}`, 409, string(tournament.UnknownTournament)},
		{"join excluded", "POST", "/v1/tournaments/t1/entries", "k9", `{"player":{"id":"p1","jurisdiction":"XX","age":30}}`, 409, string(tournament.JurisdictionExcluded)},
		{"score not joined", "POST", "/v1/tournaments/t1/scores", "k10", `{"player":"p1","score":1,"deal_seed":4242}`, 409, string(tournament.NotJoined)},
		{"settle not closed", "POST", "/v1/tournaments/t1/settle", "k11", `{"exclusions":{"version":8,"jurisdictions":[]}}`, 409, string(tournament.NotClosed)},
		{"get unknown", "GET", "/v1/tournaments/nope", "", "", 404, CodeNotFound},
		{"ledger unknown", "GET", "/v1/tournaments/nope/ledger", "", "", 404, CodeNotFound},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := do(h, tc.method, tc.path, tc.key, tc.body)
			if rec.Code != tc.wantStatus {
				t.Fatalf("status %d, want %d: %s", rec.Code, tc.wantStatus, rec.Body.String())
			}
			p := problemOf(t, rec)
			if p.Code != tc.wantCode {
				t.Errorf("code %q, want %q (detail %q)", p.Code, tc.wantCode, p.Detail)
			}
			if p.Detail == "" {
				t.Error("problem lacks a detail")
			}
		})
	}
	if rec := do(h, "GET", "/v1/tournaments", "", ""); rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("GET on a POST route = %d", rec.Code)
	}
	if rec := do(h, "GET", "/healthz", "", ""); rec.Code != 200 || rec.Body.String() != "ok\n" {
		t.Errorf("healthz = %d %q", rec.Code, rec.Body.String())
	}
}

// TestFullTournamentThroughAPI runs one tournament end to end against the
// fake and checks the success bodies, the replay flag and the reads.
func TestFullTournamentThroughAPI(t *testing.T) {
	f := newFake()
	h := newServer(t, f, nil)
	rec := do(h, "POST", "/v1/tournaments", "create", createBody)
	if rec.Code != 201 {
		t.Fatalf("create: %d %s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get(HeaderNode); got != "1" {
		t.Errorf("%s = %q", HeaderNode, got)
	}
	cr := responseOf(t, rec)
	if cr.Code != tournament.OK || cr.Replayed || cr.Slot != 1 || cr.Tournament == nil || cr.Tournament.Seed != 4242 || cr.Tournament.Status != tournament.Open {
		t.Fatalf("create response: %+v", cr)
	}
	var seed uint64
	for i, p := range []string{"p1", "p2", "p3"} {
		body := `{"player":{"id":"` + p + `","jurisdiction":"TR","age":30}}`
		rec = do(h, "POST", "/v1/tournaments/t1/entries", "join-"+p, body)
		if rec.Code != 201 {
			t.Fatalf("join %s: %d %s", p, rec.Code, rec.Body.String())
		}
		jr := responseOf(t, rec)
		if jr.Seed != 4242 || len(jr.Tournament.Entries) != i+1 {
			t.Errorf("join response: %+v", jr)
		}
		seed = jr.Seed
	}
	// A retried join replays with the original status and replayed=true.
	rec = do(h, "POST", "/v1/tournaments/t1/entries", "join-p1", `{"player":{"id":"p1","jurisdiction":"TR","age":30}}`)
	if rec.Code != 201 {
		t.Fatalf("replayed join: %d %s", rec.Code, rec.Body.String())
	}
	if jr := responseOf(t, rec); !jr.Replayed || jr.Slot != 2 || len(jr.Tournament.Entries) != 3 {
		t.Errorf("replayed join response: %+v", jr)
	}
	for i, p := range []string{"p1", "p2", "p3"} {
		body := `{"player":"` + p + `","score":` + string(rune('1'+i)) + `0,"deal_seed":` + itoa(seed) + `,"input_digest":"` + strings.Repeat("ab", 32) + `"}`
		if rec = do(h, "POST", "/v1/tournaments/t1/scores", "score-"+p, body); rec.Code != 200 {
			t.Fatalf("score %s: %d %s", p, rec.Code, rec.Body.String())
		}
	}
	if rec = do(h, "POST", "/v1/tournaments/t1/close", "close", `{}`); rec.Code != 200 {
		t.Fatalf("close: %d %s", rec.Code, rec.Body.String())
	}
	if rec = do(h, "POST", "/v1/tournaments/t1/close", "close-empty", ""); rec.Code != 409 {
		t.Fatalf("close again (empty body accepted, then not_open): %d %s", rec.Code, rec.Body.String())
	}
	rec = do(h, "POST", "/v1/tournaments/t1/settle", "settle", `{"exclusions":{"version":8,"jurisdictions":["XX","YY"]}}`)
	if rec.Code != 200 {
		t.Fatalf("settle: %d %s", rec.Code, rec.Body.String())
	}
	sr := responseOf(t, rec)
	if sr.Tournament.Status != tournament.Settled || len(sr.Tournament.Payouts) != 3 || sr.Tournament.Payouts[0].Player != "p3" {
		t.Errorf("settle response: %+v", sr.Tournament)
	}
	// Reads.
	rec = do(h, "GET", "/v1/tournaments/t1", "", "")
	if rec.Code != 200 {
		t.Fatalf("get: %d %s", rec.Code, rec.Body.String())
	}
	var tr TournamentResponse
	json.Unmarshal(rec.Body.Bytes(), &tr)
	// Slots: create 1, joins 2-4 (the replayed join took none), scores 5-7,
	// close 8, the rejected second close 9, settle 10.
	if tr.Tournament.ID != "t1" || !tr.Consistent || tr.AppliedSlot != 10 || rec.Header().Get(HeaderAppliedSlot) != "10" {
		t.Errorf("get response: id %s consistent %t applied %d, header %q", tr.Tournament.ID, tr.Consistent, tr.AppliedSlot, rec.Header().Get(HeaderAppliedSlot))
	}
	rec = do(h, "GET", "/v1/tournaments/t1?read=stale", "", "")
	json.Unmarshal(rec.Body.Bytes(), &tr)
	if rec.Code != 200 || tr.Consistent || rec.Header().Get(HeaderAppliedSlot) != "10" {
		t.Errorf("stale get: %d consistent %t applied %d", rec.Code, tr.Consistent, tr.AppliedSlot)
	}
	rec = do(h, "GET", "/v1/tournaments/t1/ledger", "", "")
	var lr LedgerResponse
	json.Unmarshal(rec.Body.Bytes(), &lr)
	if rec.Code != 200 || len(lr.Postings) != 7 || lr.Postings[3].Kind.String() != "rake" {
		t.Errorf("ledger: %d %+v", rec.Code, lr)
	}
	rec = do(h, "GET", "/v1/node", "", "")
	var nr NodeResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &nr); err != nil || nr.Self != 1 || nr.Role != replog.Leader {
		t.Errorf("node: %d %s (%v)", rec.Code, rec.Body.String(), err)
	}
	if got := f.reads; len(got) < 3 || !got[len(got)-3] || got[len(got)-2] {
		t.Errorf("read consistency flags: %v", got)
	}
}

func itoa(u uint64) string { return strconv.FormatUint(u, 10) }

func TestInFlightKeyAnswers409(t *testing.T) {
	f := newFake()
	f.block = make(chan struct{})
	f.entered = make(chan struct{}, 1)
	h := newServer(t, f, nil)
	first := make(chan *httptest.ResponseRecorder, 1)
	go func() { first <- do(h, "POST", "/v1/tournaments", "k", createBody) }()
	select {
	case <-f.entered: // the first request holds the key in flight
	case <-time.After(5 * time.Second):
		t.Fatal("first request never reached the backend")
	}
	rec := do(h, "POST", "/v1/tournaments", "k", createBody)
	if rec.Code != http.StatusConflict {
		t.Fatalf("concurrent retry = %d %s, want 409", rec.Code, rec.Body.String())
	}
	if p := problemOf(t, rec); p.Code != CodeInFlight {
		t.Fatalf("code %q, want %s", p.Code, CodeInFlight)
	}
	close(f.block)
	if rec := <-first; rec.Code != 201 {
		t.Errorf("first request = %d %s", rec.Code, rec.Body.String())
	}
	if rec := do(h, "POST", "/v1/tournaments", "k", createBody); rec.Code != 201 || !responseOf(t, rec).Replayed {
		t.Errorf("retry after completion = %d %s", rec.Code, rec.Body.String())
	}
}

// TestForwarding: a follower forwards once to the leader it knows, with the
// forwarded header and the key; a request that already carries the header
// is not forwarded again; an unknown leader answers 503 with Retry-After.
func TestForwarding(t *testing.T) {
	var got struct {
		sync.Mutex
		n    int
		key  string
		fwd  string
		path string
		body string
	}
	leader := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		got.Lock()
		got.n++
		got.key, got.fwd, got.path, got.body = r.Header.Get(HeaderIdempotencyKey), r.Header.Get(HeaderForwarded), r.URL.RequestURI(), string(b)
		got.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set(HeaderNode, "2")
		w.WriteHeader(http.StatusCreated)
		io.WriteString(w, `{"code":"ok","replayed":false,"slot":7}`)
	}))
	defer leader.Close()

	f := newFake()
	f.setStatus(replica.Status{Self: 1, Leader: 2, Role: replog.Follower})
	f.submitErr = replog.ErrNotLeader{Leader: 2}
	h := newServer(t, f, func(c *Config) { c.Peers = map[paxos.NodeID]string{1: "http://self", 2: leader.URL} })

	rec := do(h, "POST", "/v1/tournaments/t1/entries?x=1", "fwd-key", `{"player":{"id":"p","jurisdiction":"TR","age":30}}`)
	if rec.Code != 201 {
		t.Fatalf("forwarded response = %d %s", rec.Code, rec.Body.String())
	}
	if r := responseOf(t, rec); r.Slot != 7 {
		t.Errorf("forwarded body not passed through: %s", rec.Body.String())
	}
	if rec.Header().Get(HeaderNode) != "2" {
		t.Errorf("leader's node header not passed through: %q", rec.Header().Get(HeaderNode))
	}
	got.Lock()
	if got.n != 1 || got.key != "fwd-key" || got.fwd != "1" || got.path != "/v1/tournaments/t1/entries?x=1" || !strings.Contains(got.body, `"p"`) {
		t.Errorf("leader saw n=%d key=%q forwarded=%q path=%q body=%q", got.n, got.key, got.fwd, got.path, got.body)
	}
	got.Unlock()

	// Already forwarded: refuse instead of forwarding again.
	rec = do(h, "POST", "/v1/tournaments", "k2", createBody, HeaderForwarded, "1")
	if rec.Code != http.StatusServiceUnavailable || rec.Header().Get("Retry-After") != "1" {
		t.Fatalf("re-forward = %d, Retry-After %q", rec.Code, rec.Header().Get("Retry-After"))
	}
	if p := problemOf(t, rec); p.Code != CodeNotLeader || p.Leader != 2 {
		t.Errorf("problem = %+v", p)
	}
	got.Lock()
	if got.n != 1 {
		t.Errorf("leader received %d requests, want 1", got.n)
	}
	got.Unlock()

	// Consistent reads forward too; stale reads are answered locally.
	rec = do(h, "GET", "/v1/tournaments/t1", "", "")
	if rec.Code != 201 { // whatever the fake leader answers
		t.Errorf("forwarded read = %d %s", rec.Code, rec.Body.String())
	}
	rec = do(h, "GET", "/v1/tournaments/t1?read=stale", "", "")
	if rec.Code != 404 || rec.Header().Get(HeaderAppliedSlot) != "0" {
		t.Errorf("stale read on follower = %d, applied header %q", rec.Code, rec.Header().Get(HeaderAppliedSlot))
	}

	// Unknown leader: 503 with Retry-After, no forward.
	f.setStatus(replica.Status{Self: 1, Leader: 0, Role: replog.Follower})
	f.submitErr = replog.ErrNotLeader{}
	rec = do(h, "POST", "/v1/tournaments", "k3", createBody)
	if rec.Code != http.StatusServiceUnavailable || rec.Header().Get("Retry-After") != "1" {
		t.Fatalf("unknown leader = %d", rec.Code)
	}
	if p := problemOf(t, rec); p.Code != CodeNotLeader || p.Leader != 0 {
		t.Errorf("problem = %+v", p)
	}
	// Leadership lost after submit: forward using the status' leader.
	f.setStatus(replica.Status{Self: 1, Leader: 2, Role: replog.Follower})
	f.submitErr = replica.ErrLeadershipLost
	if rec = do(h, "POST", "/v1/tournaments", "k4", createBody); rec.Code != 201 {
		t.Errorf("leadership lost with a known leader = %d %s", rec.Code, rec.Body.String())
	}
	// Leader down: forward fails, 503 forward_failed.
	leader.Close()
	rec = do(h, "POST", "/v1/tournaments", "k5", createBody)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("dead leader = %d", rec.Code)
	}
	if p := problemOf(t, rec); p.Code != CodeForwardFailed {
		t.Errorf("problem = %+v", p)
	}
}

func TestSubmitTimeoutAnswers504(t *testing.T) {
	f := newFake()
	f.block = make(chan struct{})
	defer close(f.block)
	h := newServer(t, f, func(c *Config) { c.RequestTimeout = 20 * time.Millisecond })
	rec := do(h, "POST", "/v1/tournaments", "slow", createBody)
	if rec.Code != http.StatusGatewayTimeout {
		t.Fatalf("status %d, want 504: %s", rec.Code, rec.Body.String())
	}
	if p := problemOf(t, rec); p.Code != CodeOutcomeUnknown {
		t.Errorf("code %q", p.Code)
	}
}

func TestOtherBackendErrors(t *testing.T) {
	f := newFake()
	f.submitErr = replica.ErrStopped
	h := newServer(t, f, nil)
	rec := do(h, "POST", "/v1/tournaments", "k", createBody)
	if rec.Code != http.StatusServiceUnavailable || problemOf(t, rec).Code != CodeUnavailable {
		t.Errorf("stopped backend = %d %s", rec.Code, rec.Body.String())
	}
	f.submitErr = errors.New("boom")
	if rec := do(h, "POST", "/v1/tournaments", "k", createBody); rec.Code != http.StatusServiceUnavailable {
		t.Errorf("unknown error = %d", rec.Code)
	}
}

func TestDefaultSeedIsRandom(t *testing.T) {
	cfg := Config{}.withDefaults()
	a, b := cfg.Seed(), cfg.Seed()
	if a == b {
		t.Error("two default seeds are equal")
	}
	if cfg.RequestTimeout != 5*time.Second || cfg.MaxBody != 1<<20 || cfg.Client == nil || cfg.Now == nil {
		t.Errorf("defaults: %+v", cfg)
	}
}
