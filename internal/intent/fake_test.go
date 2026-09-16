package intent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/oguzhanozfe/paxos-arena/internal/game"
	"github.com/oguzhanozfe/paxos-arena/internal/ledger"
	"github.com/oguzhanozfe/paxos-arena/internal/paxos"
	"github.com/oguzhanozfe/paxos-arena/internal/replica"
	"github.com/oguzhanozfe/paxos-arena/internal/replog"
	"github.com/oguzhanozfe/paxos-arena/internal/session"
	"github.com/oguzhanozfe/paxos-arena/internal/tournament"
)

// fakeBackend applies commands to a real tournament.State on the caller's
// goroutine under a lock, so handler tests exercise real results without a
// log. submitErr, when set, is returned instead of applying; hold, when
// set, makes Submit wait until it is closed.
type fakeBackend struct {
	mu        sync.Mutex
	state     *tournament.State
	slot      paxos.Slot
	status    replica.Status
	submitErr error
	hold      chan struct{}
	holding   chan struct{}
	submits   int
	advanced  chan struct{}
}

func newFake() *fakeBackend {
	return &fakeBackend{
		state:    tournament.NewState(),
		status:   replica.Status{Self: 1, Leader: 1, Role: replog.Leader, Ready: true},
		advanced: make(chan struct{}),
	}
}

func (f *fakeBackend) Submit(ctx context.Context, cmd tournament.Command) (tournament.Result, error) {
	f.mu.Lock()
	f.submits++
	hold, holding := f.hold, f.holding
	f.mu.Unlock()
	if hold != nil {
		if holding != nil {
			holding <- struct{}{}
		}
		select {
		case <-hold:
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
	return f.applyLocked(cmd), nil
}

func (f *fakeBackend) applyLocked(cmd tournament.Command) tournament.Result {
	f.slot++
	res := f.state.Apply(f.slot, paxos.Ballot{Round: 1, Node: 1}, cmd)
	f.status.Applied = f.slot
	close(f.advanced)
	f.advanced = make(chan struct{})
	return res
}

// apply is an operator command applied directly.
func (f *fakeBackend) apply(t *testing.T, key string, op tournament.Op, now int64) tournament.Result {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	res := f.applyLocked(tournament.Command{Key: tournament.IdempotencyKey(key), ReceivedAt: now, Op: op})
	if res.Code != tournament.OK {
		t.Fatalf("%s: %s %s", tournament.OpName(op), res.Code, res.Detail)
	}
	return res
}

// skip advances the applied slot without a command.
func (f *fakeBackend) skip() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.slot++
	f.state.Skip(f.slot)
	f.status.Applied = f.slot
	close(f.advanced)
	f.advanced = make(chan struct{})
}

func (f *fakeBackend) Read(ctx context.Context, consistent bool, fn func(*tournament.State) error) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return fn(f.state)
}

func (f *fakeBackend) Status() replica.Status {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.status
}

func (f *fakeBackend) setStatus(fn func(*replica.Status)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	fn(&f.status)
}

func (f *fakeBackend) WaitApplied(ctx context.Context, after paxos.Slot) (paxos.Slot, error) {
	for {
		f.mu.Lock()
		applied, ch := f.status.Applied, f.advanced
		f.mu.Unlock()
		if applied > after {
			return applied, nil
		}
		select {
		case <-ch:
		case <-ctx.Done():
			return applied, ctx.Err()
		}
	}
}

const (
	testKeyHex    = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"
	testDealHex   = "202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f"
	testStartMs   = 1_789_200_000_000
	testDevice    = "9f86d081884c7d659a2feaa0c55ad015"
	testSecret    = "2c26b46b68ffc68ff99b453c1d30413413422d706483bfa0f98a5e886266e7ae"
	testPlayer    = "p-mzxw6ytboi4dqnbrgm2wqzlmn4"
	testTID       = "daily-2026-09-17"
	testOtherURL  = "http://n2.play.example"
	testSelfURL   = "http://n1.play.example"
	createTimeout = 5 * time.Second
)

// clock is a settable time source.
type clock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *clock) add(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

type harness struct {
	t       *testing.T
	f       *fakeBackend
	s       *Server
	h       http.Handler
	clock   *clock
	keys    session.Keyring
	players []tournament.PlayerID
}

func testKeyring(t *testing.T) session.Keyring {
	k, err := session.ParseKeyring("k1=" + testKeyHex)
	if err != nil {
		t.Fatal(err)
	}
	return k
}

func dealSecret() []byte {
	b := make([]byte, 32)
	fmt.Sscanf(testDealHex, "%x", &b)
	return b
}

// newHarness builds a server over a fake backend. Player ids are drawn from
// testPlayer, then p-2, p-3 and so on.
func newHarness(t *testing.T, mod func(*Config)) *harness {
	t.Helper()
	hs := &harness{t: t, f: newFake(), clock: &clock{now: time.UnixMilli(testStartMs)}, keys: testKeyring(t)}
	n := 0
	cfg := Config{
		Self:       1,
		PublicURLs: map[paxos.NodeID]string{1: testSelfURL, 2: testOtherURL},
		Keys:       hs.keys,
		DealSecret: dealSecret(),
		Limits:     NoLimits,
		Now:        hs.clock.Now,
		NewPlayerID: func() tournament.PlayerID {
			n++
			if n == 1 {
				return testPlayer
			}
			return tournament.PlayerID(fmt.Sprintf("p-%d", n))
		},
		MinSlotWait: 50 * time.Millisecond,
	}
	if mod != nil {
		mod(&cfg)
	}
	s, err := New(cfg, hs.f, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	hs.s, hs.h = s, s.Handler()
	return hs
}

// response is one recorded response.
type response struct {
	status int
	header http.Header
	body   []byte
}

func (r response) decode(t *testing.T, v any) {
	t.Helper()
	dec := json.NewDecoder(bytes.NewReader(r.body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		t.Fatalf("decode %T from %s: %v", v, r.body, err)
	}
}

func (r response) errorCode(t *testing.T) string {
	t.Helper()
	var e ErrorBody
	r.decode(t, &e)
	return e.Code
}

// do sends a request; headers are name, value pairs.
func (hs *harness) do(method, path, body string, headers ...string) response {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	rec := httptest.NewRecorder()
	hs.h.ServeHTTP(rec, req)
	return response{status: rec.Code, header: rec.Header(), body: rec.Body.Bytes()}
}

func bearer(tok string) []string { return []string{HeaderAuthorization, "Bearer " + tok} }

func keyed(key, tok string) []string {
	return append([]string{HeaderIdempotencyKey, key}, bearer(tok)...)
}

func (hs *harness) token(player, device string) string {
	now := hs.clock.Now().UnixMilli()
	tok, err := hs.keys.Sign(session.Claims{Player: player, Device: device, IssuedAtMs: now, ExpiresAtMs: now + time.Hour.Milliseconds()})
	if err != nil {
		hs.t.Fatal(err)
	}
	return tok
}

// session opens a session for device i (i = 0 is the Appendix A device).
func (hs *harness) session(i int) SessionResponse {
	hs.t.Helper()
	dev, secret := testDevice, testSecret
	if i > 0 {
		dev = fmt.Sprintf("%032x", i)
		secret = fmt.Sprintf("%064x", i)
	}
	body := fmt.Sprintf(`{"device_id":%q,"device_secret":%q,"jurisdiction":"TR","age":31}`, dev, secret)
	r := hs.do("POST", "/v1/session", body, HeaderIdempotencyKey, fmt.Sprintf("session-key-%08d", i))
	if r.status != http.StatusOK {
		hs.t.Fatalf("session %d: %d %s", i, r.status, r.body)
	}
	var out SessionResponse
	r.decode(hs.t, &out)
	return out
}

// createLadder creates a Ladder tournament through the operator path.
func (hs *harness) createLadder(tid string, minEntrants int) {
	hs.t.Helper()
	rules := tournament.Rules{
		EntryFee: 500, RakeBps: 1000, PrizeBps: []uint32{5000, 3000, 2000}, MinEntrants: minEntrants, MaxEntrants: 100,
		MaxScore: game.MaxTotalScore, MinAge: 18, TieBreak: tournament.EarliestSubmission,
		Exclusions: tournament.Exclusions{Version: 7, Jurisdictions: []string{"XX"}}, Game: game.LadderV1,
	}
	if minEntrants < 3 {
		rules.PrizeBps = []uint32{10000}
	}
	hs.f.apply(hs.t, "create-"+tid, tournament.CreateTournament{ID: tournament.TournamentID(tid), Seed: 1, Rules: rules}, hs.clock.Now().UnixMilli())
}

// checkWireShape decodes body into a fresh value of v's type with unknown
// fields refused, re-encodes it and requires the same bytes, and requires
// no null anywhere: every field present, lists never null.
func checkWireShape(t *testing.T, body []byte, v any) {
	t.Helper()
	if bytes.Contains(body, []byte("null")) {
		t.Errorf("body contains null: %s", body)
	}
	fresh := reflect.New(reflect.TypeOf(v)).Interface()
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(fresh); err != nil {
		t.Fatalf("decode %T: %v (%s)", v, err, body)
	}
	again, err := json.Marshal(fresh)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(again, body) {
		t.Errorf("%T re-encodes differently (a field is missing or out of order):\n%s\n%s", v, body, again)
	}
}

// view helpers for tests.
var _ = ledger.Money(0)
