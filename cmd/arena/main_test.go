package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/oguzhanozfe/paxos-arena/internal/api"
	"github.com/oguzhanozfe/paxos-arena/internal/ledger"
	"github.com/oguzhanozfe/paxos-arena/internal/paxos"
	"github.com/oguzhanozfe/paxos-arena/internal/replica"
	"github.com/oguzhanozfe/paxos-arena/internal/replog"
	"github.com/oguzhanozfe/paxos-arena/internal/replog/wal"
	"github.com/oguzhanozfe/paxos-arena/internal/tournament"
	"github.com/oguzhanozfe/paxos-arena/internal/transport"
)

// testNode is one replica over the HTTP transport, stoppable on its own.
type testNode struct {
	id     paxos.NodeID
	url    string
	cancel context.CancelFunc
	done   chan struct{}
}

// startNodes runs n replicas on loopback listeners with the HTTP transport
// and returns them. Each can be stopped independently with stop.
func startNodes(t *testing.T, n int) []*testNode {
	t.Helper()
	ids := make([]paxos.NodeID, n)
	listeners := make([]net.Listener, n)
	urls := make(map[paxos.NodeID]string, n)
	for i := range ids {
		ids[i] = paxos.NodeID(i + 1)
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners[i] = ln
		urls[ids[i]] = "http://" + ln.Addr().String()
	}
	logger := slog.New(slog.DiscardHandler)
	var nodes []*testNode
	for i, id := range ids {
		core, err := replica.NewCore(replog.DefaultConfig(id, ids), replog.NewMemStore(), seededRNG(id))
		if err != nil {
			t.Fatal(err)
		}
		var tr *transport.HTTP
		runner := replica.NewRunner(core, func(env replog.Envelope) { tr.Send(env) }, logger)
		tr = transport.NewHTTP(id, urls, runner.Deliver, nil, logger)
		mux := http.NewServeMux()
		mux.Handle("POST "+transport.Path, tr.Handler())
		mux.Handle("/", api.New(api.Config{Self: id, Peers: urls, RequestTimeout: 3 * time.Second}, runner, logger).Handler())
		srv := newHTTPServer(mux, logger)
		ctx, cancel := context.WithCancel(context.Background())
		nd := &testNode{id: id, url: urls[id], cancel: cancel, done: make(chan struct{})}
		ln := listeners[i]
		go func() {
			defer close(nd.done)
			var wg sync.WaitGroup
			wg.Add(3)
			go func() { defer wg.Done(); tr.Run(ctx) }()
			go func() { defer wg.Done(); serve(ctx, srv, ln) }()
			go func() { defer wg.Done(); runner.Run(ctx) }()
			wg.Wait()
		}()
		nodes = append(nodes, nd)
		t.Cleanup(nd.stop)
	}
	return nodes
}

func (nd *testNode) stop() {
	nd.cancel()
	<-nd.done
}

// call sends one request and decodes the JSON body into out (when non-nil).
func call(t *testing.T, method, url, key, body string, out any) (int, http.Header) {
	t.Helper()
	req, err := http.NewRequest(method, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if key != "" {
		req.Header.Set(api.HeaderIdempotencyKey, key)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if out != nil {
		if err := json.Unmarshal(b, out); err != nil {
			t.Fatalf("%s %s: status %d, body %q: %v", method, url, resp.StatusCode, b, err)
		}
	}
	return resp.StatusCode, resp.Header
}

// waitLeader polls /v1/node on the live nodes until one reports itself a
// ready leader, and returns it.
func waitLeader(t *testing.T, nodes []*testNode) *testNode {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		for _, nd := range nodes {
			var st api.NodeResponse
			if code, _ := call(t, "GET", nd.url+"/v1/node", "", "", &st); code == 200 && st.Role == replog.Leader && st.Ready {
				return nd
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("no ready leader within 15s")
	return nil
}

// TestThreeNodesOverHTTP runs a full tournament through a follower (so
// every command is forwarded), stops the leader between Close and Settle,
// settles through the remaining two, and checks D1 to D3 through the API on
// both survivors.
func TestThreeNodesOverHTTP(t *testing.T) {
	nodes := startNodes(t, 3)
	leader := waitLeader(t, nodes)
	var follower *testNode
	for _, nd := range nodes {
		if nd != leader {
			follower = nd
			break
		}
	}
	base := follower.url + "/v1/tournaments"
	post := func(path, key, body string, want int) api.CommandResponse {
		t.Helper()
		var out api.CommandResponse
		code, hdr := call(t, "POST", base+path, key, body, &out)
		if code != want {
			var p api.Problem
			call(t, "POST", base+path, key, body, &p)
			t.Fatalf("POST %s: status %d, want %d (problem %+v)", path, code, want, p)
		}
		if got := hdr.Get(api.HeaderNode); got != fmt.Sprint(leader.id) && want < 300 {
			// A forwarded response carries the leader's node header.
			t.Logf("POST %s answered by node %s (leader %d)", path, got, leader.id)
		}
		return out
	}
	rules := `{"id":"t1","rules":{"entry_fee":500,"rake_bps":1000,"prize_bps":[5000,3000,2000],"min_entrants":3,"max_entrants":100,"max_score":100000,"min_age":18,"tie_break":"earliest_submission","exclusions":{"version":7,"jurisdictions":["XX"]}}}`
	created := post("", "create-t1", rules, 201)
	if created.Tournament == nil || created.Tournament.Status != tournament.Open {
		t.Fatalf("create response: %+v", created)
	}
	var seed uint64
	for _, p := range []string{"p1", "p2", "p3", "p4"} {
		jr := post("/t1/entries", "join-"+p, `{"player":{"id":"`+p+`","jurisdiction":"TR","age":31}}`, 201)
		if jr.Seed == 0 {
			t.Fatalf("join %s returned no seed: %+v", p, jr)
		}
		seed = jr.Seed
	}
	if code, _ := call(t, "POST", base+"/t1/entries", "join-px", `{"player":{"id":"px","jurisdiction":"XX","age":31}}`, nil); code != 409 {
		t.Errorf("excluded join = %d, want 409", code)
	}
	for i, p := range []string{"p1", "p2", "p3", "p4"} {
		post("/t1/scores", "score-"+p, fmt.Sprintf(`{"player":"%s","score":%d,"deal_seed":%d}`, p, (i+1)*1000, seed), 200)
	}
	closed := post("/t1/close", "close-t1", `{}`, 200)
	if closed.Tournament.Status != tournament.Closed || closed.Tournament.Pool != 1800 || closed.Tournament.Rake != 200 {
		t.Fatalf("close response: %+v", closed.Tournament)
	}

	// Stop the leader between Close and Settle.
	leader.stop()
	var survivors []*testNode
	for _, nd := range nodes {
		if nd != leader {
			survivors = append(survivors, nd)
		}
	}
	newLeader := waitLeader(t, survivors)
	t.Logf("leader %d stopped; node %d took over", leader.id, newLeader.id)
	var other *testNode
	for _, nd := range survivors {
		if nd != newLeader {
			other = nd
		}
	}
	base = other.url + "/v1/tournaments"
	settleBody := `{"exclusions":{"version":8,"jurisdictions":["XX","YY"]}}`
	settled := post("/t1/settle", "settle-t1", settleBody, 200)
	if settled.Tournament.Status != tournament.Settled || len(settled.Tournament.Payouts) != 3 {
		t.Fatalf("settle response: %+v", settled.Tournament)
	}
	// The retry with the same key replays; a new key is rejected.
	again := post("/t1/settle", "settle-t1", settleBody, 200)
	if !again.Replayed || again.Slot != settled.Slot {
		t.Errorf("settle retry = %+v, want a replay of slot %d", again, settled.Slot)
	}
	var p api.Problem
	if code, _ := call(t, "POST", base+"/t1/settle", "settle-t1-again", settleBody, &p); code != 409 || p.Code != string(tournament.NotClosed) {
		t.Errorf("second settle = %d %+v, want 409 not_closed", code, p)
	}

	// D1 to D3 through the API on both survivors, with identical records.
	var records []api.TournamentResponse
	for _, nd := range survivors {
		var tr api.TournamentResponse
		deadline := time.Now().Add(5 * time.Second)
		for {
			code, _ := call(t, "GET", nd.url+"/v1/tournaments/t1?read=stale", "", "", &tr)
			if code == 200 && tr.Tournament.Status == tournament.Settled {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("node %d never applied the settlement (status %d, %+v)", nd.id, code, tr.Tournament)
			}
			time.Sleep(20 * time.Millisecond)
		}
		records = append(records, tr)
		tt := tr.Tournament
		if tt.Fees != 2000 || tt.Rake != 200 || tt.Pool != 1800 || tt.Pool != tt.Fees-tt.Rake {
			t.Errorf("node %d D1: fees %d rake %d pool %d", nd.id, tt.Fees, tt.Rake, tt.Pool)
		}
		var sum ledger.Money
		for _, p := range tt.Payouts {
			sum += p.Amount
		}
		if sum != tt.Pool || tt.Payouts[0].Player != "p4" || tt.Payouts[0].Amount != 900 {
			t.Errorf("node %d D2: payouts %+v", nd.id, tt.Payouts)
		}
		var lr api.LedgerResponse
		if code, _ := call(t, "GET", nd.url+"/v1/tournaments/t1/ledger?read=stale", "", "", &lr); code != 200 {
			t.Fatalf("node %d ledger: %d", nd.id, code)
		}
		keys := make(map[ledger.PostingKey]int)
		var pool ledger.Money
		for _, p := range lr.Postings {
			keys[p.Key]++
			if p.Credit == ledger.PoolAccount("t1") {
				pool += p.Amount
			}
			if p.Debit == ledger.PoolAccount("t1") {
				pool -= p.Amount
			}
		}
		if pool != 0 {
			t.Errorf("node %d D1: pool account nets to %d", nd.id, pool)
		}
		for _, p := range tt.Payouts {
			if keys[p.Key] != 1 {
				t.Errorf("node %d D2: payout key %s has %d postings", nd.id, p.Key, keys[p.Key])
			}
		}
		if len(lr.Postings) != 4+1+3 {
			t.Errorf("node %d: %d postings, want 4 fees, rake and 3 prizes", nd.id, len(lr.Postings))
		}
	}
	a := tournament.EncodeTournament(records[0].Tournament)
	b := tournament.EncodeTournament(records[1].Tournament)
	if !bytes.Equal(a, b) {
		t.Errorf("the two survivors hold different records:\n%s\n%s", a, b)
	}
	// D3: nothing changes a settled tournament.
	if code, _ := call(t, "POST", base+"/t1/entries", "join-late", `{"player":{"id":"p9","jurisdiction":"TR","age":31}}`, nil); code != 409 {
		t.Errorf("join after settle = %d, want 409", code)
	}
	var after api.TournamentResponse
	call(t, "GET", other.url+"/v1/tournaments/t1", "", "", &after)
	if !bytes.Equal(tournament.EncodeTournament(after.Tournament), a) {
		t.Error("the settled record changed after a rejected command")
	}
	if !after.Consistent {
		t.Error("a consistent read was answered as stale")
	}
}

// syncWriter is a goroutine-safe buffer for run's stdout.
type syncWriter struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (w *syncWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.b.Write(p)
}

func (w *syncWriter) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.b.String()
}

// TestRunClusterPrintsInstructions starts the one-process cluster on free
// ports, checks the printed instructions and one round trip, and stops it.
func TestRunClusterPrintsInstructions(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stdout := &syncWriter{}
	errc := make(chan error, 1)
	go func() {
		errc <- run(ctx, []string{"-nodes", "3", "-listen", "127.0.0.1:0", "-log-level", "error"}, stdout, io.Discard)
	}()
	deadline := time.Now().Add(10 * time.Second)
	for !strings.Contains(stdout.String(), "Ctrl-C") {
		if time.Now().After(deadline) {
			t.Fatalf("instructions not printed:\n%s", stdout.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
	out := stdout.String()
	for _, want := range []string{"node 1  http://127.0.0.1:", "node 3  http://127.0.0.1:", "curl -s -X POST $BASE/v1/tournaments", "Idempotency-Key", "/settle", "read=stale"} {
		if !strings.Contains(out, want) {
			t.Errorf("instructions lack %q:\n%s", want, out)
		}
	}
	var base string
	for _, line := range strings.Split(out, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "BASE=") {
			base = strings.TrimPrefix(strings.TrimSpace(line), "BASE=")
		}
	}
	if base == "" {
		t.Fatalf("no BASE line:\n%s", out)
	}
	resp, err := http.Get(base + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Errorf("healthz = %d", resp.StatusCode)
	}
	cancel()
	select {
	case err := <-errc:
		if err != nil {
			t.Errorf("run returned %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("run did not stop after cancel")
	}
}

func TestRunRejectsBadArguments(t *testing.T) {
	dir := t.TempDir()
	log := filepath.Join(dir, "node.wal")
	other := filepath.Join(dir, "other.wal")
	f, err := wal.Open(other)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Bind(2, []paxos.NodeID{1, 2}); err != nil {
		t.Fatal(err)
	}
	f.Close()
	cases := []struct {
		args []string
		want string
	}{
		{[]string{"-bogus"}, "flag provided but not defined"},
		{[]string{"-nodes", "0", "-listen", "127.0.0.1:0"}, "-nodes"},
		{[]string{"-listen", "nohostport"}, "-listen"},
		{[]string{"-id", "1"}, "-id requires -peers"},
		{[]string{"-id", "0", "-wal", log, "-peers", "1=http://127.0.0.1:1"}, "-id is required"},
		{[]string{"-id", "2", "-wal", log, "-peers", "1=http://127.0.0.1:1"}, "not in -peers"},
		{[]string{"-id", "1", "-wal", log, "-peers", "1=127.0.0.1:1"}, "base URL"},
		{[]string{"-id", "1", "-wal", log, "-peers", "x=http://127.0.0.1:1"}, "bad node id"},
		{[]string{"-id", "1", "-wal", log, "-peers", "1=http://a,1=http://b"}, "listed twice"},
		{[]string{"-log-level", "loud"}, "-log-level"},
		// One replica per process needs its durable state.
		{[]string{"-id", "1", "-listen", "127.0.0.1:0", "-peers", "1=http://127.0.0.1:1"}, "-wal is required"},
		{[]string{"-nodes", "1", "-listen", "127.0.0.1:0", "-wal", log}, "-wal requires -id and -peers"},
		// A log file written by another replica is refused.
		{[]string{"-id", "1", "-listen", "127.0.0.1:0", "-wal", other, "-peers", "1=http://127.0.0.1:1,2=http://127.0.0.1:2"}, "belongs to node 2"},
	}
	for _, tc := range cases {
		var stdout, stderr bytes.Buffer
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		err := run(ctx, tc.args, &stdout, &stderr)
		cancel()
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("run(%v) = %v, want an error containing %q", tc.args, err, tc.want)
		}
	}
}

func TestParsePeers(t *testing.T) {
	urls, ids, err := parsePeers(" 2=http://b:8082/ , 1=http://a:8081")
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 2 || ids[0] != 1 || ids[1] != 2 || urls[2] != "http://b:8082" {
		t.Errorf("parsePeers = %v %v", urls, ids)
	}
}
