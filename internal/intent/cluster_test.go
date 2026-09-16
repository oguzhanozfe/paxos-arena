package intent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/oguzhanozfe/paxos-arena/internal/game"
	"github.com/oguzhanozfe/paxos-arena/internal/paxos"
	"github.com/oguzhanozfe/paxos-arena/internal/replica"
	"github.com/oguzhanozfe/paxos-arena/internal/replog"
	"github.com/oguzhanozfe/paxos-arena/internal/session"
	"github.com/oguzhanozfe/paxos-arena/internal/tournament"
	"github.com/oguzhanozfe/paxos-arena/internal/transport"
)

// playNode is one replica with its play listener.
type playNode struct {
	id     paxos.NodeID
	runner *replica.Runner
	srv    *httptest.Server
	cancel context.CancelFunc
	done   chan struct{}
	dead   bool
}

type playCluster struct {
	t     *testing.T
	bus   *transport.Local
	nodes []*playNode
	keys  session.Keyring
}

// startPlayCluster runs n replicas on one in-memory bus, each with the play
// API on an httptest server.
func startPlayCluster(t *testing.T, n int) *playCluster {
	t.Helper()
	c := &playCluster{t: t, bus: transport.NewLocal(), keys: testKeyring(t)}
	ids := make([]paxos.NodeID, n)
	urls := make(map[paxos.NodeID]string, n)
	for i := range ids {
		ids[i] = paxos.NodeID(i + 1)
	}
	for _, id := range ids {
		srv := httptest.NewUnstartedServer(nil)
		urls[id] = "http://" + srv.Listener.Addr().String()
		c.nodes = append(c.nodes, &playNode{id: id, srv: srv})
	}
	for _, nd := range c.nodes {
		core, err := replica.NewCore(replog.DefaultConfig(nd.id, ids), replog.NewMemStore(), rand.New(rand.NewPCG(uint64(nd.id), uint64(time.Now().UnixNano()))))
		if err != nil {
			t.Fatal(err)
		}
		nd.runner = replica.NewRunner(core, c.bus.Send, nil)
		c.bus.Register(nd.id, nd.runner.Deliver)
		s, err := New(Config{Self: nd.id, PublicURLs: urls, Keys: c.keys, DealSecret: dealSecret(), Limits: NoLimits, RequestTimeout: 2 * time.Second}, nd.runner, nil)
		if err != nil {
			t.Fatal(err)
		}
		nd.srv.Config.Handler = s.Handler()
		nd.srv.Start()
		ctx, cancel := context.WithCancel(context.Background())
		nd.cancel, nd.done = cancel, make(chan struct{})
		go func(nd *playNode) {
			defer close(nd.done)
			nd.runner.Run(ctx)
		}(nd)
	}
	t.Cleanup(func() {
		for _, nd := range c.nodes {
			c.kill(nd)
		}
	})
	return c
}

func (c *playCluster) kill(nd *playNode) {
	if nd.dead {
		return
	}
	nd.dead = true
	c.bus.Unregister(nd.id)
	nd.cancel()
	<-nd.done
	nd.srv.CloseClientConnections()
	nd.srv.Close()
}

func (c *playCluster) leader() *playNode {
	c.t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		for _, nd := range c.nodes {
			if st := nd.runner.Status(); !nd.dead && st.Role == replog.Leader && st.Ready {
				return nd
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatal("no ready leader")
	return nil
}

// operator submits an operator command to whichever replica leads.
func (c *playCluster) operator(key string, op tournament.Op) tournament.Result {
	c.t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		nd := c.leader()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		res, err := nd.runner.Submit(ctx, tournament.Command{Key: tournament.IdempotencyKey(key), ReceivedAt: time.Now().UnixMilli(), Op: op})
		cancel()
		if err == nil {
			if res.Code != tournament.OK {
				c.t.Fatalf("%s: %s %s", key, res.Code, res.Detail)
			}
			return res
		}
		time.Sleep(20 * time.Millisecond)
	}
	c.t.Fatalf("%s: never applied", key)
	return tournament.Result{}
}

// playClient follows the client rules of section 9 closely enough for a
// test: one redirect followed then the leader cached, retryable answers
// retried with the same request, dead replicas skipped.
type playClient struct {
	t       *testing.T
	c       *playCluster
	http    *http.Client
	leader  string
	next    int
	token   string
	player  string
	seq     int64
	lastKey string
	keyN    int
}

func (c *playCluster) client(start int) *playClient {
	return &playClient{t: c.t, c: c, next: start, http: &http.Client{
		Timeout:       30 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}}
}

func origin(u string) string {
	if i := strings.Index(u, "/v1/"); i >= 0 {
		return u[:i]
	}
	return u
}

// send performs one request to completion: a definitive answer or a
// failure of the test after 30 seconds.
func (pc *playClient) send(method, path, key, body string) (int, http.Header, []byte) {
	pc.t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	base := pc.leader
	followed := false
	for attempt := 0; time.Now().Before(deadline); attempt++ {
		if base == "" {
			base = pc.c.nodes[pc.next%len(pc.c.nodes)].srv.URL
			pc.next++
		}
		req, _ := http.NewRequest(method, base+path, strings.NewReader(body))
		if key != "" {
			req.Header.Set(HeaderIdempotencyKey, key)
			req.Header.Set("Content-Type", "application/json")
		}
		if pc.token != "" {
			req.Header.Set(HeaderAuthorization, "Bearer "+pc.token)
		}
		resp, err := pc.http.Do(req)
		if err != nil {
			base, pc.leader = "", ""
			time.Sleep(20 * time.Millisecond)
			continue
		}
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		var e ErrorBody
		json.Unmarshal(raw, &e)
		switch {
		case resp.StatusCode == http.StatusTemporaryRedirect:
			loc := origin(resp.Header.Get(HeaderLocation))
			pc.leader = loc
			if !followed {
				base, followed = loc, true
				continue
			}
			followed = false
			time.Sleep(20 * time.Millisecond)
		case e.Retryable && resp.StatusCode != http.StatusUnauthorized:
			if e.Code == CodeNoLeader {
				pc.leader = resp.Header.Get(HeaderLeader)
				base = pc.leader
			}
			time.Sleep(time.Duration(20+attempt*10) * time.Millisecond)
		default:
			return resp.StatusCode, resp.Header, raw
		}
	}
	pc.t.Fatalf("%s %s: no definitive answer within 30 s", method, path)
	return 0, nil, nil
}

func (pc *playClient) session(device, secret string) SessionResponse {
	pc.t.Helper()
	pc.keyN++
	body := fmt.Sprintf(`{"device_id":%q,"device_secret":%q,"jurisdiction":"TR","age":30}`, device, secret)
	status, _, raw := pc.send("POST", "/v1/session", fmt.Sprintf("session-%s-%04d", device[:8], pc.keyN), body)
	if status != 200 {
		pc.t.Fatalf("session: %d %s", status, raw)
	}
	var s SessionResponse
	json.Unmarshal(raw, &s)
	pc.token, pc.player = s.SessionToken, s.PlayerID
	pc.seq = max(pc.seq, s.NextSeq-1)
	return s
}

// intent sends a sequenced intent with a fresh key and the next number and
// decodes a 2xx body into out.
func (pc *playClient) intent(path, extra string, want int, out any) (http.Header, []byte) {
	pc.t.Helper()
	pc.seq++
	pc.keyN++
	pc.lastKey = fmt.Sprintf("%s-%06d", pc.player[2:14], pc.keyN)
	body := fmt.Sprintf(`{"seq":%d%s}`, pc.seq, extra)
	status, hdr, raw := pc.send("POST", path, pc.lastKey, body)
	if status != want {
		pc.t.Fatalf("POST %s %s = %d %s", path, body, status, raw)
	}
	if out != nil {
		if err := json.Unmarshal(raw, out); err != nil {
			pc.t.Fatal(err)
		}
	}
	return hdr, raw
}

// playRound deals round and plays it greedily to its end; after move
// killAt (when not negative) it kills the leader and resends that move's
// request with the same key.
func (pc *playClient) playRound(tid string, round, killAt int) RoundView {
	pc.t.Helper()
	var r RoundResponse
	pc.intent(fmt.Sprintf("/v1/tournaments/%s/rounds/%d/deal", tid, round), "", 201, &r)
	deal := r.Round
	if deal.StockCount != 16 || deal.Seed != "" || len(deal.Columns) != 7 {
		pc.t.Fatalf("deal = %+v", deal)
	}
	v := deal
	for v.Status == "playing" {
		kind, col := "draw", -1
		if len(v.PlayableColumns) > 0 {
			kind, col = "play", int(v.PlayableColumns[0])
		}
		path := fmt.Sprintf("/v1/tournaments/%s/rounds/%d/moves", tid, round)
		extra := fmt.Sprintf(`,"move_index":%d,"kind":%q,"column":%d`, v.MoveIndex, kind, col)
		var mr RoundResponse
		hdr, raw := pc.intent(path, extra, 200, &mr)
		if int(v.MoveIndex) == killAt {
			leader := pc.c.leader()
			pc.t.Logf("killing leader node %d after move %d", leader.id, killAt)
			pc.c.kill(leader)
			body := fmt.Sprintf(`{"seq":%d%s}`, pc.seq, extra)
			status, hdr2, raw2 := pc.send("POST", path, pc.lastKey, body)
			if status != 200 || !bytes.Equal(raw2, bytes.Replace(raw, []byte(`"replayed":false`), []byte(`"replayed":true`), 1)) ||
				hdr2.Get(HeaderSlot) != hdr.Get(HeaderSlot) {
				pc.t.Fatalf("resend after the leader change = %d %s\nfirst %s", status, raw2, raw)
			}
			// The same number under a new key is stale on the new leader.
			pc.keyN++
			status, hdr3, raw3 := pc.send("POST", path, fmt.Sprintf("stale-%s-%04d", pc.player[2:10], pc.keyN), body)
			if status != 409 || !strings.Contains(string(raw3), string(tournament.StaleSeq)) || hdr3.Get(HeaderNextSeq) != fmt.Sprint(pc.seq+1) {
				pc.t.Fatalf("stale resend = %d %s", status, raw3)
			}
		}
		v = mr.Round
	}
	// Audit the revealed seed: commitment, deal and final board.
	var seed game.Seed
	if err := seed.UnmarshalText([]byte(v.Seed)); err != nil || game.Commit(seed).String() != deal.Commitment {
		pc.t.Fatalf("round %d: seed %q does not match commitment %s", round, v.Seed, deal.Commitment)
	}
	board := game.Deal(seed)
	for c := 0; c < game.ColumnCount; c++ {
		for i, card := range board.Columns[c] {
			if deal.Columns[c].Cards[i] != card.String() {
				pc.pcFatal("round %d: the revealed seed deals %s at column %d, the deal showed %s", round, card, c, deal.Columns[c].Cards[i])
			}
		}
	}
	if board.WasteCard().String() != deal.WasteTop {
		pc.pcFatal("round %d: waste differs", round)
	}
	return v
}

func (pc *playClient) pcFatal(format string, args ...any) {
	pc.t.Helper()
	pc.t.Fatalf(format, args...)
}

// TestPlayOverHTTPCluster runs whole entries through the play API of a
// three-replica cluster: sessions through a follower's redirect, joins,
// three greedy rounds with the leader killed in the middle of one and the
// move resent to the new leader, a stale resend, finishes, reads with
// min_slot, close and settle by the operator, claims with a replay and a
// second claim, the events stream, and identical state on the survivors.
func TestPlayOverHTTPCluster(t *testing.T) {
	if testing.Short() {
		t.Skip("cluster test")
	}
	c := startPlayCluster(t, 3)
	first := c.leader()
	const tid = "cup-2026-09-17"
	c.operator("create-cup", tournament.CreateTournament{ID: tid, Seed: 1, Rules: tournament.Rules{
		EntryFee: 500, RakeBps: 1000, PrizeBps: []uint32{6000, 4000}, MinEntrants: 2, MaxEntrants: 10, MaxScore: game.MaxTotalScore,
		MinAge: 18, TieBreak: tournament.EarliestSubmission, Game: game.LadderV1,
	}})
	players := make([]*playClient, 3)
	for i := range players {
		// Start each client on a follower so the first request is redirected.
		start := 0
		for j, nd := range c.nodes {
			if nd != first {
				start = j
			}
		}
		pc := c.client(start)
		s := pc.session(fmt.Sprintf("%032x", i+1), fmt.Sprintf("%064x", i+1))
		if !s.NewPlayer || pc.leader != first.srv.URL {
			t.Fatalf("player %d session = %+v via %s", i, s, pc.leader)
		}
		var list TournamentListResponse
		status, _, raw := pc.send("GET", "/v1/tournaments", "", "")
		json.Unmarshal(raw, &list)
		if status != 200 || len(list.Tournaments) != 1 || !list.Tournaments[0].Eligible {
			t.Fatalf("list = %d %s", status, raw)
		}
		var jr JoinResponse
		pc.intent("/v1/tournaments/"+tid+"/join", "", 201, &jr)
		if jr.JoinSeq != int32(i+1) {
			t.Fatalf("join = %+v", jr)
		}
		players[i] = pc
	}
	// Player 0 plays three rounds; the leader dies during round 2.
	totals := map[string]int64{}
	for round := 1; round <= 3; round++ {
		kill := -1
		if round == 2 {
			kill = 5
		}
		v := players[0].playRound(tid, round, kill)
		totals[players[0].player] += v.Score
		// Finishing again is idempotent.
		var fr RoundResponse
		players[0].intent(fmt.Sprintf("/v1/tournaments/%s/rounds/%d/finish", tid, round), "", 200, &fr)
		if fr.Round.Score != v.Score || fr.Round.Seed != v.Seed {
			t.Fatalf("finish = %+v", fr.Round)
		}
	}
	// Players 1 and 2 play one round each; the close scores their entries.
	for _, pc := range players[1:] {
		v := pc.playRound(tid, 1, -1)
		totals[pc.player] += v.Score
	}
	// A read on any survivor with min_slot of the last intent.
	for _, nd := range c.nodes {
		if nd.dead {
			continue
		}
		pc := players[1]
		req, _ := http.NewRequest("GET", fmt.Sprintf("%s/v1/tournaments/%s/rounds/1?min_slot=%d", nd.srv.URL, tid, c.leader().runner.Status().Applied), nil)
		req.Header.Set(HeaderAuthorization, "Bearer "+pc.token)
		resp, err := pc.http.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != 200 && resp.StatusCode != 307 {
			t.Errorf("min_slot read on node %d = %d", nd.id, resp.StatusCode)
		}
	}
	c.operator("close-cup", tournament.Close{Tournament: tid})
	c.operator("settle-cup", tournament.Settle{Tournament: tid})

	var board LeaderboardResponse
	status, _, raw := players[0].send("GET", "/v1/tournaments/"+tid+"/leaderboard", "", "")
	if err := json.Unmarshal(raw, &board); err != nil || status != 200 || !board.Final || len(board.Rows) != 3 {
		t.Fatalf("leaderboard = %d %s", status, raw)
	}
	var paid int64
	for _, row := range board.Rows {
		if row.TotalScore != totals[row.PlayerID] || !row.Scored {
			t.Errorf("row %+v, total %d", row, totals[row.PlayerID])
		}
		paid += row.Amount
	}
	if paid != 1350 {
		t.Errorf("payouts sum to %d, want the pool 1350", paid)
	}
	for _, pc := range players {
		var me LeaderboardRow
		for _, row := range board.Rows {
			if row.PlayerID == pc.player {
				me = row
			}
		}
		if me.Amount == 0 {
			if _, raw := pc.intent("/v1/tournaments/"+tid+"/payout/claim", "", 409, nil); !strings.Contains(string(raw), "no_payout") {
				t.Errorf("claim without payout = %s", raw)
			}
			continue
		}
		var cr ClaimResponse
		_, raw := pc.intent("/v1/tournaments/"+tid+"/payout/claim", "", 200, &cr)
		if cr.Amount != me.Amount {
			t.Errorf("claim = %+v, row %+v", cr, me)
		}
		status, _, again := pc.send("POST", "/v1/tournaments/"+tid+"/payout/claim", pc.lastKey, fmt.Sprintf(`{"seq":%d}`, pc.seq))
		if status != 200 || !bytes.Equal(again, bytes.Replace(raw, []byte(`"replayed":false`), []byte(`"replayed":true`), 1)) {
			t.Errorf("claim replay = %d %s", status, again)
		}
		if _, raw := pc.intent("/v1/tournaments/"+tid+"/payout/claim", "", 409, nil); !strings.Contains(string(raw), "already_claimed") {
			t.Errorf("second claim = %s", raw)
		}
	}
	// Player 0's event stream from the start.
	var types []string
	cursor := int64(0)
	for {
		var ev EventsResponse
		_, _, raw := players[0].send("GET", fmt.Sprintf("/v1/events?cursor=%d&wait_ms=0", cursor), "", "")
		json.Unmarshal(raw, &ev)
		for _, e := range ev.Events {
			if e.Type != "leaderboard_changed" {
				types = append(types, e.Type)
			}
		}
		if ev.Cursor < cursor {
			t.Fatalf("cursor went back from %d to %d", cursor, ev.Cursor)
		}
		cursor = ev.Cursor
		if !ev.HasMore {
			break
		}
	}
	got := strings.Join(types, " ")
	for _, want := range []string{
		"round_started round_finished round_started round_finished round_started round_finished entry_scored",
		"tournament_status tournament_status",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("events %q lack %q", got, want)
		}
	}
	// The survivors converge on one state.
	deadline := time.Now().Add(10 * time.Second)
	for {
		var hashes []string
		var applied []paxos.Slot
		for _, nd := range c.nodes {
			if !nd.dead {
				st := nd.runner.Status()
				hashes = append(hashes, st.StateHash.String())
				applied = append(applied, st.Applied)
			}
		}
		if len(hashes) == 2 && hashes[0] == hashes[1] && applied[0] == applied[1] {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("survivors differ: %v %v", hashes, applied)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
