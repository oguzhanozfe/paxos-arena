package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/oguzhanozfe/paxos-arena/internal/api"
	"github.com/oguzhanozfe/paxos-arena/internal/intent"
	"github.com/oguzhanozfe/paxos-arena/internal/paxos"
	"github.com/oguzhanozfe/paxos-arena/internal/replog"
	"github.com/oguzhanozfe/paxos-arena/internal/session"
)

const (
	testSessionKeys = "k1=000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"
	testDealSecret  = "202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f"
)

func TestPlayRefusesToStartWithoutSecrets(t *testing.T) {
	dir := t.TempDir()
	keys := filepath.Join(dir, "keys")
	os.WriteFile(keys, []byte("# signing key first\n"+testSessionKeys+"\n"), 0o600)
	weak := filepath.Join(dir, "weak")
	os.WriteFile(weak, []byte("k1=00ff\n"), 0o600)
	deal := filepath.Join(dir, "deal")
	os.WriteFile(deal, []byte(testDealSecret+"\n"), 0o600)
	short := filepath.Join(dir, "short")
	os.WriteFile(short, []byte("00ff"), 0o600)
	t.Setenv(session.EnvKeys, "")
	t.Setenv(intent.EnvDealSecret, "")
	base := []string{"-nodes", "1", "-listen", "127.0.0.1:0", "-play-listen", "127.0.0.1:0"}
	cases := []struct {
		env  map[string]string
		args []string
		want string
	}{
		{nil, base, session.EnvKeys},
		{nil, append(base, "-session-keys-file", keys), intent.EnvDealSecret},
		{map[string]string{session.EnvKeys: testSessionKeys}, base, intent.EnvDealSecret},
		{nil, append(base, "-session-keys-file", weak, "-deal-secret-file", deal), "shorter than 32 bytes"},
		{nil, append(base, "-session-keys-file", keys, "-deal-secret-file", short), "at least 32 bytes"},
		{map[string]string{session.EnvKeys: "k1", intent.EnvDealSecret: testDealSecret}, base, "id=hex"},
		{nil, append(base, "-session-keys-file", filepath.Join(dir, "missing"), "-deal-secret-file", deal), "-session-keys-file"},
		{nil, append(base, "-session-keys-file", keys, "-deal-secret-file", deal, "-session-ttl", "25h"), "-session-ttl"},
		{nil, append(base, "-session-keys-file", keys, "-deal-secret-file", deal, "-play-trusted-proxies", "proxy.local"), "not an IP address"},
		{nil, []string{"-nodes", "1", "-listen", "127.0.0.1:0", "-play-urls", "1=http://a"}, "require -play-listen"},
		{nil, []string{"-id", "1", "-listen", "127.0.0.1:0", "-wal", filepath.Join(dir, "n.wal"), "-peers", "1=http://127.0.0.1:1,2=http://127.0.0.1:2",
			"-play-listen", "127.0.0.1:0", "-session-keys-file", keys, "-deal-secret-file", deal}, "-play-urls must name every replica"},
	}
	for i, tc := range cases {
		for k, v := range tc.env {
			t.Setenv(k, v)
		}
		var stdout, stderr bytes.Buffer
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		err := run(ctx, tc.args, &stdout, &stderr)
		cancel()
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("case %d: run(%v) = %v, want an error containing %q", i, tc.args, err, tc.want)
		}
		if err != nil && (strings.Contains(err.Error(), "0102030405") || strings.Contains(err.Error(), "2122232425")) {
			t.Errorf("case %d: the error shows key material: %v", i, err)
		}
		for k := range tc.env {
			t.Setenv(k, "")
		}
	}
}

// playHTTP is a small play client: it follows one 307, caches the leader,
// and retries retryable answers and refused connections with the same
// request.
type playHTTP struct {
	t      *testing.T
	bases  []string
	next   int
	leader string
	token  string
	client *http.Client
}

func newPlayHTTP(t *testing.T, bases ...string) *playHTTP {
	return &playHTTP{t: t, bases: bases, client: &http.Client{Timeout: 20 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}
}

func (c *playHTTP) do(method, path, key, body string, out any) int {
	c.t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	base := c.leader
	followed := false
	for time.Now().Before(deadline) {
		if base == "" {
			base = c.bases[c.next%len(c.bases)]
			c.next++
		}
		req, _ := http.NewRequest(method, base+path, strings.NewReader(body))
		if key != "" {
			req.Header.Set(intent.HeaderIdempotencyKey, key)
		}
		if c.token != "" {
			req.Header.Set(intent.HeaderAuthorization, "Bearer "+c.token)
		}
		resp, err := c.client.Do(req)
		if err != nil {
			base, c.leader = "", ""
			time.Sleep(30 * time.Millisecond)
			continue
		}
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		var e intent.ErrorBody
		json.Unmarshal(raw, &e)
		if resp.StatusCode == http.StatusTemporaryRedirect {
			loc := resp.Header.Get(intent.HeaderLocation)
			c.leader = strings.TrimSuffix(loc, path)
			if !followed {
				base, followed = c.leader, true
				continue
			}
			followed = false
			time.Sleep(30 * time.Millisecond)
			continue
		}
		if e.Retryable && resp.StatusCode != http.StatusUnauthorized {
			if e.Code == intent.CodeNoLeader {
				base, c.leader = "", ""
			}
			time.Sleep(50 * time.Millisecond)
			continue
		}
		if out != nil {
			if err := json.Unmarshal(raw, out); err != nil {
				c.t.Fatalf("%s %s: %d %s", method, path, resp.StatusCode, raw)
			}
		}
		return resp.StatusCode
	}
	c.t.Fatalf("%s %s: no answer within 30 s", method, path)
	return 0
}

// TestPlayThreeProcesses starts three replicas as `arena -id -peers -wal
// -play-listen` would run, plays a round through a follower's redirect,
// kills the leader's process in the middle of the round, finishes the round
// through the new leader, and resends a move with its key.
func TestPlayThreeProcesses(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-replica test")
	}
	dir := t.TempDir()
	keys := filepath.Join(dir, "session.keys")
	deal := filepath.Join(dir, "deal.secret")
	os.WriteFile(keys, []byte(testSessionKeys+"\n"), 0o600)
	os.WriteFile(deal, []byte(testDealSecret+"\n"), 0o600)
	addrs := advFreeAddrs(t, 6)
	var peers, playURLs []string
	for i := 0; i < 3; i++ {
		peers = append(peers, fmt.Sprintf("%d=http://%s", i+1, addrs[i]))
		playURLs = append(playURLs, fmt.Sprintf("%d=http://%s", i+1, addrs[3+i]))
	}
	type proc struct {
		*advReplica
		play string
	}
	var procs []*proc
	for i := 0; i < 3; i++ {
		id := paxos.NodeID(i + 1)
		ctx, cancel := context.WithCancel(context.Background())
		p := &advReplica{id: id, addr: addrs[i], url: "http://" + addrs[i], cancel: cancel, errc: make(chan error, 1)}
		args := []string{"-id", fmt.Sprint(id), "-listen", addrs[i], "-peers", strings.Join(peers, ","), "-log-level", "error",
			"-wal", filepath.Join(dir, fmt.Sprintf("node%d.wal", id)),
			"-play-listen", addrs[3+i], "-play-urls", strings.Join(playURLs, ","),
			"-session-keys-file", keys, "-deal-secret-file", deal}
		go func() { p.errc <- run(ctx, args, io.Discard, io.Discard) }()
		t.Cleanup(p.stop)
		procs = append(procs, &proc{advReplica: p, play: "http://" + addrs[3+i]})
	}
	for _, p := range procs {
		deadline := time.Now().Add(10 * time.Second)
		for advGet(p.url+"/healthz", nil) != http.StatusOK || advGet(p.play+"/v1/nope", nil) != http.StatusNotFound {
			if time.Now().After(deadline) {
				t.Fatalf("replica %d did not start", p.id)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	leader := advWaitLeader(t, procs[0].advReplica, procs[1].advReplica, procs[2].advReplica)
	var follower *proc
	for _, p := range procs {
		if p.advReplica != leader {
			follower = p
		}
	}
	// The operator creates a play tournament on the internal API.
	create := `{"id":"ladder-1","rules":{"entry_fee":500,"rake_bps":1000,"prize_bps":[10000],"min_entrants":1,"max_entrants":100,"max_score":14400,"min_age":18,"tie_break":"earliest_submission","exclusions":{"version":1,"jurisdictions":[]},"game":"ladder-v1"}}`
	var created api.CommandResponse
	if code, _ := call(t, "POST", leader.url+"/v1/tournaments", "create-ladder-1", create, &created); code != 201 {
		t.Fatalf("create = %d %+v", code, created)
	}
	var bases []string
	bases = append(bases, follower.play)
	for _, p := range procs {
		if p != follower {
			bases = append(bases, p.play)
		}
	}
	c := newPlayHTTP(t, bases...)
	var sess intent.SessionResponse
	if code := c.do("POST", "/v1/session", "session-key-0000001",
		`{"device_id":"9f86d081884c7d659a2feaa0c55ad015","device_secret":"2c26b46b68ffc68ff99b453c1d30413413422d706483bfa0f98a5e886266e7ae","jurisdiction":"TR","age":31}`, &sess); code != 200 || !sess.NewPlayer {
		t.Fatalf("session = %d %+v", code, sess)
	}
	if c.leader != "http://"+addrs[2+int(leader.id)] {
		t.Errorf("cached leader %q, want node %d's play URL", c.leader, leader.id)
	}
	c.token = sess.SessionToken
	seq := 0
	intentDo := func(path, key, body string, want int, out any) {
		t.Helper()
		if code := c.do("POST", path, key, body, out); code != want {
			t.Fatalf("POST %s %s = %d", path, body, code)
		}
	}
	seq++
	intentDo("/v1/tournaments/ladder-1/join", "join-key-00000001", fmt.Sprintf(`{"seq":%d}`, seq), 201, nil)
	seq++
	var round intent.RoundResponse
	intentDo("/v1/tournaments/ladder-1/rounds/1/deal", "deal-key-00000001", fmt.Sprintf(`{"seq":%d}`, seq), 201, &round)
	commitment := round.Round.Commitment
	killed := false
	var lastKey, lastBody string
	var lastResp intent.RoundResponse
	for n := 0; round.Round.Status == "playing"; n++ {
		kind, col := "draw", -1
		if len(round.Round.PlayableColumns) > 0 {
			kind, col = "play", int(round.Round.PlayableColumns[0])
		}
		seq++
		lastKey = fmt.Sprintf("move-key-%08d", n)
		lastBody = fmt.Sprintf(`{"seq":%d,"move_index":%d,"kind":%q,"column":%d}`, seq, round.Round.MoveIndex, kind, col)
		var next intent.RoundResponse
		intentDo("/v1/tournaments/ladder-1/rounds/1/moves", lastKey, lastBody, 200, &next)
		if n == 10 && !killed {
			killed = true
			leader.stop()
			var again intent.RoundResponse
			intentDo("/v1/tournaments/ladder-1/rounds/1/moves", lastKey, lastBody, 200, &again)
			if !again.Replayed || again.Slot != next.Slot || again.Round.MoveIndex != next.Round.MoveIndex {
				t.Fatalf("resend after killing the leader = %+v, first %+v", again, next)
			}
		}
		round, lastResp = next, next
	}
	if !killed || round.Round.Seed == "" || lastResp.Round.Status != "finished" || commitment == "" {
		t.Fatalf("round did not finish through the new leader: %+v", round.Round)
	}
	var board intent.LeaderboardResponse
	if code := c.do("GET", "/v1/tournaments/ladder-1/leaderboard", "", "", &board); code != 200 || board.Me.TotalScore != round.Round.Score || board.Me.RoundsFinished != 1 {
		t.Fatalf("leaderboard = %d %+v", code, board)
	}
	// Every survivor has applied the same state.
	var survivors []*advReplica
	for _, p := range procs {
		if p.advReplica != leader {
			survivors = append(survivors, p.advReplica)
		}
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		var a, b api.NodeResponse
		advGet(survivors[0].url+"/v1/node", &a)
		advGet(survivors[1].url+"/v1/node", &b)
		if a.Applied == b.Applied && a.StateHash == b.StateHash && a.Applied > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("survivors differ: %+v %+v", a.Status, b.Status)
		}
		time.Sleep(20 * time.Millisecond)
	}
	var st api.NodeResponse
	advGet(survivors[0].url+"/v1/node", &st)
	if st.Role != replog.Leader && st.Leader == leader.id {
		t.Errorf("the dead node still leads: %+v", st.Status)
	}
}

// TestRunClusterServesPlay starts the one-process cluster with the play API
// on free ports and opens a session through the printed PLAY URL.
func TestRunClusterServesPlay(t *testing.T) {
	t.Setenv(session.EnvKeys, testSessionKeys)
	t.Setenv(intent.EnvDealSecret, testDealSecret)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stdout := &syncWriter{}
	errc := make(chan error, 1)
	go func() {
		errc <- run(ctx, []string{"-nodes", "3", "-listen", "127.0.0.1:0", "-play-listen", "127.0.0.1:0", "-log-level", "error"}, stdout, io.Discard)
	}()
	deadline := time.Now().Add(10 * time.Second)
	for !strings.Contains(stdout.String(), "Ctrl-C") {
		if time.Now().After(deadline) {
			t.Fatalf("instructions not printed:\n%s", stdout.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
	var plays []string
	for _, line := range strings.Split(stdout.String(), "\n") {
		if f := strings.Fields(line); len(f) == 4 && f[0] == "PLAY" {
			plays = append(plays, f[3])
		}
	}
	if len(plays) != 3 || !strings.Contains(stdout.String(), "PLAY=http://127.0.0.1:") {
		t.Fatalf("play URLs not printed:\n%s", stdout.String())
	}
	c := newPlayHTTP(t, plays...)
	var sess intent.SessionResponse
	if code := c.do("POST", "/v1/session", "session-key-0000001",
		`{"device_id":"0123456789abcdef0123456789abcdef","device_secret":"`+strings.Repeat("ab", 32)+`","jurisdiction":"TR","age":30}`, &sess); code != 200 || sess.PlayerID == "" {
		t.Fatalf("session = %d %+v", code, sess)
	}
	cancel()
	select {
	case err := <-errc:
		if err != nil {
			t.Errorf("run returned %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("run did not stop after cancel")
	}
}
