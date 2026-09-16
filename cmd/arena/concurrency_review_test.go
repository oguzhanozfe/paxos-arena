package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/oguzhanozfe/paxos-arena/internal/api"
)

const reviewRules = `"rules":{"entry_fee":500,"rake_bps":1000,"prize_bps":[5000,3000,2000],"min_entrants":3,"max_entrants":100,"max_score":100000,"min_age":18,"tie_break":"earliest_submission","exclusions":{"version":7,"jurisdictions":[%s]}}`

// TestReviewOversizedCommandWedgesHTTPCluster: the API accepts a body up to
// 1 MiB, but the Accept that carries it is base64 inside JSON and used to
// exceed the transport's 1 MiB MaxMessageBytes, so followers answered 413
// forever. The slot was never chosen, the commit index stopped, and every
// later command timed out until the leader crashed.
//
// The fix sizes the transport for any value the log accepts
// (replog.MaxValueBytes, checked by the API before proposing), bounds the
// exclusion list in validation and lowers the API's default body limit to
// 64 KiB, so this runs by default. The body is now built right up to that
// limit instead of to 800 KB; the log must carry it, the state machine
// refuses its list of some 6 000 codes with invalid_rules (409), and a small
// command after it commits. transport.TestMaxValueFitsInOneMessage covers a
// value of the log's full limit.
func TestReviewOversizedCommandWedgesHTTPCluster(t *testing.T) {
	nodes := startNodes(t, 3)
	leader := waitLeader(t, nodes)
	small := fmt.Sprintf(`{"id":"t1",`+reviewRules+`}`, `"XX"`)
	if code, _ := call(t, "POST", leader.url+"/v1/tournaments", "create-t1", small, nil); code != 201 {
		t.Fatalf("baseline create = %d", code)
	}
	const limit = api.DefaultMaxBody
	frame := fmt.Sprintf(`{"id":"big",`+reviewRules+`}`, "")
	var codes strings.Builder
	for i := 0; len(frame)+codes.Len()+11 <= limit; i++ {
		if i > 0 {
			codes.WriteByte(',')
		}
		fmt.Fprintf(&codes, `"AAAA%c%c%c%c"`, 'A'+i/17576%26, 'A'+i/676%26, 'A'+i/26%26, 'A'+i%26)
	}
	big := fmt.Sprintf(`{"id":"big",`+reviewRules+`}`, codes.String())
	t.Logf("oversized body: %d bytes (API MaxBody %d)", len(big), limit)
	// A client retries 503 and 504 with the same key (ADR 0010), and so
	// does this test, so that an unrelated leadership change is not taken
	// for the defect. With a 1 MiB body under the race detector, decoding,
	// fingerprinting and applying the command held each event loop past an
	// election timeout; the 64 KiB default limit keeps that out of the log.
	// What must not happen is the old failure: no answer and no progress
	// until the leader dies, which the 30-second bound turns into a failure.
	var p api.Problem
	start := time.Now()
	code := 0
	for attempt := 1; ; attempt++ {
		p = api.Problem{}
		code, _ = call(t, "POST", leader.url+"/v1/tournaments", "create-big", big, &p)
		t.Logf("oversized create attempt %d = %d %s after %v", attempt, code, p.Code, time.Since(start).Round(time.Millisecond))
		if (code != http.StatusServiceUnavailable && code != http.StatusGatewayTimeout) || time.Since(start) > 30*time.Second {
			break
		}
		time.Sleep(200 * time.Millisecond)
		leader = waitLeader(t, nodes)
	}
	if code != http.StatusConflict || p.Code != "invalid_rules" {
		t.Fatalf("oversized create = %d %+v; want 409 invalid_rules from the state machine, which proves the log carried it", code, p)
	}
	start = time.Now()
	for attempt := 1; ; attempt++ {
		code, _ = call(t, "POST", leader.url+"/v1/tournaments/t1/entries", "join-p1", `{"player":{"id":"p1","jurisdiction":"TR","age":31}}`, nil)
		if (code != http.StatusServiceUnavailable && code != http.StatusGatewayTimeout) || time.Since(start) > 30*time.Second {
			t.Logf("join after the oversized command: attempt %d = %d after %v", attempt, code, time.Since(start).Round(time.Millisecond))
			break
		}
		time.Sleep(200 * time.Millisecond)
		leader = waitLeader(t, nodes)
	}
	if code != 201 {
		var st api.NodeResponse
		call(t, "GET", leader.url+"/v1/node", "", "", &st)
		t.Fatalf("a small join after the oversized command = %d after %v; leader commit index %d", code, time.Since(start).Round(time.Millisecond), st.CommitIndex)
	}
}

// TestReviewGoroutinesAfterLoad runs the in-process three-node cluster
// through run(), drives concurrent forwarded commands, cancelled requests and
// consistent reads, and checks goroutine counts while idle and after stop.
func TestReviewGoroutinesAfterLoad(t *testing.T) {
	if os.Getenv("ARENA_REVIEW_REPRO") == "" {
		t.Skip("load check; set ARENA_REVIEW_REPRO=1")
	}
	before := runtime.NumGoroutine()
	ctx, cancel := context.WithCancel(context.Background())
	var out syncBuffer
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, []string{"-nodes", "3", "-listen", "127.0.0.1:0", "-log-level", "error"}, &out, io.Discard)
	}()
	re := regexp.MustCompile(`node (\d)  (http://127\.0\.0\.1:\d+)`)
	var urls []string
	deadline := time.Now().Add(5 * time.Second)
	for len(urls) < 3 && time.Now().Before(deadline) {
		urls = urls[:0]
		for _, m := range re.FindAllStringSubmatch(out.String(), 3) {
			urls = append(urls, m[2])
		}
		time.Sleep(10 * time.Millisecond)
	}
	if len(urls) < 3 {
		t.Fatalf("no URLs in output: %s", out.String())
	}
	nodes := make([]*testNode, 3)
	for i, u := range urls {
		nodes[i] = &testNode{url: u}
	}
	leader := waitLeader(t, nodes)
	started, _ := goroutinesExceptIdleConns()

	tr := &http.Transport{MaxIdleConnsPerHost: 64}
	client := &http.Client{Transport: tr, Timeout: 20 * time.Second}
	var wg sync.WaitGroup
	var mu sync.Mutex
	status := map[string]int{}
	for w := 0; w < 64; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 60; i++ {
				base := urls[(w+i)%3]
				var req *http.Request
				rctx, rcancel := context.WithCancel(context.Background())
				switch i % 4 {
				case 0, 1:
					body := fmt.Sprintf(`{"id":"w%d-%d",`+reviewRules+`}`, w, i, `"XX"`)
					req, _ = http.NewRequestWithContext(rctx, "POST", base+"/v1/tournaments", bytes.NewReader([]byte(body)))
					req.Header.Set(api.HeaderIdempotencyKey, fmt.Sprintf("load-%d-%d", w, i))
				case 2:
					req, _ = http.NewRequestWithContext(rctx, "GET", base+fmt.Sprintf("/v1/tournaments/w%d-%d", w, i-2), nil)
				case 3:
					// a client that gives up almost at once
					body := fmt.Sprintf(`{"id":"c%d-%d",`+reviewRules+`}`, w, i, `"XX"`)
					req, _ = http.NewRequestWithContext(rctx, "POST", base+"/v1/tournaments", bytes.NewReader([]byte(body)))
					req.Header.Set(api.HeaderIdempotencyKey, fmt.Sprintf("cancel-%d-%d", w, i))
					time.AfterFunc(time.Duration(i%5)*100*time.Microsecond, rcancel)
				}
				resp, err := client.Do(req)
				key := "error"
				if err == nil {
					io.Copy(io.Discard, resp.Body)
					resp.Body.Close()
					key = fmt.Sprint(resp.StatusCode)
				}
				rcancel()
				mu.Lock()
				status[key]++
				mu.Unlock()
			}
		}(w)
	}
	wg.Wait()
	tr.CloseIdleConnections()
	t.Logf("statuses: %v; leader %s", status, leader.url)
	// Followers now forward through a connection pool of their own that
	// keeps up to api.ForwardConnsPerHost keep-alive connections to the
	// leader, and each idle connection holds a reader and a writer
	// goroutine on the follower and a reader on the leader until the idle
	// timeouts expire. Those are the fix for ephemeral-port exhaustion, not
	// a leak, so the idle level ignores goroutines that are only an idle
	// HTTP connection: no frame of this module on their stack. The check
	// after stop below still counts every goroutine.
	var idle, pooled int
	quiet := time.Now().Add(10 * time.Second)
	for time.Now().Before(quiet) {
		idle, pooled = goroutinesExceptIdleConns()
		if idle <= started+8 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Logf("goroutines: before start %d, cluster idle %d, after load and quiet %d (plus %d of idle pooled connections)", before, started, idle, pooled)
	if idle > started+8 {
		buf := make([]byte, 1<<20)
		n := runtime.Stack(buf, true)
		t.Errorf("goroutines did not return to the idle level: %d vs %d\n%s", idle, started, buf[:n])
	}
	cancel()
	if err := <-done; err != nil {
		t.Errorf("run: %v", err)
	}
	http.DefaultTransport.(*http.Transport).CloseIdleConnections()
	var after int
	stop := time.Now().Add(10 * time.Second)
	for time.Now().Before(stop) {
		after = runtime.NumGoroutine()
		if after <= before {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Logf("goroutines after stop: %d (before start %d)", after, before)
	if after > before {
		buf := make([]byte, 1<<20)
		n := runtime.Stack(buf, true)
		t.Errorf("goroutines left after run returned: %d vs %d\n%s", after, before, buf[:n])
	}
}

type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// goroutinesExceptIdleConns counts the goroutines whose stack has a frame of
// this module or of the test binary's own code, and separately those that
// are only an idle net/http connection.
func goroutinesExceptIdleConns() (counted, idleConns int) {
	buf := make([]byte, 8<<20)
	buf = buf[:runtime.Stack(buf, true)]
	for _, g := range strings.Split(string(buf), "\n\n") {
		if strings.TrimSpace(g) == "" {
			continue
		}
		if !strings.Contains(g, "paxos-arena") && (strings.Contains(g, "net/http.") || strings.Contains(g, "internal/poll.")) {
			idleConns++
			continue
		}
		counted++
	}
	return counted, idleConns
}
