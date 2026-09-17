package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/oguzhanozfe/paxos-arena/internal/api"
	"github.com/oguzhanozfe/paxos-arena/internal/debugfeed"
	"github.com/oguzhanozfe/paxos-arena/internal/paxos"
)

// startCluster runs `arena -nodes 3` through run with args added, waits
// until the output contains waitFor, and returns the replicas it printed.
func startCluster(t *testing.T, waitFor string, args ...string) ([]*testNode, string) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	stdout := &syncWriter{}
	errc := make(chan error, 1)
	all := append([]string{"-nodes", "3", "-listen", "127.0.0.1:0", "-log-level", "error"}, args...)
	go func() { errc <- run(ctx, all, stdout, io.Discard) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-errc:
			if err != nil {
				t.Errorf("run returned %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Error("run did not stop after cancel")
		}
	})
	deadline := time.Now().Add(10 * time.Second)
	for !strings.Contains(stdout.String(), waitFor) {
		if time.Now().After(deadline) {
			t.Fatalf("output lacks %q:\n%s", waitFor, stdout.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
	out := stdout.String()
	var nodes []*testNode
	for _, line := range strings.Split(out, "\n") {
		var id paxos.NodeID
		var url string
		if n, _ := fmt.Sscanf(line, "  node %d  %s", &id, &url); n == 2 && strings.HasPrefix(url, "http://") {
			nodes = append(nodes, &testNode{id: id, url: url})
		}
	}
	if len(nodes) != 3 {
		t.Fatalf("found %d replicas in the output:\n%s", len(nodes), out)
	}
	return nodes, out
}

// readFeed follows the debug feed at base, from the start, until the events
// read so far satisfy every matcher in want, and returns them. extra is
// appended to the query.
func readFeed(t *testing.T, base, extra string, want []eventMatcher) []debugfeed.Event {
	t.Helper()
	var all []debugfeed.Event
	var after uint64
	deadline := time.Now().Add(10 * time.Second)
	for {
		var resp debugfeed.Response
		url := fmt.Sprintf("%s%s?after=%d&limit=%d&wait_ms=500%s", base, debugfeed.Path, after, debugfeed.MaxLimit, extra)
		if code, _ := call(t, "GET", url, "", "", &resp); code != http.StatusOK {
			t.Fatalf("GET %s = %d", url, code)
		}
		if resp.Dropped != 0 {
			t.Logf("%s dropped %d events after %d", base, resp.Dropped, after)
		}
		all = append(all, resp.Events...)
		after = resp.Next
		lacking := missing(all, want)
		if len(lacking) == 0 {
			return all
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s: after %d events the feed still lacks %v", base, len(all), lacking)
		}
	}
}

// eventMatcher is one event a test looks for.
type eventMatcher struct {
	what string
	ok   func(debugfeed.Event) bool
}

// missing returns the matchers no event satisfies.
func missing(evs []debugfeed.Event, ms []eventMatcher) []string {
	var out []string
	for _, m := range ms {
		found := false
		for _, ev := range evs {
			if m.ok(ev) {
				found = true
				break
			}
		}
		if !found {
			out = append(out, m.what)
		}
	}
	return out
}

// slotTraffic describes what a leader records for a slot chosen with the
// value summary value.
func slotTraffic(leader paxos.NodeID, slot paxos.Slot, value string) []eventMatcher {
	return []eventMatcher{
		{"send accept", func(e debugfeed.Event) bool {
			return e.Kind == debugfeed.KindSend && e.Type == "accept" && e.From == leader && e.To != leader && e.Slot == slot && e.Value == value
		}},
		{"recv accepted", func(e debugfeed.Event) bool {
			return e.Kind == debugfeed.KindRecv && e.Type == "accepted" && e.To == leader && e.From != leader && e.Slot == slot
		}},
		{"send learn", func(e debugfeed.Event) bool {
			return e.Kind == debugfeed.KindSend && e.Type == "learn" && e.From == leader && e.Slot == slot && e.Value == value
		}},
		{"commit", func(e debugfeed.Event) bool { return e.Kind == debugfeed.KindCommit && e.CommitIndex >= slot }},
		{"applied ok", func(e debugfeed.Event) bool {
			return e.Kind == debugfeed.KindApplied && e.Slot == slot && e.Value == value && e.Detail == "ok"
		}},
	}
}

const feedRules = `{"id":"t-feed","rules":{"entry_fee":500,"rake_bps":1000,"prize_bps":[10000],"min_entrants":1,"max_entrants":10,"max_score":1000,"min_age":18,"tie_break":"earliest_submission","exclusions":{"version":1,"jurisdictions":[]}}}`

// TestDebugFeedOneProcess starts three replicas in one process with
// -debug-feed, creates a tournament through the API, and finds its slot's
// Accept, Accepted, Learn, commit and apply on the leader's feed, the
// leader's election before it, and the Accept arriving on a follower's feed.
func TestDebugFeedOneProcess(t *testing.T) {
	nodes, out := startCluster(t, "-debug-feed is on", "-debug-feed")
	if !strings.Contains(out, "GET /debug/events") || !strings.Contains(out, "loopback only") {
		t.Errorf("the startup notice does not name the route and the loopback warning:\n%s", out)
	}
	leader := waitLeader(t, nodes)
	var created api.CommandResponse
	if code, _ := call(t, "POST", leader.url+"/v1/tournaments", "create-feed", feedRules, &created); code != http.StatusCreated {
		t.Fatalf("create answered %d: %+v", code, created)
	}
	want := append(slotTraffic(leader.id, created.Slot, "create_tournament t-feed"),
		eventMatcher{"its election: send prepare", func(e debugfeed.Event) bool {
			return e.Kind == debugfeed.KindSend && e.Type == "prepare" && e.From == leader.id && e.BallotNode == leader.id
		}},
		eventMatcher{"its election: recv promise", func(e debugfeed.Event) bool {
			return e.Kind == debugfeed.KindRecv && e.Type == "promise" && e.To == leader.id && e.BallotNode == leader.id
		}},
		eventMatcher{"role leader", func(e debugfeed.Event) bool {
			return e.Kind == debugfeed.KindRole && e.Type == "leader" && e.BallotNode == leader.id
		}},
	)
	evs := readFeed(t, leader.url, "", want)
	var last uint64
	for _, ev := range evs {
		if ev.Node != leader.id || ev.Seq <= last {
			t.Fatalf("event %+v: want node %d and a sequence number above %d", ev, leader.id, last)
		}
		last = ev.Seq
		if ev.Type == "heartbeat" || ev.Type == "heartbeat_ack" {
			t.Fatalf("heartbeats=0 returned %+v", ev)
		}
	}

	for _, nd := range nodes {
		if nd == leader {
			continue
		}
		readFeed(t, nd.url, "&heartbeats=1", []eventMatcher{
			{"recv accept", func(e debugfeed.Event) bool {
				return e.Kind == debugfeed.KindRecv && e.Type == "accept" && e.From == leader.id && e.To == nd.id && e.Slot == created.Slot
			}},
			{"recv heartbeat", func(e debugfeed.Event) bool {
				return e.Kind == debugfeed.KindRecv && e.Type == "heartbeat" && e.From == leader.id
			}},
			{"send heartbeat_ack", func(e debugfeed.Event) bool {
				return e.Kind == debugfeed.KindSend && e.Type == "heartbeat_ack" && e.To == leader.id
			}},
			{"applied", func(e debugfeed.Event) bool {
				return e.Kind == debugfeed.KindApplied && e.Slot == created.Slot && e.Node == nd.id
			}},
		})
	}
}

// TestDebugFeedPerProcess runs one replica per process over the HTTP
// transport with -debug-feed and finds a created tournament's slot on the
// leader's feed.
func TestDebugFeedPerProcess(t *testing.T) {
	addrs := advFreeAddrs(t, 3)
	var parts []string
	for i, a := range addrs {
		parts = append(parts, fmt.Sprintf("%d=http://%s", i+1, a))
	}
	peers := strings.Join(parts, ",")
	dir := t.TempDir()
	var reps []*advReplica
	for i, a := range addrs {
		reps = append(reps, advStartReplica(t, paxos.NodeID(i+1), a, peers, dir, "-debug-feed"))
	}
	leader := advWaitLeader(t, reps...)
	var created api.CommandResponse
	if code, _ := call(t, "POST", leader.url+"/v1/tournaments", "create-feed", feedRules, &created); code != http.StatusCreated {
		t.Fatalf("create answered %d: %+v", code, created)
	}
	readFeed(t, leader.url, "", slotTraffic(leader.id, created.Slot, "create_tournament t-feed"))
}

// TestDebugFeedOffByDefault: without -debug-feed the route does not exist
// and no notice is printed.
func TestDebugFeedOffByDefault(t *testing.T) {
	nodes, out := startCluster(t, "Ctrl-C")
	if strings.Contains(out, "debug-feed") {
		t.Errorf("notice printed without -debug-feed:\n%s", out)
	}
	for _, nd := range nodes {
		for _, path := range []string{debugfeed.Path, debugfeed.Path + "?after=0&wait_ms=100"} {
			resp, err := http.Get(nd.url + path)
			if err != nil {
				t.Fatal(err)
			}
			resp.Body.Close()
			if resp.StatusCode != http.StatusNotFound {
				t.Errorf("node %d GET %s = %d, want 404", nd.id, path, resp.StatusCode)
			}
		}
	}
	// The API itself still answers.
	if code, _ := call(t, "GET", nodes[0].url+"/healthz", "", "", nil); code != http.StatusOK {
		t.Errorf("healthz = %d", code)
	}
}

// TestFeedMissingReportsUnmatched keeps the matcher helper honest: an empty
// feed misses every event, and a matching one misses none.
func TestFeedMissingReportsUnmatched(t *testing.T) {
	want := slotTraffic(1, 4, "noop")
	if got := missing(nil, want); len(got) != len(want) {
		t.Errorf("missing(nil) = %v, want all %d", got, len(want))
	}
	evs := []debugfeed.Event{
		{Kind: debugfeed.KindSend, Type: "accept", From: 1, To: 2, Slot: 4, Value: "noop"},
		{Kind: debugfeed.KindRecv, Type: "accepted", From: 2, To: 1, Slot: 4},
		{Kind: debugfeed.KindSend, Type: "learn", From: 1, To: 2, Slot: 4, Value: "noop"},
		{Kind: debugfeed.KindCommit, CommitIndex: 4},
		{Kind: debugfeed.KindApplied, Slot: 4, Value: "noop", Detail: "ok"},
	}
	if got := missing(evs, want); len(got) != 0 {
		t.Errorf("missing = %v, want none", got)
	}
}
