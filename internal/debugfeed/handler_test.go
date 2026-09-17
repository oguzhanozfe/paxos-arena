package debugfeed

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/oguzhanozfe/paxos-arena/internal/paxos"
	"github.com/oguzhanozfe/paxos-arena/internal/replog"
)

// get serves one request to f's handler and returns the recorder.
func get(f *Feed, method, target string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	f.Handler().ServeHTTP(rec, httptest.NewRequest(method, target, nil))
	return rec
}

func decodeResponse(t *testing.T, rec *httptest.ResponseRecorder) Response {
	t.Helper()
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d, body %s", rec.Code, rec.Body)
	}
	var resp Response
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("body %s: %v", rec.Body, err)
	}
	return resp
}

// traffic records a leader's heartbeat round and one slot on feed f of node 1.
func traffic(f *Feed) {
	b := paxos.Ballot{Round: 1, Node: 1}
	f.Sent(replog.Envelope{From: 1, To: 2, Msg: replog.Heartbeat{Ballot: b, CommitIndex: 1}})
	f.Received(replog.Envelope{From: 2, To: 1, Msg: replog.HeartbeatAck{Ballot: b, Promised: b, CommitIndex: 1}})
	f.Sent(replog.Envelope{From: 1, To: 2, Msg: replog.Accept{Ballot: b, Slot: 2}})
	f.Received(replog.Envelope{From: 2, To: 1, Msg: replog.Accepted{Ballot: b, Slot: 2}})
	f.Sent(replog.Envelope{From: 1, To: 2, Msg: replog.Heartbeat{Ballot: b, CommitIndex: 2}})
}

// TestHandlerJSONShape checks the exact members of the response and of an
// event, and that every number is an integer: the shape a JsonUtility class
// mirrors.
func TestHandlerJSONShape(t *testing.T) {
	f := New(1)
	traffic(f)
	rec := get(f, http.MethodGet, Path+"?heartbeats=1")
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q", ct)
	}
	if cc := rec.Header().Get("Cache-Control"); cc != "no-store" {
		t.Errorf("Cache-Control = %q", cc)
	}
	var body map[string]json.RawMessage
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("body %s: %v", rec.Body, err)
	}
	if got, want := sortedKeys(body), []string{"dropped", "events", "next", "node"}; !slices.Equal(got, want) {
		t.Errorf("response members %v, want %v", got, want)
	}
	for _, k := range []string{"node", "next", "dropped"} {
		if !isUint(body[k]) {
			t.Errorf("%s = %s, want a non-negative integer", k, body[k])
		}
	}
	var events []map[string]json.RawMessage
	if err := json.Unmarshal(body["events"], &events); err != nil || len(events) != 5 {
		t.Fatalf("events %s: %v", body["events"], err)
	}
	wantKeys := []string{"at_ms", "ballot_node", "ballot_round", "commit_index", "detail", "from", "kind", "node", "seq", "slot", "to", "type", "value"}
	strs := map[string]bool{"kind": true, "type": true, "value": true, "detail": true}
	for i, ev := range events {
		if got := sortedKeys(ev); !slices.Equal(got, wantKeys) {
			t.Errorf("event %d members %v, want %v", i, got, wantKeys)
		}
		for k, raw := range ev {
			if strs[k] != (len(raw) > 0 && raw[0] == '"') {
				t.Errorf("event %d: %s = %s has the wrong JSON type", i, k, raw)
			}
			if !strs[k] && !isUint(raw) {
				t.Errorf("event %d: %s = %s, want a non-negative integer", i, k, raw)
			}
		}
	}
	resp := decodeResponse(t, rec)
	if resp.Node != 1 || resp.Next != 5 || resp.Dropped != 0 {
		t.Errorf("response node %d next %d dropped %d, want 1, 5, 0", resp.Node, resp.Next, resp.Dropped)
	}
	accept := resp.Events[2]
	if accept.Kind != KindSend || accept.Type != "accept" || accept.From != 1 || accept.To != 2 || accept.Slot != 2 ||
		accept.BallotRound != 1 || accept.BallotNode != 1 || accept.Value != "noop" || accept.Seq != 3 || accept.AtMs == 0 {
		t.Errorf("accept event = %+v", accept)
	}

	// Nothing after the cursor: an empty list, not null.
	rec = get(f, http.MethodGet, Path+"?after=5")
	if !strings.Contains(rec.Body.String(), `"events":[]`) {
		t.Errorf("empty page body %s, want \"events\":[]", rec.Body)
	}
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

func isUint(raw json.RawMessage) bool {
	if len(raw) == 0 {
		return false
	}
	for _, c := range raw {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

func TestHandlerHeartbeatFilter(t *testing.T) {
	f := New(1)
	traffic(f)
	cases := []struct {
		query string
		types []string
		next  uint64
	}{
		{"", []string{"accept", "accepted"}, 5},
		{"?heartbeats=0", []string{"accept", "accepted"}, 5},
		{"?heartbeats=1", []string{"heartbeat", "heartbeat_ack", "accept", "accepted", "heartbeat"}, 5},
		{"?after=2", []string{"accept", "accepted"}, 5},
		{"?after=4", []string{}, 5}, // only a heartbeat after 4: next moves past it
		{"?limit=1", []string{"accept"}, 3},
		{"?after=3&limit=1&heartbeats=1", []string{"accepted"}, 4},
	}
	for _, tc := range cases {
		resp := decodeResponse(t, get(f, http.MethodGet, Path+tc.query))
		var types []string
		for _, ev := range resp.Events {
			types = append(types, ev.Type)
		}
		if types == nil {
			types = []string{}
		}
		if !slices.Equal(types, tc.types) || resp.Next != tc.next {
			t.Errorf("GET %s: types %v next %d, want %v next %d", tc.query, types, resp.Next, tc.types, tc.next)
		}
	}
}

// TestHandlerLongPollWakes runs the handler on a fake clock: a request with
// wait_ms is answered as soon as an event it returns is recorded, not when
// the wait ends, and a request with nothing to return is answered empty
// when the wait ends.
func TestHandlerLongPollWakes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := New(1)
		traffic(f)
		start := time.Now()
		done := make(chan *httptest.ResponseRecorder)
		go func() { done <- get(f, http.MethodGet, Path+"?after=5&wait_ms=2000") }()
		synctest.Wait()
		time.Sleep(300 * time.Millisecond)
		b := paxos.Ballot{Round: 1, Node: 1}
		f.Sent(replog.Envelope{From: 1, To: 2, Msg: replog.Heartbeat{Ballot: b, CommitIndex: 2}}) // skipped
		synctest.Wait()
		select {
		case rec := <-done:
			t.Fatalf("answered for a heartbeat it leaves out: %s", rec.Body)
		default:
		}
		time.Sleep(200 * time.Millisecond)
		f.Received(replog.Envelope{From: 2, To: 1, Msg: replog.Nack{Ballot: b, Promised: paxos.Ballot{Round: 2, Node: 3}, Slot: 3}})
		resp := decodeResponse(t, <-done)
		if waited := time.Since(start); waited != 500*time.Millisecond {
			t.Errorf("answered after %v, want 500ms", waited)
		}
		if len(resp.Events) != 1 || resp.Events[0].Type != "nack" || resp.Events[0].Seq != 7 || resp.Next != 7 {
			t.Errorf("long poll = %+v, want the nack, seq 7", resp)
		}

		start = time.Now()
		resp = decodeResponse(t, get(f, http.MethodGet, Path+"?after=7&wait_ms=1200"))
		if waited := time.Since(start); waited != 1200*time.Millisecond || len(resp.Events) != 0 || resp.Next != 7 {
			t.Errorf("empty long poll = %+v after %v, want no events, next 7, after 1.2s", resp, waited)
		}
	})
}

func TestHandlerRejectsBadRequests(t *testing.T) {
	f := New(1)
	cases := []struct {
		method, query string
		status        int
		code          string
	}{
		{http.MethodGet, "?after=-1", 400, "malformed_request"},
		{http.MethodGet, "?after=x", 400, "malformed_request"},
		{http.MethodGet, "?after=18446744073709551616", 400, "malformed_request"},
		{http.MethodGet, "?after=", 400, "malformed_request"},
		{http.MethodGet, "?limit=0", 400, "malformed_request"},
		{http.MethodGet, "?limit=1025", 400, "malformed_request"},
		{http.MethodGet, "?limit=+5", 400, "malformed_request"},
		{http.MethodGet, "?wait_ms=2001", 400, "malformed_request"},
		{http.MethodGet, "?wait_ms=1.5", 400, "malformed_request"},
		{http.MethodGet, "?heartbeats=2", 400, "malformed_request"},
		{http.MethodGet, "?heartbeats=true", 400, "malformed_request"},
		{http.MethodPost, "", 405, "method_not_allowed"},
		{http.MethodDelete, "", 405, "method_not_allowed"},
		{http.MethodGet, "?after=18446744073709551615&limit=1024&wait_ms=0&heartbeats=1", 200, ""},
		{http.MethodHead, "", 200, ""},
	}
	for _, tc := range cases {
		rec := get(f, tc.method, Path+tc.query)
		if rec.Code != tc.status {
			t.Errorf("%s %s = %d %s, want %d", tc.method, tc.query, rec.Code, rec.Body, tc.status)
			continue
		}
		if tc.code == "" {
			continue
		}
		var p problem
		if err := json.Unmarshal(rec.Body.Bytes(), &p); err != nil || p.Code != tc.code || p.Status != tc.status ||
			rec.Header().Get("Content-Type") != "application/problem+json" {
			t.Errorf("%s %s: problem %+v (%v), content type %q", tc.method, tc.query, p, err, rec.Header().Get("Content-Type"))
		}
		if tc.status == 405 && rec.Header().Get("Allow") != "GET, HEAD" {
			t.Errorf("%s: Allow = %q", tc.method, rec.Header().Get("Allow"))
		}
	}
}
