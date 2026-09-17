package debugfeed

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/oguzhanozfe/paxos-arena/internal/paxos"
)

// Path is where Feed.Handler is mounted on a replica's operator listener.
const Path = "/debug/events"

// Query parameters of GET Path.
const (
	// QueryAfter is the sequence number to read after, 0 by default.
	QueryAfter = "after"
	// QueryLimit bounds the events of one response, 1 to MaxLimit.
	QueryLimit = "limit"
	// QueryWaitMs is how long to wait for an event when there is none, 0 to
	// MaxWait in milliseconds.
	QueryWaitMs = "wait_ms"
	// QueryHeartbeats is 1 to include heartbeat and heartbeat_ack messages,
	// 0 (the default) to leave them out.
	QueryHeartbeats = "heartbeats"
)

// Bounds of the query parameters.
const (
	DefaultLimit = 256
	MaxLimit     = 1024
	MaxWait      = 2 * time.Second
)

// Response is the body of GET Path.
type Response struct {
	// Node is the replica whose events these are.
	Node paxos.NodeID `json:"node"`
	// Next is the value of after for the next request.
	Next uint64 `json:"next"`
	// Dropped counts events after the requested sequence number that the
	// buffer overwrote before this request read them.
	Dropped uint64 `json:"dropped"`
	// Events are the events, oldest first; an empty list, never null.
	Events []Event `json:"events"`
}

// problem is the RFC 9457 error body, in the shape the API uses.
type problem struct {
	Type   string `json:"type"`
	Title  string `json:"title"`
	Status int    `json:"status"`
	Detail string `json:"detail,omitempty"`
	Code   string `json:"code"`
}

// Handler serves GET and HEAD on Path; any other method is 405. It answers
// 400 malformed_request for a query parameter out of its range. With
// wait_ms it holds the request until an event it would return is recorded,
// the wait passes or the client goes away.
func (f *Feed) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.Header().Set("Allow", "GET, HEAD")
			writeProblem(w, http.StatusMethodNotAllowed, "method_not_allowed", "the debug feed is read-only")
			return
		}
		q := r.URL.Query()
		after, ok := queryUint(w, q, QueryAfter, 0, 0, math.MaxUint64)
		if !ok {
			return
		}
		limit, ok := queryUint(w, q, QueryLimit, DefaultLimit, 1, MaxLimit)
		if !ok {
			return
		}
		waitMs, ok := queryUint(w, q, QueryWaitMs, 0, 0, uint64(MaxWait.Milliseconds()))
		if !ok {
			return
		}
		heartbeats, ok := queryUint(w, q, QueryHeartbeats, 0, 0, 1)
		if !ok {
			return
		}
		p := f.ring.Wait(r.Context(), after, int(limit), heartbeats == 1, time.Duration(waitMs)*time.Millisecond)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		json.NewEncoder(w).Encode(Response{Node: f.self, Next: p.Next, Dropped: p.Dropped, Events: p.Events})
	})
}

// queryUint parses the decimal parameter name, which must lie in lo..hi,
// and returns def when it is absent. It answers 400 and returns false for
// anything else.
func queryUint(w http.ResponseWriter, q url.Values, name string, def, lo, hi uint64) (uint64, bool) {
	if !q.Has(name) {
		return def, true
	}
	n, err := strconv.ParseUint(q.Get(name), 10, 64)
	if err != nil || n < lo || n > hi {
		writeProblem(w, http.StatusBadRequest, "malformed_request", fmt.Sprintf("%s must be an integer from %d to %d", name, lo, hi))
		return 0, false
	}
	return n, true
}

func writeProblem(w http.ResponseWriter, status int, code, detail string) {
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(problem{Type: "about:blank", Title: http.StatusText(status), Status: status, Detail: detail, Code: code})
}
