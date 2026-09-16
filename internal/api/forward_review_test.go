package api

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/oguzhanozfe/paxos-arena/internal/paxos"
	"github.com/oguzhanozfe/paxos-arena/internal/replica"
	"github.com/oguzhanozfe/paxos-arena/internal/replog"
)

// TestReviewForwardedResponseIsNotTruncated: a follower that forwarded a
// consistent read copied at most MaxBody bytes of the leader's answer, so a
// record larger than 1 MiB reached the client cut off, with status 200. The
// follower now relays the whole answer, so this runs by default.
func TestReviewForwardedResponseIsNotTruncated(t *testing.T) {
	big := `{"tournament":{"id":"t1","pad":"` + strings.Repeat("x", 2<<20) + `"},"applied_slot":9,"consistent":true}`
	leader := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		io.WriteString(w, big)
	}))
	defer leader.Close()
	f := newFake()
	f.setStatus(replica.Status{Self: 1, Leader: 2, Role: replog.Follower})
	h := newServer(t, f, func(c *Config) { c.Peers = map[paxos.NodeID]string{1: "http://self", 2: leader.URL} })
	rec := do(h, "GET", "/v1/tournaments/t1", "", "")
	t.Logf("leader body %d bytes; forwarded body %d bytes, status %d", len(big), rec.Body.Len(), rec.Code)
	if rec.Code != http.StatusOK || !json.Valid(rec.Body.Bytes()) || rec.Body.Len() != len(big) {
		t.Fatalf("forwarded response: status %d, %d of %d bytes delivered, valid JSON %t",
			rec.Code, rec.Body.Len(), len(big), json.Valid(rec.Body.Bytes()))
	}
}
