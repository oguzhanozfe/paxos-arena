package api

import (
	"context"
	"fmt"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/oguzhanozfe/paxos-arena/internal/paxos"
	"github.com/oguzhanozfe/paxos-arena/internal/replica"
	"github.com/oguzhanozfe/paxos-arena/internal/replog"
	"github.com/oguzhanozfe/paxos-arena/internal/tournament"
)

// cancelAfterSubmit wraps a real Runner and cancels the request context as
// soon as Submit has returned, as a client that disconnects at the moment its
// command commits would.
type cancelAfterSubmit struct {
	*replica.Runner
	cancel context.CancelFunc
}

func (b *cancelAfterSubmit) Submit(ctx context.Context, cmd tournament.Command) (tournament.Result, error) {
	res, err := b.Runner.Submit(ctx, cmd)
	b.cancel()
	return res, err
}

func startSingleRunner(t *testing.T) *replica.Runner {
	t.Helper()
	core, err := replica.NewCore(replog.DefaultConfig(1, []paxos.NodeID{1}), replog.NewMemStore(), rand.New(rand.NewPCG(1, 1)))
	if err != nil {
		t.Fatal(err)
	}
	r := replica.NewRunner(core, func(replog.Envelope) {}, nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); r.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })
	deadline := time.Now().Add(5 * time.Second)
	for !r.Status().Ready {
		if time.Now().After(deadline) {
			t.Fatal("single runner never became ready")
		}
		time.Sleep(time.Millisecond)
	}
	return r
}

// TestReviewCommandReadRacesWithEventLoop: after a successful Submit the
// handler read the tournament through Backend.Read with the request context.
// When that context had ended, Runner.Read could return ctx.Err() while the
// event loop still ran fn later; fn wrote resp.Tournament while the handler
// goroutine read resp in writeJSON. The handler now reads into a variable of
// its own under a context of its own and uses it only when Read returned
// nil, so this runs by default: under -race it fails on any data race, and
// it requires every response to carry the record.
func TestReviewCommandReadRacesWithEventLoop(t *testing.T) {
	r := startSingleRunner(t)
	b := &cancelAfterSubmit{Runner: r}
	h := New(Config{Self: 1}, b, nil).Handler()
	missing := 0
	for i := 0; i < 300; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		b.cancel = cancel
		body := strings.Replace(createBody, `"id":"t1"`, fmt.Sprintf(`"id":"t%d"`, i), 1)
		req := httptest.NewRequest("POST", "/v1/tournaments", strings.NewReader(body)).WithContext(ctx)
		req.Header.Set(HeaderIdempotencyKey, fmt.Sprintf("race-%d", i))
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		cancel()
		if rec.Code != http.StatusCreated {
			t.Fatalf("iteration %d: %d %s", i, rec.Code, rec.Body.String())
		}
		if !strings.Contains(rec.Body.String(), `"tournament"`) {
			missing++
		}
	}
	if missing > 0 {
		t.Fatalf("%d of 300 successful create responses carried no tournament record", missing)
	}
}
