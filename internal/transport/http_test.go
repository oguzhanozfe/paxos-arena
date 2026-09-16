package transport

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/oguzhanozfe/paxos-arena/internal/paxos"
	"github.com/oguzhanozfe/paxos-arena/internal/replog"
)

// TestHTTPDeliversBetweenServers runs two transports on httptest servers
// and checks that a message sent by one arrives at the other's deliver
// callback with the same content.
func TestHTTPDeliversBetweenServers(t *testing.T) {
	got := make(chan replog.Envelope, 16)
	var a, b *HTTP
	srvA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { a.Handler().ServeHTTP(w, r) }))
	defer srvA.Close()
	srvB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { b.Handler().ServeHTTP(w, r) }))
	defer srvB.Close()
	peers := map[paxos.NodeID]string{1: srvA.URL, 2: srvB.URL}
	a = NewHTTP(1, peers, func(e replog.Envelope) { got <- e }, nil, nil)
	b = NewHTTP(2, peers, func(e replog.Envelope) { got <- e }, nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); a.Run(ctx) }()
	go func() { defer wg.Done(); b.Run(ctx) }()

	want := replog.Envelope{From: 1, To: 2, Msg: replog.Accept{Ballot: paxos.Ballot{Round: 3, Node: 1}, Slot: 9, Value: paxos.Value("v")}}
	a.Send(want)
	select {
	case e := <-got:
		if e.From != want.From || e.To != want.To {
			t.Errorf("got %+v", e)
		}
		acc, ok := e.Msg.(replog.Accept)
		if !ok || acc.Slot != 9 || string(acc.Value) != "v" {
			t.Errorf("message %+v", e.Msg)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("message not delivered")
	}
	b.Send(replog.Envelope{From: 2, To: 1, Msg: replog.Accepted{Ballot: paxos.Ballot{Round: 3, Node: 1}, Slot: 9}})
	select {
	case e := <-got:
		if e.To != 1 {
			t.Errorf("got %+v", e)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("reply not delivered")
	}
	a.Send(replog.Envelope{From: 1, To: 1, Msg: replog.Heartbeat{}}) // self: dropped
	a.Send(replog.Envelope{From: 1, To: 9, Msg: replog.Heartbeat{}}) // unknown: dropped
	// The deliver callback runs before the peer's 204 is read, so wait for
	// the senders to record the acknowledgements before stopping them.
	for deadline := time.Now().Add(5 * time.Second); (a.Stats().Posted < 1 || b.Stats().Posted < 1) && time.Now().Before(deadline); {
		time.Sleep(time.Millisecond)
	}
	cancel()
	wg.Wait()
	st := a.Stats()
	if st.Sent != 3 || st.Posted != 1 || st.Dropped != 2 || st.Received != 1 {
		t.Errorf("a stats = %+v", st)
	}
	if st := b.Stats(); st.Received != 1 || st.Posted != 1 {
		t.Errorf("b stats = %+v", st)
	}
}

// TestHTTPSendNeverBlocksWhenPeerIsDown: with the peer unreachable, Send
// returns at once even past the queue capacity, the overflow is counted as
// dropped, and the failed POSTs are counted once Run drains the queue.
func TestHTTPSendNeverBlocksWhenPeerIsDown(t *testing.T) {
	dead := httptest.NewServer(http.NotFoundHandler())
	url := dead.URL
	dead.Close()
	tr := NewHTTP(1, map[paxos.NodeID]string{2: url}, func(replog.Envelope) {}, &http.Client{Timeout: 200 * time.Millisecond}, nil)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < QueueSize+50; i++ {
			tr.Send(replog.Envelope{From: 1, To: 2, Msg: replog.Heartbeat{}})
		}
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Send blocked with the peer down")
	}
	st := tr.Stats()
	if st.Sent != QueueSize+50 || st.Dropped != 50 {
		t.Errorf("stats before Run = %+v", st)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	tr.Run(ctx)
	if st := tr.Stats(); st.Failed == 0 || st.Posted != 0 {
		t.Errorf("stats after Run = %+v", st)
	}
}

func TestHTTPHandlerRejects(t *testing.T) {
	delivered := 0
	tr := NewHTTP(2, nil, func(replog.Envelope) { delivered++ }, nil, nil)
	h := tr.Handler()
	valid, _ := replog.Encode(replog.Envelope{From: 1, To: 2, Msg: replog.Heartbeat{Ballot: paxos.Ballot{Round: 1, Node: 1}}})
	wrongTo, _ := replog.Encode(replog.Envelope{From: 1, To: 3, Msg: replog.Heartbeat{Ballot: paxos.Ballot{Round: 1, Node: 1}}})
	cases := []struct {
		name   string
		method string
		body   string
		want   int
	}{
		{"get", "GET", "", http.StatusMethodNotAllowed},
		{"bad json", "POST", "{", http.StatusBadRequest},
		{"unknown type", "POST", `{"from":1,"to":2,"type":"x","body":{}}`, http.StatusBadRequest},
		{"wrong recipient", "POST", string(wrongTo), http.StatusBadRequest},
		{"too large", "POST", strings.Repeat("x", MaxMessageBytes+1), http.StatusRequestEntityTooLarge},
		{"valid", "POST", string(valid), http.StatusNoContent},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, Path, strings.NewReader(tc.body))
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != tc.want {
				t.Errorf("status %d, want %d: %s", rec.Code, tc.want, rec.Body.String())
			}
		})
	}
	if delivered != 1 {
		t.Errorf("delivered %d, want 1", delivered)
	}
	if st := tr.Stats(); st.Rejected != 5 || st.Received != 1 {
		t.Errorf("stats = %+v", st)
	}
}
