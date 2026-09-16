package transport

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/oguzhanozfe/paxos-arena/internal/paxos"
	"github.com/oguzhanozfe/paxos-arena/internal/replog"
)

// Path is where HTTP.Handler must be mounted on every replica.
const Path = "/internal/paxos"

// Sizing of the HTTP transport.
const (
	// QueueSize is the number of outbound messages buffered per peer. Send
	// drops the message when the queue is full.
	QueueSize = 1024
	// MaxMessageBytes bounds an inbound message body.
	MaxMessageBytes = 1 << 20
	// DefaultSendTimeout bounds one POST to a peer when the caller passes no
	// client of its own.
	DefaultSendTimeout = 2 * time.Second
)

// HTTPStats counts what the HTTP transport did.
type HTTPStats struct {
	// Sent counts calls to Send.
	Sent uint64
	// Posted counts messages a peer acknowledged.
	Posted uint64
	// Dropped counts messages discarded because the peer's queue was full or
	// the peer is unknown.
	Dropped uint64
	// Failed counts POSTs that returned an error or a non-2xx status.
	Failed uint64
	// Received counts messages the handler delivered.
	Received uint64
	// Rejected counts inbound requests the handler refused.
	Rejected uint64
}

// HTTP carries protocol messages between processes: every message is one
// POST of the replog wire encoding to the peer's Path. It is best effort:
// Send never blocks, a full per-peer queue drops the message, and a failed
// POST is dropped; the protocol's retransmission covers the loss. Safe for
// concurrent use.
type HTTP struct {
	self    paxos.NodeID
	peers   map[paxos.NodeID]string
	deliver Deliver
	client  *http.Client
	log     *slog.Logger
	queues  map[paxos.NodeID]chan []byte

	sent, posted, dropped, failed, received, rejected atomic.Uint64
}

// NewHTTP builds the transport for self. peers maps every other node to its
// base URL (scheme and host, no path); an entry for self is ignored.
// deliver receives every valid inbound message addressed to self. A nil
// client gets a default with DefaultSendTimeout.
func NewHTTP(self paxos.NodeID, peers map[paxos.NodeID]string, deliver Deliver, client *http.Client, log *slog.Logger) *HTTP {
	if client == nil {
		client = &http.Client{Timeout: DefaultSendTimeout}
	}
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	t := &HTTP{
		self:    self,
		peers:   make(map[paxos.NodeID]string, len(peers)),
		deliver: deliver,
		client:  client,
		log:     log.With("node", self),
		queues:  make(map[paxos.NodeID]chan []byte, len(peers)),
	}
	for id, url := range peers {
		if id == self {
			continue
		}
		t.peers[id] = url
		t.queues[id] = make(chan []byte, QueueSize)
	}
	return t
}

// Send encodes env and queues it for its peer. It never blocks: an unknown
// peer or a full queue drops the message.
func (t *HTTP) Send(env replog.Envelope) {
	t.sent.Add(1)
	q, ok := t.queues[env.To]
	if !ok {
		t.dropped.Add(1)
		return
	}
	b, err := replog.Encode(env)
	if err != nil {
		t.dropped.Add(1)
		t.log.Warn("encode message", "err", err)
		return
	}
	select {
	case q <- b:
	default:
		t.dropped.Add(1)
	}
}

// Handler returns the inbound endpoint: POST only, one wire-encoded
// message per request, addressed to this node.
func (t *HTTP) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.rejected.Add(1)
			w.Header().Set("Allow", http.MethodPost)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, MaxMessageBytes))
		if err != nil {
			t.rejected.Add(1)
			var tooBig *http.MaxBytesError
			if errors.As(err, &tooBig) {
				http.Error(w, "message too large", http.StatusRequestEntityTooLarge)
				return
			}
			http.Error(w, "read body: "+err.Error(), http.StatusBadRequest)
			return
		}
		env, err := replog.Decode(body)
		if err != nil {
			t.rejected.Add(1)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if env.To != t.self {
			t.rejected.Add(1)
			http.Error(w, fmt.Sprintf("message addressed to node %d, this is node %d", env.To, t.self), http.StatusBadRequest)
			return
		}
		t.received.Add(1)
		t.deliver(env)
		w.WriteHeader(http.StatusNoContent)
	})
}

// Run starts one sender goroutine per peer and returns when ctx is done
// and every sender has stopped. Messages queued after Run returns are never
// sent.
func (t *HTTP) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for id, q := range t.queues {
		wg.Add(1)
		go func(id paxos.NodeID, url string, q chan []byte) {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case b := <-q:
					t.post(ctx, id, url, b)
				}
			}
		}(id, t.peers[id], q)
	}
	wg.Wait()
}

func (t *HTTP) post(ctx context.Context, id paxos.NodeID, url string, b []byte) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url+Path, bytes.NewReader(b))
	if err != nil {
		t.failed.Add(1)
		t.log.Warn("build request", "peer", id, "err", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := t.client.Do(req)
	if err != nil {
		t.failed.Add(1)
		if ctx.Err() == nil {
			t.log.Debug("post to peer failed", "peer", id, "err", err)
		}
		return
	}
	io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		t.failed.Add(1)
		t.log.Debug("peer refused message", "peer", id, "status", resp.StatusCode)
		return
	}
	t.posted.Add(1)
}

// Stats returns the counters so far.
func (t *HTTP) Stats() HTTPStats {
	return HTTPStats{
		Sent: t.sent.Load(), Posted: t.posted.Load(), Dropped: t.dropped.Load(),
		Failed: t.failed.Load(), Received: t.received.Load(), Rejected: t.rejected.Load(),
	}
}
