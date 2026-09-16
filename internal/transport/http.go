package transport

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
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
	// BulkQueueSize is the number of large messages buffered per peer, in a
	// queue of their own (see BulkBytes).
	BulkQueueSize = 16
	// BulkBytes is the value payload above which a message (an Accept or a
	// Learn with a large value, a Promise reporting large values) travels in
	// the peer's bulk queue. Heartbeats and acknowledgements never wait
	// behind it, and a retransmission of a bulk message that is still
	// queued is dropped (HTTPStats.Coalesced): the leader re-sends a pending
	// Accept every heartbeat interval, and a megabyte Accept that takes
	// longer than that to post would otherwise pile up copies and starve
	// the heartbeats that keep the leader in office.
	BulkBytes = 64 << 10
	// MaxMessageBytes bounds an inbound message body. It must admit an
	// Accept or a Learn carrying a value of replog.MaxValueBytes: the wire
	// encoding is JSON with the value in base64, 4/3 of its length plus a
	// few hundred bytes of envelope (TestMaxValueFitsInOneMessage). The
	// rest of the headroom is for Promise, which carries every value the
	// acceptor accepted above the candidate's commit index.
	MaxMessageBytes = 16 << 20
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
	// Dropped counts messages discarded because the peer's queue was full,
	// the peer is unknown or the message did not encode.
	Dropped uint64
	// Coalesced counts bulk messages discarded because an identical one
	// was still queued for the peer.
	Coalesced uint64
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
// POST is dropped; the protocol's retransmission covers the loss. Messages
// are encoded by the sender goroutines, not by the caller of Send, so the
// replica's event loop never spends time encoding large values. Safe for
// concurrent use.
type HTTP struct {
	self    paxos.NodeID
	peers   map[paxos.NodeID]*peer
	deliver Deliver
	client  *http.Client
	log     *slog.Logger

	sent, posted, dropped, coalesced, failed, received, rejected atomic.Uint64
}

// peer is one destination: a queue for ordinary messages, a queue for bulk
// messages, and the bulk messages waiting in it.
type peer struct {
	id    paxos.NodeID
	url   string
	queue chan replog.Envelope
	bulk  chan replog.Envelope

	mu      sync.Mutex
	pending map[bulkKey]bool
}

// bulkKey identifies a bulk message up to retransmission: the leader's
// re-sent Accept or Learn for one (ballot, slot) carries the same value, and
// any Promise for one ballot is as good as another (values accepted after it
// was built carry that ballot or a higher one).
type bulkKey struct {
	typ    string
	ballot paxos.Ballot
	slot   paxos.Slot
}

// bulkKeyOf reports whether msg is a bulk message and its key.
func bulkKeyOf(msg replog.Message) (bulkKey, bool) {
	switch m := msg.(type) {
	case replog.Accept:
		return bulkKey{"accept", m.Ballot, m.Slot}, len(m.Value) > BulkBytes
	case replog.Learn:
		return bulkKey{"learn", m.Ballot, m.Slot}, len(m.Value) > BulkBytes
	case replog.Promise:
		n := 0
		for _, pv := range m.Accepted {
			n += len(pv.Value)
		}
		return bulkKey{"promise", m.Ballot, 0}, n > BulkBytes
	}
	return bulkKey{}, false
}

// NewHTTP builds the transport for self. peers maps every other node to its
// base URL (scheme and host, no path); an entry for self is ignored.
// deliver receives every valid inbound message addressed to self. A nil
// client gets a default with DefaultSendTimeout and a connection pool of its
// own (see NewClient).
func NewHTTP(self paxos.NodeID, peers map[paxos.NodeID]string, deliver Deliver, client *http.Client, log *slog.Logger) *HTTP {
	if client == nil {
		// One sender goroutine per peer uses at most one connection at a
		// time; a few idle ones cover the handover between requests.
		client = NewClient(DefaultSendTimeout, 4)
	}
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	t := &HTTP{
		self:    self,
		peers:   make(map[paxos.NodeID]*peer, len(peers)),
		deliver: deliver,
		client:  client,
		log:     log.With("node", self),
	}
	for id, url := range peers {
		if id == self {
			continue
		}
		t.peers[id] = &peer{
			id: id, url: url,
			queue:   make(chan replog.Envelope, QueueSize),
			bulk:    make(chan replog.Envelope, BulkQueueSize),
			pending: make(map[bulkKey]bool),
		}
	}
	return t
}

// Send queues env for its peer; a sender goroutine of Run encodes and posts
// it. It never blocks: an unknown peer or a full queue drops the message,
// and so does a bulk message identical to one still queued.
func (t *HTTP) Send(env replog.Envelope) {
	t.sent.Add(1)
	p, ok := t.peers[env.To]
	if !ok {
		t.dropped.Add(1)
		return
	}
	key, bulk := bulkKeyOf(env.Msg)
	if !bulk {
		select {
		case p.queue <- env:
		default:
			t.dropped.Add(1)
		}
		return
	}
	p.mu.Lock()
	if p.pending[key] {
		p.mu.Unlock()
		t.coalesced.Add(1)
		return
	}
	p.pending[key] = true
	p.mu.Unlock()
	select {
	case p.bulk <- env:
	default:
		p.forget(key)
		t.dropped.Add(1)
	}
}

func (p *peer) forget(key bulkKey) {
	p.mu.Lock()
	delete(p.pending, key)
	p.mu.Unlock()
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

// Run starts two sender goroutines per peer, one for each queue, and
// returns when ctx is done and every sender has stopped. Messages queued
// after Run returns are never sent.
func (t *HTTP) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for _, p := range t.peers {
		wg.Add(2)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case env := <-p.queue:
					t.encodeAndPost(ctx, p, env)
				}
			}
		}()
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case env := <-p.bulk:
					// From here on a retransmission queues a fresh copy.
					key, _ := bulkKeyOf(env.Msg)
					p.forget(key)
					t.encodeAndPost(ctx, p, env)
				}
			}
		}()
	}
	wg.Wait()
}

func (t *HTTP) encodeAndPost(ctx context.Context, p *peer, env replog.Envelope) {
	b, err := replog.Encode(env)
	if err != nil {
		t.dropped.Add(1)
		t.log.Warn("encode message", "peer", p.id, "err", err)
		return
	}
	t.post(ctx, p.id, p.url, b)
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

// NewClient returns an HTTP client with its own connection pool, never
// http.DefaultTransport: timeout bounds each request, perHost bounds both
// the open and the idle connections to one host, and dials time out after
// DialTimeout. Sharing DefaultTransport, which keeps two idle connections
// per host, makes every request beyond two concurrent ones open and close
// a TCP connection, and at a few thousand requests a second the sockets
// left in TIME_WAIT exhaust the ephemeral ports.
func NewClient(timeout time.Duration, perHost int) *http.Client {
	return &http.Client{
		Timeout: timeout,
		Transport: &http.Transport{
			DialContext:           (&net.Dialer{Timeout: DialTimeout, KeepAlive: 30 * time.Second}).DialContext,
			MaxIdleConns:          4 * perHost,
			MaxIdleConnsPerHost:   perHost,
			MaxConnsPerHost:       perHost,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   DialTimeout,
			ExpectContinueTimeout: time.Second,
		},
	}
}

// DialTimeout bounds opening one connection in clients built by NewClient.
const DialTimeout = 5 * time.Second

// Stats returns the counters so far.
func (t *HTTP) Stats() HTTPStats {
	return HTTPStats{
		Sent: t.sent.Load(), Posted: t.posted.Load(), Dropped: t.dropped.Load(), Coalesced: t.coalesced.Load(),
		Failed: t.failed.Load(), Received: t.received.Load(), Rejected: t.rejected.Load(),
	}
}
