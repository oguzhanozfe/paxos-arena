package replica

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/oguzhanozfe/paxos-arena/internal/paxos"
	"github.com/oguzhanozfe/paxos-arena/internal/replog"
	"github.com/oguzhanozfe/paxos-arena/internal/tournament"
)

// ErrLeadershipLost ends a pending Submit when this replica stops leading
// before the key was applied. The command may still be chosen later; a
// retry with the same key receives the recorded result.
var ErrLeadershipLost = errors.New("replica: leadership lost while the command was pending")

// ErrStopped is returned by Submit and Read after Run has returned.
var ErrStopped = errors.New("replica: runner stopped")

// InboxSize is the capacity of the inbound message queue. Deliver drops
// messages when it is full; the protocol's retransmission covers the loss.
const InboxSize = 4096

type submitReply struct {
	res tournament.Result
	err error
}

type submitReq struct {
	cmd   tournament.Command
	reply chan submitReply
}

type readReq struct {
	consistent bool
	fn         func(*tournament.State) error
	reply      chan error
	index      paxos.Slot
}

// Runner is the one goroutine per process that owns a Core. Transport
// receive goroutines and HTTP handler goroutines hand it work through
// channels and wait on reply channels or their context. Run starts no
// goroutine of its own; the caller runs it.
type Runner struct {
	core  *Core
	send  func(replog.Envelope)
	log   *slog.Logger
	tick  time.Duration
	start time.Time

	inbox   chan replog.Envelope
	submits chan submitReq
	reads   chan readReq
	done    chan struct{}
	dropped atomic.Uint64

	waiters      map[tournament.IdempotencyKey][]chan submitReply
	pendingReads map[uint64]*readReq
	applyWaits   []*readReq

	mu     sync.RWMutex
	status Status
}

// NewRunner wraps core. send is called on the event-loop goroutine for
// every outbound message and must not block. A nil log discards records.
func NewRunner(core *Core, send func(replog.Envelope), log *slog.Logger) *Runner {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	r := &Runner{
		core:         core,
		send:         send,
		log:          log.With("node", core.Log().Self()),
		tick:         core.Log().Config().HeartbeatInterval / 2,
		inbox:        make(chan replog.Envelope, InboxSize),
		submits:      make(chan submitReq),
		reads:        make(chan readReq),
		done:         make(chan struct{}),
		waiters:      make(map[tournament.IdempotencyKey][]chan submitReply),
		pendingReads: make(map[uint64]*readReq),
	}
	r.status = core.Status()
	return r
}

// Dropped returns the number of inbound messages Deliver discarded because
// the inbox was full.
func (r *Runner) Dropped() uint64 { return r.dropped.Load() }

// now is the runner's monotonic clock, the only place time enters the core.
func (r *Runner) now() time.Duration { return time.Since(r.start) }

// Run executes the event loop until ctx is done, then fails every pending
// request and returns nil. It returns the store error if the core fails.
func (r *Runner) Run(ctx context.Context) error {
	r.start = time.Now()
	ticker := time.NewTicker(r.tick)
	defer ticker.Stop()
	defer close(r.done)
	// Replay whatever the store already holds before taking requests.
	r.after(r.core.Tick(r.now()))
	for {
		select {
		case <-ctx.Done():
			r.failAll(ErrStopped)
			return nil
		case env := <-r.inbox:
			r.after(r.core.Step(r.now(), env))
		case <-ticker.C:
			r.after(r.core.Tick(r.now()))
		case req := <-r.submits:
			r.handleSubmit(req)
		case req := <-r.reads:
			r.handleRead(req)
		}
		if err := r.core.Failed(); err != nil {
			r.log.Error("core failed", "err", err)
			r.failAll(err)
			return err
		}
	}
}

// Deliver is the transport callback: it enqueues env without blocking and
// drops it when the inbox is full.
func (r *Runner) Deliver(env replog.Envelope) {
	select {
	case r.inbox <- env:
	default:
		r.dropped.Add(1)
	}
}

// Submit proposes cmd and waits until the local state machine applies a
// command with cmd's key, from whichever slot. It returns
// replog.ErrNotLeader at once when this replica does not lead, unless the
// key's result is already recorded locally, in which case that result is
// returned with Replayed set. It returns ErrLeadershipLost when leadership
// changes before the key is applied, and ctx.Err() when ctx ends first (the
// command may still be applied later).
func (r *Runner) Submit(ctx context.Context, cmd tournament.Command) (tournament.Result, error) {
	req := submitReq{cmd: cmd, reply: make(chan submitReply, 1)}
	select {
	case r.submits <- req:
	case <-ctx.Done():
		return tournament.Result{}, ctx.Err()
	case <-r.done:
		return tournament.Result{}, ErrStopped
	}
	select {
	case rep := <-req.reply:
		return rep.res, rep.err
	case <-ctx.Done():
		return tournament.Result{}, ctx.Err()
	}
}

// Read runs fn on the event-loop goroutine against the state machine. With
// consistent set, it first runs the read-index barrier on the leader and
// waits until the applied slot reaches the index, so fn sees every command
// completed before Read was called; it returns replog.ErrNotLeader or
// replog.ErrNotReady when the barrier cannot start. Without it, fn runs at
// once on whatever this replica has applied. fn must not retain the state.
func (r *Runner) Read(ctx context.Context, consistent bool, fn func(*tournament.State) error) error {
	req := readReq{consistent: consistent, fn: fn, reply: make(chan error, 1)}
	select {
	case r.reads <- req:
	case <-ctx.Done():
		return ctx.Err()
	case <-r.done:
		return ErrStopped
	}
	select {
	case err := <-req.reply:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Status returns the last snapshot taken after an event.
func (r *Runner) Status() Status {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.status
}

// --- event-loop internals ---

func (r *Runner) handleSubmit(req submitReq) {
	if res, ok := r.core.State().Replay(req.cmd); ok {
		req.reply <- submitReply{res: res}
		return
	}
	outs, err := r.core.Submit(r.now(), req.cmd)
	if err != nil {
		req.reply <- submitReply{err: err}
		r.after(outs)
		return
	}
	r.waiters[req.cmd.Key] = append(r.waiters[req.cmd.Key], req.reply)
	r.after(outs)
}

func (r *Runner) handleRead(req readReq) {
	if !req.consistent {
		req.reply <- req.fn(r.core.State())
		return
	}
	seq, outs, err := r.core.ReadIndex(r.now())
	if err != nil {
		req.reply <- err
		r.after(outs)
		return
	}
	rq := req
	r.pendingReads[seq] = &rq
	r.after(outs)
}

// after sends the core's output, reacts to its events, applies what became
// committed and refreshes the status snapshot.
func (r *Runner) after(outs []replog.Envelope) {
	for _, env := range outs {
		r.send(env)
	}
	for _, ev := range r.core.Events() {
		switch e := ev.(type) {
		case replog.LeaderChanged:
			if e.Self {
				r.log.Info("became leader", "ballot", e.Ballot.String())
			} else {
				r.log.Info("leader changed", "leader", e.Leader, "ballot", e.Ballot.String())
			}
		case replog.ReadReady:
			if rq, ok := r.pendingReads[e.Seq]; ok {
				delete(r.pendingReads, e.Seq)
				rq.index = e.Index
				r.applyWaits = append(r.applyWaits, rq)
			}
		case replog.ReadFailed:
			if rq, ok := r.pendingReads[e.Seq]; ok {
				delete(r.pendingReads, e.Seq)
				rq.reply <- e.Err
			}
		case replog.LearnConflict:
			r.log.Error("learn conflict: two values for one slot", "slot", e.Slot,
				"have", e.Have.Ballot.String(), "got", e.Got.Ballot.String())
		}
	}
	for _, a := range r.core.ApplyCommitted() {
		if a.Err != nil {
			r.log.Warn("skipped undecodable entry", "slot", a.Slot, "err", a.Err)
		}
		if a.NoOp {
			continue
		}
		if chans, ok := r.waiters[a.Key]; ok {
			delete(r.waiters, a.Key)
			for _, ch := range chans {
				ch <- submitReply{res: a.Result}
			}
		}
	}
	if len(r.waiters) > 0 && r.core.Log().Role() != replog.Leader {
		for key, chans := range r.waiters {
			for _, ch := range chans {
				ch <- submitReply{err: ErrLeadershipLost}
			}
			delete(r.waiters, key)
		}
	}
	if len(r.applyWaits) > 0 {
		applied := r.core.State().Applied()
		kept := r.applyWaits[:0]
		for _, rq := range r.applyWaits {
			if applied >= rq.index {
				rq.reply <- rq.fn(r.core.State())
			} else {
				kept = append(kept, rq)
			}
		}
		r.applyWaits = kept
	}
	st := r.core.Status()
	r.mu.Lock()
	r.status = st
	r.mu.Unlock()
}

// failAll answers every pending request with err.
func (r *Runner) failAll(err error) {
	for key, chans := range r.waiters {
		for _, ch := range chans {
			ch <- submitReply{err: err}
		}
		delete(r.waiters, key)
	}
	for seq, rq := range r.pendingReads {
		rq.reply <- err
		delete(r.pendingReads, seq)
	}
	for _, rq := range r.applyWaits {
		rq.reply <- err
	}
	r.applyWaits = nil
}
