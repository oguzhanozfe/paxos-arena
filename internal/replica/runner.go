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

// waiter is one Submit waiting for its key to be applied. fp is the
// fingerprint of its command: a waiter is only given the result of a command
// with the same fingerprint. abandoned is set by Submit when its context
// ends, so the event loop can forget the waiter.
type waiter struct {
	cmd       tournament.Command
	fp        [32]byte
	reply     chan submitReply
	abandoned atomic.Bool
}

// readReq is one Read. abandoned is set by Read when its context ends, so
// the event loop skips fn. fn may still be running, or run later, when Read
// has already returned ctx.Err(): a caller must not use anything fn writes
// unless Read returned nil.
type readReq struct {
	consistent bool
	fn         func(*tournament.State) error
	reply      chan error
	index      paxos.Slot
	abandoned  atomic.Bool
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
	submits chan *waiter
	reads   chan *readReq
	done    chan struct{}
	dropped atomic.Uint64

	waiters      map[tournament.IdempotencyKey][]*waiter
	nwaiters     int // total length of the waiters lists
	pendingReads map[uint64]*readReq
	applyWaits   []*readReq
	held         atomic.Int64 // requests held, as of the last event

	mu     sync.RWMutex
	status Status
	// advanced is closed, and replaced, whenever the status snapshot's
	// applied slot grows; WaitApplied waits on it. Guarded by mu.
	advanced chan struct{}
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
		submits:      make(chan *waiter),
		reads:        make(chan *readReq),
		done:         make(chan struct{}),
		advanced:     make(chan struct{}),
		waiters:      make(map[tournament.IdempotencyKey][]*waiter),
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
			r.sweepAbandoned()
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
// command with cmd's key, from whichever slot. When the command applied
// under the key has the same fingerprint as cmd, its result is returned;
// when it has a different one, cmd is answered as its own application would
// be, with tournament.KeyReused. It returns replog.NotLeaderError at once
// when this replica does not lead, unless the key's result is already
// recorded locally, in which case that result is returned with Replayed set;
// replog.ErrBusy when the leader's proposal queue is full; and
// ErrLeadershipLost when leadership changes before the key is applied. It
// returns ctx.Err() when ctx ends first; the command may still be applied
// later, and the runner forgets the wait at its next tick.
func (r *Runner) Submit(ctx context.Context, cmd tournament.Command) (tournament.Result, error) {
	w := &waiter{cmd: cmd, reply: make(chan submitReply, 1)}
	select {
	case r.submits <- w:
	case <-ctx.Done():
		return tournament.Result{}, ctx.Err()
	case <-r.done:
		return tournament.Result{}, ErrStopped
	}
	select {
	case rep := <-w.reply:
		return rep.res, rep.err
	case <-ctx.Done():
		w.abandoned.Store(true)
		return tournament.Result{}, ctx.Err()
	}
}

// Read runs fn on the event-loop goroutine against the state machine. With
// consistent set, it first runs the read-index barrier on the leader and
// waits until the applied slot reaches the index, so fn sees every command
// completed before Read was called; it returns replog.NotLeaderError or
// replog.ErrNotReady when the barrier cannot start. Without it, fn runs at
// once on whatever this replica has applied. fn must not retain the state.
//
// When ctx ends first, Read returns ctx.Err() at once. fn may be running on
// the event loop at that moment, or start later, so the caller must not use
// anything fn writes unless Read returned nil; nil is returned only after fn
// has returned.
func (r *Runner) Read(ctx context.Context, consistent bool, fn func(*tournament.State) error) error {
	req := &readReq{consistent: consistent, fn: fn, reply: make(chan error, 1)}
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
		req.abandoned.Store(true)
		return ctx.Err()
	}
}

// Status returns the last snapshot taken after an event.
func (r *Runner) Status() Status {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.status
}

// WaitApplied returns the applied slot of the status snapshot as soon as it
// exceeds after: at once when it already does, otherwise when the event
// loop applies further. It returns the snapshot's applied slot with ctx's
// error when ctx ends first, and with ErrStopped once Run has returned. A
// Read that starts after WaitApplied returned sees at least the slot it
// returned.
func (r *Runner) WaitApplied(ctx context.Context, after paxos.Slot) (paxos.Slot, error) {
	for {
		r.mu.RLock()
		applied, advanced := r.status.Applied, r.advanced
		r.mu.RUnlock()
		if applied > after {
			return applied, nil
		}
		select {
		case <-advanced:
		case <-ctx.Done():
			return applied, ctx.Err()
		case <-r.done:
			return applied, ErrStopped
		}
	}
}

// --- event-loop internals ---

func (r *Runner) handleSubmit(w *waiter) {
	if w.abandoned.Load() {
		return
	}
	if res, ok := r.core.State().Replay(w.cmd); ok {
		w.reply <- submitReply{res: res}
		return
	}
	outs, err := r.core.Submit(r.now(), w.cmd)
	if err != nil {
		w.reply <- submitReply{err: err}
		r.after(outs)
		return
	}
	w.fp = tournament.Fingerprint(w.cmd)
	r.waiters[w.cmd.Key] = append(r.waiters[w.cmd.Key], w)
	r.nwaiters++
	r.after(outs)
}

func (r *Runner) handleRead(req *readReq) {
	if req.abandoned.Load() {
		return
	}
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
	r.pendingReads[seq] = req
	r.after(outs)
}

// sweepAbandoned forgets every waiter and every read whose caller has
// returned, so that a leader that cannot get slots chosen does not
// accumulate one entry per timed-out request.
func (r *Runner) sweepAbandoned() {
	for key, ws := range r.waiters {
		kept := ws[:0]
		for _, w := range ws {
			if !w.abandoned.Load() {
				kept = append(kept, w)
			}
		}
		r.nwaiters -= len(ws) - len(kept)
		clear(ws[len(kept):])
		if len(kept) == 0 {
			delete(r.waiters, key)
		} else {
			r.waiters[key] = kept
		}
	}
	for seq, rq := range r.pendingReads {
		if rq.abandoned.Load() {
			delete(r.pendingReads, seq)
		}
	}
	kept := r.applyWaits[:0]
	for _, rq := range r.applyWaits {
		if !rq.abandoned.Load() {
			kept = append(kept, rq)
		}
	}
	clear(r.applyWaits[len(kept):])
	r.applyWaits = kept
	r.held.Store(int64(r.nwaiters + len(r.pendingReads) + len(r.applyWaits)))
}

// Held returns the number of Submit and consistent Read calls the event loop
// was holding after its last event, including calls whose context has ended
// and that the next tick will forget.
func (r *Runner) Held() int { return int(r.held.Load()) }

// after sends the core's output, reacts to its events, applies what became
// committed, refreshes the status snapshot, and only then answers the
// requests the step completed, so that a caller woken here already sees the
// new snapshot in Status.
func (r *Runner) after(outs []replog.Envelope) {
	for _, env := range outs {
		r.send(env)
	}
	type failedRead struct {
		rq  *readReq
		err error
	}
	var failed []failedRead
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
				failed = append(failed, failedRead{rq, e.Err})
			}
		case replog.LearnConflict:
			r.log.Error("learn conflict: two values for one slot", "slot", e.Slot,
				"have", e.Have.Ballot.String(), "got", e.Got.Ballot.String())
		}
	}
	applied := r.core.ApplyCommitted()

	st := r.core.Status()
	r.mu.Lock()
	if st.Applied > r.status.Applied {
		close(r.advanced)
		r.advanced = make(chan struct{})
	}
	r.status = st
	r.mu.Unlock()

	for _, f := range failed {
		f.rq.reply <- f.err
	}
	for _, a := range applied {
		if a.Err != nil {
			r.log.Warn("skipped undecodable entry", "slot", a.Slot, "err", a.Err)
		}
		if a.NoOp {
			continue
		}
		ws, ok := r.waiters[a.Key]
		if !ok {
			continue
		}
		delete(r.waiters, a.Key)
		r.nwaiters -= len(ws)
		fp := tournament.Fingerprint(a.Command)
		for _, w := range ws {
			if w.fp == fp {
				w.reply <- submitReply{res: a.Result}
				continue
			}
			// Same key, different command: the key is now recorded for the
			// command that was applied, and this one is answered as its own
			// application will be.
			if res, recorded := r.core.State().Replay(w.cmd); recorded {
				w.reply <- submitReply{res: res}
			} else {
				r.waiters[a.Key] = append(r.waiters[a.Key], w)
				r.nwaiters++
			}
		}
	}
	if len(r.waiters) > 0 && st.Role != replog.Leader {
		for key, ws := range r.waiters {
			for _, w := range ws {
				w.reply <- submitReply{err: ErrLeadershipLost}
			}
			delete(r.waiters, key)
		}
		r.nwaiters = 0
	}
	if len(r.applyWaits) > 0 {
		kept := r.applyWaits[:0]
		for _, rq := range r.applyWaits {
			switch {
			case rq.abandoned.Load():
			case st.Applied >= rq.index:
				rq.reply <- rq.fn(r.core.State())
			default:
				kept = append(kept, rq)
			}
		}
		clear(r.applyWaits[len(kept):])
		r.applyWaits = kept
	}
	r.held.Store(int64(r.nwaiters + len(r.pendingReads) + len(r.applyWaits)))
}

// failAll answers every pending request with err.
func (r *Runner) failAll(err error) {
	for key, ws := range r.waiters {
		for _, w := range ws {
			w.reply <- submitReply{err: err}
		}
		delete(r.waiters, key)
	}
	r.nwaiters = 0
	for seq, rq := range r.pendingReads {
		rq.reply <- err
		delete(r.pendingReads, seq)
	}
	for _, rq := range r.applyWaits {
		rq.reply <- err
	}
	r.applyWaits = nil
	r.held.Store(0)
}
