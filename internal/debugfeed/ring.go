package debugfeed

import (
	"context"
	"sync"
	"time"
)

// Ring is a fixed-capacity buffer of the most recent events of one replica.
// Append numbers events from 1 and, once the buffer is full, overwrites the
// oldest. A reader asks for the events after a sequence number and learns
// how many of them were overwritten before it asked. Safe for concurrent
// use. Append does constant work and never allocates; a reader holds the
// lock only while it examines and copies one page.
type Ring struct {
	mu   sync.Mutex
	buf  []Event
	last uint64        // sequence number of the newest event; 0 when empty
	wake chan struct{} // closed by the next Append; nil when nobody waits
}

// NewRing returns an empty ring of capacity events. It panics when capacity
// is not positive.
func NewRing(capacity int) *Ring {
	if capacity < 1 {
		panic("debugfeed: ring capacity must be positive")
	}
	return &Ring{buf: make([]Event, capacity)}
}

// Append stores ev under the next sequence number, which it writes into
// ev.Seq and returns, and wakes every waiting reader.
func (r *Ring) Append(ev Event) uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.appendLocked(ev)
}

// appendLocked is Append with r.mu held.
func (r *Ring) appendLocked(ev Event) uint64 {
	r.last++
	ev.Seq = r.last
	r.buf[(r.last-1)%uint64(len(r.buf))] = ev
	if r.wake != nil {
		close(r.wake)
		r.wake = nil
	}
	return ev.Seq
}

// Last returns the sequence number of the newest event, 0 when there is
// none.
func (r *Ring) Last() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.last
}

// Page is the answer to one read.
type Page struct {
	// Next is the sequence number to read after next time: the last event
	// the read examined, whether returned or skipped as a heartbeat, or the
	// cursor it started from when there was nothing to examine.
	Next uint64
	// Dropped counts the events after the cursor that were overwritten
	// before the read reached them.
	Dropped uint64
	// Events are the events returned, oldest first; never nil.
	Events []Event
}

// Read returns up to limit events numbered above after, oldest first,
// skipping heartbeat and heartbeat_ack messages unless heartbeats is set; a
// limit below 1 is taken as 1. An after above the newest sequence number is
// a cursor from before the replica restarted and numbered its events from 1
// again: Read answers it as after 0, so Next comes back lower than after.
func (r *Ring) Read(after uint64, limit int, heartbeats bool) Page {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.read(after, limit, heartbeats)
}

// read is Read with r.mu held.
func (r *Ring) read(after uint64, limit int, heartbeats bool) Page {
	if after > r.last {
		after = 0
	}
	limit = max(limit, 1)
	p := Page{Next: after, Events: []Event{}}
	first := after + 1
	if n := uint64(len(r.buf)); r.last > n && first <= r.last-n {
		oldest := r.last - n + 1
		p.Dropped = oldest - first
		first = oldest
	}
	for s := first; s <= r.last && len(p.Events) < limit; s++ {
		ev := &r.buf[(s-1)%uint64(len(r.buf))]
		p.Next = s
		if heartbeats || !ev.heartbeat() {
			p.Events = append(p.Events, *ev)
		}
	}
	return p
}

// Wait is Read that, when there is no event to return, waits until an
// appended event can be returned, wait has passed or ctx is done, and
// answers with what it has then. Dropped covers every event after the
// original cursor that was overwritten before Wait reached it.
func (r *Ring) Wait(ctx context.Context, after uint64, limit int, heartbeats bool, wait time.Duration) Page {
	r.mu.Lock()
	p := r.read(after, limit, heartbeats)
	if len(p.Events) > 0 || wait <= 0 {
		r.mu.Unlock()
		return p
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	for {
		if r.wake == nil {
			r.wake = make(chan struct{})
		}
		wake := r.wake
		r.mu.Unlock()
		select {
		case <-wake:
		case <-timer.C:
			return p
		case <-ctx.Done():
			return p
		}
		r.mu.Lock()
		dropped := p.Dropped
		// Continue from the last event examined: the ones before it were
		// skipped heartbeats, and p.Next never exceeds r.last.
		p = r.read(p.Next, limit, heartbeats)
		p.Dropped += dropped
		if len(p.Events) > 0 {
			r.mu.Unlock()
			return p
		}
	}
}
