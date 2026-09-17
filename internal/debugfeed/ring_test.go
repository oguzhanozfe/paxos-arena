package debugfeed

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/oguzhanozfe/paxos-arena/internal/paxos"
)

// accept and heartbeat are events of the two classes the readers tell apart.
func accept(slot uint64) Event {
	return Event{Kind: KindSend, Type: "accept", Slot: paxos.Slot(slot)}
}

func heartbeat() Event { return Event{Kind: KindRecv, Type: "heartbeat_ack"} }

// fill appends n events to r, a heartbeat for every sequence number in hb
// and an accept otherwise.
func fill(r *Ring, n int, hb func(seq uint64) bool) {
	for i := 1; i <= n; i++ {
		if hb != nil && hb(uint64(i)) {
			r.Append(heartbeat())
		} else {
			r.Append(accept(uint64(i)))
		}
	}
}

func seqs(evs []Event) string { return fmt.Sprint(seqList(evs)) }

func seqList(evs []Event) []uint64 {
	out := make([]uint64, len(evs))
	for i, ev := range evs {
		out[i] = ev.Seq
	}
	return out
}

func TestRingWrapAndDropped(t *testing.T) {
	odd := func(seq uint64) bool { return seq%2 == 1 }
	cases := []struct {
		name       string
		appended   int
		hb         func(uint64) bool
		after      uint64
		limit      int
		heartbeats bool
		want       string
		next       uint64
		dropped    uint64
	}{
		{"empty", 0, nil, 0, 10, true, "[]", 0, 0},
		{"not full", 3, nil, 0, 10, true, "[1 2 3]", 3, 0},
		{"not full, after the middle", 3, nil, 2, 10, true, "[3]", 3, 0},
		{"caught up", 3, nil, 3, 10, true, "[]", 3, 0},
		{"exactly full", 4, nil, 0, 10, true, "[1 2 3 4]", 4, 0},
		{"wrapped once", 5, nil, 0, 10, true, "[2 3 4 5]", 5, 1},
		{"wrapped, cursor lost six", 10, nil, 0, 10, true, "[7 8 9 10]", 10, 6},
		{"wrapped, cursor lost one", 10, nil, 5, 10, true, "[7 8 9 10]", 10, 1},
		{"wrapped, cursor at the oldest's predecessor", 10, nil, 6, 10, true, "[7 8 9 10]", 10, 0},
		{"wrapped, cursor inside", 10, nil, 8, 10, true, "[9 10]", 10, 0},
		{"wrapped, limit", 10, nil, 0, 2, true, "[7 8]", 8, 6},
		{"limit below one is one", 10, nil, 8, 0, true, "[9]", 9, 0},
		{"cursor from before a restart", 10, nil, 99, 10, true, "[7 8 9 10]", 10, 6},
		{"cursor from before a restart, empty", 0, nil, 99, 10, true, "[]", 0, 0},
		{"heartbeats skipped", 4, odd, 0, 10, false, "[2 4]", 4, 0},
		{"heartbeats kept", 4, odd, 0, 10, true, "[1 2 3 4]", 4, 0},
		// The limit counts returned events; next stops at the last one.
		{"heartbeats skipped, limit", 10, odd, 6, 1, false, "[8]", 8, 0},
		// Only skipped events after the cursor: next moves past them.
		{"only heartbeats after the cursor", 9, odd, 8, 10, false, "[]", 9, 0},
		{"heartbeats skipped, wrapped", 10, odd, 0, 10, false, "[8 10]", 10, 6},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := NewRing(4)
			fill(r, tc.appended, tc.hb)
			p := r.Read(tc.after, tc.limit, tc.heartbeats)
			if got := seqs(p.Events); got != tc.want || p.Next != tc.next || p.Dropped != tc.dropped {
				t.Errorf("Read(%d, %d, %v) = events %s next %d dropped %d, want %s next %d dropped %d",
					tc.after, tc.limit, tc.heartbeats, got, p.Next, p.Dropped, tc.want, tc.next, tc.dropped)
			}
			if p.Events == nil {
				t.Error("Events is nil, want an empty list")
			}
			for _, ev := range p.Events {
				if ev.Type == "accept" && uint64(ev.Slot) != ev.Seq {
					t.Errorf("event %d holds the payload of event %d", ev.Seq, ev.Slot)
				}
			}
		})
	}
}

// TestRingDroppedAccountingAcrossPages reads a ring that keeps wrapping with
// a small limit and checks that every appended event is either returned
// once or counted as dropped once.
func TestRingDroppedAccountingAcrossPages(t *testing.T) {
	r := NewRing(8)
	var after, returned, dropped uint64
	for round := 0; round < 50; round++ {
		fill(r, round%13, nil)
		for {
			p := r.Read(after, 3, true)
			for i, ev := range p.Events {
				if ev.Seq <= after || (i > 0 && ev.Seq != p.Events[i-1].Seq+1) {
					t.Fatalf("round %d: events %s after %d are not in order", round, seqs(p.Events), after)
				}
			}
			returned += uint64(len(p.Events))
			dropped += p.Dropped
			if p.Next == after {
				break
			}
			after = p.Next
		}
	}
	if last := r.Last(); returned+dropped != last {
		t.Errorf("returned %d + dropped %d = %d, want every appended event, %d", returned, dropped, returned+dropped, last)
	}
	if dropped == 0 {
		t.Error("no event was dropped; the test does not exercise wrapping")
	}
}

// TestRingConcurrentAppends appends from many goroutines while others read,
// under the race detector in make check: sequence numbers are unique and
// contiguous, and every reader sees them in increasing order.
func TestRingConcurrentAppends(t *testing.T) {
	const writers, perWriter = 8, 2000
	r := NewRing(Capacity)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var readers sync.WaitGroup
	errs := make(chan error, 4)
	for i := 0; i < 4; i++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			var after uint64
			for ctx.Err() == nil {
				p := r.Wait(ctx, after, 256, i%2 == 0, 5*time.Millisecond)
				for _, ev := range p.Events {
					if ev.Seq <= after {
						errs <- fmt.Errorf("reader got seq %d after %d", ev.Seq, after)
						return
					}
					after = ev.Seq
				}
				after = max(after, p.Next)
			}
		}()
	}
	var writersWG sync.WaitGroup
	seen := make([][]uint64, writers)
	for w := 0; w < writers; w++ {
		writersWG.Add(1)
		go func() {
			defer writersWG.Done()
			for i := 0; i < perWriter; i++ {
				ev := accept(uint64(i))
				if i%3 == 0 {
					ev = heartbeat()
				}
				seen[w] = append(seen[w], r.Append(ev))
			}
		}()
	}
	writersWG.Wait()
	cancel()
	readers.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	total := uint64(writers * perWriter)
	if r.Last() != total {
		t.Fatalf("last = %d, want %d", r.Last(), total)
	}
	used := make([]bool, total+1)
	for _, s := range seen {
		for _, seq := range s {
			if seq == 0 || seq > total || used[seq] {
				t.Fatalf("sequence number %d returned twice or out of range", seq)
			}
			used[seq] = true
		}
	}
	p := r.Read(0, Capacity, true)
	if p.Dropped != total-Capacity || len(p.Events) != Capacity || p.Next != total {
		t.Errorf("final read: dropped %d, %d events, next %d; want %d, %d, %d", p.Dropped, len(p.Events), p.Next, total-Capacity, Capacity, total)
	}
	for i, ev := range p.Events {
		if want := total - Capacity + 1 + uint64(i); ev.Seq != want {
			t.Fatalf("event %d has seq %d, want %d", i, ev.Seq, want)
		}
	}
}

// TestRingWaitWakesOnAppend runs on a fake clock: a waiting reader stays
// asleep through heartbeats it does not want, wakes at once for an event it
// does, and a wait with nothing to return ends when its time is up.
func TestRingWaitWakesOnAppend(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := NewRing(16)
		r.Append(accept(1))
		start := time.Now()
		var p Page
		done := make(chan struct{})
		go func() {
			defer close(done)
			p = r.Wait(context.Background(), 1, 10, false, 2*time.Second)
		}()
		synctest.Wait()
		for i := 0; i < 5; i++ {
			r.Append(heartbeat())
			synctest.Wait()
			select {
			case <-done:
				t.Fatalf("Wait returned for heartbeat %d it skips: %+v", i, p)
			default:
			}
		}
		time.Sleep(700 * time.Millisecond)
		r.Append(accept(7))
		<-done
		if seqs(p.Events) != "[7]" || p.Next != 7 || p.Dropped != 0 {
			t.Errorf("Wait = events %s next %d dropped %d, want [7] next 7 dropped 0", seqs(p.Events), p.Next, p.Dropped)
		}
		if waited := time.Since(start); waited != 700*time.Millisecond {
			t.Errorf("Wait returned after %v, want 700ms (when the event was appended)", waited)
		}

		// Nothing to return: the wait runs out, and next still moves past
		// the heartbeats examined.
		r.Append(heartbeat())
		start = time.Now()
		p = r.Wait(context.Background(), 7, 10, false, 1500*time.Millisecond)
		if waited := time.Since(start); waited != 1500*time.Millisecond || len(p.Events) != 0 || p.Next != 8 {
			t.Errorf("Wait = %d events next %d after %v, want none, next 8, after 1.5s", len(p.Events), p.Next, waited)
		}

		// A cancelled context ends the wait.
		ctx, cancel := context.WithCancel(context.Background())
		done = make(chan struct{})
		go func() {
			defer close(done)
			p = r.Wait(ctx, 8, 10, false, 2*time.Second)
		}()
		synctest.Wait()
		cancel()
		<-done
		if len(p.Events) != 0 || p.Next != 8 {
			t.Errorf("cancelled Wait = %d events next %d, want none, next 8", len(p.Events), p.Next)
		}

		// Without a wait, Wait is Read.
		start = time.Now()
		if p = r.Wait(context.Background(), 8, 10, false, 0); len(p.Events) != 0 || time.Since(start) != 0 {
			t.Errorf("Wait without a wait = %+v after %v", p, time.Since(start))
		}
	})
}

// TestRingWaitCountsDroppedWhileWaiting: events overwritten between the
// first read and the wake-up are counted, and the ones before the cursor
// that were already examined are not.
func TestRingWaitCountsDroppedWhileWaiting(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := NewRing(4)
		fill(r, 2, func(uint64) bool { return true })
		var p Page
		done := make(chan struct{})
		go func() {
			defer close(done)
			p = r.Wait(context.Background(), 0, 10, false, 2*time.Second)
		}()
		synctest.Wait()
		// Six appends while the reader sleeps: it is woken by the first but
		// cannot take the lock until all six are in, as a slow reader.
		r.mu.Lock()
		for i := 0; i < 5; i++ {
			r.appendLocked(heartbeat())
		}
		r.appendLocked(accept(8))
		r.mu.Unlock()
		<-done
		// Heartbeats 1 and 2 were examined; 3 and 4 were overwritten unseen.
		if seqs(p.Events) != "[8]" || p.Next != 8 || p.Dropped != 2 {
			t.Errorf("Wait = events %s next %d dropped %d, want [8] next 8 dropped 2", seqs(p.Events), p.Next, p.Dropped)
		}
	})
}
