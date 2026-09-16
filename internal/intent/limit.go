package intent

import (
	"sync"
	"time"
)

// limiterSweepAt is the number of tracked keys at which a limiter forgets
// the keys whose buckets are full again.
const limiterSweepAt = 4096

// limiter is a set of token buckets keyed by string, kept as the generic
// cell rate algorithm: per key, the theoretical arrival time of the next
// request. A request is allowed when that time is at most Burst-1
// intervals ahead of now, and pushes it one interval further.
type limiter struct {
	rate Rate
	mu   sync.Mutex
	tat  map[string]time.Time
}

func newLimiter(r Rate) *limiter { return &limiter{rate: r, tat: make(map[string]time.Time)} }

// allow reports whether a request for key may proceed at now and, when it
// may not, how long until it may.
func (l *limiter) allow(key string, now time.Time) (time.Duration, bool) {
	if l.rate.Burst < 0 || l.rate.Every <= 0 {
		return 0, true
	}
	burst := l.rate.Burst
	if burst == 0 {
		burst = 1
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	tat, ok := l.tat[key]
	if !ok || tat.Before(now) {
		tat = now
	}
	tolerance := time.Duration(burst-1) * l.rate.Every
	if wait := tat.Sub(now) - tolerance; wait > 0 {
		return wait, false
	}
	if !ok && len(l.tat) >= limiterSweepAt {
		for k, t := range l.tat {
			if !t.After(now) {
				delete(l.tat, k)
			}
		}
	}
	l.tat[key] = tat.Add(l.rate.Every)
	return 0, true
}

// size returns the number of keys tracked.
func (l *limiter) size() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.tat)
}
