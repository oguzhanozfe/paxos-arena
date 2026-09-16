package intent

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/oguzhanozfe/paxos-arena/internal/paxos"
	"github.com/oguzhanozfe/paxos-arena/internal/replog"
	"github.com/oguzhanozfe/paxos-arena/internal/session"
	"github.com/oguzhanozfe/paxos-arena/internal/tournament"
)

// beginRead runs steps 2 and 3 of a GET: token and rate limit.
func (s *Server) beginRead(w http.ResponseWriter, r *http.Request, l *limiter) (session.Claims, bool) {
	c, ok := s.authenticate(w, r)
	if !ok {
		return session.Claims{}, false
	}
	if !s.allow(w, l, c.Player) {
		return session.Claims{}, false
	}
	return c, true
}

// queryInt parses an optional decimal query parameter within lo..hi.
func (s *Server) queryInt(w http.ResponseWriter, q url.Values, name string, def, lo, hi int64) (int64, bool) {
	if !q.Has(name) {
		return def, true
	}
	v := q.Get(name)
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || v == "" || v[0] == '+' || n < lo || n > hi {
		s.malformed(w, fmt.Sprintf("%s must be an integer from %d to %d", name, lo, hi))
		return 0, false
	}
	return n, true
}

const maxSlotQuery = 1<<63 - 1

// waitMinSlot waits up to MinSlotWait for this replica to apply slot
// minSlot; when it does not, a follower that knows the leader answers 307
// and any other replica 503 replica_behind.
func (s *Server) waitMinSlot(w http.ResponseWriter, r *http.Request, minSlot int64) bool {
	if minSlot <= 0 {
		return true
	}
	ctx, cancel := context.WithTimeout(r.Context(), s.cfg.MinSlotWait)
	defer cancel()
	if _, err := s.backend.WaitApplied(ctx, paxos.Slot(minSlot-1)); err == nil {
		return true
	}
	st := s.backend.Status()
	if st.Role != replog.Leader && st.Leader != 0 && st.Leader != s.cfg.Self {
		if _, known := s.cfg.PublicURLs[st.Leader]; known {
			s.redirect(w, r, st.Leader)
			return false
		}
	}
	retryAfter(w, time.Second)
	s.writeError(w, http.StatusServiceUnavailable, CodeReplicaBehind, fmt.Sprintf("this replica has not applied slot %d yet; retry the same request", minSlot), true)
	return false
}

// notFound answers 404 with a state machine code.
func (s *Server) notFound(w http.ResponseWriter, applied paxos.Slot, code tournament.Code, msg string) {
	setSlot(w.Header(), HeaderAppliedSlot, uint64(applied))
	s.writeError(w, http.StatusNotFound, string(code), msg, false)
}

func (s *Server) handleTournaments(w http.ResponseWriter, r *http.Request) {
	c, ok := s.beginRead(w, r, s.readLimit)
	if !ok {
		return
	}
	q := r.URL.Query()
	status := "open"
	if q.Has(QueryStatus) {
		status = q.Get(QueryStatus)
	}
	switch status {
	case "open", "closed", "voided", "settled", "all":
	default:
		s.malformed(w, "status must be open, closed, voided, settled or all")
		return
	}
	offset, ok := s.queryInt(w, q, QueryOffset, 0, 0, 1<<31-1)
	if !ok {
		return
	}
	limit, ok := s.queryInt(w, q, QueryLimit, DefaultPageLimit, 1, MaxPageLimit)
	if !ok {
		return
	}
	pid := tournament.PlayerID(c.Player)
	resp := TournamentListResponse{Offset: int32(offset), Limit: int32(limit), Tournaments: []TournamentSummary{}}
	if !s.readState(w, func(st *tournament.State) error {
		resp.AppliedSlot = int64(st.Applied())
		rec, _ := st.Player(pid)
		n := int64(0)
		for _, id := range st.Tournaments() {
			h, _ := st.Header(id)
			if h.Rules.Game == "" || (status != "all" && h.Status.String() != status) {
				continue
			}
			if n >= offset && n < offset+limit {
				resp.Tournaments = append(resp.Tournaments, tournamentSummary(st, h, pid, rec.Binding))
			}
			n++
		}
		resp.Total = int32(n)
		return nil
	}) {
		return
	}
	setSlot(w.Header(), HeaderAppliedSlot, uint64(resp.AppliedSlot))
	s.writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleRound(w http.ResponseWriter, r *http.Request) {
	c, ok := s.beginRead(w, r, s.readLimit)
	if !ok {
		return
	}
	tid, ok := s.pathTournament(w, r)
	if !ok {
		return
	}
	round, ok := s.pathRound(w, r)
	if !ok {
		return
	}
	minSlot, ok := s.queryInt(w, r.URL.Query(), QueryMinSlot, 0, 0, maxSlotQuery)
	if !ok || !s.waitMinSlot(w, r, minSlot) {
		return
	}
	pid := tournament.PlayerID(c.Player)
	var (
		resp    RoundResponse
		missing tournament.Code
		applied paxos.Slot
	)
	if !s.readState(w, func(st *tournament.State) error {
		applied = st.Applied()
		_, joined, exists := st.Entry(tid, pid)
		switch {
		case !exists:
			missing = tournament.UnknownTournament
			return nil
		case !joined:
			missing = tournament.NotJoined
			return nil
		}
		rec, dealt := st.Round(tid, pid, round)
		v, built := BuildRoundView(st, tid, pid, round, 0)
		if !dealt || !built {
			missing = tournament.RoundNotStarted
			return nil
		}
		p, _ := st.Player(pid)
		resp = RoundResponse{Slot: int64(rec.UpdatedAt), NextSeq: int64(p.LastSeq + 1), Round: v}
		return nil
	}) {
		return
	}
	if missing != "" {
		s.notFound(w, applied, missing, fmt.Sprintf("no round %d of player %s in tournament %s (%s)", round, pid, tid, missing))
		return
	}
	setSlot(w.Header(), HeaderAppliedSlot, uint64(applied))
	s.writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleLeaderboard(w http.ResponseWriter, r *http.Request) {
	c, ok := s.beginRead(w, r, s.readLimit)
	if !ok {
		return
	}
	tid, ok := s.pathTournament(w, r)
	if !ok {
		return
	}
	q := r.URL.Query()
	offset, ok := s.queryInt(w, q, QueryOffset, 0, 0, 1<<31-1)
	if !ok {
		return
	}
	limit, ok := s.queryInt(w, q, QueryLimit, DefaultPageLimit, 1, MaxPageLimit)
	if !ok {
		return
	}
	minSlot, ok := s.queryInt(w, q, QueryMinSlot, 0, 0, maxSlotQuery)
	if !ok || !s.waitMinSlot(w, r, minSlot) {
		return
	}
	pid := tournament.PlayerID(c.Player)
	var (
		resp  LeaderboardResponse
		found bool
	)
	if !s.readState(w, func(st *tournament.State) error {
		resp.AppliedSlot = int64(st.Applied())
		t, ok := st.Tournament(tid)
		if !ok || t.Rules.Game == "" {
			return nil
		}
		found = true
		rows, me := leaderboard(st, t, pid)
		resp.TournamentID = string(tid)
		resp.Status = t.Status.String()
		resp.Final = t.Status != tournament.Open
		resp.Entrants = int32(len(t.Entries))
		resp.Offset, resp.Limit = int32(offset), int32(limit)
		resp.Me = me
		resp.Rows = []LeaderboardRow{}
		if offset < int64(len(rows)) {
			resp.Rows = rows[offset:min(int64(len(rows)), offset+limit)]
		}
		return nil
	}) {
		return
	}
	if !found {
		s.notFound(w, paxos.Slot(resp.AppliedSlot), tournament.UnknownTournament, fmt.Sprintf("no play tournament %q", tid))
		return
	}
	setSlot(w.Header(), HeaderAppliedSlot, uint64(resp.AppliedSlot))
	s.writeJSON(w, http.StatusOK, resp)
}

// poll is one open events request.
type poll struct {
	cancel     context.CancelFunc
	superseded atomic.Bool
}

// openPoll registers a player's events request and ends the previous one.
func (s *Server) openPoll(p tournament.PlayerID, cancel context.CancelFunc) *poll {
	s.mu.Lock()
	defer s.mu.Unlock()
	if prev := s.polls[p]; prev != nil {
		prev.superseded.Store(true)
		prev.cancel()
	}
	pl := &poll{cancel: cancel}
	s.polls[p] = pl
	return pl
}

func (s *Server) closePoll(p tournament.PlayerID, pl *poll) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.polls[p] == pl {
		delete(s.polls, p)
	}
}

func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	c, ok := s.beginRead(w, r, s.pollLimit)
	if !ok {
		return
	}
	q := r.URL.Query()
	cursor, ok := s.queryInt(w, q, QueryCursor, 0, 0, maxSlotQuery)
	if !ok {
		return
	}
	waitMs, ok := s.queryInt(w, q, QueryWaitMs, DefaultEventsWait.Milliseconds(), 0, MaxEventsWait.Milliseconds())
	if !ok {
		return
	}
	var tid tournament.TournamentID
	if q.Has(QueryTournamentID) {
		tid = tournament.TournamentID(q.Get(QueryTournamentID))
		if err := tournament.ValidateTournamentID(tid); err != nil {
			s.malformed(w, "tournament_id: "+err.Error())
			return
		}
	}
	pid := tournament.PlayerID(c.Player)
	ctx, cancel := context.WithTimeout(r.Context(), time.Duration(waitMs)*time.Millisecond)
	defer cancel()
	pl := s.openPoll(pid, cancel)
	defer s.closePoll(pid, pl)

	from := paxos.Slot(cursor)
	for first := true; ; first = false {
		var (
			resp    = EventsResponse{Cursor: cursor, Events: []EventItem{}}
			missing tournament.Code
			applied paxos.Slot
		)
		if !s.readState(w, func(st *tournament.State) error {
			applied = st.Applied()
			resp.AppliedSlot = int64(applied)
			if tid != "" {
				if _, joined, exists := st.Entry(tid, pid); !exists {
					missing = tournament.UnknownTournament
					return nil
				} else if !joined {
					missing = tournament.NotJoined
					return nil
				}
			}
			if applied < from {
				return nil // behind the client's cursor: keep the cursor
			}
			evs := st.Events(from, tid, pid, MaxEventsPerResponse)
			resp.Cursor = int64(applied)
			if len(evs) == 0 {
				return nil
			}
			last := evs[len(evs)-1].Slot
			if len(st.Events(last, tid, pid, 1)) > 0 {
				resp.HasMore = true
				resp.Cursor = int64(last)
			}
			resp.Events = eventItems(evs)
			return nil
		}) {
			return
		}
		if first && missing != "" {
			s.notFound(w, applied, missing, fmt.Sprintf("tournament %s: %s", tid, missing))
			return
		}
		if len(resp.Events) > 0 || ctx.Err() != nil {
			setSlot(w.Header(), HeaderAppliedSlot, uint64(applied))
			s.writeJSON(w, http.StatusOK, resp)
			return
		}
		_, err := s.backend.WaitApplied(ctx, applied)
		switch {
		case r.Context().Err() != nil:
			return // the client went away
		case pl.superseded.Load():
			// A newer request of the player ends this one at once, with no
			// events and the cursor of the last read.
			setSlot(w.Header(), HeaderAppliedSlot, uint64(applied))
			s.writeJSON(w, http.StatusOK, resp)
			return
		case err != nil && ctx.Err() == nil:
			retryAfter(w, time.Second)
			s.writeError(w, http.StatusServiceUnavailable, CodeUnavailable, "the replica is stopping; retry the same request", true)
			return
		}
	}
}
