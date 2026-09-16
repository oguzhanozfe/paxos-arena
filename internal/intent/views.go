package intent

import (
	"sort"

	"github.com/oguzhanozfe/paxos-arena/internal/game"
	"github.com/oguzhanozfe/paxos-arena/internal/ledger"
	"github.com/oguzhanozfe/paxos-arena/internal/paxos"
	"github.com/oguzhanozfe/paxos-arena/internal/tournament"
)

// BuildRoundView is the view of section 7.2 of round round of player p in
// tournament t, with every accepted move, as of slot asOf (0: current), or
// false when the round does not exist or was dealt after asOf. The status
// is finished, with the reason, the final score and the seed, only when the
// round finished at or before asOf. It is a pure function of the state: the
// simulator's checker calls it for invariant P3.
func BuildRoundView(st *tournament.State, t tournament.TournamentID, p tournament.PlayerID, round int, asOf paxos.Slot) (RoundView, bool) {
	return BuildRoundViewAt(st, t, p, round, -1, asOf)
}

// BuildRoundViewAt is BuildRoundView with the board replayed from the seed
// and the first moves accepted moves only (all of them when moves is
// negative or above their number): the view a command's result describes,
// whose outcome recorded the move count, rebuilt identically at any later
// time.
func BuildRoundViewAt(st *tournament.State, t tournament.TournamentID, p tournament.PlayerID, round, moves int, asOf paxos.Slot) (RoundView, bool) {
	rec, ok := st.Round(t, p, round)
	if !ok || (asOf != 0 && asOf < rec.StartedAt) {
		return RoundView{}, false
	}
	if moves < 0 || moves > len(rec.Moves) {
		moves = len(rec.Moves)
	}
	board, _ := game.Replay(rec.Seed, rec.Moves[:moves])
	finished := rec.Status == tournament.RoundDone && (asOf == 0 || rec.FinishedAt <= asOf)
	v := RoundView{
		TournamentID:    string(t),
		Round:           int32(round),
		Status:          string(tournament.RoundInPlay),
		MoveIndex:       int32(moves),
		Columns:         make([]ColumnView, game.ColumnCount),
		WasteTop:        board.WasteCard().String(),
		WasteCount:      int32(len(board.Waste)),
		StockCount:      int32(len(board.Stock)),
		Cleared:         int32(board.Cleared()),
		Score:           board.Score(),
		PlayableColumns: []int32{},
		LastMove:        MoveView{Column: game.NoColumn},
		StartedAtMs:     rec.StartedAtMs,
		DeadlineMs:      rec.DeadlineMs,
		Commitment:      game.Commit(rec.Seed).String(),
	}
	for c, col := range board.Columns {
		cards := make([]string, len(col))
		for i, card := range col {
			cards[i] = card.String()
		}
		v.Columns[c] = ColumnView{Cards: cards}
	}
	if moves > 0 {
		m := rec.Moves[moves-1]
		v.LastMove = MoveView{Kind: string(m.Kind), Column: int32(m.Column), Card: board.WasteCard().String()}
	}
	if finished {
		v.Status = string(tournament.RoundDone)
		v.FinishReason = string(rec.Reason)
		v.Score = rec.Score
		v.Seed = rec.Seed.String()
		return v, true
	}
	for _, c := range board.Playable() {
		v.PlayableColumns = append(v.PlayableColumns, int32(c))
	}
	v.CanDraw = len(board.Stock) > 0
	return v, true
}

// bps is floor(x * b / 10000) without overflow for the module's money
// bounds, as the state machine computes the rake.
func bps(x ledger.Money, b uint32) ledger.Money {
	q, r := x/10000, x%10000
	return q*ledger.Money(b) + r*ledger.Money(b)/10000
}

// tournamentSummary describes tournament h to player p, whose binding is b
// (zero when unknown).
func tournamentSummary(st *tournament.State, h tournament.Header, p tournament.PlayerID, b tournament.Binding) TournamentSummary {
	prize := make([]int32, len(h.Rules.PrizeBps))
	for i, x := range h.Rules.PrizeBps {
		prize[i] = int32(x)
	}
	pool := h.Pool
	if h.Status == tournament.Open {
		fees := h.Rules.EntryFee * ledger.Money(h.Entrants)
		pool = fees - bps(fees, h.Rules.RakeBps)
	}
	s := TournamentSummary{
		TournamentID:     string(h.ID),
		Status:           h.Status.String(),
		Game:             string(h.Rules.Game),
		Rounds:           game.Rounds,
		RoundTimeLimitMs: game.RoundTimeLimitMs,
		EntryFee:         int64(h.Rules.EntryFee),
		RakeBps:          int32(h.Rules.RakeBps),
		PrizeBps:         prize,
		MinEntrants:      int32(h.Rules.MinEntrants),
		MaxEntrants:      int32(h.Rules.MaxEntrants),
		MinAge:           int32(h.Rules.MinAge),
		MaxScore:         h.Rules.MaxScore,
		Entrants:         int32(h.Entrants),
		ProjectedPool:    int64(pool),
		CreatedSlot:      int64(h.CreatedAt),
	}
	_, joined, _ := st.Entry(h.ID, p)
	s.Joined = joined
	open := h.Status == tournament.Open
	s.Eligible = open && !joined && b.Player == p && h.Entrants < h.Rules.MaxEntrants &&
		!h.Rules.Exclusions.Contains(b.Jurisdiction) && b.Age >= h.Rules.MinAge
	if joined {
		prog := st.Progress(h.ID, p)
		s.RoundsFinished = int32(prog.Finished)
		s.RoundInPlay = int32(prog.InPlay)
		if open {
			s.NextRound = int32(prog.Next)
		}
	}
	return s
}

// leaderboard builds every row of play tournament t in place order, and
// player p's own row.
func leaderboard(st *tournament.State, t tournament.Tournament, p tournament.PlayerID) ([]LeaderboardRow, LeaderboardRow) {
	return leaderboardPage(st, t, p, 0, int64(len(t.Entries)))
}

// leaderboardPage builds the rows of play tournament t at places offset+1
// to offset+limit, and player p's own row (zero when p has no entry). It
// ranks every entry once, in O(n log n) with a few map lookups per entry,
// and builds the rows it returns only, so a page costs O(n log n + limit)
// whatever the size of the tournament.
func leaderboardPage(st *tournament.State, t tournament.Tournament, p tournament.PlayerID, offset, limit int64) ([]LeaderboardRow, LeaderboardRow) {
	book := st.Ledger()
	tid := string(t.ID)
	// ranked is one entrant in place order: the entry's index, and for an
	// open tournament the progress the ranking uses.
	type ranked struct {
		player   tournament.PlayerID
		entry    int
		standing int
		prog     tournament.EntryProgress
		join     uint32
	}
	var payouts map[tournament.PlayerID][]int
	if len(t.Payouts) > 0 {
		payouts = make(map[tournament.PlayerID][]int, len(t.Payouts))
		for i, po := range t.Payouts {
			payouts[po.Player] = append(payouts[po.Player], i)
		}
	}
	rs := make([]ranked, 0, len(t.Entries))
	if t.Status == tournament.Open {
		for i, e := range t.Entries {
			rs = append(rs, ranked{player: e.Player.ID, entry: i, standing: -1, prog: st.Progress(t.ID, e.Player.ID), join: e.JoinSeq})
		}
		sort.SliceStable(rs, func(i, j int) bool {
			a, b := &rs[i], &rs[j]
			switch {
			case a.prog.Total != b.prog.Total:
				return a.prog.Total > b.prog.Total
			case a.prog.Finished != b.prog.Finished:
				return a.prog.Finished > b.prog.Finished
			case a.prog.ReachedAt != b.prog.ReachedAt:
				return a.prog.ReachedAt < b.prog.ReachedAt
			}
			return a.join < b.join
		})
	} else {
		for i, sd := range t.Standings {
			rs = append(rs, ranked{player: sd.Player, entry: -1, standing: i})
		}
	}
	row := func(place int, r ranked) LeaderboardRow {
		prog := r.prog
		if r.standing >= 0 {
			prog = st.Progress(t.ID, r.player)
		}
		out := LeaderboardRow{Place: int32(place), PlayerID: string(r.player), TotalScore: prog.Total, RoundsFinished: int32(prog.Finished),
			Claimed: book.Has(ledger.ClaimKey(tid, string(r.player)))}
		if r.entry >= 0 {
			out.Scored = t.Entries[r.entry].Scored
		}
		if r.standing >= 0 {
			sd := t.Standings[r.standing]
			out.Place = int32(sd.Place)
			out.TotalScore = sd.Score
			out.Scored = sd.Scored
		}
		for _, i := range payouts[r.player] {
			out.Amount += int64(t.Payouts[i].Amount)
			out.Withheld = out.Withheld || t.Payouts[i].Withheld
		}
		return out
	}
	rows := []LeaderboardRow{}
	if offset < int64(len(rs)) {
		end := int64(len(rs))
		if limit < end-offset {
			end = offset + limit
		}
		rows = make([]LeaderboardRow, 0, end-offset)
		for i := offset; i < end; i++ {
			rows = append(rows, row(int(i)+1, rs[i]))
		}
	}
	me := LeaderboardRow{}
	for i := len(rs) - 1; i >= 0; i-- {
		if rs[i].player == p {
			me = row(i+1, rs[i])
			break
		}
	}
	return rows, me
}

// eventItem renders one event.
func eventItem(ev tournament.EventRecord) EventItem {
	return EventItem{
		Slot:           int64(ev.Slot),
		Type:           string(ev.Type),
		TournamentID:   string(ev.Tournament),
		PlayerID:       string(ev.Player),
		Round:          int32(ev.Round),
		Status:         ev.Status,
		Score:          ev.Score,
		TotalScore:     ev.TotalScore,
		RoundsFinished: int32(ev.RoundsFinished),
		Amount:         int64(ev.Amount),
		TimeMs:         ev.TimeMs,
	}
}

// eventItems renders events, keeping only the last leaderboard_changed of
// each tournament.
func eventItems(evs []tournament.EventRecord) []EventItem {
	last := make(map[tournament.TournamentID]int)
	for i, ev := range evs {
		if ev.Type == tournament.EventLeaderboardChanged {
			last[ev.Tournament] = i
		}
	}
	out := make([]EventItem, 0, len(evs))
	for i, ev := range evs {
		if ev.Type == tournament.EventLeaderboardChanged && last[ev.Tournament] != i {
			continue
		}
		out = append(out, eventItem(ev))
	}
	return out
}
