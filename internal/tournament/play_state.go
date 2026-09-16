package tournament

import (
	"fmt"
	"sort"

	"github.com/oguzhanozfe/paxos-arena/internal/game"
	"github.com/oguzhanozfe/paxos-arena/internal/ledger"
	"github.com/oguzhanozfe/paxos-arena/internal/paxos"
)

// roundKey identifies one round of one entry.
type roundKey struct {
	t TournamentID
	p PlayerID
	r int
}

// playerState is a player record with the tournaments the player entered,
// in entry order. entered is derived from the Enter commands applied and is
// not part of the encoded record.
type playerState struct {
	rec     PlayerRecord
	entered []TournamentID
}

// playState is the part of State that play commands own.
type playState struct {
	bindings map[DeviceID]*Binding
	players  map[PlayerID]*playerState
	rounds   map[roundKey]*RoundRecord
	events   []EventRecord
	// shared indexes, per tournament, the events every entrant sees;
	// own indexes, per player, the events only that player sees. Both hold
	// positions in events, ascending.
	shared map[TournamentID][]int
	own    map[PlayerID][]int
	// boardAt remembers, per tournament, the last slot that recorded a
	// leaderboard_changed event, so a slot records at most one.
	boardAt map[TournamentID]paxos.Slot

	// What the command being applied changed, in the order it changed it,
	// for the hash chain.
	dirtyBindings []DeviceID
	dirtyPlayers  []PlayerID
	dirtyRounds   []roundKey
	eventsBefore  int
}

func newPlayState() playState {
	return playState{
		bindings: make(map[DeviceID]*Binding),
		players:  make(map[PlayerID]*playerState),
		rounds:   make(map[roundKey]*RoundRecord),
		shared:   make(map[TournamentID][]int),
		own:      make(map[PlayerID][]int),
		boardAt:  make(map[TournamentID]paxos.Slot),
	}
}

// begin clears the change tracking before a command is applied.
func (p *playState) begin() {
	p.dirtyBindings = p.dirtyBindings[:0]
	p.dirtyPlayers = p.dirtyPlayers[:0]
	p.dirtyRounds = p.dirtyRounds[:0]
	p.eventsBefore = len(p.events)
}

func (p *playState) touchPlayer(id PlayerID) {
	for _, x := range p.dirtyPlayers {
		if x == id {
			return
		}
	}
	p.dirtyPlayers = append(p.dirtyPlayers, id)
}

func (p *playState) touchRound(k roundKey) {
	for _, x := range p.dirtyRounds {
		if x == k {
			return
		}
	}
	p.dirtyRounds = append(p.dirtyRounds, k)
}

// mixPlay writes the canonical encoding of every binding, player record and
// round record the applied command changed and every event it recorded. It
// writes nothing for a command that touched no play state.
func (s *State) mixPlay(write func([]byte)) {
	p := &s.play
	line := func(prefix string, v any) {
		write([]byte(prefix))
		write(mustMarshal(v))
		write([]byte{'\n'})
	}
	for _, d := range p.dirtyBindings {
		line("binding:", p.bindings[d])
	}
	for _, id := range p.dirtyPlayers {
		line("player:", p.players[id].rec)
	}
	for _, k := range p.dirtyRounds {
		line("round:", p.rounds[k])
	}
	for _, ev := range p.events[p.eventsBefore:] {
		line("event:", ev)
	}
}

// --- reads ---

// Clock returns the state machine's clock: the largest ReceivedAt of every
// command applied so far, in Unix milliseconds.
func (s *State) Clock() int64 { return s.clock }

// Binding returns the binding of device d.
func (s *State) Binding(d DeviceID) (Binding, bool) {
	b, ok := s.play.bindings[d]
	if !ok {
		return Binding{}, false
	}
	return *b, true
}

// Player returns the record of player id.
func (s *State) Player(id PlayerID) (PlayerRecord, bool) {
	ps, ok := s.play.players[id]
	if !ok {
		return PlayerRecord{}, false
	}
	return ps.rec, true
}

// Round returns a deep copy of round r of player p's entry in tournament t.
func (s *State) Round(t TournamentID, p PlayerID, r int) (RoundRecord, bool) {
	rec, ok := s.play.rounds[roundKey{t, p, r}]
	if !ok {
		return RoundRecord{}, false
	}
	c := *rec
	c.Moves = append([]game.Move{}, rec.Moves...)
	return c, true
}

// EntryProgress summarises one entry's rounds.
type EntryProgress struct {
	// Dealt counts the rounds dealt.
	Dealt int
	// Finished counts the finished rounds.
	Finished int
	// Total sums the scores of the finished rounds.
	Total int64
	// ReachedAt is the slot at which the last finished round finished, the
	// slot at which the entry reached Total; 0 before any round finished.
	ReachedAt paxos.Slot
	// InPlay is the round in play, 0 when none.
	InPlay int
	// Next is the round the player may deal now, 0 when none: the lowest
	// round not dealt, once every earlier round is finished.
	Next int
}

// Progress returns the progress of player p's entry in tournament t. It
// does not check that the entry exists.
func (s *State) Progress(t TournamentID, p PlayerID) EntryProgress {
	var out EntryProgress
	next := 0
	for r := 1; r <= game.Rounds; r++ {
		rec, ok := s.play.rounds[roundKey{t, p, r}]
		if !ok {
			if next == 0 && out.InPlay == 0 {
				next = r
			}
			continue
		}
		out.Dealt++
		if rec.Status == RoundDone {
			out.Finished++
			out.Total += rec.Score
			if rec.FinishedAt > out.ReachedAt {
				out.ReachedAt = rec.FinishedAt
			}
		} else {
			out.InPlay = r
		}
	}
	if out.InPlay == 0 {
		out.Next = next
	}
	return out
}

// Events returns the events player p sees after slot after, in slot order
// and recording order within a slot: the events of tournaments p entered
// whose Player is empty or p, limited to tournament t when t is not empty.
// It returns at most max events (no limit when max is not positive) but
// never splits one slot's events, so it may return more than max when one
// slot recorded more.
func (s *State) Events(after paxos.Slot, t TournamentID, p PlayerID, max int) []EventRecord {
	ps, ok := s.play.players[p]
	if !ok {
		return nil
	}
	var lists [][]int
	lists = append(lists, s.play.own[p])
	for _, tid := range ps.entered {
		if t == "" || tid == t {
			lists = append(lists, s.play.shared[tid])
		}
	}
	var picked []int
	for _, list := range lists {
		i := sort.Search(len(list), func(i int) bool { return s.play.events[list[i]].Slot > after })
		n := 0
		last := paxos.Slot(0)
		for ; i < len(list); i++ {
			ev := s.play.events[list[i]]
			if t != "" && ev.Tournament != t {
				continue
			}
			if max > 0 && n >= max && ev.Slot != last {
				break
			}
			picked = append(picked, list[i])
			n++
			last = ev.Slot
		}
	}
	sort.Ints(picked)
	out := make([]EventRecord, 0, len(picked))
	for i, idx := range picked {
		ev := s.play.events[idx]
		if max > 0 && i >= max && ev.Slot != out[len(out)-1].Slot {
			break
		}
		out = append(out, ev)
	}
	return out
}

// EventCount returns the number of events recorded.
func (s *State) EventCount() int { return len(s.play.events) }

// --- commands ---

// playReject is a rejection of a play command with its outcome.
func playReject(code Code, detail string, out PlayOutcome) Result {
	return Result{Code: code, Detail: detail, Play: out}
}

func validLowerHex(s string, n int) bool {
	if len(s) != n {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !('0' <= c && c <= '9' || 'a' <= c && c <= 'f') {
			return false
		}
	}
	return true
}

// deviceIDLen is the length of a device id: session.DeviceIDLen, which this
// package does not import.
const deviceIDLen = 32

func (s *State) openSession(slot paxos.Slot, receivedAt int64, op OpenSession) Result {
	if !validLowerHex(string(op.Device), deviceIDLen) || op.Verifier == (Digest{}) {
		return reject(InvalidDevice, "the device id must be 32 lower-case hex characters and the verifier not zero")
	}
	if b, bound := s.play.bindings[op.Device]; bound {
		if b.Verifier != op.Verifier {
			return reject(DeviceMismatch, "the device secret does not match the device's binding")
		}
		ps := s.play.players[b.Player]
		return Result{Code: OK, Play: PlayOutcome{Player: b.Player, IssuedAtMs: receivedAt, NextSeq: ps.rec.LastSeq + 1}}
	}
	if err := ValidatePlayer(Player{ID: op.Player, Jurisdiction: op.Jurisdiction, Age: op.Age}); err != nil {
		return reject(InvalidPlayer, err.Error())
	}
	if _, taken := s.play.players[op.Player]; taken {
		return reject(PlayerExists, fmt.Sprintf("player %q is bound to another device", op.Player))
	}
	b := &Binding{Device: op.Device, Verifier: op.Verifier, Player: op.Player,
		Jurisdiction: op.Jurisdiction, Age: op.Age, BoundAt: slot}
	s.play.bindings[op.Device] = b
	s.play.players[op.Player] = &playerState{rec: PlayerRecord{Binding: *b}}
	s.play.dirtyBindings = append(s.play.dirtyBindings, op.Device)
	s.play.touchPlayer(op.Player)
	return Result{Code: OK, Play: PlayOutcome{Player: op.Player, NewPlayer: true, IssuedAtMs: receivedAt, NextSeq: 1}}
}

// sequenced runs the rules every sequenced command begins with (section
// 6.2): the player exists, the sequence number is the next one and is then
// consumed, the tournament exists and takes play commands. On success it
// returns the player, the tournament and ok; otherwise the rejection.
func (s *State) sequenced(pid PlayerID, seq uint64, tid TournamentID) (*playerState, *Tournament, Result, bool) {
	ps, ok := s.play.players[pid]
	if !ok {
		return nil, nil, playReject(UnknownPlayer, fmt.Sprintf("no device is bound to player %q", pid), PlayOutcome{Player: pid}), false
	}
	out := PlayOutcome{Player: pid, NextSeq: ps.rec.LastSeq + 1}
	if seq <= ps.rec.LastSeq {
		return nil, nil, playReject(StaleSeq, fmt.Sprintf("seq %d is not above the last consumed %d", seq, ps.rec.LastSeq), out), false
	}
	if seq > ps.rec.LastSeq+1 {
		return nil, nil, playReject(SeqGap, fmt.Sprintf("seq %d skips from the last consumed %d", seq, ps.rec.LastSeq), out), false
	}
	ps.rec.LastSeq = seq
	s.play.touchPlayer(pid)
	out.NextSeq = seq + 1
	t, ok := s.tournaments[tid]
	if !ok {
		return ps, nil, playReject(UnknownTournament, fmt.Sprintf("no tournament %q", tid), out), false
	}
	if t.Rules.Game == "" {
		return ps, nil, playReject(NotPlayTournament, fmt.Sprintf("tournament %q takes client-reported scores", tid), out), false
	}
	return ps, t, Result{}, true
}

// outcome returns the outcome every result of a sequenced command carries.
func outcome(pid PlayerID, ps *playerState) PlayOutcome {
	return PlayOutcome{Player: pid, NextSeq: ps.rec.LastSeq + 1}
}

func (s *State) enterPlay(slot paxos.Slot, ballot paxos.Ballot, op Enter) Result {
	ps, t, rej, ok := s.sequenced(op.Player, op.Seq, op.Tournament)
	if !ok {
		return rej
	}
	b := ps.rec.Binding
	res := s.admit(slot, ballot, t, Player{ID: op.Player, Jurisdiction: b.Jurisdiction, Age: b.Age})
	res.Play = outcome(op.Player, ps)
	if res.Code == OK {
		res.Play.JoinSeq = t.Entries[len(t.Entries)-1].JoinSeq
		ps.entered = append(ps.entered, t.ID)
	}
	return res
}

func validRound(r int) bool { return r >= 1 && r <= game.Rounds }

func (s *State) startRound(slot paxos.Slot, op StartRound) Result {
	ps, t, rej, ok := s.sequenced(op.Player, op.Seq, op.Tournament)
	if !ok {
		return rej
	}
	out := outcome(op.Player, ps)
	out.Round = op.Round
	if t.Status != Open {
		return playReject(NotOpen, fmt.Sprintf("tournament %q is %s", t.ID, t.Status), out)
	}
	if _, joined := t.Entry(op.Player); !joined {
		return playReject(NotJoined, fmt.Sprintf("player %q has not entered", op.Player), out)
	}
	if !validRound(op.Round) {
		return playReject(InvalidRound, fmt.Sprintf("round %d is outside 1..%d", op.Round, game.Rounds), out)
	}
	key := roundKey{t.ID, op.Player, op.Round}
	if _, dealt := s.play.rounds[key]; dealt {
		return playReject(RoundAlreadyStarted, fmt.Sprintf("round %d was dealt before", op.Round), out)
	}
	if op.Round > 1 {
		prev, dealt := s.play.rounds[roundKey{t.ID, op.Player, op.Round - 1}]
		if !dealt || prev.Status != RoundDone {
			return playReject(PreviousRoundUnfinished, fmt.Sprintf("round %d is not finished", op.Round-1), out)
		}
	}
	rec := &RoundRecord{
		Tournament: t.ID, Player: op.Player, Round: op.Round, Seed: op.Seed, Moves: []game.Move{},
		Status: RoundInPlay, StartedAt: slot, StartedAtMs: s.clock, DeadlineMs: s.clock + game.RoundTimeLimitMs,
		UpdatedAt: slot,
	}
	s.play.rounds[key] = rec
	s.play.touchRound(key)
	s.recordEvent(EventRecord{Slot: slot, Type: EventRoundStarted, Tournament: t.ID, Player: op.Player,
		Round: op.Round, Status: string(RoundInPlay)})
	return Result{Code: OK, Play: out}
}

// roundFor runs the entry and round rules shared by PlayMove and
// FinishRound: not_joined, invalid_round, round_not_started.
func (s *State) roundFor(t *Tournament, pid PlayerID, round int, out PlayOutcome) (*RoundRecord, int, Result, bool) {
	idx := -1
	for i := range t.Entries {
		if t.Entries[i].Player.ID == pid {
			idx = i
			break
		}
	}
	if idx < 0 {
		return nil, 0, playReject(NotJoined, fmt.Sprintf("player %q has not entered", pid), out), false
	}
	if !validRound(round) {
		return nil, 0, playReject(InvalidRound, fmt.Sprintf("round %d is outside 1..%d", round, game.Rounds), out), false
	}
	rec, dealt := s.play.rounds[roundKey{t.ID, pid, round}]
	if !dealt {
		return nil, 0, playReject(RoundNotStarted, fmt.Sprintf("round %d was never dealt", round), out), false
	}
	return rec, idx, Result{}, true
}

func (s *State) playMove(slot paxos.Slot, op PlayMove) Result {
	ps, t, rej, ok := s.sequenced(op.Player, op.Seq, op.Tournament)
	if !ok {
		return rej
	}
	out := outcome(op.Player, ps)
	out.Round = op.Round
	if t.Status != Open {
		return playReject(NotOpen, fmt.Sprintf("tournament %q is %s", t.ID, t.Status), out)
	}
	rec, idx, rej, ok := s.roundFor(t, op.Player, op.Round, out)
	if !ok {
		return rej
	}
	out.MoveIndex = len(rec.Moves)
	if rec.Status == RoundDone {
		return playReject(RoundFinished, fmt.Sprintf("round %d finished (%s)", op.Round, rec.Reason), out)
	}
	if s.clock > rec.DeadlineMs {
		return playReject(RoundExpired, fmt.Sprintf("the deadline %d has passed (clock %d)", rec.DeadlineMs, s.clock), out)
	}
	if op.MoveIndex != len(rec.Moves) {
		return playReject(MoveIndexMismatch, fmt.Sprintf("move_index %d, the round has %d accepted moves", op.MoveIndex, len(rec.Moves)), out)
	}
	board, err := game.Replay(rec.Seed, rec.Moves)
	if err != nil {
		// Unreachable: every recorded move was accepted on this board.
		return playReject(IllegalMove, err.Error(), out)
	}
	if err := board.Apply(op.Move); err != nil {
		return playReject(IllegalMove, err.Error(), out)
	}
	rec.Moves = append(rec.Moves, op.Move)
	rec.UpdatedAt = slot
	s.play.touchRound(roundKey{t.ID, op.Player, op.Round})
	out.MoveIndex = len(rec.Moves)
	if reason, over := board.Over(); over {
		s.finishRound(slot, t, idx, rec, &board, reason, false)
	}
	return Result{Code: OK, Play: out}
}

func (s *State) finishRoundCmd(slot paxos.Slot, op FinishRound) Result {
	ps, t, rej, ok := s.sequenced(op.Player, op.Seq, op.Tournament)
	if !ok {
		return rej
	}
	out := outcome(op.Player, ps)
	out.Round = op.Round
	rec, idx, rej, ok := s.roundFor(t, op.Player, op.Round, out)
	if !ok {
		return rej
	}
	out.MoveIndex = len(rec.Moves)
	if rec.Status == RoundDone {
		return Result{Code: OK, Play: out}
	}
	if t.Status != Open {
		// Unreachable: Close finishes every round in play.
		return playReject(NotOpen, fmt.Sprintf("tournament %q is %s", t.ID, t.Status), out)
	}
	reason := game.Resigned
	if s.clock > rec.DeadlineMs {
		reason = game.Expired
	}
	s.finishRound(slot, t, idx, rec, nil, reason, false)
	return Result{Code: OK, Play: out}
}

// finishRound ends a round in play (section 6.9): status, reason, cleared,
// score and slots; the round_finished event; one leaderboard_changed per
// slot unless the tournament is closing, which records its own; and, for
// round 3 of an unscored entry, the entry's score. board, when not nil, is
// the round's current board.
func (s *State) finishRound(slot paxos.Slot, t *Tournament, entry int, rec *RoundRecord, board *game.Board, reason game.FinishReason, closing bool) {
	if board == nil {
		// Replay cannot fail: every recorded move was accepted on this
		// board. Were it to, the board before the refused move would score.
		b, _ := game.Replay(rec.Seed, rec.Moves)
		board = &b
	}
	rec.Status = RoundDone
	rec.Reason = reason
	rec.Cleared = board.Cleared()
	rec.Score = board.Score()
	rec.FinishedAt = slot
	rec.UpdatedAt = slot
	s.play.touchRound(roundKey{rec.Tournament, rec.Player, rec.Round})
	prog := s.Progress(t.ID, rec.Player)
	s.recordEvent(EventRecord{Slot: slot, Type: EventRoundFinished, Tournament: t.ID, Player: rec.Player,
		Round: rec.Round, Status: string(RoundDone), Score: rec.Score, TotalScore: prog.Total, RoundsFinished: prog.Finished})
	if !closing {
		s.recordLeaderboard(slot, t.ID)
	}
	if rec.Round == game.Rounds && !t.Entries[entry].Scored {
		s.scoreEntry(slot, t, entry)
	}
}

// scoreEntry fixes an entry's score as the sum of its finished rounds and
// records entry_scored.
func (s *State) scoreEntry(slot paxos.Slot, t *Tournament, entry int) {
	e := &t.Entries[entry]
	prog := s.Progress(t.ID, e.Player.ID)
	e.Scored = true
	e.Score = prog.Total
	e.SubmitSeq = uint32(t.ScoredCount())
	s.recordEvent(EventRecord{Slot: slot, Type: EventEntryScored, Tournament: t.ID, Player: e.Player.ID,
		TotalScore: prog.Total, RoundsFinished: prog.Finished})
}

// closeRounds is the play part of Close: it finishes every round in play,
// in join order, then scores every unscored entry with a dealt round.
func (s *State) closeRounds(slot paxos.Slot, t *Tournament) {
	for i := range t.Entries {
		pid := t.Entries[i].Player.ID
		for r := 1; r <= game.Rounds; r++ {
			rec, ok := s.play.rounds[roundKey{t.ID, pid, r}]
			if !ok || rec.Status == RoundDone {
				continue
			}
			reason := game.Closed
			if s.clock > rec.DeadlineMs {
				reason = game.Expired
			}
			s.finishRound(slot, t, i, rec, nil, reason, true)
		}
	}
	for i := range t.Entries {
		if t.Entries[i].Scored {
			continue
		}
		if s.Progress(t.ID, t.Entries[i].Player.ID).Dealt > 0 {
			s.scoreEntry(slot, t, i)
		}
	}
}

// recordStatus records tournament_status and leaderboard_changed for Close
// and Settle.
func (s *State) recordStatus(slot paxos.Slot, t *Tournament) {
	s.recordEvent(EventRecord{Slot: slot, Type: EventTournamentStatus, Tournament: t.ID, Status: t.Status.String()})
	s.recordLeaderboard(slot, t.ID)
}

// recordLeaderboard records leaderboard_changed for tournament tid unless
// this slot already did.
func (s *State) recordLeaderboard(slot paxos.Slot, tid TournamentID) {
	if at, ok := s.play.boardAt[tid]; ok && at == slot {
		return
	}
	s.play.boardAt[tid] = slot
	s.recordEvent(EventRecord{Slot: slot, Type: EventLeaderboardChanged, Tournament: tid})
}

// recordEvent appends ev, stamped with the clock, and indexes it.
func (s *State) recordEvent(ev EventRecord) {
	ev.TimeMs = s.clock
	idx := len(s.play.events)
	s.play.events = append(s.play.events, ev)
	if ev.Player == "" {
		s.play.shared[ev.Tournament] = append(s.play.shared[ev.Tournament], idx)
	} else {
		s.play.own[ev.Player] = append(s.play.own[ev.Player], idx)
	}
}

func (s *State) claimPayout(slot paxos.Slot, ballot paxos.Ballot, op ClaimPayout) Result {
	ps, t, rej, ok := s.sequenced(op.Player, op.Seq, op.Tournament)
	if !ok {
		return rej
	}
	out := outcome(op.Player, ps)
	if _, joined := t.Entry(op.Player); !joined {
		return playReject(NotJoined, fmt.Sprintf("player %q has not entered", op.Player), out)
	}
	if t.Status != Settled {
		return playReject(NotSettled, fmt.Sprintf("tournament %q is %s", t.ID, t.Status), out)
	}
	amount := ClaimableAmount(t, op.Player)
	if amount <= 0 {
		return playReject(NoPayout, fmt.Sprintf("player %q has nothing to claim", op.Player), out)
	}
	tid, pid := string(t.ID), string(op.Player)
	key := ledger.ClaimKey(tid, pid)
	if s.book.Has(key) {
		return playReject(AlreadyClaimed, fmt.Sprintf("posting %q exists", key), out)
	}
	post := ledger.Posting{
		Key: key, Kind: ledger.Claim, Debit: ledger.PlayerAccount(pid), Credit: ledger.ClaimsAccount(tid),
		Amount: amount, Tournament: tid, Player: pid, Slot: slot, Ballot: ballot,
	}
	if _, posted, err := s.book.Post(post); err != nil || !posted {
		// Unreachable: the amount is positive, the accounts differ and the
		// key was checked.
		return playReject(LedgerConflict, fmt.Sprintf("claim posting %q not appended (%v)", key, err), out)
	}
	out.Amount = amount
	s.recordEvent(EventRecord{Slot: slot, Type: EventPayoutClaimed, Tournament: t.ID, Player: op.Player, Amount: amount})
	return Result{Code: OK, Play: out}
}

// ClaimableAmount returns the sum of player p's payouts in t that are not
// withheld: what ClaimPayout moves once t is settled.
func ClaimableAmount(t *Tournament, p PlayerID) ledger.Money {
	var sum ledger.Money
	for _, po := range t.Payouts {
		if po.Player == p && !po.Withheld {
			sum += po.Amount
		}
	}
	return sum
}
