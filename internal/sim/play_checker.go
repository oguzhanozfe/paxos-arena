package sim

import (
	"github.com/oguzhanozfe/paxos-arena/internal/game"
	"github.com/oguzhanozfe/paxos-arena/internal/intent"
	"github.com/oguzhanozfe/paxos-arena/internal/ledger"
	"github.com/oguzhanozfe/paxos-arena/internal/paxos"
	"github.com/oguzhanozfe/paxos-arena/internal/replica"
	"github.com/oguzhanozfe/paxos-arena/internal/tournament"
)

// The play invariants P1-P6 of docs/UNITY-INTEGRATION.md section 12,
// evaluated after every applied play command on every node, on every round
// view the simulated play API builds, and on every play tournament at the
// end of a run.

// playRoundKey identifies one round of one entry.
type playRoundKey struct {
	t tournament.TournamentID
	p tournament.PlayerID
	r int
}

// playRoundTrack is what the checker last saw of a round on one node.
type playRoundTrack struct {
	dealt    bool
	moves    int
	finished bool
}

// playNodeRecord is the play accounting of one incarnation of a node's
// state machine; a restart replays from slot 1 and starts a new one.
type playNodeRecord struct {
	// lastSeq is each player's last consumed sequence number (P2).
	lastSeq map[tournament.PlayerID]uint64
	// rounds is each round as of the last command that addressed it.
	rounds map[playRoundKey]playRoundTrack
}

func newPlayNodeRecord() *playNodeRecord {
	return &playNodeRecord{
		lastSeq: make(map[tournament.PlayerID]uint64),
		rounds:  make(map[playRoundKey]playRoundTrack),
	}
}

// trackRound reads round k from st.
func trackRound(st *tournament.State, k playRoundKey) playRoundTrack {
	rec, ok := st.Round(k.t, k.p, k.r)
	if !ok {
		return playRoundTrack{}
	}
	return playRoundTrack{dealt: true, moves: len(rec.Moves), finished: rec.Status == tournament.RoundDone}
}

// checkPlayApply evaluates P1, P2, P4, P5 and P6 for one applied command.
// Commands other than the play commands, and Close and Settle of
// tournaments without a game, are not play commands and are skipped.
func (c *Checker) checkPlayApply(r *Run, nd *simNode, st *tournament.State, a replica.Applied, snap applySnapshot) {
	rec := c.nodes[int(nd.id)-1].play
	var (
		pid       tournament.PlayerID
		seq       uint64
		tid       tournament.TournamentID
		round     int
		sequenced = true
	)
	switch op := a.Command.Op.(type) {
	case tournament.OpenSession:
		sequenced = false
	case tournament.Enter:
		tid, pid, seq = op.Tournament, op.Player, op.Seq
	case tournament.StartRound:
		tid, pid, seq, round = op.Tournament, op.Player, op.Seq, op.Round
	case tournament.PlayMove:
		tid, pid, seq, round = op.Tournament, op.Player, op.Seq, op.Round
	case tournament.FinishRound:
		tid, pid, seq, round = op.Tournament, op.Player, op.Seq, op.Round
	case tournament.ClaimPayout:
		tid, pid, seq = op.Tournament, op.Player, op.Seq
	case tournament.Close:
		tid, sequenced = op.Tournament, false
	case tournament.Settle:
		tid, sequenced = op.Tournament, false
	default:
		return
	}
	if !sequenced && tid != "" {
		if h, ok := st.Header(tid); !ok || h.Rules.Game == "" {
			return
		}
	}
	res := a.Result
	// P6: the state machine's clock never decreases.
	c.count("P6")
	if st.Clock() < snap.clock {
		c.fail(r, "P6", "node %d: the state clock went back from %d to %d at slot %d", nd.id, snap.clock, st.Clock(), a.Slot)
		return
	}
	if res.Code == tournament.KeyReused || res.Replayed {
		return // S8 proves that nothing changed
	}
	if sequenced {
		if !c.checkSequence(r, nd, st, rec, a, pid, seq, tid, round, snap) {
			return
		}
	}
	key := playRoundKey{tid, pid, round}
	switch op := a.Command.Op.(type) {
	case tournament.StartRound, tournament.FinishRound:
		c.checkRound(r, nd, st, key)
		rec.rounds[key] = trackRound(st, key)
	case tournament.PlayMove:
		if res.Code == tournament.OK {
			c.count("P6")
			if rr, ok := st.Round(tid, pid, round); ok && st.Clock() > rr.DeadlineMs {
				c.fail(r, "P6", "node %d: move of %s round %d in %s accepted at slot %d with the clock %d past the deadline %d",
					nd.id, pid, round, tid, a.Slot, st.Clock(), rr.DeadlineMs)
				return
			}
		}
		c.checkRound(r, nd, st, key)
		rec.rounds[key] = trackRound(st, key)
	case tournament.ClaimPayout:
		c.checkClaims(r, nd, st, tid)
		if res.Code == tournament.OK {
			if po, ok := st.Ledger().Get(ledger.ClaimKey(string(tid), string(pid))); !ok || po.Amount != res.Play.Amount {
				c.fail(r, "P1", "node %d: claim of %s in %s answered %d, posting %+v", nd.id, pid, tid, res.Play.Amount, po)
			}
		}
	case tournament.Settle:
		c.checkClaims(r, nd, st, tid)
	case tournament.Close:
		c.checkPlayTournament(r, nd, st, op.Tournament)
		for k := range rec.rounds {
			if k.t == op.Tournament {
				rec.rounds[k] = trackRound(st, k)
			}
		}
	}
}

// checkSequence evaluates P2 for one sequenced command that was not a
// replay: a command consumes a number only if it is the player's last
// consumed number plus one, and stale_seq, seq_gap and unknown_player
// change nothing but the results table. It reports whether the checks
// passed.
func (c *Checker) checkSequence(r *Run, nd *simNode, st *tournament.State, rec *playNodeRecord, a replica.Applied,
	pid tournament.PlayerID, seq uint64, tid tournament.TournamentID, round int, snap applySnapshot) bool {
	c.count("P2")
	prev := rec.lastSeq[pid]
	pr, bound := st.Player(pid)
	after := pr.LastSeq
	op := tournament.OpName(a.Command.Op)
	unchanged := func() bool {
		switch {
		case after != prev:
			c.fail(r, "P2", "node %d: %s of %s with seq %d answered %s at slot %d moved the last consumed number from %d to %d",
				nd.id, op, pid, seq, a.Result.Code, a.Slot, prev, after)
		case st.EventCount() != snap.events || st.Ledger().Len() != snap.postings:
			c.fail(r, "P2", "node %d: %s of %s with seq %d answered %s at slot %d recorded events or postings", nd.id, op, pid, seq, a.Result.Code, a.Slot)
		case round != 0 && trackRound(st, playRoundKey{tid, pid, round}) != rec.rounds[playRoundKey{tid, pid, round}]:
			c.fail(r, "P2", "node %d: %s of %s with seq %d answered %s at slot %d changed round %d", nd.id, op, pid, seq, a.Result.Code, a.Slot, round)
		default:
			return true
		}
		return false
	}
	switch a.Result.Code {
	case tournament.UnknownPlayer:
		if bound {
			c.fail(r, "P2", "node %d: %s of bound player %s answered unknown_player at slot %d", nd.id, op, pid, a.Slot)
			return false
		}
		return unchanged()
	case tournament.StaleSeq:
		if seq > prev {
			c.fail(r, "P2", "node %d: %s of %s with seq %d answered stale_seq at slot %d, but the last consumed number is %d", nd.id, op, pid, seq, a.Slot, prev)
			return false
		}
		return unchanged()
	case tournament.SeqGap:
		if seq <= prev+1 {
			c.fail(r, "P2", "node %d: %s of %s with seq %d answered seq_gap at slot %d, but the last consumed number is %d", nd.id, op, pid, seq, a.Slot, prev)
			return false
		}
		return unchanged()
	}
	if seq != prev+1 {
		c.fail(r, "P2", "node %d: %s of %s with seq %d applied as %s at slot %d after the last consumed number %d: a stale or skipped sequence number was accepted",
			nd.id, op, pid, seq, a.Result.Code, a.Slot, prev)
		return false
	}
	if after != seq {
		c.fail(r, "P2", "node %d: %s of %s with seq %d applied as %s at slot %d left the last consumed number at %d", nd.id, op, pid, seq, a.Result.Code, a.Slot, after)
		return false
	}
	rec.lastSeq[pid] = seq
	return true
}

// checkRound evaluates P4 and P5 on one round: the moves replay legally
// from the seed, a finished round's score and cleared count are the
// replay's, a round the rules ended is finished, and the seed is the one the
// deal secret derives.
func (c *Checker) checkRound(r *Run, nd *simNode, st *tournament.State, k playRoundKey) {
	rec, ok := st.Round(k.t, k.p, k.r)
	if !ok {
		return
	}
	c.count("P5")
	if want := game.DeriveSeed(simDealSecret, string(k.t), string(k.p), k.r); rec.Seed != want {
		c.fail(r, "P5", "node %d: round %d of %s in %s has seed %s, the deal secret derives %s", nd.id, k.r, k.p, k.t, rec.Seed, want)
		return
	}
	c.count("P4")
	b, err := game.Replay(rec.Seed, rec.Moves)
	if err != nil {
		c.fail(r, "P4", "node %d: round %d of %s in %s: an accepted move does not replay: %v", nd.id, k.r, k.p, k.t, err)
		return
	}
	reason, over := b.Over()
	if rec.Status == tournament.RoundDone {
		if rec.Score != b.Score() || rec.Cleared != b.Cleared() {
			c.fail(r, "P4", "node %d: round %d of %s in %s scored %d with %d cleared, the replay gives %d with %d",
				nd.id, k.r, k.p, k.t, rec.Score, rec.Cleared, b.Score(), b.Cleared())
			return
		}
		if (rec.Reason == game.Cleared || rec.Reason == game.Blocked) && (!over || reason != rec.Reason) {
			c.fail(r, "P4", "node %d: round %d of %s in %s finished %s, the replay says over=%t %s", nd.id, k.r, k.p, k.t, rec.Reason, over, reason)
		}
		return
	}
	if over {
		c.fail(r, "P4", "node %d: round %d of %s in %s is %s by the rules but still in play", nd.id, k.r, k.p, k.t, reason)
	}
}

// checkPlayTournament evaluates P1 and P4 on every entry and round of a play
// tournament: after Close, and at the end of a run.
func (c *Checker) checkPlayTournament(r *Run, nd *simNode, st *tournament.State, tid tournament.TournamentID) {
	t, ok := st.Tournament(tid)
	if !ok || t.Rules.Game == "" {
		return
	}
	for _, e := range t.Entries {
		for round := 1; round <= game.Rounds; round++ {
			c.checkRound(r, nd, st, playRoundKey{tid, e.Player.ID, round})
		}
		if e.Scored {
			c.count("P4")
			if prog := st.Progress(tid, e.Player.ID); e.Score != prog.Total {
				c.fail(r, "P4", "node %d: entry %s in %s scored %d, its finished rounds sum to %d", nd.id, e.Player.ID, tid, e.Score, prog.Total)
			}
		}
	}
	c.checkClaims(r, nd, st, tid)
}

// checkClaims evaluates P1 on one tournament: at most one claim posting per
// player, only in a settled tournament, for exactly the player's payouts
// that are not withheld, from the player's account to the claims account.
func (c *Checker) checkClaims(r *Run, nd *simNode, st *tournament.State, tid tournament.TournamentID) {
	c.count("P1")
	var t tournament.Tournament
	loaded := false
	seen := make(map[string]bool)
	for _, po := range st.Ledger().ForTournament(string(tid)) {
		if po.Kind != ledger.Claim {
			continue
		}
		if !loaded {
			t, _ = st.Tournament(tid)
			loaded = true
		}
		if seen[po.Player] {
			c.fail(r, "P1", "node %d: player %s claimed twice in %s", nd.id, po.Player, tid)
			return
		}
		seen[po.Player] = true
		want := tournament.ClaimableAmount(&t, tournament.PlayerID(po.Player))
		switch {
		case t.Status != tournament.Settled:
			c.fail(r, "P1", "node %d: claim %s in tournament %s that is %s", nd.id, po.Key, tid, t.Status)
		case po.Amount != want:
			c.fail(r, "P1", "node %d: claim %s moved %d, the player's payouts not withheld sum to %d", nd.id, po.Key, po.Amount, want)
		case po.Key != ledger.ClaimKey(string(tid), po.Player) || po.Debit != ledger.PlayerAccount(po.Player) || po.Credit != ledger.ClaimsAccount(string(tid)):
			c.fail(r, "P1", "node %d: claim posting %+v has the wrong key or accounts", nd.id, po)
		}
	}
}

// observeView evaluates P3 on a round view built from st on node nd as of
// slot asOf (0: the current state): the round's deal was chosen in the log
// with the seed the state holds, the view shows only the tableau, the first
// waste card and the stock cards drawn by the moves it counts, and it
// carries the seed exactly when the round was finished as of the view.
func (c *Checker) observeView(r *Run, nd *simNode, st *tournament.State, tid tournament.TournamentID, pid tournament.PlayerID, round int, v intent.RoundView, asOf paxos.Slot) {
	c.count("P3")
	rec, ok := st.Round(tid, pid, round)
	if !ok {
		c.fail(r, "P3", "node %d built a view of round %d of %s in %s without a round record", nd.id, round, pid, tid)
		return
	}
	if asOf != 0 && asOf < rec.StartedAt {
		c.fail(r, "P3", "node %d built a view of round %d of %s as of slot %d, before its deal at slot %d", nd.id, round, pid, asOf, rec.StartedAt)
		return
	}
	value, chosen := c.chosen[rec.StartedAt]
	cmd, err := tournament.Decode([]byte(value))
	sr, isDeal := cmd.Op.(tournament.StartRound)
	if !chosen || err != nil || !isDeal || sr.Tournament != tid || sr.Player != pid || sr.Round != round || sr.Seed != rec.Seed {
		c.fail(r, "P3", "node %d showed round %d of %s in %s, but slot %d is not its chosen deal with that seed", nd.id, round, pid, tid, rec.StartedAt)
		return
	}
	moves := int(v.MoveIndex)
	if moves > len(rec.Moves) {
		c.fail(r, "P3", "node %d: view of round %d of %s counts %d moves, the round has %d", nd.id, round, pid, moves, len(rec.Moves))
		return
	}
	draws := 0
	for _, m := range rec.Moves[:moves] {
		if m.Kind == game.Draw {
			draws++
		}
	}
	deck := game.Shuffle(rec.Seed)
	shown := make(map[string]bool, game.TableauSize+1+draws)
	for _, card := range deck[:game.TableauSize+1+draws] {
		shown[card.String()] = true
	}
	cards := []string{v.WasteTop, v.LastMove.Card}
	for _, col := range v.Columns {
		cards = append(cards, col.Cards...)
	}
	for _, card := range cards {
		if card != "" && !shown[card] {
			c.fail(r, "P3", "node %d: view of round %d of %s after %d moves shows %s, a stock card not drawn", nd.id, round, pid, moves, card)
			return
		}
	}
	finished := rec.Status == tournament.RoundDone && (asOf == 0 || rec.FinishedAt <= asOf)
	switch {
	case int(v.StockCount) != game.StockSize-draws:
		c.fail(r, "P3", "node %d: view of round %d of %s shows %d stock cards after %d draws", nd.id, round, pid, v.StockCount, draws)
	case v.Commitment != game.Commit(rec.Seed).String():
		c.fail(r, "P3", "node %d: view of round %d of %s carries a commitment that is not the seed's", nd.id, round, pid)
	case (v.Seed != "") != finished:
		c.fail(r, "P3", "node %d: view of round %d of %s as of slot %d carries seed %q while the round finished=%t", nd.id, round, pid, asOf, v.Seed, finished)
	case v.Seed != "" && v.Seed != rec.Seed.String():
		c.fail(r, "P3", "node %d: view of round %d of %s reveals a seed that is not the round's", nd.id, round, pid)
	}
}

// checkPlayAtEnd sweeps every play tournament of a live node (P1, P4, P5).
func (c *Checker) checkPlayAtEnd(r *Run, nd *simNode, st *tournament.State) {
	for _, tid := range st.Tournaments() {
		c.checkPlayTournament(r, nd, st, tid)
		if c.err != nil {
			return
		}
	}
}
