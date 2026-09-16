package sim

import (
	"errors"
	"fmt"
	"time"

	"github.com/oguzhanozfe/paxos-arena/internal/game"
	"github.com/oguzhanozfe/paxos-arena/internal/intent"
	"github.com/oguzhanozfe/paxos-arena/internal/ledger"
	"github.com/oguzhanozfe/paxos-arena/internal/paxos"
	"github.com/oguzhanozfe/paxos-arena/internal/replog"
	"github.com/oguzhanozfe/paxos-arena/internal/session"
	"github.com/oguzhanozfe/paxos-arena/internal/tournament"
)

// The play workload runs server-authoritative tournaments
// (docs/UNITY-INTEGRATION.md) through a model of the play API in front of
// each node: the leader checks the session token at its own clock, draws
// player ids, derives deal seeds from the deal secret and builds round views
// from applied state, as package intent does over HTTP. Each play client
// creates one ladder-v1 tournament, plays one round for each of its players
// (a session, the entry, the deal, a few moves, sometimes an illegal one,
// and a finish), closes and settles it, and claims every player's payout.

// Constants of the play workload.
const (
	// playEntrantsDefault is the number of players per play tournament.
	playEntrantsDefault = 3
	// playMaxMoves bounds the moves a player sends in a round before it
	// finishes the round.
	playMaxMoves = 8
	// playIllegalP is the probability that a move names a column the rules
	// refuse.
	playIllegalP = 0.15
	// playRoundReadP is the probability that a play client reads a round
	// view from a random live node on its turn.
	playRoundReadP = 0.3
	// playDropTargetAfter is the number of submissions of one intent without
	// a result after which the client tries another node.
	playDropTargetAfter = 3
	// playRound is the round every simulated entry plays. The other rounds
	// stay undealt; Close scores the entry with round 1 (section 4.5).
	playRound = 1
)

// simDealSecret is the deal secret of the simulated play API (section 5.1).
// Invariant P5 recomputes every seed from it.
var simDealSecret = []byte("sim-deal-secret-of-at-least-32-bytes!!")

// simKeys is the session keyring of the simulated play API (section 2.3).
var simKeys = session.Keyring{Keys: []session.Key{{ID: "sim", Secret: []byte("sim-session-key-of-at-least-32-bytes")}}}

// playKind is what a play intent asks for.
type playKind uint8

const (
	kindCreate playKind = iota
	kindSession
	kindEnter
	kindDeal
	kindMove
	kindFinish
	kindClose
	kindSettle
	kindClaim
	kindAttack
)

func (k playKind) String() string {
	return [...]string{"create", "session", "enter", "deal", "move", "finish", "close", "settle", "claim", "attack"}[k]
}

// needsToken reports whether the play API authenticates the intent with a
// session token. Create, close and settle are operator commands.
func (k playKind) needsToken() bool {
	switch k {
	case kindEnter, kindDeal, kindMove, kindFinish, kindClaim, kindAttack:
		return true
	}
	return false
}

// playStage is where one player is in its entry.
type playStage uint8

const (
	stageSession playStage = iota
	stageEnter
	stageDeal
	stageMove
	stageFinish
	stageClaim
	stageClaimAgain
	stageDone
)

// sentIntent is a sequenced intent with its recorded result, which the
// stale_sequence_replay scenario captures and sends again.
type sentIntent struct {
	key  tournament.IdempotencyKey
	op   tournament.Op
	code tournament.Code
}

// playPlayer is one device and its player.
type playPlayer struct {
	index        int
	device       tournament.DeviceID
	verifier     tournament.Digest
	jurisdiction string
	age          int
	pid          tournament.PlayerID // empty before the first session
	token        string
	sessionKey   tournament.IdempotencyKey // the last session key sent
	sessions     int
	intents      int
	lastSeq      uint64 // the last sequence number assigned
	stage        playStage
	view         intent.RoundView
	moves        int // moves sent in the round, illegal ones included
	wantMoves    int
	suspendAt    int // move count at which the app is suspended, -1 for never
	staleSession bool
	claim        ledger.Money
	sent         []sentIntent
}

// playWorkflow is one play tournament.
type playWorkflow struct {
	tid     tournament.TournamentID
	rules   tournament.Rules
	players []*playPlayer
	step    int // playCreate .. playDone
	cur     int // the player in play or claiming
}

// Workflow steps.
const (
	playCreate = iota
	playPlayers
	playClose
	playSettle
	playClaims
	playDone
)

// playIntent is the intent a play client waits on.
type playIntent struct {
	kind     playKind
	key      tournament.IdempotencyKey
	op       tournament.Op
	player   *playPlayer
	expect   tournament.Code // the result code the client expects
	attack   string          // for kindAttack: stale, gap or replay
	issued   time.Duration
	lastSent time.Duration
	attempts int
	lastNode paxos.NodeID
}

// playClient runs play tournaments one intent at a time.
type playClient struct {
	id          int
	remaining   int
	created     int
	wf          *playWorkflow
	pending     *playIntent
	resume      *playIntent // the intent a session renewal interrupted
	target      paxos.NodeID
	issued      int
	pausedUntil time.Duration
	extras      []playExtra
}

// playExtra is a completed intent resent with its key.
type playExtra struct {
	in  *playIntent
	due time.Duration
}

func newPlayClient(id int) *playClient { return &playClient{id: id, remaining: 1} }

// idle reports whether the client has nothing left to do in this batch.
func (c *playClient) idle() bool {
	return c.pending == nil && c.resume == nil && c.wf == nil && c.remaining == 0 && len(c.extras) == 0
}

// playEntrants is the number of players per play tournament.
func (p Params) playEntrantCount() int {
	if p.playEntrants < 1 {
		return playEntrantsDefault
	}
	return p.playEntrants
}

// sessionTTL is the lifetime of the tokens the simulated play API signs.
func (p Params) sessionTTL() time.Duration {
	if p.playSessionTTL <= 0 {
		return session.DefaultTTL
	}
	return p.playSessionTTL
}

// nowMs is the node's clock in milliseconds, which the leader stamps on the
// commands it proposes and verifies tokens against.
func (nd *simNode) nowMs(t time.Duration) int64 { return int64(nd.now(t) / time.Millisecond) }

// randomHex draws n bytes as lower-case hex.
func (r *Run) randomHex(n int) string {
	const digits = "0123456789abcdef"
	b := make([]byte, 2*n)
	for i := range b {
		b[i] = digits[r.rng.IntN(16)]
	}
	return string(b)
}

// drawPlayerID draws a player id of the play API's shape (section 2.2).
func (r *Run) drawPlayerID() tournament.PlayerID {
	const alphabet = "abcdefghijklmnopqrstuvwxyz234567"
	b := make([]byte, intent.PlayerIDRandomLen)
	for i := range b {
		b[i] = alphabet[r.rng.IntN(len(alphabet))]
	}
	return tournament.PlayerID(intent.PlayerIDPrefix + string(b))
}

// newPlayWorkflow draws the rules and players of the client's next play
// tournament.
func (r *Run) newPlayWorkflow(c *playClient) *playWorkflow {
	c.created++
	n := r.p.playEntrantCount()
	w := &playWorkflow{tid: tournament.TournamentID(fmt.Sprintf("pc%d-t%d", c.id, c.created))}
	places := 1 + r.rng.IntN(min(3, n))
	w.rules = tournament.Rules{
		EntryFee:    ledger.Money(100 + r.rng.Int64N(900)),
		RakeBps:     uint32(r.rng.IntN(2001)),
		PrizeBps:    randomPrizeTable(r, places),
		MinEntrants: places,
		MaxEntrants: n,
		MaxScore:    game.MaxTotalScore,
		MinAge:      18,
		TieBreak:    tournament.EarliestSubmission,
		Exclusions:  tournament.Exclusions{Version: listVersionAtCreate, Jurisdictions: []string{creationExcluded}},
		Game:        game.LadderV1,
	}
	if r.rng.IntN(2) == 0 {
		w.rules.TieBreak = tournament.Split
	}
	suspended := -1
	if r.p.playSuspend {
		suspended = r.rng.IntN(n)
	}
	for i := 0; i < n; i++ {
		pl := &playPlayer{
			index:        i,
			device:       tournament.DeviceID(r.randomHex(16)),
			jurisdiction: jurisdictions[r.rng.IntN(len(jurisdictions))],
			age:          18 + r.rng.IntN(50),
			wantMoves:    2 + r.rng.IntN(playMaxMoves-1),
			suspendAt:    -1,
		}
		v, err := session.NewVerifier(r.randomHex(32))
		if err != nil {
			panic("sim: " + err.Error())
		}
		pl.verifier = tournament.Digest(v)
		if i == suspended {
			pl.suspendAt = 1 + r.rng.IntN(pl.wantMoves-1)
		}
		w.players = append(w.players, pl)
	}
	return w
}

// playTurn is one play client action: observe the result of the pending
// intent, issue the next one, re-submit one whose result has not appeared,
// send due resends, and sometimes read a round view or issue a consistent
// read. A suspended client does nothing.
func (r *Run) playTurn(c *playClient) {
	if r.clock < c.pausedUntil {
		return
	}
	for attempt := 0; attempt < 3; attempt++ {
		if c.pending == nil {
			if !r.playAdvance(c) {
				break
			}
			if r.clock < c.pausedUntil {
				return
			}
		}
		nd := r.pickPlayTarget(c)
		if nd == nil {
			break
		}
		// A captured request replayed under its own key already has a
		// result; the attacker receives it only by sending the request.
		if res, ok := nd.core.State().Result(c.pending.key); ok && (c.pending.attack != "replay" || c.pending.attempts > 0) {
			r.playObserve(c, nd, res)
			continue
		}
		if c.pending.attempts == 0 || r.clock >= c.pending.lastSent+retryInterval {
			r.playSubmit(c, nd, c.pending, false)
		}
		break
	}
	r.playSendExtras(c)
	if c.wf != nil && r.rng.Float64() < playRoundReadP {
		r.playReadRound(c)
	}
	if r.rng.Float64() < readP {
		if nd := r.pickPlayTarget(c); nd != nil {
			r.issueRead(nd, -2-c.id)
		}
	}
}

// pickPlayTarget returns the client's target if it is live and in the
// core, otherwise a random live core node.
func (r *Run) pickPlayTarget(c *playClient) *simNode {
	if c.target != 0 {
		if nd := r.nodeByID(c.target); nd != nil && nd.alive && r.inCore(nd) {
			return nd
		}
		c.target = 0
	}
	alive := r.aliveNodes()
	if len(alive) == 0 {
		return nil
	}
	nd := alive[r.rng.IntN(len(alive))]
	c.target = nd.id
	return nd
}

// otherLiveNode returns a random live core node other than nd, or nil.
func (r *Run) otherLiveNode(nd *simNode) *simNode {
	var out []*simNode
	for _, o := range r.aliveNodes() {
		if o != nd {
			out = append(out, o)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out[r.rng.IntN(len(out))]
}

// playAdvance sets c.pending to the client's next intent, starting a new
// tournament as needed. It returns false when the client has nothing more
// to do.
func (r *Run) playAdvance(c *playClient) bool {
	for {
		if c.wf == nil {
			if c.remaining == 0 {
				if !r.faultsOn || r.p.singleBatch {
					return false
				}
				c.remaining = 1
			}
			c.remaining--
			c.wf = r.newPlayWorkflow(c)
		}
		in := r.nextPlayIntent(c, c.wf)
		if in == nil {
			c.wf = nil
			continue
		}
		in.issued = r.clock
		c.pending = in
		c.issued++
		return true
	}
}

// operatorIntent is a create, close or settle of the workflow's tournament.
func (r *Run) operatorIntent(c *playClient, w *playWorkflow, kind playKind, op tournament.Op) *playIntent {
	return &playIntent{kind: kind, key: tournament.IdempotencyKey(fmt.Sprintf("op.%s.%s", w.tid, kind)), op: op, expect: tournament.OK}
}

// nextPlayIntent builds the workflow's next intent without advancing it;
// playObserve advances. It returns nil when the workflow is complete.
func (r *Run) nextPlayIntent(c *playClient, w *playWorkflow) *playIntent {
	for {
		switch w.step {
		case playCreate:
			return r.operatorIntent(c, w, kindCreate, tournament.CreateTournament{ID: w.tid, Rules: w.rules})
		case playPlayers, playClaims:
			if w.cur >= len(w.players) {
				if w.step == playPlayers {
					w.step = playClose
				} else {
					w.step = playDone
				}
				w.cur = 0
				continue
			}
			pl := w.players[w.cur]
			if in := r.nextPlayerIntent(c, w, pl); in != nil {
				return in
			}
			w.cur++
		case playClose:
			return r.operatorIntent(c, w, kindClose, tournament.Close{Tournament: w.tid})
		case playSettle:
			return r.operatorIntent(c, w, kindSettle, tournament.Settle{Tournament: w.tid,
				Exclusions: tournament.Exclusions{Version: listVersionAtSettle, Jurisdictions: []string{creationExcluded}}})
		default:
			return nil
		}
	}
}

// sessionIntent opens a session for pl: with a new key, or with the last
// key sent when a session request was in flight as the app was suspended.
func (r *Run) sessionIntent(pl *playPlayer, reuse bool) *playIntent {
	if !reuse || pl.sessionKey == "" {
		pl.sessions++
		pl.sessionKey = tournament.IdempotencyKey(fmt.Sprintf("d.%s.s%d", pl.device, pl.sessions))
	}
	return &playIntent{kind: kindSession, key: pl.sessionKey, player: pl, expect: tournament.OK,
		op: tournament.OpenSession{Device: pl.device, Verifier: pl.verifier, Jurisdiction: pl.jurisdiction, Age: pl.age}}
}

// seqIntent assigns pl's next sequence number to the op build returns.
func (r *Run) seqIntent(pl *playPlayer, kind playKind, expect tournament.Code, build func(seq uint64) tournament.Op) *playIntent {
	pl.lastSeq++
	pl.intents++
	return &playIntent{kind: kind, key: tournament.IdempotencyKey(fmt.Sprintf("u.%s.k%d", pl.pid, pl.intents)),
		op: build(pl.lastSeq), player: pl, expect: expect}
}

// nextPlayerIntent builds pl's next intent, or returns nil when pl has
// nothing left in the current workflow step.
func (r *Run) nextPlayerIntent(c *playClient, w *playWorkflow, pl *playPlayer) *playIntent {
	if w.step == playPlayers && pl.stage >= stageClaim || w.step == playClaims && pl.stage == stageDone {
		return nil
	}
	if r.p.playAttackP > 0 && len(pl.sent) > 0 && r.rng.Float64() < r.p.playAttackP {
		return r.attackIntent(pl)
	}
	tid := w.tid
	switch pl.stage {
	case stageSession:
		return r.sessionIntent(pl, false)
	case stageEnter:
		return r.seqIntent(pl, kindEnter, tournament.OK, func(seq uint64) tournament.Op {
			return tournament.Enter{Tournament: tid, Player: pl.pid, Seq: seq}
		})
	case stageDeal:
		return r.seqIntent(pl, kindDeal, tournament.OK, func(seq uint64) tournament.Op {
			return tournament.StartRound{Tournament: tid, Player: pl.pid, Seq: seq, Round: playRound}
		})
	case stageMove:
		v := pl.view
		if v.Status != string(tournament.RoundInPlay) || pl.moves >= pl.wantMoves || (len(v.PlayableColumns) == 0 && !v.CanDraw) {
			pl.stage = stageFinish
			return r.nextPlayerIntent(c, w, pl)
		}
		move := game.Move{Kind: game.Draw, Column: game.NoColumn}
		if len(v.PlayableColumns) > 0 {
			move = game.Move{Kind: game.Play, Column: int(v.PlayableColumns[0])}
		}
		expect := tournament.OK
		if r.rng.Float64() < playIllegalP {
			if col := illegalColumn(v); col >= 0 {
				move, expect = game.Move{Kind: game.Play, Column: col}, tournament.IllegalMove
			}
		}
		in := r.seqIntent(pl, kindMove, expect, func(seq uint64) tournament.Op {
			return tournament.PlayMove{Tournament: tid, Player: pl.pid, Seq: seq, Round: playRound, MoveIndex: int(v.MoveIndex), Move: move}
		})
		if pl.suspendAt == pl.moves {
			// The app is suspended with this move stored and unsent, for
			// longer than the session token lives, while a session request
			// of its own was in flight.
			pl.suspendAt = -1
			pl.staleSession = true
			c.pausedUntil = r.clock + r.p.sessionTTL() + session.Leeway + time.Second + r.jitter(2*time.Second)
			r.report.Marks["play.suspended"]++
			r.tracef("play client %d: player %s suspends with move seq %d stored until t=%v", c.id, pl.pid, pl.lastSeq, c.pausedUntil)
			if r.expiry != nil {
				r.scheduleScript(r.clock+300*time.Millisecond, expiryCrashLeader)
			}
		}
		return in
	case stageFinish:
		return r.seqIntent(pl, kindFinish, tournament.OK, func(seq uint64) tournament.Op {
			return tournament.FinishRound{Tournament: tid, Player: pl.pid, Seq: seq, Round: playRound}
		})
	case stageClaim, stageClaimAgain:
		expect := tournament.OK
		switch {
		case pl.stage == stageClaimAgain:
			expect = tournament.AlreadyClaimed
		case pl.claim <= 0:
			expect = tournament.NoPayout
		}
		return r.seqIntent(pl, kindClaim, expect, func(seq uint64) tournament.Op {
			return tournament.ClaimPayout{Tournament: tid, Player: pl.pid, Seq: seq}
		})
	}
	return nil
}

// illegalColumn returns a non-empty column whose top card does not fit the
// waste card, or -1.
func illegalColumn(v intent.RoundView) int {
	for c, col := range v.Columns {
		if len(col.Cards) == 0 {
			continue
		}
		playable := false
		for _, p := range v.PlayableColumns {
			if int(p) == c {
				playable = true
			}
		}
		if !playable {
			return c
		}
	}
	return -1
}

// withSeq returns a sequenced op with its sequence number replaced.
func withSeq(op tournament.Op, seq uint64) tournament.Op {
	switch o := op.(type) {
	case tournament.Enter:
		o.Seq = seq
		return o
	case tournament.StartRound:
		o.Seq = seq
		return o
	case tournament.PlayMove:
		o.Seq = seq
		return o
	case tournament.FinishRound:
		o.Seq = seq
		return o
	case tournament.ClaimPayout:
		o.Seq = seq
		return o
	}
	return op
}

// attackIntent replays one of pl's earlier intents as an attacker holding
// a captured request and a valid token would: under a new key with its used
// sequence number (stale_seq), with a number skipped ahead (seq_gap), or
// under its own key (the recorded result, replayed).
func (r *Run) attackIntent(pl *playPlayer) *playIntent {
	old := pl.sent[r.rng.IntN(len(pl.sent))]
	pl.intents++
	key := tournament.IdempotencyKey(fmt.Sprintf("u.%s.x%d", pl.pid, pl.intents))
	switch r.rng.IntN(3) {
	case 0:
		r.report.Marks["stale.stale_sent"]++
		return &playIntent{kind: kindAttack, attack: "stale", key: key, op: old.op, player: pl, expect: tournament.StaleSeq}
	case 1:
		r.report.Marks["stale.gap_sent"]++
		op := withSeq(old.op, pl.lastSeq+2+uint64(r.rng.IntN(3)))
		return &playIntent{kind: kindAttack, attack: "gap", key: key, op: op, player: pl, expect: tournament.SeqGap}
	}
	r.report.Marks["stale.replay_sent"]++
	return &playIntent{kind: kindAttack, attack: "replay", key: old.key, op: old.op, player: pl, expect: old.code}
}

// playSubmit is the play API of node nd receiving one intent: a follower
// redirects (NotLeaderError), the leader checks the session token at its
// own clock, stamps the command, draws the player id of a session, derives
// the seed of a deal, and proposes.
func (r *Run) playSubmit(c *playClient, nd *simNode, in *playIntent, extra bool) {
	if !extra {
		in.lastSent = r.clock
		in.attempts++
		if (in.attempts > playDropTargetAfter || r.p.playRetryElsewhere && in.attempts > 1) && nd.id == in.lastNode {
			// No result from this node: try another one, as a client whose
			// request timed out moves on to the next base URL.
			if other := r.otherLiveNode(nd); other != nil {
				nd = other
				c.target = nd.id
			}
		}
		in.lastNode = nd.id
	}
	nowMs := nd.nowMs(r.clock)
	if in.kind.needsToken() && nd.node.Role() == replog.Leader {
		claims, err := simKeys.Verify(in.player.token, nowMs)
		if err != nil || claims.Player != string(in.player.pid) {
			r.report.Marks["play.session_refused"]++
			if errors.Is(err, session.ErrExpired) {
				r.report.Marks["play.session_expired"]++
			}
			r.tracef("play client %d: node %d refuses %s of %s: 401 (%v)", c.id, nd.id, in.key, in.player.pid, err)
			if extra {
				return
			}
			// Open a session, then resend this intent unchanged.
			c.resume = in
			c.pending = r.sessionIntent(in.player, in.player.staleSession)
			in.player.staleSession = false
			c.pending.issued = r.clock
			c.issued++
			return
		}
	}
	op := in.op
	switch o := op.(type) {
	case tournament.OpenSession:
		o.Player = r.drawPlayerID()
		op = o
	case tournament.StartRound:
		o.Seed = game.DeriveSeed(simDealSecret, string(o.Tournament), string(o.Player), o.Round)
		op = o
	case tournament.CreateTournament:
		o.Seed = r.rng.Uint64()
		op = o
	}
	cmd := tournament.Command{Key: in.key, ReceivedAt: nowMs, Op: op}
	if r.scenario != nil && r.scenario.onSubmit != nil {
		r.scenario.onSubmit(r, nil, nd, cmd)
	}
	var err error
	r.callNode(nd, nil, func(now time.Duration) []replog.Envelope {
		outs, e := nd.core.Submit(now, cmd)
		err = e
		return outs
	})
	r.report.Submits++
	if err != nil {
		var nl replog.NotLeaderError
		if errors.As(err, &nl) {
			c.target = nl.Leader
		} else {
			c.target = 0
		}
		r.tracef("play client %d: submit %s to node %d: %v", c.id, in.key, nd.id, err)
		return
	}
	c.target = nd.id
	r.tracef("play client %d: submitted %s (%s) to node %d (extra %t)", c.id, in.key, tournament.OpName(op), nd.id, extra)
	if r.scenario != nil && r.scenario.afterSubmit != nil && nd.alive {
		r.scenario.afterSubmit(r, nil, nd, cmd)
	}
}

// playSendExtras resends every due completed intent with its key.
func (r *Run) playSendExtras(c *playClient) {
	kept := c.extras[:0]
	for _, e := range c.extras {
		if e.due > r.clock {
			kept = append(kept, e)
			continue
		}
		nd := r.pickPlayTarget(c)
		if nd == nil {
			kept = append(kept, e)
			continue
		}
		r.report.Marks["play.resends"]++
		r.report.ExtraRetries++
		r.playSubmit(c, nd, e.in, true)
	}
	c.extras = kept
}

// playView is the view the play API answers a round intent with, built
// from the answering node's applied state as of the result (section 7.2),
// and checked for invariant P3.
func (r *Run) playView(nd *simNode, tid tournament.TournamentID, pid tournament.PlayerID, res tournament.Result) intent.RoundView {
	st := nd.core.State()
	v, ok := intent.BuildRoundViewAt(st, tid, pid, playRound, res.Play.MoveIndex, res.Slot)
	if !ok {
		r.checker.fail(r, "P3", "node %d holds the result of slot %d for %s but no round record", nd.id, res.Slot, pid)
		return v
	}
	r.checker.observeView(r, nd, st, tid, pid, playRound, v, res.Slot)
	return v
}

// playReadRound reads the round of a random player of the client's
// tournament from a random live node, which may lag behind the deal.
func (r *Run) playReadRound(c *playClient) {
	w := c.wf
	pl := w.players[r.rng.IntN(len(w.players))]
	alive := r.aliveNodes()
	if pl.pid == "" || len(alive) == 0 {
		return
	}
	nd := alive[r.rng.IntN(len(alive))]
	st := nd.core.State()
	v, ok := intent.BuildRoundView(st, w.tid, pl.pid, playRound, 0)
	if !ok {
		return
	}
	r.report.Marks["play.round_reads"]++
	r.checker.observeView(r, nd, st, w.tid, pl.pid, playRound, v, 0)
}

// playObserve handles the result of the pending intent and advances the
// workflow.
func (r *Run) playObserve(c *playClient, nd *simNode, res tournament.Result) {
	in := c.pending
	w := c.wf
	c.pending = nil
	r.report.Completed++
	pl := in.player
	ok := res.Code == in.expect
	if !ok {
		r.report.Unexpected++
		r.tracef("play client %d: UNEXPECTED %s for %s %s on node %d, expected %s: %s", c.id, res.Code, in.kind, in.key, nd.id, in.expect, res.Detail)
	} else {
		r.tracef("play client %d: %s %s -> %s (slot %d, replayed %t) on node %d", c.id, in.kind, in.key, res.Code, res.Slot, res.Replayed, nd.id)
	}
	switch in.kind {
	case kindCreate:
		if res.Code == tournament.OK {
			w.step = playPlayers
		}
	case kindSession:
		if res.Code != tournament.OK {
			c.pending, c.resume = c.resume, nil
			return
		}
		if pl.pid != "" && pl.pid != res.Play.Player || pl.pid == "" && !res.Play.NewPlayer {
			r.report.Unexpected++
			r.tracef("play client %d: UNEXPECTED session of device %s for player %s (new %t), had %q", c.id, pl.device, res.Play.Player, res.Play.NewPlayer, pl.pid)
		}
		pl.pid = res.Play.Player
		exp := res.Play.IssuedAtMs + r.p.sessionTTL().Milliseconds()
		if exp <= nd.nowMs(r.clock) {
			// A replay of an old key carries an expired token: ask again
			// with a new key (section 9.2.3).
			r.report.Marks["play.session_replay_expired"]++
			c.pending = r.sessionIntent(pl, false)
			c.pending.issued = r.clock
			c.issued++
			return
		}
		token, err := simKeys.Sign(session.Claims{Player: string(pl.pid), Device: string(pl.device), IssuedAtMs: res.Play.IssuedAtMs, ExpiresAtMs: exp})
		if err != nil {
			panic("sim: " + err.Error())
		}
		pl.token = token
		if next := res.Play.NextSeq; next > 0 && next-1 > pl.lastSeq {
			pl.lastSeq = next - 1
		}
		if pl.stage == stageSession {
			pl.stage = stageEnter
		} else {
			r.report.Marks["play.session_renewed"]++
		}
		if c.resume != nil {
			c.pending, c.resume = c.resume, nil
			c.pending.lastSent = 0
		}
		return
	case kindEnter:
		if res.Code == tournament.OK {
			pl.stage = stageDeal
		}
	case kindDeal:
		if res.Code == tournament.OK {
			pl.view = r.playView(nd, w.tid, pl.pid, res)
			pl.stage = stageMove
		}
	case kindMove:
		pl.moves++
		switch res.Code {
		case tournament.OK:
			pl.view = r.playView(nd, w.tid, pl.pid, res)
		case tournament.IllegalMove:
			r.report.Marks["play.illegal_rejected"]++
		}
	case kindFinish:
		if res.Code == tournament.OK {
			pl.view = r.playView(nd, w.tid, pl.pid, res)
			pl.stage = stageClaim
		}
	case kindClose:
		if res.Code == tournament.OK {
			w.step = playSettle
		}
	case kindSettle:
		if res.Code == tournament.OK {
			t, _ := nd.core.State().Tournament(w.tid)
			for _, p := range w.players {
				p.claim = tournament.ClaimableAmount(&t, p.pid)
			}
			r.report.Marks["play.settled"]++
			w.step = playClaims
			w.cur = 0
		}
	case kindClaim:
		switch {
		case res.Code == tournament.OK:
			r.report.Marks["play.claims"]++
			if res.Play.Amount != pl.claim {
				r.report.Unexpected++
				r.tracef("play client %d: UNEXPECTED claim of %d for %s, settled payout %d", c.id, res.Play.Amount, pl.pid, pl.claim)
			}
			pl.stage = stageDone
			if r.rng.Float64() < r.p.playClaimAgainP {
				pl.stage = stageClaimAgain
			}
		case pl.stage == stageClaimAgain:
			r.report.Marks["play.claims_refused_again"]++
			pl.stage = stageDone
		default:
			pl.stage = stageDone
		}
	case kindAttack:
		r.report.Marks["stale."+in.attack+"_answered"]++
	}
	if in.kind != kindAttack && in.kind.needsToken() && res.Code != tournament.StaleSeq && res.Code != tournament.SeqGap {
		pl.sent = append(pl.sent, sentIntent{key: in.key, op: in.op, code: res.Code})
		if r.p.playResendP > 0 && (in.kind == kindMove || in.kind == kindClaim || in.kind == kindDeal) && r.rng.Float64() < r.p.playResendP {
			c.extras = append(c.extras, playExtra{in: in, due: r.clock + r.jitter(300*time.Millisecond)})
		}
	}
}
