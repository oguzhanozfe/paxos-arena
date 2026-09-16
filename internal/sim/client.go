package sim

import (
	"errors"
	"fmt"
	"time"

	"github.com/oguzhanozfe/paxos-arena/internal/ledger"
	"github.com/oguzhanozfe/paxos-arena/internal/paxos"
	"github.com/oguzhanozfe/paxos-arena/internal/replog"
	"github.com/oguzhanozfe/paxos-arena/internal/tournament"
)

// jurisdictions is the pool of ordinary entrant jurisdictions.
var jurisdictions = []string{"TR", "DE", "US", "GB", "FR"}

// Jurisdictions on the exclusion lists of every workflow: creationExcluded
// is on the creation-time list (version 7), settleExcluded is added by the
// settlement-time list (version 8).
const (
	creationExcluded    = "XX"
	settleExcluded      = "YY"
	listVersionAtCreate = 7
	listVersionAtSettle = 8
)

// workflow is one tournament a client runs: create, join every entrant,
// submit every score, close, settle. The steps are numbered 0..2E+2.
type workflow struct {
	tid      tournament.TournamentID
	rules    tournament.Rules
	players  []tournament.Player
	scores   []int64
	skip     []bool // entrant never submits a score
	joined   []bool
	seed     uint64 // learned from the first OK Join result
	settleEx tournament.Exclusions
	step     int
}

func (w *workflow) entrants() int { return len(w.players) }

// done reports whether every step, settle included, has completed.
func (w *workflow) done() bool { return w.step > 2*w.entrants()+2 }

// pendingCmd is the command a client is waiting on.
type pendingCmd struct {
	key      tournament.IdempotencyKey
	op       tournament.Op
	step     int
	issued   time.Duration
	lastSent time.Duration
	attempts int
}

// extraRetry is a duplicate submission scheduled for the retry storm.
type extraRetry struct {
	key     tournament.IdempotencyKey
	op      tournament.Op
	due     time.Duration
	mutated bool
}

// client runs tournament workflows one command at a time against the node
// it believes is the leader, follows NotLeaderError hints, learns results from
// the state machine of the node it talks to, and retries with the same key
// when a result does not appear.
type client struct {
	id        int
	perBatch  int
	remaining int // tournaments left in the current batch
	created   int // tournaments started so far, for identifiers
	wf        *workflow
	pending   *pendingCmd
	target    paxos.NodeID
	seq       int
	issued    int
	done      int
	submits   int
	extras    []extraRetry
}

// readKey identifies one pending read-index barrier. gen ties it to one
// incarnation of the node so a restart cannot complete an old read.
type readKey struct {
	node paxos.NodeID
	gen  uint64
	seq  uint64
}

// pendingRead records what the checker needs to verify S7 when the barrier
// completes: the highest commit index observed anywhere when the read was
// issued.
type pendingRead struct {
	client    int
	maxCommit paxos.Slot
	issued    time.Duration
}

func newClient(id, perBatch int) *client {
	return &client{id: id, perBatch: perBatch, remaining: perBatch}
}

// idle reports whether the client has nothing left to do in this batch.
func (c *client) idle() bool {
	return c.pending == nil && c.wf == nil && c.remaining == 0 && len(c.extras) == 0
}

// newWorkflow draws the rules, entrants and scores of the client's next
// tournament.
func (r *Run) newWorkflow(c *client) *workflow {
	c.created++
	n := r.p.entrants()
	if r.p.exclusionChange && n < 3 {
		n = 3
	}
	w := &workflow{tid: tournament.TournamentID(fmt.Sprintf("c%d-t%d", c.id, c.created))}
	maxPlaces := min(3, n)
	if r.p.exclusionChange {
		// One entrant is rejected at Join, so at most n-1 can score.
		maxPlaces = min(3, n-1)
	}
	places := 1 + r.rng.IntN(maxPlaces)
	w.rules = tournament.Rules{
		EntryFee:    ledger.Money(100 + r.rng.Int64N(900)),
		RakeBps:     uint32(r.rng.IntN(2001)),
		PrizeBps:    randomPrizeTable(r, places),
		MinEntrants: places,
		MaxEntrants: n,
		MaxScore:    1000,
		MinAge:      18,
		TieBreak:    tournament.EarliestSubmission,
		Exclusions:  tournament.Exclusions{Version: listVersionAtCreate, Jurisdictions: []string{creationExcluded}},
	}
	if r.rng.IntN(2) == 0 {
		w.rules.TieBreak = tournament.Split
	}
	w.settleEx = tournament.Exclusions{Version: listVersionAtSettle, Jurisdictions: []string{creationExcluded}}
	if r.p.exclusionChange {
		w.settleEx.Jurisdictions = []string{creationExcluded, settleExcluded}
	}
	w.players = make([]tournament.Player, n)
	w.scores = make([]int64, n)
	w.skip = make([]bool, n)
	w.joined = make([]bool, n)
	for i := range w.players {
		w.players[i] = tournament.Player{
			ID:           tournament.PlayerID(fmt.Sprintf("p%d-%d", c.id, i+1)),
			Jurisdiction: jurisdictions[r.rng.IntN(len(jurisdictions))],
			Age:          18 + r.rng.IntN(50),
		}
		if r.rng.IntN(2) == 0 {
			w.scores[i] = int64(r.rng.IntN(6)) * 100 // ties are likely
		} else {
			w.scores[i] = r.rng.Int64N(1001)
		}
		w.skip[i] = r.p.skipScoreP > 0 && r.rng.Float64() < r.p.skipScoreP
	}
	if r.p.exclusionChange {
		// One entrant on the creation-time list, rejected at Join; one whose
		// jurisdiction the settlement-time list adds, who scores highest so
		// that the withheld payout is a prize.
		w.players[0].Jurisdiction = creationExcluded
		w.players[1].Jurisdiction = settleExcluded
		w.scores[1] = 1000
		w.skip[1] = false
	}
	return w
}

func randomPrizeTable(r *Run, places int) []uint32 {
	out := make([]uint32, places)
	remaining := uint32(10000)
	for i := 0; i < places-1; i++ {
		maxHere := remaining - uint32(places-1-i)
		out[i] = 1 + uint32(r.rng.IntN(int(maxHere)))
		remaining -= out[i]
	}
	out[places-1] = remaining
	return out
}

// nextCommand returns the workflow's next command op, skipping score steps
// of entrants that did not join or do not score, and nil when the
// workflow is complete.
func (w *workflow) nextCommand() (tournament.Op, bool) {
	n := w.entrants()
	for !w.done() {
		s := w.step
		switch {
		case s == 0:
			return tournament.CreateTournament{ID: w.tid, Rules: w.rules}, true
		case s <= n:
			return tournament.Join{Tournament: w.tid, Player: w.players[s-1]}, true
		case s <= 2*n:
			i := s - n - 1
			if !w.joined[i] || w.skip[i] {
				w.step++
				continue
			}
			var digest tournament.Digest
			digest[0] = byte(i)
			return tournament.SubmitScore{Tournament: w.tid, Player: w.players[i].ID, Score: w.scores[i], DealSeed: w.seed, InputDigest: digest}, true
		case s == 2*n+1:
			return tournament.Close{Tournament: w.tid}, true
		default:
			return tournament.Settle{Tournament: w.tid, Exclusions: w.settleEx}, true
		}
	}
	return nil, false
}

// stepName names a workflow step for traces and unexpected-result counts.
func (w *workflow) stepName(step int) string {
	n := w.entrants()
	switch {
	case step == 0:
		return "create"
	case step <= n:
		return "join"
	case step <= 2*n:
		return "score"
	case step == 2*n+1:
		return "close"
	}
	return "settle"
}

// clientTurn is one client action: observe the result of the pending
// command, issue the next one, re-submit a pending one whose result has not
// appeared, send due duplicates in the retry storm, and sometimes issue a
// consistent read. While faults are on, a client that finishes a batch
// starts another; after Heal it finishes what it has and stops.
func (r *Run) clientTurn(c *client) {
	for attempt := 0; attempt < 2; attempt++ {
		if c.pending == nil {
			if !r.advance(c) {
				break
			}
		}
		nd := r.pickTarget(c)
		if nd == nil {
			break
		}
		if res, ok := nd.core.State().Result(c.pending.key); ok {
			r.observeResult(c, nd, res)
			continue
		}
		if c.pending.attempts == 0 || r.clock >= c.pending.lastSent+retryInterval {
			r.submit(c, nd, c.pending.key, c.pending.op, false)
			c.pending.lastSent = r.clock
			c.pending.attempts++
		}
		break
	}
	r.sendExtras(c)
	if r.rng.Float64() < readP {
		if nd := r.pickTarget(c); nd != nil {
			r.issueRead(nd, c.id)
		}
	}
}

// advance sets c.pending to the next command of the client's workflows,
// starting a new tournament or a new batch as needed. It returns false when
// the client has nothing more to do.
func (r *Run) advance(c *client) bool {
	for {
		if c.wf == nil {
			if c.remaining == 0 {
				if !r.faultsOn || r.p.singleBatch {
					return false
				}
				c.remaining = c.perBatch
			}
			c.remaining--
			c.wf = r.newWorkflow(c)
		}
		op, ok := c.wf.nextCommand()
		if !ok {
			c.wf = nil
			continue
		}
		c.seq++
		c.issued++
		c.pending = &pendingCmd{
			key:    tournament.IdempotencyKey(fmt.Sprintf("c%d-%d", c.id, c.seq)),
			op:     op,
			step:   c.wf.step,
			issued: r.clock,
		}
		return true
	}
}

// observeResult handles the result of the pending command and moves the
// workflow on.
func (r *Run) observeResult(c *client, nd *simNode, res tournament.Result) {
	p := c.pending
	w := c.wf
	c.pending = nil
	c.done++
	r.report.Completed++
	name := w.stepName(p.step)
	expected := res.Code == tournament.OK
	n := w.entrants()
	switch {
	case p.step >= 1 && p.step <= n:
		i := p.step - 1
		if res.Code == tournament.OK {
			w.joined[i] = true
			w.seed = res.Seed
		} else if res.Code == tournament.JurisdictionExcluded && w.rules.Exclusions.Contains(w.players[i].Jurisdiction) {
			expected = true
			r.report.Marks["client.join_rejected_excluded"]++
		}
	case p.step == 2*n+2 && res.Code == tournament.OK:
		r.report.Marks["client.settled"]++
	}
	if !expected {
		r.report.Unexpected++
		r.tracef("client %d: UNEXPECTED %s result for %s (%s) on node %d: %s", c.id, res.Code, name, p.key, nd.id, res.Detail)
	} else {
		r.tracef("client %d: %s %s -> %s (slot %d, replayed %t) on node %d", c.id, name, p.key, res.Code, res.Slot, res.Replayed, nd.id)
	}
	w.step++
	if r.p.RetryP > 0 && r.rng.Float64() < r.p.RetryP {
		k := 1 + r.rng.IntN(maxExtraRetries)
		for i := 0; i < k; i++ {
			c.extras = append(c.extras, extraRetry{
				key: p.key, op: p.op, due: r.clock + r.jitter(500*time.Millisecond),
				mutated: r.rng.Float64() < mutateP,
			})
		}
	}
}

// sendExtras submits every due duplicate of the retry storm.
func (r *Run) sendExtras(c *client) {
	kept := c.extras[:0]
	for _, e := range c.extras {
		if e.due > r.clock {
			kept = append(kept, e)
			continue
		}
		nd := r.pickTarget(c)
		if nd == nil {
			kept = append(kept, e)
			continue
		}
		op := e.op
		if e.mutated {
			op = mutate(op)
			r.report.Mutated++
		}
		r.report.ExtraRetries++
		r.submit(c, nd, e.key, op, true)
	}
	c.extras = kept
}

// mutate returns a copy of op with one payload field changed, so that the
// fingerprint differs and the state machine must answer key_reused.
func mutate(op tournament.Op) tournament.Op {
	switch o := op.(type) {
	case tournament.CreateTournament:
		o.Rules.EntryFee++
		return o
	case tournament.Join:
		o.Player.Age++
		return o
	case tournament.SubmitScore:
		o.Score++
		return o
	case tournament.Close:
		o.Tournament += "-x"
		return o
	case tournament.Settle:
		o.Exclusions.Version++
		return o
	}
	return op
}

// submit proposes a command to nd under key. A CreateTournament gets a
// fresh server-side seed on every submission, as the leader's API would
// stamp one; the fingerprint excludes it.
func (r *Run) submit(c *client, nd *simNode, key tournament.IdempotencyKey, op tournament.Op, extra bool) {
	if ct, ok := op.(tournament.CreateTournament); ok {
		ct.Seed = r.rng.Uint64()
		op = ct
	}
	cmd := tournament.Command{Key: key, ReceivedAt: int64(r.clock / time.Millisecond), Op: op}
	if r.scenario != nil && r.scenario.onSubmit != nil {
		r.scenario.onSubmit(r, c, nd, cmd)
	}
	var err error
	r.callNode(nd, nil, func(now time.Duration) []replog.Envelope {
		outs, e := nd.core.Submit(now, cmd)
		err = e
		return outs
	})
	c.submits++
	r.report.Submits++
	if err != nil {
		var nl replog.NotLeaderError
		if errors.As(err, &nl) {
			c.target = nl.Leader
		} else {
			c.target = 0
		}
		r.tracef("client %d: submit %s to node %d: %v", c.id, key, nd.id, err)
		return
	}
	r.tracef("client %d: submitted %s (%s) to node %d (extra %t)", c.id, key, tournament.OpName(op), nd.id, extra)
	if r.scenario != nil && r.scenario.afterSubmit != nil && nd.alive {
		r.scenario.afterSubmit(r, c, nd, cmd)
	}
}

// pickTarget returns the client's current target if it is live and in the
// core, otherwise a random live core node, which becomes the target.
func (r *Run) pickTarget(c *client) *simNode {
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

// issueRead starts a read-index barrier on nd for client (or -1 for the
// simulator's own read at a leadership change) and registers it so that
// ReadReady can be checked against the commit index reached before the call.
func (r *Run) issueRead(nd *simNode, clientID int) {
	if !nd.alive {
		return
	}
	maxCommit := r.checker.maxCommit()
	gen := nd.gen
	r.checker.beforeCall(nd, nil)
	seq, outs, err := nd.core.ReadIndex(nd.now(r.clock))
	if err == nil {
		r.report.Reads++
		r.reads[readKey{node: nd.id, gen: gen, seq: seq}] = pendingRead{client: clientID, maxCommit: maxCommit, issued: r.clock}
		r.tracef("client %d: read %d issued on node %d (commit %d)", clientID, seq, nd.id, maxCommit)
	} else {
		r.tracef("client %d: read on node %d refused: %v", clientID, nd.id, err)
	}
	r.afterCall(nd, nil, outs)
}

// dropReadsFor forgets every pending read on a node that crashed.
func (r *Run) dropReadsFor(id paxos.NodeID) {
	for k := range r.reads {
		if k.node == id {
			delete(r.reads, k)
		}
	}
}
