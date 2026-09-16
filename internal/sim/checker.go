package sim

import (
	"bytes"
	"fmt"

	"github.com/oguzhanozfe/paxos-arena/internal/ledger"
	"github.com/oguzhanozfe/paxos-arena/internal/paxos"
	"github.com/oguzhanozfe/paxos-arena/internal/replica"
	"github.com/oguzhanozfe/paxos-arena/internal/replog"
	"github.com/oguzhanozfe/paxos-arena/internal/tournament"
)

// nodeRecord is what the checker remembers about one node between calls.
type nodeRecord struct {
	promised     paxos.Ballot // last observed promise (durable, survives restarts)
	commit       paxos.Slot   // last observed commit index
	prePromised  paxos.Ballot // promise before the call in progress
	preSlot      paxos.Slot   // slot of the Accept delivered by the call in progress, or 0
	preAccepted  paxos.PValue // accepted pvalue for preSlot before the call
	preHad       bool
	crashDurable *replog.Durable // durable state at the last crash, until restart

	// Per incarnation of the state machine (reset on restart): the first
	// result this node produced per key, and how many non-replayed results
	// it produced per key (S8).
	results     map[tournament.IdempotencyKey]tournament.Result
	nonReplayed map[tournament.IdempotencyKey]int
	// play is the play accounting of this incarnation (P2).
	play *playNodeRecord
}

func newNodeRecord() *nodeRecord {
	return &nodeRecord{
		results:     make(map[tournament.IdempotencyKey]tournament.Result),
		nonReplayed: make(map[tournament.IdempotencyKey]int),
		play:        newPlayNodeRecord(),
	}
}

// resetState forgets the state-machine accounting after a restart, when
// the node replays the log from slot 1.
func (rec *nodeRecord) resetState() {
	rec.results = make(map[tournament.IdempotencyKey]tournament.Result)
	rec.nonReplayed = make(map[tournament.IdempotencyKey]int)
	rec.play = newPlayNodeRecord()
}

type slotValue struct {
	slot  paxos.Slot
	value string
}

type ballotSlot struct {
	ballot paxos.Ballot
	slot   paxos.Slot
}

// applySnapshot is what beforeApply records so observeApply can tell what
// one application changed.
type applySnapshot struct {
	mutations uint64
	recorded  int
	events    int
	postings  int
	clock     int64
}

// settledSnapshot is the replicated part of a tournament at the moment its
// Settle was first observed applied, for D3.
type settledSnapshot struct {
	record   []byte
	postings []byte
	voided   bool
}

// Checker observes every node call, every message the nodes send and every
// applied entry, and reports the first violation of the log's safety
// invariants S1 through S8 and the domain invariants D1 through D6 (design
// section 6). It has a global view of every node's state through the
// observation methods of replog.Node and tournament.State.
//
// The checks are incremental: each hook examines only what the current
// event could have changed, so the cost per event is small. AtEnd replays
// the chosen log into a fresh state machine and sweeps every tournament on
// every live node.
type Checker struct {
	// chosen records the value first observed chosen for each slot, from
	// Learn messages and from nodes' Chosen after they learn (S1, S2).
	chosen map[paxos.Slot]paxos.Value
	// hashAt records the state hash after applying each slot (S2).
	hashAt map[paxos.Slot][32]byte
	// majority records a value held by a quorum of acceptors for a slot (S1
	// derived from acceptor state rather than from Learn).
	majority map[paxos.Slot]string
	// proposedWhenMax records, for the first Accept seen for (slot, value),
	// the highest slot chosen anywhere at that moment (S6).
	proposedWhenMax map[slotValue]paxos.Slot
	// ballotOwner and ballotSlotValue enforce ballot uniqueness (S4).
	ballotOwner     map[paxos.Ballot]paxos.NodeID
	ballotSlotValue map[ballotSlot]string
	// maxChosen is the highest slot observed chosen anywhere; maxCommitSeen
	// the highest commit index observed on any node (S6, S7).
	maxChosen     paxos.Slot
	maxCommitSeen paxos.Slot
	nodes         []*nodeRecord

	// firstResult is the first non-replayed result observed for each key on
	// any node; every other node must produce the same (S2, S8).
	firstResult map[tournament.IdempotencyKey]tournament.Result
	// settled holds the D3 snapshot per settled tournament.
	settled map[tournament.TournamentID]settledSnapshot
	// settleEx is the settlement-time exclusion list per tournament (D5).
	settleEx map[tournament.TournamentID]tournament.Exclusions
	// counters for the report
	applies, replays, keyReused, rejections int
	checks                                  map[string]int
	err                                     error
}

func newChecker(r *Run) *Checker {
	c := &Checker{
		chosen:          make(map[paxos.Slot]paxos.Value),
		hashAt:          make(map[paxos.Slot][32]byte),
		majority:        make(map[paxos.Slot]string),
		proposedWhenMax: make(map[slotValue]paxos.Slot),
		ballotOwner:     make(map[paxos.Ballot]paxos.NodeID),
		ballotSlotValue: make(map[ballotSlot]string),
		firstResult:     make(map[tournament.IdempotencyKey]tournament.Result),
		settled:         make(map[tournament.TournamentID]settledSnapshot),
		settleEx:        make(map[tournament.TournamentID]tournament.Exclusions),
		checks:          make(map[string]int),
	}
	for range r.nodes {
		c.nodes = append(c.nodes, newNodeRecord())
	}
	return c
}

// fail records the first violation.
func (c *Checker) fail(r *Run, invariant, format string, args ...any) {
	if c.err != nil {
		return
	}
	c.err = &Violation{
		Invariant: invariant,
		Detail:    fmt.Sprintf(format, args...),
		Seed:      r.p.Seed,
		Step:      r.steps,
		Scenario:  r.p.Scenario,
		At:        r.clock,
	}
	r.tracef("VIOLATION %v", c.err)
}

// count notes that invariant inv was evaluated once.
func (c *Checker) count(inv string) { c.checks[inv]++ }

// maxCommit returns the highest commit index observed on any node so far,
// crashed nodes included.
func (c *Checker) maxCommit() paxos.Slot { return c.maxCommitSeen }

// Chosen returns the recorded chosen value for a slot.
func (c *Checker) Chosen(s paxos.Slot) (paxos.Value, bool) {
	v, ok := c.chosen[s]
	return v, ok
}

// beforeCall snapshots what a node call may change: its promise, and the
// accepted pvalue of the slot an Accept being delivered names.
func (c *Checker) beforeCall(nd *simNode, delivered *replog.Envelope) {
	rec := c.nodes[int(nd.id)-1]
	rec.prePromised = nd.node.Promised()
	rec.preSlot = 0
	if delivered != nil {
		if a, ok := delivered.Msg.(replog.Accept); ok {
			rec.preSlot = a.Slot
			rec.preAccepted, rec.preHad = nd.node.Accepted(a.Slot)
		}
	}
}

// observeSend inspects a message a node is about to send: ballot ownership
// and one value per (ballot, slot) for S4, the proposal time of each
// (slot, value) for S6, and chosen values announced by Learn for S1.
func (c *Checker) observeSend(r *Run, nd *simNode, env replog.Envelope) {
	switch m := env.Msg.(type) {
	case replog.Accept:
		c.checkBallotOwner(r, m.Ballot, env.From)
		c.checkBallotSlotValue(r, m.Ballot, m.Slot, m.Value, env.From)
		if len(m.Value) > 0 {
			key := slotValue{m.Slot, string(m.Value)}
			if _, ok := c.proposedWhenMax[key]; !ok {
				c.proposedWhenMax[key] = c.maxChosen
			}
		}
	case replog.Prepare:
		c.checkBallotOwner(r, m.Ballot, env.From)
	case replog.Heartbeat:
		c.checkBallotOwner(r, m.Ballot, env.From)
	case replog.Learn:
		c.recordChosen(r, nd, m.Slot, replog.Entry{Slot: m.Slot, Ballot: m.Ballot, Value: m.Value}, "Learn sent by node")
	}
}

func (c *Checker) checkBallotOwner(r *Run, b paxos.Ballot, from paxos.NodeID) {
	c.count("S4")
	if b.Node != from {
		c.fail(r, "S4", "node %d uses ballot %v whose node component is not its own", from, b)
		return
	}
	if owner, ok := c.ballotOwner[b]; ok && owner != from {
		c.fail(r, "S4", "ballot %v used by node %d and node %d", b, owner, from)
		return
	}
	c.ballotOwner[b] = from
}

func (c *Checker) checkBallotSlotValue(r *Run, b paxos.Ballot, s paxos.Slot, v paxos.Value, from paxos.NodeID) {
	c.count("S4")
	key := ballotSlot{b, s}
	if prev, ok := c.ballotSlotValue[key]; ok {
		if prev != string(v) {
			c.fail(r, "S4", "two values proposed for ballot %v slot %d: %q and %q (node %d)", b, s, prev, v, from)
		}
		return
	}
	c.ballotSlotValue[key] = string(v)
}

// afterCall checks what one node call changed.
func (c *Checker) afterCall(r *Run, nd *simNode, delivered *replog.Envelope, outs []replog.Envelope) {
	rec := c.nodes[int(nd.id)-1]
	n := nd.node
	// S3: the promise never decreases.
	c.count("S3")
	if n.Promised().Less(rec.prePromised) {
		c.fail(r, "S3", "node %d promise decreased from %v to %v", nd.id, rec.prePromised, n.Promised())
	}
	// S3: an acceptance made during this call is at or above the promise
	// in force when the call began.
	if rec.preSlot != 0 {
		if pv, ok := n.Accepted(rec.preSlot); ok {
			changed := !rec.preHad || pv.Ballot != rec.preAccepted.Ballot || !paxos.ValueEqual(pv.Value, rec.preAccepted.Value)
			if changed {
				c.count("S3")
				if pv.Ballot.Less(rec.prePromised) {
					c.fail(r, "S3", "node %d accepted ballot %v for slot %d while promised %v", nd.id, pv.Ballot, pv.Slot, rec.prePromised)
				}
				c.checkBallotSlotValue(r, pv.Ballot, pv.Slot, pv.Value, pv.Ballot.Node)
				c.checkMajority(r, rec.preSlot)
			}
		}
	}
	// Commit index is monotone within one incarnation.
	if n.CommitIndex() < rec.commit {
		c.fail(r, "S2", "node %d commit index decreased from %d to %d", nd.id, rec.commit, n.CommitIndex())
	}
	rec.commit = n.CommitIndex()
	rec.promised = n.Promised()
	if rec.commit > c.maxCommitSeen {
		c.maxCommitSeen = rec.commit
	}
	// S4: a candidate or leader's ballot names itself and nobody else uses it.
	if n.Role() != replog.Follower {
		c.checkBallotOwner(r, n.Ballot(), nd.id)
	}
	// S1: what the node learned in this call agrees with the global record.
	if delivered != nil {
		if l, ok := delivered.Msg.(replog.Learn); ok {
			if e, ok := n.Chosen(l.Slot); ok {
				c.recordChosen(r, nd, l.Slot, e, "Chosen on node after Learn")
			}
		}
	}
	for _, o := range outs {
		if l, ok := o.Msg.(replog.Learn); ok {
			if e, ok := n.Chosen(l.Slot); ok {
				c.recordChosen(r, nd, l.Slot, e, "Chosen on node sending Learn")
			}
		}
	}
}

// checkMajority derives "chosen" from acceptor state: a (ballot, value)
// held by a quorum of live acceptors for a slot. Two different such values
// over time, or a difference from a learned value, violate S1.
func (c *Checker) checkMajority(r *Run, s paxos.Slot) {
	c.count("S1")
	counts := make(map[string]int)
	values := make(map[string]string)
	for _, o := range r.nodes {
		if !o.alive {
			continue
		}
		pv, ok := o.node.Accepted(s)
		if !ok {
			continue
		}
		k := pv.Ballot.String() + "|" + string(pv.Value)
		counts[k]++
		values[k] = string(pv.Value)
	}
	for k, cnt := range counts {
		if cnt < r.quorum {
			continue
		}
		v := values[k]
		if prev, ok := c.majority[s]; ok && prev != v {
			c.fail(r, "S1", "slot %d: a quorum accepted %s after a quorum had accepted %s", s, describeValue(paxos.Value(v)), describeValue(paxos.Value(prev)))
			return
		}
		c.majority[s] = v
		if cv, ok := c.chosen[s]; ok && string(cv) != v {
			c.fail(r, "S1", "slot %d: a quorum accepted %s but %s was learned as chosen", s, describeValue(paxos.Value(v)), describeValue(cv))
			return
		}
	}
}

// recordChosen compares an observed chosen entry with the record, adding it
// on first sight, and applies the gap rule (S6).
func (c *Checker) recordChosen(r *Run, nd *simNode, s paxos.Slot, e replog.Entry, how string) {
	c.count("S1")
	if v, ok := c.chosen[s]; ok {
		if !paxos.ValueEqual(v, e.Value) {
			c.fail(r, "S1", "slot %d chosen as %s (%s %d, ballot %v) but recorded as %s", s, describeValue(e.Value), how, nd.id, e.Ballot, describeValue(v))
		}
		return
	}
	c.chosen[s] = e.Value
	if !e.NoOp() {
		c.count("S6")
		if m, ok := c.proposedWhenMax[slotValue{s, string(e.Value)}]; ok && m > s {
			c.fail(r, "S6", "slot %d chosen with command %s that was first proposed after slot %d was already chosen", s, describeValue(e.Value), m)
		}
	}
	if mv, ok := c.majority[s]; ok && mv != string(e.Value) {
		c.fail(r, "S1", "slot %d learned as %s but a quorum had accepted %s", s, describeValue(e.Value), describeValue(paxos.Value(mv)))
	}
	if s > c.maxChosen {
		c.maxChosen = s
	}
}

// beforeApply snapshots the state-machine counters before one application.
func (c *Checker) beforeApply(nd *simNode) applySnapshot {
	st := nd.core.State()
	return applySnapshot{mutations: st.Mutations(), recorded: st.Recorded(), events: st.EventCount(), postings: st.Ledger().Len(), clock: st.Clock()}
}

// observeApply checks one applied slot: the chosen record and the state
// hash (S2), the results table discipline (S8) and the domain invariants of
// the tournament the command addressed (D1 to D6).
func (c *Checker) observeApply(r *Run, nd *simNode, a replica.Applied, snap applySnapshot) {
	st := nd.core.State()
	e, ok := nd.node.Chosen(a.Slot)
	if !ok {
		c.fail(r, "S2", "node %d applied slot %d which its log does not hold as chosen", nd.id, a.Slot)
		return
	}
	if v, ok := c.chosen[a.Slot]; ok {
		if !paxos.ValueEqual(v, e.Value) {
			c.fail(r, "S2", "node %d applied %s at slot %d but %s was chosen", nd.id, describeValue(e.Value), a.Slot, describeValue(v))
			return
		}
	} else {
		c.recordChosen(r, nd, a.Slot, e, "applied on node")
	}
	c.count("S2")
	hash := st.Hash()
	if h, ok := c.hashAt[a.Slot]; ok {
		if h != hash {
			c.fail(r, "S2", "node %d state hash after slot %d differs from the hash first recorded at that slot", nd.id, a.Slot)
			return
		}
	} else {
		c.hashAt[a.Slot] = hash
	}
	if a.NoOp {
		if a.Err != nil {
			c.fail(r, "S2", "node %d could not decode the chosen value of slot %d: %v", nd.id, a.Slot, a.Err)
		}
		return
	}
	c.applies++
	rec := c.nodes[int(nd.id)-1]
	res := a.Result
	key := a.Key
	dm := st.Mutations() - snap.mutations
	dr := st.Recorded() - snap.recorded
	c.count("S8")
	switch {
	case res.Code == tournament.KeyReused:
		c.keyReused++
		if dm != 0 || dr != 0 {
			c.fail(r, "S8", "node %d: key_reused for %s at slot %d changed the state (mutations +%d, recorded +%d)", nd.id, key, a.Slot, dm, dr)
		}
	case res.Replayed:
		c.replays++
		if dm != 0 || dr != 0 {
			c.fail(r, "S8", "node %d: replay of %s at slot %d changed the state (mutations +%d, recorded +%d)", nd.id, key, a.Slot, dm, dr)
		}
		first, ok := rec.results[key]
		if !ok {
			c.fail(r, "S8", "node %d replayed %s at slot %d without having produced a result for it", nd.id, key, a.Slot)
		} else if !sameResult(first, res) {
			c.fail(r, "S8", "node %d replayed %s at slot %d as %+v, but its recorded result was %+v", nd.id, key, a.Slot, res, first)
		}
	default:
		rec.nonReplayed[key]++
		if rec.nonReplayed[key] > 1 {
			c.fail(r, "S8", "node %d produced a second non-replayed result for %s at slot %d (%s)", nd.id, key, a.Slot, res.Code)
		}
		rec.results[key] = res
		if first, ok := c.firstResult[key]; ok {
			if !sameResult(first, res) {
				c.fail(r, "S2", "node %d produced %+v for %s but another node produced %+v", nd.id, res, key, first)
			}
		} else {
			c.firstResult[key] = res
		}
		if res.Code == tournament.OK {
			if dm != 1 || dr != 1 {
				c.fail(r, "S8", "node %d: first application of %s at slot %d changed mutations by %d and recorded by %d, want 1 and 1", nd.id, key, a.Slot, dm, dr)
			}
		} else {
			c.rejections++
			if dm != 0 || dr != 1 {
				c.fail(r, "S8", "node %d: rejection of %s at slot %d changed mutations by %d and recorded by %d, want 0 and 1", nd.id, key, a.Slot, dm, dr)
			}
		}
	}
	tid := tournament.TournamentOf(a.Command.Op)
	if s, ok := a.Command.Op.(tournament.Settle); ok && res.Code == tournament.OK && !res.Replayed {
		if _, seen := c.settleEx[tid]; !seen {
			c.settleEx[tid] = tournament.Canonical(s).(tournament.Settle).Exclusions
		}
	}
	c.checkTournament(r, nd, st, tid)
	c.checkPlayApply(r, nd, st, a, snap)
	c.count("D6")
	if err := st.Ledger().CheckBalances(); err != nil {
		c.fail(r, "D6", "node %d after slot %d: %v", nd.id, a.Slot, err)
	}
	if c.applies%64 == 0 {
		if err := st.Ledger().Check(); err != nil {
			c.fail(r, "D6", "node %d after slot %d: %v", nd.id, a.Slot, err)
		}
	}
}

// sameResult compares results ignoring the Replayed flag.
func sameResult(a, b tournament.Result) bool {
	a.Replayed, b.Replayed = false, false
	return a == b
}

// checkTournament evaluates D1 to D5 on one tournament of one node.
func (c *Checker) checkTournament(r *Run, nd *simNode, st *tournament.State, tid tournament.TournamentID) {
	t, ok := st.Tournament(tid)
	if !ok {
		return
	}
	book := st.Ledger()
	posts := book.ForTournament(string(tid))
	// D5: entries respect the creation-time list and the minimum age and
	// carry the creation-time version.
	c.count("D5")
	for _, e := range t.Entries {
		if t.Rules.Exclusions.Contains(e.Player.Jurisdiction) {
			c.fail(r, "D5", "node %d: tournament %s has entrant %s from excluded jurisdiction %s", nd.id, tid, e.Player.ID, e.Player.Jurisdiction)
		}
		if e.Player.Age < t.Rules.MinAge {
			c.fail(r, "D5", "node %d: tournament %s has entrant %s aged %d below the minimum %d", nd.id, tid, e.Player.ID, e.Player.Age, t.Rules.MinAge)
		}
		if e.ExclusionVersion != t.Rules.Exclusions.Version {
			c.fail(r, "D5", "node %d: tournament %s entrant %s carries list version %d, want %d", nd.id, tid, e.Player.ID, e.ExclusionVersion, t.Rules.Exclusions.Version)
		}
	}
	// D2: exactly one fee posting per entry, and no payout postings before
	// Settle.
	c.count("D2")
	kinds := make(map[ledger.Kind]int)
	byKey := make(map[ledger.PostingKey]ledger.Posting, len(posts))
	for _, p := range posts {
		kinds[p.Kind]++
		byKey[p.Key] = p
	}
	if kinds[ledger.EntryFee] != len(t.Entries) {
		c.fail(r, "D2", "node %d: tournament %s has %d entries but %d fee postings", nd.id, tid, len(t.Entries), kinds[ledger.EntryFee])
	}
	for _, e := range t.Entries {
		p, ok := byKey[ledger.FeeKey(string(tid), string(e.Player.ID))]
		if !ok || p.Amount != t.Rules.EntryFee || p.ExclusionVersion != t.Rules.Exclusions.Version {
			c.fail(r, "D2", "node %d: tournament %s entrant %s fee posting missing or wrong: %+v", nd.id, tid, e.Player.ID, p)
		}
	}
	if t.Status != tournament.Settled && (kinds[ledger.Prize]+kinds[ledger.Withheld]+kinds[ledger.Refund]+kinds[ledger.Rake]) > 0 {
		c.fail(r, "D2", "node %d: tournament %s is %s but has payout or rake postings", nd.id, tid, t.Status)
	}
	if t.Status == tournament.Open {
		return
	}
	// D1: the pool arithmetic of the record.
	c.count("D1")
	fees, rake, pool := tournament.ComputePool(&t)
	if t.Fees != fees || t.Rake != rake || t.Pool != pool || t.Pool != t.Fees-t.Rake {
		c.fail(r, "D1", "node %d: tournament %s records fees %d rake %d pool %d, recomputed %d %d %d", nd.id, tid, t.Fees, t.Rake, t.Pool, fees, rake, pool)
	}
	if t.Status == tournament.Voided && t.Rake != 0 {
		c.fail(r, "D1", "node %d: voided tournament %s has rake %d", nd.id, tid, t.Rake)
	}
	if (t.Status == tournament.Voided) != t.Voids() && t.Status != tournament.Settled {
		c.fail(r, "D1", "node %d: tournament %s status %s disagrees with its scored count", nd.id, tid, t.Status)
	}
	// D4: standings are a function of the scores and the tie-break rule.
	c.count("D4")
	want := tournament.ComputeStandings(&t)
	if len(want) != len(t.Standings) {
		c.fail(r, "D4", "node %d: tournament %s has %d standings, recomputed %d", nd.id, tid, len(t.Standings), len(want))
	} else {
		for i := range want {
			if want[i] != t.Standings[i] {
				c.fail(r, "D4", "node %d: tournament %s standing %d is %+v, recomputed %+v", nd.id, tid, i, t.Standings[i], want[i])
				break
			}
		}
	}
	if t.Status != tournament.Settled {
		return
	}
	// D1 on the book: the pool nets to zero and the rake account holds the
	// rake.
	c.count("D1")
	if got := book.Balance(ledger.PoolAccount(string(tid))); got != 0 {
		c.fail(r, "D1", "node %d: settled tournament %s pool account holds %d", nd.id, tid, got)
	}
	if got := book.Balance(ledger.RakeAccount(string(tid))); got != t.Rake {
		c.fail(r, "D1", "node %d: settled tournament %s rake account holds %d, want %d", nd.id, tid, got, t.Rake)
	}
	// D2 and D5 on the payouts.
	c.count("D2")
	c.count("D5")
	ex, haveEx := c.settleEx[tid]
	var sum ledger.Money
	seen := make(map[[2]string]bool)
	for _, p := range t.Payouts {
		sum += p.Amount
		pair := [2]string{string(p.Player), fmt.Sprint(p.Place)}
		if seen[pair] {
			c.fail(r, "D2", "node %d: tournament %s pays %s twice for place %d", nd.id, tid, p.Player, p.Place)
		}
		seen[pair] = true
		post, has := byKey[p.Key]
		if p.Amount > 0 {
			if !has || post.Amount != p.Amount || post.Player != string(p.Player) || post.Place != p.Place || post.ExclusionVersion != p.ExclusionVersion {
				c.fail(r, "D2", "node %d: tournament %s payout %+v has no matching posting (%+v)", nd.id, tid, p, post)
			}
		} else if has {
			c.fail(r, "D2", "node %d: tournament %s posted a zero payout %+v", nd.id, tid, p)
		}
		if haveEx {
			e, _ := t.Entry(p.Player)
			excluded := ex.Contains(e.Player.Jurisdiction)
			if p.Withheld != excluded || p.ExclusionVersion != ex.Version {
				c.fail(r, "D5", "node %d: tournament %s payout to %s (%s) withheld=%t version=%d, settle list v%d %v", nd.id, tid, p.Player, e.Player.Jurisdiction, p.Withheld, p.ExclusionVersion, ex.Version, ex.Jurisdictions)
			}
			if p.Withheld {
				r.report.Marks["checker.withheld_payouts"]++
			}
		}
	}
	if t.Voids() {
		if len(t.Payouts) != len(t.Entries) || sum != t.Fees || kinds[ledger.Refund] != len(t.Payouts) || kinds[ledger.Prize]+kinds[ledger.Withheld]+kinds[ledger.Rake] != 0 {
			c.fail(r, "D2", "node %d: voided tournament %s: %d payouts for %d entries summing to %d (fees %d), postings %v", nd.id, tid, len(t.Payouts), len(t.Entries), sum, t.Fees, kinds)
		}
	} else {
		if len(t.Payouts) != len(t.Rules.PrizeBps) || sum != t.Pool {
			c.fail(r, "D2", "node %d: settled tournament %s: %d payouts for %d places summing to %d (pool %d)", nd.id, tid, len(t.Payouts), len(t.Rules.PrizeBps), sum, t.Pool)
		}
		if kinds[ledger.Refund] != 0 || (t.Rake > 0) != (kinds[ledger.Rake] == 1) {
			c.fail(r, "D2", "node %d: settled tournament %s has postings %v for rake %d", nd.id, tid, kinds, t.Rake)
		}
	}
	// Every payout posting belongs to a payout.
	payoutKeys := make(map[ledger.PostingKey]bool, len(t.Payouts))
	for _, p := range t.Payouts {
		payoutKeys[p.Key] = true
	}
	for _, p := range posts {
		switch p.Kind {
		case ledger.Prize, ledger.Withheld, ledger.Refund:
			if !payoutKeys[p.Key] {
				c.fail(r, "D2", "node %d: tournament %s has posting %s with no payout", nd.id, tid, p.Key)
			}
		}
	}
	// D3: the replicated record and postings never change after Settle.
	// A player's claim of a settled payout (docs/UNITY-INTEGRATION.md
	// section 6.8) is the one posting that follows a settlement; it moves
	// money out of a player account, not out of the settled tournament.
	c.count("D3")
	enc := tournament.EncodeTournament(t)
	settledPosts := make([]ledger.Posting, 0, len(posts))
	for _, p := range posts {
		if p.Kind != ledger.Claim {
			settledPosts = append(settledPosts, p)
		}
	}
	pe := tournament.EncodePostings(settledPosts)
	if snap, ok := c.settled[tid]; ok {
		if !bytes.Equal(snap.record, enc) {
			c.fail(r, "D3", "node %d: settled tournament %s record differs from its record at settlement", nd.id, tid)
		}
		if !bytes.Equal(snap.postings, pe) {
			c.fail(r, "D3", "node %d: settled tournament %s postings differ from those at settlement", nd.id, tid)
		}
	} else {
		c.settled[tid] = settledSnapshot{record: enc, postings: pe, voided: t.Voids()}
	}
}

// observeCrash keeps the durable state at the moment of a crash.
func (c *Checker) observeCrash(nd *simNode, d replog.Durable) {
	c.nodes[int(nd.id)-1].crashDurable = &d
}

// observeRestart checks that a restarted node recovered its durable state
// (S5) and that its promise and commit index did not regress, and resets
// the state-machine accounting because the node replays from slot 1.
func (c *Checker) observeRestart(r *Run, nd *simNode) {
	rec := c.nodes[int(nd.id)-1]
	n := nd.node
	c.count("S5")
	if d := rec.crashDurable; d != nil {
		if n.Promised().Less(d.Promised) {
			c.fail(r, "S5", "node %d restarted with promise %v below the durable %v", nd.id, n.Promised(), d.Promised)
		}
		if n.MaxRound() < d.MaxRound {
			c.fail(r, "S5", "node %d restarted with max round %d below the durable %d", nd.id, n.MaxRound(), d.MaxRound)
		}
		for _, pv := range d.Accepted {
			got, ok := n.Accepted(pv.Slot)
			if !ok {
				c.fail(r, "S5", "node %d lost the accepted pvalue for slot %d (%v) across a restart", nd.id, pv.Slot, pv.Ballot)
				break
			}
			if got.Ballot.Less(pv.Ballot) || (got.Ballot == pv.Ballot && !paxos.ValueEqual(got.Value, pv.Value)) {
				c.fail(r, "S5", "node %d restarted with accepted %v for slot %d, durable was %v", nd.id, got.Ballot, pv.Slot, pv.Ballot)
				break
			}
		}
		for _, e := range d.Chosen {
			got, ok := n.Chosen(e.Slot)
			if !ok || !paxos.ValueEqual(got.Value, e.Value) {
				c.fail(r, "S5", "node %d lost the chosen entry for slot %d across a restart", nd.id, e.Slot)
				break
			}
		}
		rec.crashDurable = nil
	}
	if n.Promised().Less(rec.promised) {
		c.fail(r, "S3", "node %d promise decreased across a restart from %v to %v", nd.id, rec.promised, n.Promised())
	}
	if n.CommitIndex() < rec.commit {
		c.fail(r, "S5", "node %d commit index regressed across a restart from %d to %d", nd.id, rec.commit, n.CommitIndex())
	}
	rec.promised = n.Promised()
	rec.commit = n.CommitIndex()
	rec.resetState()
}

// observeReadReady checks S7: the index a read barrier returns is at least
// the highest commit index reached anywhere before the read was issued.
func (c *Checker) observeReadReady(r *Run, nd *simNode, pr pendingRead, index paxos.Slot) {
	c.count("S7")
	if index < pr.maxCommit {
		c.fail(r, "S7", "node %d answered a read issued at t=%v with index %d, but commit index %d had been reached before the call",
			nd.id, pr.issued, index, pr.maxCommit)
	}
}

// AfterEvent returns the first violation found so far. The checks
// themselves run in the hooks the run loop calls during each event.
func (c *Checker) AfterEvent(r *Run) error { return c.err }

// AtEnd replays the chosen prefix into a fresh state machine and compares
// its hashes with those every node produced (S2), requires live nodes that
// applied the same slot to hold the same state hash (S2), checks every live
// node's chosen prefix against the record (S1), sweeps every tournament on
// every live node (D1 to D6, and P1, P4 and P5 for play tournaments), and
// requires every node that has applied a key to hold exactly one result for
// it (S8).
func (c *Checker) AtEnd(r *Run) error {
	if c.err != nil {
		return c.err
	}
	hashes := make(map[paxos.Slot][32]byte)
	for _, nd := range r.nodes {
		if !nd.alive {
			continue
		}
		st := nd.core.State()
		c.count("S2")
		if h, ok := hashes[st.Applied()]; ok && h != st.Hash() {
			c.fail(r, "S2", "node %d ends at applied slot %d with a state hash that differs from another node's at that slot", nd.id, st.Applied())
			return c.err
		}
		hashes[st.Applied()] = st.Hash()
	}
	st := tournament.NewState()
	for s := paxos.Slot(1); ; s++ {
		v, ok := c.chosen[s]
		if !ok {
			break
		}
		if len(v) == 0 {
			st.Skip(s)
		} else {
			cmd, err := tournament.Decode([]byte(v))
			if err != nil {
				st.Skip(s)
			} else {
				st.Apply(s, paxos.Ballot{}, cmd)
			}
		}
		c.count("S2")
		if rh, ok := c.hashAt[s]; ok && rh != st.Hash() {
			c.fail(r, "S2", "replaying the chosen log gives a different state hash at slot %d than the nodes recorded", s)
			return c.err
		}
	}
	for _, nd := range r.nodes {
		if !nd.alive {
			continue
		}
		for s := paxos.Slot(1); s <= nd.node.CommitIndex(); s++ {
			e, ok := nd.node.Chosen(s)
			if !ok {
				c.fail(r, "S2", "node %d: commit index %d but slot %d is not chosen", nd.id, nd.node.CommitIndex(), s)
				return c.err
			}
			if v, ok := c.chosen[s]; ok && !paxos.ValueEqual(v, e.Value) {
				c.fail(r, "S1", "node %d holds %s at slot %d, recorded chosen value is %s", nd.id, describeValue(e.Value), s, describeValue(v))
				return c.err
			}
		}
		state := nd.core.State()
		for _, tid := range state.Tournaments() {
			c.checkTournament(r, nd, state, tid)
			if c.err != nil {
				return c.err
			}
		}
		c.checkPlayAtEnd(r, nd, state)
		if c.err != nil {
			return c.err
		}
		c.count("D6")
		if err := state.Ledger().Check(); err != nil {
			c.fail(r, "D6", "node %d at the end: %v", nd.id, err)
			return c.err
		}
		rec := c.nodes[int(nd.id)-1]
		for key, n := range rec.nonReplayed {
			c.count("S8")
			if n != 1 {
				c.fail(r, "S8", "node %d holds %d non-replayed results for %s", nd.id, n, key)
				return c.err
			}
		}
	}
	return c.err
}

// describeValue renders a log value for messages: the command's op and key
// when it decodes, the raw bytes otherwise.
func describeValue(v paxos.Value) string {
	if len(v) == 0 {
		return "no-op"
	}
	cmd, err := tournament.Decode([]byte(v))
	if err != nil {
		return fmt.Sprintf("%q", v)
	}
	return fmt.Sprintf("%s(%s)", tournament.OpName(cmd.Op), cmd.Key)
}
