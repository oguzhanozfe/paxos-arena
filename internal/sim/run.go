package sim

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"strings"
	"time"

	"github.com/oguzhanozfe/paxos-arena/internal/paxos"
	"github.com/oguzhanozfe/paxos-arena/internal/replica"
	"github.com/oguzhanozfe/paxos-arena/internal/replog"
	"github.com/oguzhanozfe/paxos-arena/internal/transport"
)

// Run is one deterministic simulation of a cluster. Create it with New, run
// the fault phase with RunFaults, call Heal, then RunLiveness. Step runs one
// event at a time for finer control.
type Run struct {
	p           Params
	rng         *rand.Rand
	clock       time.Duration
	events      eventHeap
	seq         uint64
	net         *transport.Network
	nodes       []*simNode
	clients     []*client
	playClients []*playClient
	checker     *Checker
	trace       io.Writer
	scenario    *scenario
	steps       int
	faultsOn    bool
	healed      bool
	partitioned bool
	filter      func(replog.Envelope) bool
	core        map[paxos.NodeID]bool
	reads       map[readKey]pendingRead
	eager       []*simNode
	late        *lateLearner
	mid         *midSettlement
	dup         *dupIntents
	claimSplit  *claimPartition
	expiry      *tokenExpiry
	dealChange  *dealChange
	report      Report
	violation   error
	quorum      int
}

// New builds a run from p. trace, when non-nil, receives one line per event
// in a compact format that is byte-identical across runs with the same
// Params.
func New(p Params, trace io.Writer) (*Run, error) {
	if err := p.Validate(); err != nil {
		return nil, err
	}
	sc := findScenario(p.Scenario)
	if sc != nil && sc.params != nil {
		sc.params(&p)
		if err := p.Validate(); err != nil {
			return nil, fmt.Errorf("sim: scenario %q produced invalid params: %w", sc.name, err)
		}
	}
	r := &Run{
		p:        p,
		rng:      rand.New(rand.NewPCG(p.Seed, 0)),
		trace:    trace,
		scenario: sc,
		faultsOn: true,
		reads:    make(map[readKey]pendingRead),
		quorum:   paxos.Quorum(p.Nodes),
	}
	r.report.Marks = make(map[string]int)
	r.report.Nodes = p.Nodes
	r.report.LeaseEnabled = !p.NoLease
	r.net = transport.NewNetwork(r.rng, p.Faults)
	ids := make([]paxos.NodeID, p.Nodes)
	for i := range ids {
		ids[i] = paxos.NodeID(i + 1)
	}
	for _, id := range ids {
		cfg := replog.DefaultConfig(id, ids)
		if p.NoLease {
			cfg.LeaseDuration = 0
		}
		cfg.Unsafe = p.unsafe
		nd := &simNode{id: id, cfg: cfg, store: newFaultStore(), rate: 1}
		if p.ClockSkewMax > 0 {
			nd.offset = time.Duration(r.rng.Int64N(int64(p.ClockSkewMax) + 1))
			nd.rate = 0.5 + 1.5*r.rng.Float64()
		}
		r.nodes = append(r.nodes, nd)
	}
	r.checker = newChecker(r)
	for _, nd := range r.nodes {
		if err := r.start(nd); err != nil {
			return nil, err
		}
	}
	for i := 0; i < p.Clients; i++ {
		r.clients = append(r.clients, newClient(i, p.tournamentsPerBatch()))
		r.schedule(event{at: r.jitter(clientInterval), kind: evClient, client: i})
	}
	for i := 0; i < p.PlayClients; i++ {
		r.playClients = append(r.playClients, newPlayClient(i))
		r.schedule(event{at: r.jitter(clientInterval), kind: evPlayClient, client: i})
	}
	r.schedule(event{at: faultInterval, kind: evFault})
	if sc != nil && sc.setup != nil {
		sc.setup(r)
	}
	return r, nil
}

// Clock returns the virtual time.
func (r *Run) Clock() time.Duration { return r.clock }

// Steps returns the number of events processed so far.
func (r *Run) Steps() int { return r.steps }

// Params returns the effective parameters, after the scenario's adjustments.
func (r *Run) Params() Params { return r.p }

// Violation returns the first invariant violation found, or nil.
func (r *Run) Violation() error { return r.violation }

func (r *Run) schedule(ev event) {
	r.seq++
	ev.seq = r.seq
	heap.Push(&r.events, ev)
}

// scheduleScript runs fn at time at, if faults are still on.
func (r *Run) scheduleScript(at time.Duration, fn func(*Run)) {
	r.schedule(event{at: at, kind: evScript, fn: fn})
}

func (r *Run) jitter(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	return time.Duration(r.rng.Int64N(int64(d) + 1))
}

func (r *Run) index(nd *simNode) int { return int(nd.id) - 1 }

func (r *Run) nodeByID(id paxos.NodeID) *simNode {
	i := int(id) - 1
	if i < 0 || i >= len(r.nodes) {
		return nil
	}
	return r.nodes[i]
}

func (r *Run) inCore(nd *simNode) bool { return r.core == nil || r.core[nd.id] }

// coreNodes returns every node, or the healed core after HealCore.
func (r *Run) coreNodes() []*simNode {
	if r.core == nil {
		return r.nodes
	}
	out := make([]*simNode, 0, len(r.core))
	for _, nd := range r.nodes {
		if r.core[nd.id] {
			out = append(out, nd)
		}
	}
	return out
}

func (r *Run) aliveNodes() []*simNode {
	out := make([]*simNode, 0, len(r.nodes))
	for _, nd := range r.nodes {
		if nd.alive && r.inCore(nd) {
			out = append(out, nd)
		}
	}
	return out
}

// leaderNode returns the live node whose role is Leader, if exactly one.
func (r *Run) leaderNode() *simNode {
	var l *simNode
	for _, nd := range r.nodes {
		if nd.alive && nd.node.Role() == replog.Leader {
			if l != nil {
				return nil
			}
			l = nd
		}
	}
	return l
}

// start builds the core from the node's store and schedules its ticks.
func (r *Run) start(nd *simNode) error {
	core, err := replica.NewCore(nd.cfg, nd.store, r.rng)
	if err != nil {
		return fmt.Errorf("sim: node %d: %w", nd.id, err)
	}
	nd.core = core
	nd.node = core.Log()
	nd.applied = 0
	nd.alive = true
	r.schedule(event{at: r.clock + tickInterval, kind: evTick, node: r.index(nd), gen: nd.gen})
	return nil
}

// Step processes the earliest event or message delivery. A non-nil error is
// an invariant violation (or the run having nothing left to do); once a
// violation is found every later Step returns it.
func (r *Run) Step() error {
	if r.violation != nil {
		return r.violation
	}
	netAt, netOK := r.net.PeekTime()
	if len(r.events) == 0 && !netOK {
		return errors.New("sim: nothing left to run")
	}
	if netOK && (len(r.events) == 0 || netAt <= r.events[0].at) {
		at, env, _ := r.net.Next()
		if at > r.clock {
			r.clock = at
		}
		r.deliver(env)
	} else {
		ev := heap.Pop(&r.events).(event)
		if ev.at > r.clock {
			r.clock = ev.at
		}
		r.handleEvent(ev)
	}
	r.steps++
	r.report.Steps = r.steps
	if r.violation == nil && r.scenario != nil && r.scenario.onEvent != nil {
		r.scenario.onEvent(r)
	}
	if r.violation == nil {
		r.violation = r.checker.AfterEvent(r)
	}
	return r.violation
}

func (r *Run) deliver(env replog.Envelope) {
	nd := r.nodeByID(env.To)
	if nd == nil || !nd.alive {
		r.tracef("drop %d->%d %s: node down", env.From, env.To, describe(env.Msg))
		return
	}
	r.tracef("deliver %d->%d %s", env.From, env.To, describe(env.Msg))
	r.callNode(nd, &env, func(now time.Duration) []replog.Envelope { return nd.core.Step(now, env) })
}

// callNode wraps one call into a node with the checker's before and after
// hooks, sends the node's output and applies what became committed.
func (r *Run) callNode(nd *simNode, delivered *replog.Envelope, fn func(now time.Duration) []replog.Envelope) {
	r.checker.beforeCall(nd, delivered)
	outs := fn(nd.now(r.clock))
	r.afterCall(nd, delivered, outs)
}

func (r *Run) afterCall(nd *simNode, delivered *replog.Envelope, outs []replog.Envelope) {
	if err := nd.core.Failed(); err != nil {
		r.tracef("node %d: %v", nd.id, err)
		r.crash(nd, "torn write")
		return
	}
	for _, e := range outs {
		r.checker.observeSend(r, nd, e)
		r.tracef("send %d->%d %s", e.From, e.To, describe(e.Msg))
		r.net.Send(r.clock, e)
	}
	for _, ev := range nd.node.Events() {
		r.onNodeEvent(nd, ev)
	}
	if r.scenario != nil && r.scenario.onCall != nil {
		r.scenario.onCall(r, nd, delivered, outs)
		if !nd.alive {
			return
		}
	}
	r.applyCommitted(nd)
	if !nd.alive {
		return
	}
	r.checker.afterCall(r, nd, delivered, outs)
	// Reads queued by events of this call run now, after the checker has
	// seen the call, so their own before/after hooks do not nest.
	for len(r.eager) > 0 {
		next := r.eager[0]
		r.eager = r.eager[1:]
		if next.alive {
			r.issueRead(next, -1)
		}
	}
}

func (r *Run) onNodeEvent(nd *simNode, ev replog.Event) {
	switch e := ev.(type) {
	case replog.LeaderChanged:
		if e.Self {
			r.report.Elections++
			r.tracef("node %d became leader at %v (commit %d)", nd.id, e.Ballot, nd.node.CommitIndex())
			r.eager = append(r.eager, nd)
		} else {
			r.report.LeaderChanges++
			r.tracef("node %d sees leader %d at %v", nd.id, e.Leader, e.Ballot)
		}
	case replog.ReadReady:
		key := readKey{node: nd.id, gen: nd.gen, seq: e.Seq}
		if pr, ok := r.reads[key]; ok {
			delete(r.reads, key)
			r.report.ReadsCompleted++
			r.tracef("node %d read %d ready at index %d (commit %d at call)", nd.id, e.Seq, e.Index, pr.maxCommit)
			r.checker.observeReadReady(r, nd, pr, e.Index)
		}
	case replog.ReadFailed:
		key := readKey{node: nd.id, gen: nd.gen, seq: e.Seq}
		if _, ok := r.reads[key]; ok {
			delete(r.reads, key)
			r.report.ReadsFailed++
			r.tracef("node %d read %d failed: %v", nd.id, e.Seq, e.Err)
		}
	case replog.LearnConflict:
		r.checker.fail(r, "S1", "node %d received Learn for slot %d with value %s at %v but had chosen %s at %v",
			nd.id, e.Slot, describeValue(e.Got.Value), e.Got.Ballot, describeValue(e.Have.Value), e.Have.Ballot)
	}
}

// applyCommitted feeds up to applyBatch chosen entries above the node's
// applied slot into its state machine, in order, with the checker's hooks
// around each one. A scripted scenario may crash the node from its
// onApplied hook, which ends the batch.
func (r *Run) applyCommitted(nd *simNode) {
	for i := 0; i < applyBatch && nd.alive; i++ {
		snap := r.checker.beforeApply(nd)
		a, ok := nd.core.ApplyNext()
		if !ok {
			return
		}
		nd.applied = a.Slot
		if a.NoOp {
			r.tracef("node %d applied slot %d: no-op", nd.id, a.Slot)
		} else {
			r.tracef("node %d applied slot %d: %s(%s) -> %s replayed=%t", nd.id, a.Slot, describeOp(a), a.Key, a.Result.Code, a.Result.Replayed)
		}
		r.checker.observeApply(r, nd, a, snap)
		if r.scenario != nil && r.scenario.onApplied != nil {
			r.scenario.onApplied(r, nd, a)
		}
	}
}

func describeOp(a replica.Applied) string {
	if a.Command.Op == nil {
		return "?"
	}
	return fmt.Sprintf("%T", a.Command.Op)[len("tournament."):]
}

func (r *Run) handleEvent(ev event) {
	switch ev.kind {
	case evTick:
		nd := r.nodes[ev.node]
		if !nd.alive || nd.gen != ev.gen {
			return
		}
		r.callNode(nd, nil, func(now time.Duration) []replog.Envelope { return nd.core.Tick(now) })
		if nd.alive && nd.gen == ev.gen {
			r.schedule(event{at: r.clock + tickInterval, kind: evTick, node: ev.node, gen: ev.gen})
		}
	case evClient:
		r.clientTurn(r.clients[ev.client])
		r.schedule(event{at: r.clock + clientInterval + r.jitter(clientInterval), kind: evClient, client: ev.client})
	case evPlayClient:
		r.playTurn(r.playClients[ev.client])
		r.schedule(event{at: r.clock + clientInterval + r.jitter(clientInterval), kind: evPlayClient, client: ev.client})
	case evFault:
		if r.faultsOn {
			r.randomFaults()
		}
		r.schedule(event{at: r.clock + faultInterval, kind: evFault})
	case evRestart:
		nd := r.nodes[ev.node]
		if !nd.alive && nd.gen == ev.gen && !nd.frozen {
			r.restart(nd)
		}
	case evHardCrash:
		nd := r.nodes[ev.node]
		if nd.alive && nd.gen == ev.gen && r.faultsOn {
			nd.store.armed = false
			r.crash(nd, "crash after an unfired torn write")
		}
	case evScript:
		if r.faultsOn && ev.fn != nil {
			ev.fn(r)
		}
	}
}

// ctxCheckEvery is how many steps the context-aware loops run between
// checks of ctx.Err().
const ctxCheckEvery = 256

// RunFaults processes events until Steps events have run. It returns the
// first invariant violation.
func (r *Run) RunFaults() error { return r.RunFaultsContext(context.Background()) }

// RunFaultsContext is RunFaults with cancellation: it returns ctx.Err() when
// the context ends before the fault phase does.
func (r *Run) RunFaultsContext(ctx context.Context) error {
	for i := 0; r.steps < r.p.Steps; i++ {
		if i%ctxCheckEvery == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		if err := r.Step(); err != nil {
			return err
		}
	}
	return nil
}

// RunLiveness processes events after Heal until every client workflow has
// completed and every core node has applied through a common commit index,
// or until LivenessSteps events have run. It then runs the checker's
// end-of-run comparison. It returns a *Violation, a *LivenessError, or nil.
func (r *Run) RunLiveness() error { return r.RunLivenessContext(context.Background()) }

// RunLivenessContext is RunLiveness with cancellation: it returns ctx.Err()
// when the context ends before the liveness phase completes.
func (r *Run) RunLivenessContext(ctx context.Context) error {
	start := r.steps
	for i := 0; r.steps-start < r.p.LivenessSteps && !r.complete(); i++ {
		if i%ctxCheckEvery == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		if err := r.Step(); err != nil {
			return err
		}
	}
	if err := r.checker.AtEnd(r); err != nil {
		r.violation = err
		return err
	}
	if !r.complete() {
		return &LivenessError{Detail: r.progress(), Seed: r.p.Seed, Step: r.steps, Scenario: r.p.Scenario}
	}
	r.report.Participants = r.participants()
	return nil
}

// Check runs the checker's end-of-run comparison without requiring the
// liveness phase.
func (r *Run) Check() error {
	if r.violation != nil {
		return r.violation
	}
	r.violation = r.checker.AtEnd(r)
	return r.violation
}

// complete reports whether every client is done and the core is quiescent:
// every core node is live, holds the ready leader's ballot as its promise,
// and has applied through the leader's commit index. Requiring the promise
// as well as the commit index means completion is not declared in the
// middle of a leader change, and a member that never takes part keeps the
// run from completing instead of being masked.
func (r *Run) complete() bool {
	for _, c := range r.clients {
		if !c.idle() {
			return false
		}
	}
	for _, c := range r.playClients {
		if !c.idle() {
			return false
		}
	}
	nodes := r.coreNodes()
	var lb paxos.Ballot
	var ci paxos.Slot
	found := false
	for _, nd := range nodes {
		if !nd.alive {
			return false
		}
		if nd.node.Ready() {
			lb, ci, found = nd.node.Ballot(), nd.node.CommitIndex(), true
		}
	}
	if !found || ci == 0 || ci < r.checker.maxChosen {
		// A slot chosen anywhere but not yet in the committed prefix means
		// a gap is still open; completion waits until it is filled.
		return false
	}
	for _, nd := range nodes {
		if nd.node.Promised() != lb || nd.node.CommitIndex() != ci || nd.core.State().Applied() != ci {
			return false
		}
	}
	return true
}

// participants counts core nodes holding the ready leader's ballot as their
// promise and its commit index.
func (r *Run) participants() int {
	var lb paxos.Ballot
	var ci paxos.Slot
	found := false
	for _, nd := range r.coreNodes() {
		if nd.alive && nd.node.Ready() {
			lb, ci, found = nd.node.Ballot(), nd.node.CommitIndex(), true
		}
	}
	if !found {
		return 0
	}
	n := 0
	for _, nd := range r.coreNodes() {
		if nd.alive && nd.node.Promised() == lb && nd.node.CommitIndex() == ci {
			n++
		}
	}
	return n
}

// progress describes the state of the run for a liveness failure message.
func (r *Run) progress() string {
	var b strings.Builder
	pending := 0
	for _, c := range r.clients {
		if !c.idle() {
			pending++
			if c.pending != nil {
				fmt.Fprintf(&b, " client %d waits on %s (step %s, %d attempts);", c.id, c.pending.key, c.wf.stepName(c.pending.step), c.pending.attempts)
			}
		}
	}
	for _, c := range r.playClients {
		if !c.idle() {
			pending++
			if c.pending != nil {
				fmt.Fprintf(&b, " play client %d waits on %s (%s, %d attempts);", c.id, c.pending.key, c.pending.kind, c.pending.attempts)
			} else if c.pausedUntil > r.clock {
				fmt.Fprintf(&b, " play client %d is suspended until t=%v;", c.id, c.pausedUntil)
			}
		}
	}
	fmt.Fprintf(&b, " %d of %d clients pending;", pending, len(r.clients)+len(r.playClients))
	for _, nd := range r.coreNodes() {
		if !nd.alive {
			fmt.Fprintf(&b, " node %d down;", nd.id)
			continue
		}
		fmt.Fprintf(&b, " node %d %s commit %d applied %d promised %v;", nd.id, nd.node.Role(),
			nd.node.CommitIndex(), nd.core.State().Applied(), nd.node.Promised())
	}
	return strings.TrimSpace(b.String())
}

// Report returns the counters so far.
func (r *Run) Report() Report {
	rep := r.report
	rep.Net = r.net.Stats()
	rep.SimTime = r.clock
	rep.Applied, rep.Commit = 0, 0
	for _, nd := range r.nodes {
		if nd.applied > rep.Applied {
			rep.Applied = nd.applied
		}
		if nd.alive && nd.node.CommitIndex() > rep.Commit {
			rep.Commit = nd.node.CommitIndex()
		}
	}
	for _, nd := range r.nodes {
		rep.TornWrites += nd.store.torn
	}
	rep.Issued = 0
	for _, c := range r.clients {
		rep.Issued += c.issued
	}
	for _, c := range r.playClients {
		rep.Issued += c.issued
	}
	c := r.checker
	rep.Settled, rep.Voided = 0, 0
	for _, s := range c.settled {
		rep.Settled++
		if s.voided {
			rep.Voided++
		}
	}
	rep.Keys = len(c.firstResult)
	rep.Applies, rep.Replays, rep.KeyReused, rep.Rejections = c.applies, c.replays, c.keyReused, c.rejections
	rep.Checks = make(map[string]int, len(c.checks))
	for k, v := range c.checks {
		rep.Checks[k] = v
	}
	marks := make(map[string]int, len(r.report.Marks))
	for k, v := range r.report.Marks {
		marks[k] = v
	}
	rep.Marks = marks
	return rep
}

func (r *Run) tracef(format string, args ...any) {
	if r.trace == nil {
		return
	}
	fmt.Fprintf(r.trace, "%9d %7d ", r.clock.Microseconds(), r.steps)
	fmt.Fprintf(r.trace, format, args...)
	io.WriteString(r.trace, "\n")
}

// describe renders a message compactly for the trace.
func describe(m replog.Message) string {
	switch x := m.(type) {
	case replog.Prepare:
		return fmt.Sprintf("prepare %v from=%d", x.Ballot, x.FromSlot)
	case replog.Promise:
		return fmt.Sprintf("promise %v n=%d ci=%d", x.Ballot, len(x.Accepted), x.CommitIndex)
	case replog.Accept:
		return fmt.Sprintf("accept %v s=%d v=%s", x.Ballot, x.Slot, describeValue(x.Value))
	case replog.Accepted:
		return fmt.Sprintf("accepted %v s=%d", x.Ballot, x.Slot)
	case replog.Nack:
		return fmt.Sprintf("nack %v promised=%v s=%d lease=%v", x.Ballot, x.Promised, x.Slot, x.LeaseRemaining)
	case replog.Learn:
		return fmt.Sprintf("learn s=%d %v v=%s", x.Slot, x.Ballot, describeValue(x.Value))
	case replog.LearnRequest:
		return fmt.Sprintf("learn_request from=%d max=%d", x.FromSlot, x.MaxCount)
	case replog.Heartbeat:
		return fmt.Sprintf("heartbeat %v ci=%d rs=%d", x.Ballot, x.CommitIndex, x.ReadSeq)
	case replog.HeartbeatAck:
		return fmt.Sprintf("heartbeat_ack %v promised=%v ci=%d rs=%d", x.Ballot, x.Promised, x.CommitIndex, x.ReadSeq)
	}
	return fmt.Sprintf("%T", m)
}
