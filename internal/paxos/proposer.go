package paxos

import "errors"

// ErrNotReady is returned by Proposer.Propose before a quorum of promises has
// arrived for the current ballot, or after the attempt was abandoned.
var ErrNotReady = errors.New("paxos: proposer has no quorum of promises for its ballot")

// Proposer is the reference single-decree proposer. One Proposer runs one
// attempt at a time: Start picks a ballot, OnPromise collects Phase 1
// replies, Propose picks the value the promises allow, and OnAccepted counts
// Phase 2 replies until the value is chosen.
type Proposer struct {
	self      NodeID
	n         int
	ballot    Ballot
	promises  map[NodeID]Promise
	accepts   map[NodeID]struct{}
	abandoned bool
	proposed  bool
	value     Value
}

// NewProposer returns a proposer for node self in a group of n acceptors.
func NewProposer(self NodeID, n int) *Proposer {
	return &Proposer{self: self, n: n}
}

// Start begins a new attempt with ballot (round, self) and forgets every
// reply of the previous attempt. It returns the ballot to put in Prepare.
func (p *Proposer) Start(round uint64) Ballot {
	p.ballot = Ballot{Round: round, Node: p.self}
	p.promises = make(map[NodeID]Promise)
	p.accepts = make(map[NodeID]struct{})
	p.abandoned = false
	p.proposed = false
	p.value = nil
	return p.ballot
}

// Ballot returns the ballot of the current attempt.
func (p *Proposer) Ballot() Ballot { return p.ballot }

// OnPromise records a Phase 1 reply. Replies for other ballots are ignored.
// It reports whether a quorum of promises has been collected.
func (p *Proposer) OnPromise(from NodeID, m Promise) (ready bool) {
	if p.abandoned || m.Ballot != p.ballot || p.promises == nil {
		return p.ready()
	}
	p.promises[from] = m
	return p.ready()
}

func (p *Proposer) ready() bool {
	return !p.abandoned && p.promises != nil && len(p.promises) >= Quorum(p.n)
}

// OnNack abandons the attempt when the refusal is for the current ballot.
// The caller starts a new attempt with a round above m.Promised.Round.
func (p *Proposer) OnNack(m Nack) {
	if m.Ballot == p.ballot {
		p.abandoned = true
	}
}

// Propose returns the ballot and the value to send in Accept: the value of
// the highest-ballot promise, or want when no promise reports a value. It
// returns ErrNotReady before a quorum of promises or after a Nack.
func (p *Proposer) Propose(want Value) (Ballot, Value, error) {
	if !p.ready() {
		return Ballot{}, nil, ErrNotReady
	}
	reports := make([]PValue, 0, len(p.promises))
	for _, m := range p.promises {
		if !m.Accepted.IsZero() {
			reports = append(reports, PValue{Ballot: m.Accepted, Value: m.Value})
		}
	}
	p.value = Choose(reports, want)
	p.proposed = true
	return p.ballot, p.value, nil
}

// OnAccepted records a Phase 2 reply for ballot b. Replies for other ballots
// or before Propose are ignored. It reports whether the value is chosen.
func (p *Proposer) OnAccepted(from NodeID, b Ballot) (chosen bool) {
	if p.abandoned || !p.proposed || b != p.ballot {
		return false
	}
	p.accepts[from] = struct{}{}
	return len(p.accepts) >= Quorum(p.n)
}

// Value returns the value passed to Accept by the last Propose call, or nil.
func (p *Proposer) Value() Value { return p.value }
