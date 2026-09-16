package paxos

// Acceptor is the reference single-decree acceptor. Promised is the highest
// ballot it has promised; Accepted and Value are the most recently accepted
// proposal, with Accepted.IsZero() meaning nothing has been accepted yet.
//
// All three fields are the durable state of the acceptor: an implementation
// that persists them before replying may crash and restart at any point
// without breaking agreement.
type Acceptor struct {
	// Promised is the highest ballot promised so far.
	Promised Ballot
	// Accepted is the ballot of the most recently accepted proposal, zero
	// when nothing has been accepted.
	Accepted Ballot
	// Value is the most recently accepted value.
	Value Value
}

// Promise is the Phase 1 reply of an acceptor that promised Ballot. Accepted
// and Value report its most recently accepted proposal; Accepted.IsZero()
// means it has accepted nothing.
type Promise struct {
	// Ballot is the promised ballot.
	Ballot Ballot
	// Accepted is the ballot of the most recently accepted proposal, or zero.
	Accepted Ballot
	// Value is the most recently accepted value.
	Value Value
}

// Nack is the refusal of a Prepare or an Accept at Ballot. Promised is the
// acceptor's current promise, which is at least Ballot.
type Nack struct {
	// Ballot is the refused ballot.
	Ballot Ballot
	// Promised is the acceptor's current promise.
	Promised Ballot
}

// OnPrepare handles a Phase 1 request for ballot b. When MayPromise allows
// it, the acceptor records b as its promise and returns a Promise with
// ok=true; otherwise it returns a Nack with ok=false and changes nothing.
func (a *Acceptor) OnPrepare(b Ballot) (Promise, Nack, bool) {
	if !MayPromise(a.Promised, b) {
		return Promise{}, Nack{Ballot: b, Promised: a.Promised}, false
	}
	a.Promised = b
	return Promise{Ballot: b, Accepted: a.Accepted, Value: a.Value}, Nack{}, true
}

// OnAccept handles a Phase 2 request for ballot b and value v. When MayAccept
// allows it, the acceptor records (b, v) as accepted, raises its promise to b
// if b is higher, and returns ok=true; otherwise it returns a Nack with
// ok=false and changes nothing.
func (a *Acceptor) OnAccept(b Ballot, v Value) (Nack, bool) {
	if !MayAccept(a.Promised, b) {
		return Nack{Ballot: b, Promised: a.Promised}, false
	}
	if a.Promised.Less(b) {
		a.Promised = b
	}
	a.Accepted = b
	a.Value = v
	return Nack{}, true
}
