# ADR 0007: S6, S7 and the liveness predicate under out-of-order choice

Status: accepted, 2026-09-16. Design reference: `DESIGN.md` sections 6.1 (S6, S7) and 6.3.

## Context

The leader keeps up to `Window` slots in flight, so slots are chosen out of
order in normal operation. Two invariants in the design assumed otherwise:

- S6, "a slot chosen after a higher slot was chosen contains a no-op", is
  false under pipelining: slot 41 can be chosen after slot 42 because its
  `Accepted` messages arrived later, and it holds a client command.
- S7 compared a consistent read's index with the highest slot chosen
  anywhere before the read. A slot chosen above an open gap has not
  completed (it is not applied on any replica), so a read that does not
  cover it is not stale.

The liveness predicate ("every client workflow settled and every applied
index reaches the commit index") also let a run pass with a slot that was
never chosen, as long as the clients had finished, which hid a bug.

## Decision

- S6 is keyed on proposal time: the checker records the first `Accept` seen
  for each (slot, value). If that proposal began after a higher slot was
  already chosen, the value must be the no-op. A gap filled by a new leader
  is exactly this case; a pipelined client command is not.
- S7 compares the read index with the highest commit index observed on any
  replica before the `ReadIndex` call, and the state served must be applied
  at least through the read index.
- The liveness predicate after `Heal` additionally requires the commit index
  on every core replica to cover every slot the checker ever saw chosen, and
  every core replica's acceptor to hold the leader's ballot as its promise.

## Consequences

- `TestCheckerDetectsKnownBugs` still catches the four planted bugs:
  `AcceptBelowPromise` as S3, `IgnorePhase1Reports` as S1,
  `SkipLeadershipNoOp` as S7 and `ForgetAcceptedOnRestart` as S5.
- The strengthened liveness predicate found the retransmission bug described
  in ADR 0001: a slot whose `Accept` was dropped stayed open while reads kept
  the leader's heartbeat timer fresh.
