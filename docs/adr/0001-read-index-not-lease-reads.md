# ADR 0001: Consistent reads use a read-index barrier, not the lease

Status: accepted, 2026-09-16. Design reference: `DESIGN.md` sections 3.3 and 3.5.

## Context

A consistent read must reflect every command completed before the read was
issued (requirement 6, invariant S7). Two ways to serve it from a leader are
common: trust a time-based lease and answer from local state, or run one
round of messages to confirm leadership before answering. A lease read is
correct only while clock drift between replicas stays under a known bound.
The chaos simulation gives every node its own clock with an offset and a
rate between 0.5 and 2 (`clock_skew` scenario), so a lease read would be
wrong by construction in that scenario.

## Decision

The lease exists for elections only: an acceptor that heard from the leader
of its promised ballot less than `LeaseDuration` ago refuses a `Prepare` from
another node with a `Nack` that carries the remaining lease time. It never
lowers its promise, so the refusal cannot affect safety.

A consistent read runs the barrier of design section 3.5: the leader must
have committed its leadership no-op (`Ready()`), it records the commit index
as the read index, sends a `Heartbeat` carrying a read sequence number,
waits for `HeartbeatAck`s from a majority that report its own ballot as their
promise, and the host then waits until the applied index reaches the read
index. Any ack reporting a higher promise makes the leader step down and the
read fails.

## Consequences

- Every consistent read costs one heartbeat round trip to a majority. Each
  `ReadIndex` call sends its own `Heartbeat`; the design's refinement of
  sharing one round between reads issued between two heartbeats is not
  implemented.
- A leader isolated in a minority cannot answer consistent reads; the read
  fails with `NotLeaderError` after the step-down or waits until the deadline.
  Stale reads (`?read=stale`) remain available on every replica and carry
  `X-Arena-Applied-Slot`.
- The barrier's heartbeat must not be mistaken for the leader's periodic
  tick. A first implementation let `ReadIndex` refresh the heartbeat timer;
  with frequent reads, pending `Accept`s were then never retransmitted and a
  slot whose `Accept` or `Accepted` was dropped stayed open. The
  `late_learner` liveness assertion caught it. Retransmission now runs on
  each proposal's own timer (see ADR 0009).
