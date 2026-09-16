# ADR 0009: Acceptor, leader, lease and clock details that differ from the design's pseudocode

Status: accepted, 2026-09-16. Design reference: `DESIGN.md` sections 3.3, 4.2, 5.2, 5.3 and 7 (`clock_skew`).

## Context

The pseudocode in the design was written before the simulator existed. Each
item below was changed because the simulator showed a liveness problem or
because a rule needed a precise reading. None changes the safety argument:
an acceptor never lowers its promise, and refusing or re-answering a message
is always allowed.

## Decisions

1. A retransmitted `Prepare` at exactly the promised ballot is answered with
   a `Promise` again. The pseudocode's `MayPromise(promised, b)` requires
   `promised < b`, which would `Nack` the candidate's own retry after the
   first `Promise` was lost. The promise is unchanged and any later
   acceptance carries a ballot at least equal to it, so the second `Promise`
   reports the same or later state.
2. A candidate or leader steps down as soon as its own acceptor raises its
   promise above its ballot on a `Prepare`, `Accept` or `Heartbeat`, not only
   on a `Nack`, a `HeartbeatAck` or a lost majority. The information is
   already local; waiting for a reply would only prolong a duel.
3. A node's own `Prepare` does not refresh its own lease; its own `Accept`
   and `Heartbeat` do. Otherwise a candidate that fails Phase 1 would keep
   refusing every other candidate for a full lease.
4. `Config.Validate` requires `ElectionTimeoutMin >= HeartbeatInterval`
   (a follower must see at least one heartbeat per election timeout) and
   treats `LeaseDuration == 0` as "no lease" rather than "use the default";
   `DefaultConfig` supplies 150 ms. The `dueling_leaders` scenario uses the
   zero value to exercise the protocol without the lease.
5. Retransmission of a pending `Accept` runs on each proposal's own timer,
   not on the leader's heartbeat tick, so a `ReadIndex` heartbeat cannot
   starve it (ADR 0001).
6. Clock skew is modelled as a consistent per-node clock: an offset plus a
   rate in [0.5, 2] applied to every call into the node (`Step`, `Tick`,
   `Propose`, `ReadIndex`), not an offset added only before `Tick`. Rates
   return to 1 at `Heal` with a continuous clock, so the liveness phase runs
   with well-behaved timers.

## Consequences

- Liveness under message loss improved measurably in the simulator; safety
  invariants S1-S8 are checked exactly as before.
- With the lease on, a node returning from a long partition with an
  inflated round still deposes a working leader through `HeartbeatAck`, as
  the design accepts (section 3.3); the lease only stops it from winning the
  election before the others' leases expire. The `dueling_leaders` test
  bounds elections to `3 * partitions + 3` in the leased variant as one
  reading of "bounded by the number of partitions"; observed counts are far
  below the bound.
