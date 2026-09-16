# ADR 0006: A store error halts the node without replying

Status: accepted, 2026-09-16. Design reference: `DESIGN.md` sections 3.6, 5.2 and 6.1 (S5).

## Context

An acceptor must persist its promise and every accepted value before it
replies. The design's pseudocode says what to persist but not what to do
when the store fails, and for a conflicting `Learn` it says "panic in
tests, log and ignore in production". The simulator needs a precise model of
a torn write: a crash during a `Save*` call must leave the previous durable
record intact, never a partial one, and the volatile update must be lost.

## Decision

`replog.Node` treats any error from `Store` as fatal for the node: it sets
`Failed()`, sends no reply for the message that caused the write, and
ignores every later call until the host restarts it from the store. Nothing
that was not durably saved is ever acknowledged. A `Learn` whose value
contradicts an already chosen value emits a `LearnConflict` event rather
than panicking; the simulator's checker reports it as an S1 violation, and
a production host is expected to log it and stop the replica.

The simulator implements torn writes with a store wrapper that fails the
save after the crash decision has been made, so the node halts, the store
keeps the old record, and the restart loads exactly what a disk would have.

## Consequences

- A replica with a failing disk is silent rather than wrong. Its peers see
  a missing acceptor, which a majority tolerates.
- S5 (durability across crash, torn writes keep the old record) is checked
  on every restart in `crash_restart_storm` and in the random schedule.
- The only store shipped is `MemStore`. The design's append-only file store
  with a truncated torn tail (`internal/replog/wal`) is not built; the
  `Store` interface is where it would go.
