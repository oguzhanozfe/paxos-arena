# ADR 0013: Smaller API, concurrency and naming corrections after review

Status: accepted, 2026-09-17. Design reference: `DESIGN.md` sections 4.1,
4.2, 4.5, 4.6 and 4.7.

## Context

The review of milestone 3 listed, beside the defects of ADRs 0011 and 0012,
a data race, a stale status snapshot, a reference proposer that could send
two values under one ballot, and a set of names and JSON shapes that did not
follow Go conventions or were awkward for clients. None changes the
protocol; together they change several exported names.

## Decisions

- `api.Server` no longer shares memory with the event loop after `Read`
  fails. The post-commit read that fills a command response writes into a
  variable of its own under a context of its own, and the handler uses it
  only when `Read` returned nil. `Runner.Read` documents that `fn` may still
  run after `Read` returned an error.
- `Runner` refreshes its `Status` snapshot before it answers any request an
  event completed, so a caller woken with `ErrLeadershipLost` never reads a
  snapshot that still names it leader.
- `paxos.Proposer.Propose` keeps the first value it chose for a ballot and
  returns it on every later call; `OnPromise` ignores promises that arrive
  after it. `paxos.ErrNotReady` is renamed `ErrNoQuorum`, to be told apart
  from `replog.ErrNotReady`.
- `replog.ErrNotLeader` is renamed `NotLeaderError`, since it is an error
  type rather than a sentinel value; `errors.As` works as before.
  `tournament.New` is renamed `NewState`.
- `Core.Status` counts tournaments with `State.TournamentCount` instead of
  copying the identifier list on every event. `Status.StateHash` is a
  `tournament.Digest` and renders as hex. API responses render empty lists
  as `[]`, not `null`; the canonical encoding behind the state hash is
  unchanged.
- One strict JSON decoding helper, `internal/jsonx.DecodeStrict`, replaces
  the three copies in `replog`, `tournament` and `api`. It accepts only JSON
  whitespace after the value.
- `ledger.Book.Check` recomputes the balances from the postings instead of
  summing the cached balances only, which could not fail.
- `sim.Params.Validate` checks its probabilities in a fixed order, so one
  command line always names the same field.

## Consequences

- The design's code sketches are updated to the new names.
- The review's reproductions for the race, the stale snapshot and the
  proposer run by default under `-race`.
