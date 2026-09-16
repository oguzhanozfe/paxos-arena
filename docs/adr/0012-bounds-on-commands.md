# ADR 0012: Bounds on what a command may carry, and on what a leader holds

Status: accepted, 2026-09-17. Design reference: `DESIGN.md` sections 3.4,
4.2, 4.3, 4.7 and 5.4.

## Context

A review found four defects with one root: nothing bounded the shape or the
size of a command on its way into the log.

- Identifiers could contain any byte. Account names and posting keys join
  identifiers with `:`, so tournament `t` with player `x:p` and tournament
  `t:x` with player `p` shared every posting key. `ledger.Book.Post` treats a
  repeated key as an idempotent retry and books nothing, and `Join` and
  `Settle` ignored that, so one entry fee was never charged and one prize
  never paid while both records said they were. Every replica agreed, so
  only the domain invariants D1 and D2 caught it. Two non-UTF-8 identifiers
  could also share a JSON encoding and a fingerprint.
- The API accepted 1 MiB bodies, but the `Accept` carrying the command is
  base64 inside JSON and exceeded the HTTP transport's 1 MiB message limit.
  Followers answered `413` forever, heartbeats still got through, the leader
  kept its office and the commit index stopped until its process died.
- While slots could not be chosen, every timed-out request left a queued
  proposal in `replog.Node` and a waiter in `replica.Runner`, without bound.
- Megabyte commands held every replica's event loop (decode, fingerprint,
  apply) for longer than an election timeout under the race detector.

## Decision

- Tournament and player identifiers are 1 to 64 characters from `A-Z`,
  `a-z`, `0-9`, `.`, `_` and `-`; the state machine rejects others with
  `invalid_rules` or `invalid_player`. `Join` and `Settle` also check that
  none of their posting keys exists before posting anything, and reject with
  the new code `ledger_conflict` if one does, rather than rely on the
  identifier rule alone.
- `prize_bps` has at most 1000 places and an exclusion list at most 1000
  codes.
- `replog.MaxValueBytes` (1 MiB) bounds a value; `Propose` refuses a longer
  one with `ErrValueTooLarge`. `transport.MaxMessageBytes` is 16 MiB, which
  carries an `Accept` or `Learn` of that size with room for a `Promise`
  reporting several. The HTTP transport encodes messages on its sender
  goroutines, keeps large messages in a queue of their own and drops a
  retransmission of one that is still queued.
- The API refuses a body over `MaxBody` (now 64 KiB by default; the largest
  valid command is under 20 KiB) or a command whose canonical encoding
  exceeds `MaxValueBytes` with `413 body_too_large`, before proposing.
- `replog.Config.QueueLimit` (1024) bounds the values a leader queues beyond
  `Window`; `Propose` answers `ErrBusy`, which the API maps to `503` with
  `Retry-After`. `Runner` marks a `Submit` or `Read` whose context ended and
  forgets it at its next tick.

Size bounds are enforced before the log and rules after it. Validating every
rule before proposing would keep more out of the log, but a rejection given
outside the log is not recorded under its key, so a retry of that key with a
corrected body would be applied instead of answered `key_reused`.

## Consequences

- The review's tests for these defects run by default:
  `replica.TestAttackPostingKeysCollideAcrossTournaments` and
  `sim.TestAttackSimCollidingIdentifiers` (with the `:` identifiers now
  expected to be refused and the D1/D2 checks run on the nearest legal
  pair), `cmd/arena.TestReviewOversizedCommandWedgesHTTPCluster` and
  `replica.TestReviewAbandonedSubmitsAreRetained`.
- Identifiers that were accepted before and contain other characters are
  now rejected. There is no stored state to migrate: logs are rebuilt from
  scratch.
- `ledger_conflict` is unreachable with valid identifiers; it exists so that
  the invariant "an entry has its fee posting" holds by construction in the
  state machine, not only by the identifier rule.
- A `Promise` is still one message. A candidate that lags a leader by more
  than 16 MiB of accepted values cannot collect promises over HTTP until it
  catches up through that leader.
