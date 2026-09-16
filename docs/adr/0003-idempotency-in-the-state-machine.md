# ADR 0003: The results table in the state machine is the authority on repeated keys

Status: accepted, 2026-09-16. Design reference: `DESIGN.md` sections 3.4, 4.7 and 5.4.

## Context

Clients retry. A retry must not execute a command twice (requirement 2,
invariant S8), and it must get the same answer after any number of crashes
and leader changes. An idempotency map kept in one process's memory dies
with the process.

## Decision

`tournament.State.Apply` records, for every idempotency key it applies, the
fingerprint of the command and the `Result` it produced, for successes and
rejections alike. A later command with the same key and fingerprint returns
the recorded result with `Replayed: true` and changes nothing; the same key
with a different fingerprint returns `key_reused` and is not recorded. The
table is replicated with the rest of the state and never pruned.

Two layers above it are optimisations only and never the authority:

- `api.Server` keeps an in-flight map of keys whose `Submit` has not
  returned. A concurrent request with the same key answers `409 in_flight`.
  The map is cleared when `Submit` returns and is never consulted for a
  completed key.
- `replica.Runner.Submit` answers a key already present in the local results
  table without proposing, also on a follower. The answer is the recorded
  result, which is what the log would have produced.

## Consequences

- The log may contain several entries with one key (a command that lost its
  slot and was re-proposed by the client's retry). The state machine applies
  the first and replays for the rest. The simulator's `client_retry_storm`
  scenario sends every command up to ten times, 30% with a mutated payload,
  and S8 counts exactly one non-replayed result per key per node.
- A replayed success returns the original status with `"replayed": true`;
  `key_reused` is `422`.
- The table grows without bound for the life of the process (design section
  9: no compaction).
