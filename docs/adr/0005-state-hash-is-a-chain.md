# ADR 0005: `State.Hash` is a chain over applied slots

Status: accepted, 2026-09-16. Design reference: `DESIGN.md` section 4.5.

## Context

The design describes `State.Hash()` as a hash of a canonical encoding of
everything in the state. The simulator compares hashes on every replica
after every applied slot; encoding the whole state each time makes a run
quadratic in the number of applied slots, and a 200 000-step run applies
about 7 000 slots on each of five replicas.

## Decision

`State.Hash` is a SHA-256 chain. Each applied slot folds in the canonical
encoding of the command, its result, the affected tournament record after the
command and the postings the command produced. Each skipped slot (a no-op or
an undecodable entry) folds in its slot number. The chain
value after slot s depends on the whole applied prefix through s and on
nothing else.

## Consequences

- Equal applied prefixes give equal hashes, and replaying the chosen log
  into a fresh `State` reproduces the hash, which is what S2 needs.
- The hash is not a fingerprint of the current state alone; two different
  histories that happen to reach the same state have different hashes. That
  is acceptable because the log defines the history.
- Immutability of a settled record (D3) is checked separately, by
  comparing the canonical encoding of the record and its postings against a
  snapshot taken when `Settle` was applied.
