# ADR 0004: The ballot on records and postings is per-replica audit metadata

Status: accepted, 2026-09-16. Design reference: `DESIGN.md` sections 5.4 and 5.5 (record types).

## Context

The design gives `Tournament.Ballot` and `Posting.Ballot` as part of the
record so that a reviewer can ask "under which leader was this settled".
During implementation it turned out that two replicas can legitimately learn
one slot under different ballots: a value chosen at ballot b1 whose `Learn`
is lost is re-proposed by the next leader at b2 (Phase 1 reports it) and
learned by the late replica under b2. The value is the same; the ballot is
not. Including the ballot in the replicated record would make identical
applied prefixes hash differently, and invariant S2 would fail on a correct
system.

## Decision

The ballot is recorded per replica as the ballot under which that replica
learned the slot. It is excluded from `State.Hash`, from the canonical
encodings `EncodeTournament` and `EncodePostings`, and from every
cross-replica comparison in the checker and in `cmd/arena`'s tests. It is
still returned by the API on tournament records and ledger postings.

## Consequences

- The answer to "under which leader" is "the ballot under which the replica
  you asked learned the slot", which is what an operator inspecting that
  replica's log would also see. It is not guaranteed to be equal across
  replicas.
- Two replicas' `GET /v1/tournaments/{id}` responses can differ in the
  `ballot` fields (and in the `consistent` flag of the response wrapper)
  while being identical in every field that the state machine decides.
- Everything else in the record (entries, seed, scores, standings, payouts,
  slots at which events happened) is replicated state and is compared
  byte for byte.
