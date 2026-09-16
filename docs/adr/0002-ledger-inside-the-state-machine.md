# ADR 0002: The ledger is part of the replicated state

Status: accepted, 2026-09-16. Design reference: `DESIGN.md` sections 2.3, 4.4 and 5.5.

## Context

The failure the service exists to prevent is a prize paid twice or not at
all when the settling process dies. If the money movement happens outside
the replicated state (a wallet service, a payment provider), "paid exactly
once" becomes a reconciliation property between two systems and cannot be
asserted by a test that inspects one.

## Decision

`ledger.Book` is a field of `tournament.State`. Every money movement is one
posting with an idempotency key (`fee:<tid>:<pid>`, `rake:<tid>`,
`prize:<tid>:<pid>:<place>`, `withheld:...`, `refund:...`); posting an
existing key returns the existing posting and changes nothing. Balances are
derived from postings. Settlement posts the rake and every payout in one
`Apply` call, so a replica either applies the whole settlement or none of it.

Two details differ from the design text:

- `Book.Post` returns `(Posting, bool, error)`. The error covers postings
  that are invalid on their face (non-positive amount, empty key, debit and
  credit equal). The state machine never produces one; the check exists so
  the ledger cannot be misused by a future caller.
- A payout share can be 0 when the pool is tiny (integer division). Such a
  `Payout` is recorded with `Amount: 0` and posts nothing, so invariant D2
  reads "one posting per payout with a positive amount".

## Consequences

- D1 (pool equals fees minus rake, pool account nets to zero), D2 (every
  payout once, summing to the pool) and D6 (all balances sum to zero) are
  checked on every replica after every applied slot, and again by replaying
  the chosen log into a fresh state at the end of every simulation run.
- No money moves outside the service. Player accounts are counterparty
  accounts; deposits, withdrawals and balance floors are out of scope.
- An external wallet would need two more commands (`PayoutIssued`,
  `PayoutConfirmed`), an outbox driven by the leader with the ballot as a
  fencing token, and reconciliation against the provider (design section 9).
