# Documentation index

Reading order for someone new to the repository: the top-level `README.md`,
then `DESIGN.md`, then the ADRs, then the research notes when a design
choice needs its source.

## `DESIGN.md`

The specification the code follows: problem statement, why a replicated log
rather than one database, architecture (Multi-Paxos log with a stable
leader, lease for elections only, read-index reads), package layout and
public types, the nine protocol messages, acceptor and leader rules as
pseudocode, the tournament commands with their validation order, the ledger
record, the fourteen invariants (S1-S8, D1-D6) with where each is checked,
the nine chaos scenarios, milestones and non-goals. Written before the
implementation; where the implementation deviates, an ADR records it and
the ADR takes precedence over the affected paragraph.

## `UNITY-INTEGRATION.md`

The contract for milestone 4, server-authoritative play from a mobile game
client built with the Unity engine, written so that the Go server and the
C# client can be built in parallel: the trust model; device-bound sessions
with HMAC-SHA256 tokens and key rotation; idempotency keys and per-player
sequence numbers; Ladder, the single-player card puzzle the tournaments
play, with its deal committed to the log before any card is shown; the six
play commands and their validation order; every route with its JSON bodies,
status codes, error body, leader redirect and rate limits; the JSON rules a
`JsonUtility` client needs; the client's caching, resend, backoff, clock and
election rules; the Go functions and flags to build; the C# SDK layout; new
invariants and chaos scenarios; and test vectors. So far only the Go types
and `unity-client/README.md` exist.

## `adr/`

One short record per decision that was taken or changed while implementing
the design. Each has context, decision and consequences.

| ADR | Decision |
|---|---|
| [0001](adr/0001-read-index-not-lease-reads.md) | Consistent reads use a read-index barrier; the lease is never used to serve reads. |
| [0002](adr/0002-ledger-inside-the-state-machine.md) | The ledger is part of the replicated state, so "paid exactly once" is checkable state. |
| [0003](adr/0003-idempotency-in-the-state-machine.md) | The results table in the state machine is the authority on repeated keys; the API's in-flight map only coalesces concurrent duplicates. |
| [0004](adr/0004-ballot-is-audit-metadata.md) | The ballot on tournament records and postings is per-replica audit data, excluded from hashes and comparisons. |
| [0005](adr/0005-state-hash-is-a-chain.md) | `State.Hash` is a chain over applied slots, not a hash of the whole state. |
| [0006](adr/0006-store-error-halts-the-node.md) | A store error halts the node without replying; a torn write leaves the previous durable record. |
| [0007](adr/0007-checker-definitions-under-pipelining.md) | How S6, S7 and the liveness predicate are defined when slots are chosen out of order. |
| [0008](adr/0008-arena-runs-in-one-process-by-default.md) | `cmd/arena` starts N replicas in one process by default; one replica per process remains available. |
| [0009](adr/0009-protocol-details-that-differ-from-the-pseudocode.md) | Acceptor, leader, lease and clock details that differ from the design's pseudocode. |
| [0010](adr/0010-api-status-codes.md) | HTTP status codes and forwarding rules that the design did not fix. |
| [0011](adr/0011-file-store-for-per-process-replicas.md) | Per-process replicas keep their log state in an append-only file, required by `arena -peers`. |
| [0012](adr/0012-bounds-on-commands.md) | Identifier shape, list and value size bounds, a bounded proposal queue, and forgetting abandoned requests. |
| [0013](adr/0013-api-and-naming-corrections.md) | Smaller API, concurrency and naming corrections after review. |

## `research/`

Working notes written before the design, on 2026-09-16. They cite their
sources; the design cites them. Nothing in them is normative.

- [`research/consensus.md`](research/consensus.md): single-decree Paxos and
  how its rules are derived; Multi-Paxos with a stable leader (takeover,
  skipping Phase 1, no-op fills, persistence, read-only requests); the
  engineering lessons of a production Paxos deployment; how Raft differs;
  the safety invariants a test suite must check; liveness caveats; testing
  approaches (deterministic simulation, history checking, specifications).
  Twenty-four sources, listed in its section 10.
- [`research/domain.md`](research/domain.md): what a paid, skill-based
  competitive card game commits its backend to: identical deals from one
  seed, entry fees, rake and prize pools, server-side score validation,
  settlement that pays each prize once, leaderboards across replicas, audit
  trails, regulatory constraints as data; which of these are consistency
  problems; and the draft problem statement that `DESIGN.md` section 1
  supersedes. Publishers are described by role; URLs are given for
  regulators, encyclopedias, papers and open projects only.
- [`research/go-practice.md`](research/go-practice.md): the Go release line
  and the `go` directive, repository layout, concurrency rules, a
  deterministic in-memory transport, table-driven and property-style tests,
  `net/http` without a framework, `log/slog`, the CI workflow, `gofmt` and
  `go vet` as gates, the README outline this repository follows, and the
  decisions taken for this repository.
