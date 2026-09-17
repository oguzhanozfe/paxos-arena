# Onboarding

A path through paxos-arena for someone new to the code: what it achieves, how to see it work in ten minutes,
and the order in which to read it. The [README](../README.md) is the full reference; this page is the way in.

## What the project achieves

| | |
|---|---|
| Problem | Paid tournaments for a skill-based mobile card game must pay every prize exactly once, even when servers crash mid-payout, the network splits, and two servers both think they lead. |
| Approach | Three to five replicas agree on one ordered log of commands with **Multi-Paxos**. A **deterministic state machine** applies the log and owns a **double-entry ledger**, so every replica holds identical tournaments and balances. Every command carries an **idempotency key** whose result is replicated, so retries never pay twice. |
| Game clients | A **server-authoritative play API** for a Unity mobile client: the phone sends intents (join, deal, play, finish, claim), the cluster decides every card, move, score and payout. A **C# SDK** (`unity-client/`) keeps intents on the device and resends them safely across app kills and leader changes. |
| Proof | A deterministic **simulator** runs whole clusters in one goroutine under crashes, torn writes, partitions, loss, duplication, reordering and clock skew, checking **20 invariants** after every event (S1-S8 consensus, D1-D6 money, P1-P6 play). **14 named fault scenarios**, reviewers' adversarial tests, an end-to-end run of the C# SDK against three real processes with the leader killed mid-round. |
| Size | 98 Go files (about 30 400 lines, standard library only) and 30 C# files (about 8 200 lines); 14 architecture decision records. |

## See it work (10 minutes)

Requirements: Go 1.26+, curl, python3. For the Unity SDK checks, a .NET SDK 8+.

```sh
git clone https://github.com/oguzhanozfe/paxos-arena.git && cd paxos-arena

# 1. The guided tour: three replicas as separate processes, a tournament from creation to payout,
#    the leader SIGKILLed before settlement, the dead replica restarted from its log file,
#    then one seeded simulator run. Each step prints what it demonstrates. About 30 seconds.
scripts/tour.sh
TOUR_PAUSE=1 scripts/tour.sh      # the same, waiting for Enter between steps

# 2. The whole test suite without the race detector (about 20 s), then as CI runs it (about 2.5 min)
go test ./...
make check

# 3. The simulator on its own: every named fault scenario, ten seeds each (about 10 s)
go run ./cmd/chaos -scenario all -seeds 10 -log-level warn
go run ./cmd/chaos -list-scenarios

# 4. The Unity C# SDK against three replicas, twice, the second time killing the leader mid-round (about 70 s)
scripts/e2e.sh

# 5. Play with the service by hand: three replicas in one process, curl commands printed on start
go run ./cmd/arena
```

What to look for in the tour:

- **Step 5**: resending a request with the same `Idempotency-Key` returns `replayed: true`; the same key with a
  different body is `422 key_reused`. This is what makes client retries safe.
- **Steps 7-8**: after the leader dies, the first settle attempt gets `503`, the retry with the same key gets `200`
  from the new leader, a resend is a replay and a second settle is refused. One settlement, 675 + 405 + 270 = 1350.
- **Step 9**: the restarted replica reads its log file, catches up, and reports the same state hash as the others.
- **Step 11**: the invariant table. A violation would print the seed and the exact command that replays it.

## Read the code in this order

Each layer only depends on the ones above it. The core packages (`paxos`, `replog`, `ledger`, `tournament`,
`replica.Core`) have no goroutines, locks, `time.Now` or global randomness: time and randomness are arguments,
which is what lets the simulator replay any run byte for byte.

| # | Where | Read | You will understand |
|---|---|---|---|
| 1 | `internal/paxos/paxos.go` | `Quorum`, `MayPromise`, `MayAccept`, `Choose` | the four rules of single-decree Paxos, each one line |
| 2 | `internal/paxos/acceptor.go`, `proposer.go` | the reference acceptor and proposer | Phase 1 (prepare/promise) and Phase 2 (accept/accepted) |
| 3 | `internal/replog/node.go` | `Step`, `Tick`, `Propose`, `ReadIndex` | Multi-Paxos: one log of slots, a stable leader, elections, no-op gap filling, the read-index barrier |
| 4 | `internal/replog/store.go`, `replog/wal/wal.go` | `Store`, the append-only file | what must be on disk before an acceptor answers, and torn-write recovery |
| 5 | `internal/ledger/ledger.go` | `Book.Post`, `Check` | double-entry postings with idempotent keys; money that cannot appear or vanish |
| 6 | `internal/tournament/state.go`, `compute.go` | `State.Apply` | the deterministic state machine: validation, standings, pool, payouts, the results table |
| 7 | `internal/replica/core.go`, `runner.go` | `Core.Submit`, `ApplyCommitted`, the event loop | how the log and the state machine are glued together in one process |
| 8 | `internal/transport`, `internal/api/server.go` | HTTP transport, handlers | forwarding to the leader, idempotency at the edge, problem+json errors |
| 9 | `cmd/arena/main.go` | flags and startup | one-process and one-replica-per-process modes, `-wal` |
| 10 | `internal/sim/run.go`, `faults.go`, `checker.go` | the event heap, fault schedule, `Checker` | how the simulator drives a cluster and where S1-S8 and D1-D6 are checked |
| 11 | `internal/game`, `internal/session`, `internal/intent` | rules, tokens, the play API | the server-authoritative game: seeded deals committed before any card is shown, move validation, sessions |
| 12 | `unity-client/Runtime/Core/ArenaClient.cs`, `IntentQueue.cs` | the SDK core | write-ahead intents, sequence numbers, leader redirects, resync |

Then the decisions: [`docs/adr/`](adr/) (why read-index instead of lease reads, why the ledger lives inside the
state machine, why a store error halts a node), and [`docs/DESIGN.md`](DESIGN.md) for the full specification.

## Explaining it in five minutes

1. **The failure.** One settlement worker and one database: a crash between "credit prize" and "record credit"
   pays twice or never; a failover to an async replica loses acknowledged writes; a lagging replica ranks
   players differently.
2. **The fix.** A majority-agreed log gives one order of commands; a deterministic state machine gives identical
   state from that order; ballots fence a deposed leader out, because acceptors that promised a higher ballot
   refuse it; replicated idempotency results make retries replay instead of re-execute.
3. **Why Paxos and not Raft.** Both are fine; the project is about the Paxos rules and their invariants, and a new
   Paxos leader learns what was chosen from its Phase 1 majority and re-proposes it, filling gaps with no-ops.
4. **How we know it is right.** Invariants are checked after every simulated event across thousands of seeded
   fault schedules; the checker is itself tested by planting four known bugs and requiring it to catch each.
5. **Game clients.** The client sends intents, the cluster decides outcomes. Deals come from a seed derived with a
   secret only the replicas hold and written to the log before any card is shown; scores are computed by the
   server by replaying the accepted moves; claims are once per player by construction.
6. **What is not done.** No log compaction or snapshots, fixed membership, no real payment provider, TLS in front of
   the service rather than inside it. Each is a stated boundary in the README.

## Change something safely

- Run `make check` before every push; it is exactly what CI runs.
- A new command: add it to `internal/tournament` (validation, `Apply`, codec), give it a route in `internal/api`,
  add an invariant or extend one in `internal/sim/checker.go`, and add a scenario or seed that exercises it.
- A new fault: add it to `internal/sim/faults.go` and run `make sim-long`.
- A failing seed: `go run ./cmd/chaos -seed N -trace trace.txt` reproduces it exactly; add the seed to
  `testdata/seeds.txt` once fixed.
