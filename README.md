# paxos-arena

A replicated settlement log for paid tournaments in a skill-based competitive
mobile card game, written in Go with the standard library only. Three to five
replicas agree on one order of tournament commands (create, join, submit
score, close, settle) with Multi-Paxos, apply them through a deterministic
state machine that owns a double-entry ledger, and expose an HTTP API with
idempotency keys. A deterministic simulator runs whole clusters in one
goroutine under seeded crashes, partitions, message loss, duplication,
reordering, torn writes and clock skew, and checks fourteen invariants after
every event. The repository is a case study in consensus and fault-injection
testing, not a production service: state lives in memory, membership is
fixed, and no money moves outside the ledger.

Contents: [The problem](#the-problem) |
[Why consensus](#why-consensus-rather-than-a-single-database) |
[Architecture](#architecture) | [Running it](#running-it) |
[How it is tested](#how-it-is-tested) | [Trade-offs](#trade-offs) |
[What is not done](#what-is-not-done) | [Further work](#further-work) |
[Sources](#sources) | [Layout](#layout) | [License](#license)

## The problem

A small studio runs a skill-based competitive mobile card game with paid
tournaments. Each tournament has a fixed entry fee, a rake in basis points, a
prize table as shares of the pool by placement, a minimum and maximum number
of entrants, a minimum age, a versioned list of excluded jurisdictions, and
one server-generated deal seed that every entrant plays. An entrant pays the
fee, receives the seed, plays the deal once and uploads a score. At close the
entrants are ranked, the pool is computed as fees minus rake, and prizes are
credited to player accounts in a ledger.

Settlement ran as one process against one database. When that process died
during payout, prizes were paid twice or not at all, and support staff
reconciled by hand. The service in this repository runs on three to five
nodes and must stay correct when any node crashes at any point, when the
network partitions, when messages are lost, duplicated or reordered, and when
two nodes both believe they lead. Concretely it must provide:

1. One agreed order of commands on every node, applied by a deterministic
   state machine, so every node holds the same tournament records and ledger.
2. At-most-once execution of every client command under arbitrary retries,
   identified by a client-supplied idempotency key.
3. Settlement that happens once in full or not at all: one prize record per
   (tournament, player, placement), prizes summing exactly to the pool, and a
   settled tournament that never changes afterwards.
4. Nothing proposed by a node after it lost leadership can be chosen.
5. Eligibility (jurisdiction, age) checked at entry and again at payout, with
   the list version recorded on the entry and on the payout.
6. Reads that reflect every command completed before the read was issued.
7. Reconstruction from the log, for any tournament, of who entered, which
   seed they played, which score was accepted, who was paid what, and under
   which leader ballot.

The full statement, including what the service does not do, is
[`docs/DESIGN.md`](docs/DESIGN.md) sections 1 and 9.

## Why consensus rather than a single database

The single-process design fails in three specific ways.

- Double payout. A worker credits first prize and dies before recording that
  it did; the restarted worker, or a second worker that took over the job,
  credits it again. Retrying a whole job is at-least-once execution; a credit
  is not idempotent. A job lock with a time-to-live has the same outcome
  when the first worker pauses (garbage collection, a slow disk) past the
  lock's expiry: a lock with a timeout is a hint, not mutual exclusion.
- Split-brain settlement. The database fails over to an asynchronously
  replicated replica; writes acknowledged by the old primary may not exist on
  the new one; the old primary keeps taking writes from a worker that has not
  noticed. Two settlement records exist on two machines that each hold "the
  truth", and which survives is decided by an operator later.
- Leaderboard divergence. Standings are computed from the scores a node can
  see. A node reading a lagging replica ranks entrants differently from the
  primary. If both pay from what they computed, the ledger holds payouts for
  two rankings of one tournament.

A log agreed by a majority of nodes gives one total order of commands, and a
deterministic state machine applying that order gives identical state on
every node without any node reading another's state. Standings are computed
once, by the `Close` command, and the result is part of the agreed state. A
leader proposes with a ballot number; acceptors that have promised a higher
ballot refuse lower ones, so a node that lost leadership cannot get anything
chosen no matter how long it believes it still leads. Fencing is part of the
protocol rather than bolted onto the payout path. The state machine records,
for every idempotency key, the result it produced; a retry that reaches any
leader after any number of crashes replays that result and moves no money,
and the record is replicated with everything else.

What the log does not give: it does not make an external payment idempotent,
does not validate a score, does not decide which jurisdictions are permitted,
and does not make a display leaderboard consistent. The ledger is kept
inside the replicated state so that "paid exactly once" is a property of
replicated state that a test can check directly
([ADR 0002](docs/adr/0002-ledger-inside-the-state-machine.md)).

## Architecture

Every replica runs the same binary and holds a Multi-Paxos participant
(acceptor, learner and potential leader for every slot of one log), the
deterministic tournament state machine with its ledger, an HTTP API, and a
transport. Every replica applies every chosen entry; any replica answers
stale reads from its own state; only the leader proposes and only the leader
answers consistent reads.

```
  client                 replica 2 (follower)             replica 1 (leader)
    |  POST /v1/... key=K   |                                   |
    |---------------------->|  forward, X-Arena-Forwarded: 1    |
    |                       |---------------------------------->|
    |                       |                                   | Propose(cmd) -> slot s
    |                       |     Accept(b, s, cmd)             |
    |                       |<----------------------------------|----> replica 3
    |                       |     Accepted(b, s)                |
    |                       |---------------------------------->|<---- replica 3
    |                       |     Learn(s, cmd)                 | quorum: slot s chosen
    |                       |<----------------------------------|----> replica 3
    |                       |                                   |
    |                       | apply chosen slots in order       | apply chosen slots in order
    |                       | tournament.State + ledger.Book    | tournament.State + ledger.Book
    |                       |                                   | result for key K
    |                       |<----------------------------------|
    |<----------------------|  200 {result}                     |
```

Packages and the dependency direction (enforced by `TestNoForbiddenImports`
in `replog` and `tournament`, which inspect `go list -deps`):

```
  internal/paxos       single-decree Paxos: Ballot, PValue, the rules (Quorum, MayPromise,
                       MayAccept, Choose) stated once, a reference acceptor and proposer
  internal/replog      Multi-Paxos log: Node (acceptor, learner, candidate, leader), lease,
                       heartbeats, read index, catch-up, Store/MemStore, strict JSON codec
  internal/ledger      append-only double-entry Book with idempotent postings
  internal/tournament  deterministic State: commands, validation, standings, pool, payouts,
                       results table keyed by idempotency key, canonical codec, hash chain
  internal/replica     Core (one Node + one State, no goroutines) and Runner (the one
                       event-loop goroutine per process)
  internal/transport   Network (in-memory, seeded drop/dup/delay/partition), Local (in-process
                       bus), HTTP (POST /internal/paxos between processes)
  internal/api         net/http handlers: idempotency keys, forwarding, problem+json errors
  internal/sim         virtual clock, event heap, fault schedule, clients, scenarios, Checker
  cmd/arena            the service; cmd/chaos: the simulator's command line

  paxos <- ledger <- tournament <- replica <- api
  paxos <- replog  <- replica    <- sim
  replog <- transport
```

`paxos`, `replog`, `tournament`, `ledger` and `replica.Core` contain no
goroutines, channels, locks, `time.Now` or global random functions. Time
enters as a `time.Duration` argument and randomness as an injected
`*rand.Rand`, which is what lets the simulator drive a whole cluster from
one goroutine and replay any seed byte for byte.

Protocol, in brief (details in `docs/DESIGN.md` sections 3 and 5):

- The log is a sequence of slots, each one single-decree Paxos instance. A
  slot is chosen when a majority accepted the same (ballot, value). Slots may
  be chosen out of order (up to `Window` = 64 in flight) and are applied
  strictly in order; the commit index is the longest chosen prefix.
- Ballots are (round, node), so no two nodes share one. A follower that hears
  nothing for a randomised election timeout (150-300 ms) sends one `Prepare`
  covering every slot from its commit index. With a majority of `Promise`s it
  re-proposes every value they report at its own ballot, fills gaps with
  no-ops, proposes one leadership no-op, and only then takes client commands.
- The lease (150 ms) exists for liveness only: an acceptor that heard from
  the current leader recently refuses other candidates' `Prepare`s with a
  `Nack` carrying the remaining time, without changing its promise. It is
  never used to serve reads.
- Consistent reads use a read-index barrier: the leader records its commit
  index, heartbeats with a sequence number, waits for a majority of acks at
  its own ballot, then waits until that index is applied
  ([ADR 0001](docs/adr/0001-read-index-not-lease-reads.md)).
- Before replying, an acceptor persists its promise and every accepted value
  through the `Store` interface. The only store shipped is in memory; a
  store error halts the node instead of letting it answer
  ([ADR 0006](docs/adr/0006-store-error-halts-the-node.md)).

HTTP API (Go 1.22 method patterns; every POST requires `Idempotency-Key`;
errors are RFC 9457 `application/problem+json` with a `code` member):

| Route | Command | Success |
|---|---|---|
| `POST /v1/tournaments` | CreateTournament | 201 |
| `POST /v1/tournaments/{id}/entries` | Join | 201, body carries the deal `seed` |
| `POST /v1/tournaments/{id}/scores` | SubmitScore | 200 |
| `POST /v1/tournaments/{id}/close` | Close: computes standings and pool | 200 |
| `POST /v1/tournaments/{id}/settle` | Settle: posts rake and payouts | 200 |
| `GET /v1/tournaments/{id}` | record, standings, payouts (`?read=stale` from any replica) | 200 |
| `GET /v1/tournaments/{id}/ledger` | postings of the tournament | 200 |
| `GET /v1/node`, `GET /healthz` | replica status | 200 |

A repeated key with the same command returns the recorded result with
`"replayed": true` and changes nothing; the same key with a different
command is `422 key_reused`; a state-machine rejection is `409` with the
rejection code; a follower forwards once to the leader it knows and answers
`503` with `Retry-After` when it cannot
([ADR 0003](docs/adr/0003-idempotency-in-the-state-machine.md),
[ADR 0010](docs/adr/0010-api-status-codes.md)).

## Running it

Requirements: Go 1.26 or newer (`go.mod` says `go 1.26.0`; the output below
is from go1.27.1 on darwin/arm64). No other dependencies.

### `cmd/arena`: the service

Without flags, `arena` starts three replicas in one process on ports
8081-8083, connected by an in-process bus, and prints the `curl` commands
for a complete tournament. The run below used `-listen 127.0.0.1:18081`.

```
$ go run ./cmd/arena -nodes 3 -listen 127.0.0.1:18081 -log-level warn
arena: 3 replicas in one process, connected by an in-memory bus
  node 1  http://127.0.0.1:18081
  node 2  http://127.0.0.1:18082
  node 3  http://127.0.0.1:18083

Any node accepts commands; a follower forwards to the leader. Every POST needs an Idempotency-Key.
Repeating a POST with the same key returns the recorded result with "replayed": true and changes nothing.

  BASE=http://127.0.0.1:18081
  curl -s $BASE/v1/node
  curl -s -X POST $BASE/v1/tournaments -H 'Idempotency-Key: create-t1' -H 'Content-Type: application/json' \
    -d '{"id":"t1","rules":{"entry_fee":500,"rake_bps":1000,"prize_bps":[5000,3000,2000],"min_entrants":3, ...
  ...
Ctrl-C stops every replica. State is in memory and is lost on exit.
```

In another shell, the printed commands, trimmed. Node 1 was a follower here
and forwarded to node 2; `X-Arena-Node` names the replica that answered.

```
$ BASE=http://127.0.0.1:18081
$ curl -s -i -X POST $BASE/v1/tournaments -H 'Idempotency-Key: create-t1' -H 'Content-Type: application/json' \
    -d '{"id":"t1","rules":{"entry_fee":500,"rake_bps":1000,"prize_bps":[5000,3000,2000],"min_entrants":3,"max_entrants":100,"max_score":100000,"min_age":18,"tie_break":"earliest_submission","exclusions":{"version":7,"jurisdictions":["XX"]}}}'
HTTP/1.1 201 Created
Content-Type: application/json
X-Arena-Node: 2

{
  "code": "ok",
  "replayed": false,
  "slot": 2,
  "tournament": {
    "id": "t1",
    "seed": 2923914604470535353,
    "rules": { "entry_fee": 500, "rake_bps": 1000, "prize_bps": [5000, 3000, 2000], ... },
    "status": "open",
    ...
  }
}

$ for p in p1 p2 p3; do
    curl -s -X POST $BASE/v1/tournaments/t1/entries -H "Idempotency-Key: join-$p" -H 'Content-Type: application/json' \
      -d "{\"player\":{\"id\":\"$p\",\"jurisdiction\":\"TR\",\"age\":31}}"
  done
{ "code": "ok", "replayed": false, "slot": 3, "seed": 2923914604470535353, "tournament": { ... } }
{ "code": "ok", "replayed": false, "slot": 4, "seed": 2923914604470535353, "tournament": { ... } }
{ "code": "ok", "replayed": false, "slot": 5, "seed": 2923914604470535353, "tournament": { ... } }

$ # the same request again: replayed, nothing charged twice
$ curl -s -X POST $BASE/v1/tournaments/t1/entries -H "Idempotency-Key: join-p1" -H 'Content-Type: application/json' \
    -d '{"player":{"id":"p1","jurisdiction":"TR","age":31}}'
{ "code": "ok", "replayed": true, "slot": 3, "seed": 2923914604470535353, "tournament": { ... } }

$ # the same key with a different body
$ curl -s -i -X POST $BASE/v1/tournaments/t1/entries -H "Idempotency-Key: join-p1" -H 'Content-Type: application/json' \
    -d '{"player":{"id":"p1","jurisdiction":"TR","age":32}}'
HTTP/1.1 422 Unprocessable Entity
Content-Type: application/problem+json
{"type":"about:blank","title":"Unprocessable Entity","status":422,"detail":"idempotency key was used with a different command","code":"key_reused"}

$ # an excluded jurisdiction
$ curl -s -i -X POST $BASE/v1/tournaments/t1/entries -H "Idempotency-Key: join-p9" -H 'Content-Type: application/json' \
    -d '{"player":{"id":"p9","jurisdiction":"XX","age":40}}'
HTTP/1.1 409 Conflict
Content-Type: application/problem+json
{"type":"about:blank","title":"Conflict","status":409,"detail":"jurisdiction \"XX\" is excluded by list version 7","code":"jurisdiction_excluded"}

$ SEED=2923914604470535353   # from the join responses; a score must quote the deal it played
$ i=0; for p in p1 p2 p3; do i=$((i+1))
    curl -s -X POST $BASE/v1/tournaments/t1/scores -H "Idempotency-Key: score-$p" -H 'Content-Type: application/json' \
      -d "{\"player\":\"$p\",\"score\":$((i*1000)),\"deal_seed\":$SEED}"
  done
{ "code": "ok", "replayed": false, "slot": 7, ... }
{ "code": "ok", "replayed": false, "slot": 8, ... }
{ "code": "ok", "replayed": false, "slot": 9, ... }

$ curl -s -X POST $BASE/v1/tournaments/t1/close -H 'Idempotency-Key: close-t1' -H 'Content-Type: application/json' -d '{}'
{
  "code": "ok", "replayed": false, "slot": 11,
  "tournament": {
    ...
    "status": "closed",
    "fees": 1500, "rake": 150, "pool": 1350,
    "standings": [ { "place": 1, "player": "p3", "score": 3000, ... }, { "place": 2, "player": "p2", ... }, { "place": 3, "player": "p1", ... } ],
    "closed_at": 11,
    ...
  }
}

$ curl -s -X POST $BASE/v1/tournaments/t1/settle -H 'Idempotency-Key: settle-t1' -H 'Content-Type: application/json' \
    -d '{"exclusions":{"version":8,"jurisdictions":["XX","YY"]}}'
{
  "code": "ok", "replayed": false, "slot": 12,
  "tournament": {
    ...
    "status": "settled",
    "payouts": [
      { "player": "p3", "place": 1, "amount": 675, "withheld": false, "exclusion_version": 8, "key": "prize:t1:p3:1" },
      { "player": "p2", "place": 2, "amount": 405, "withheld": false, "exclusion_version": 8, "key": "prize:t1:p2:2" },
      { "player": "p1", "place": 3, "amount": 270, "withheld": false, "exclusion_version": 8, "key": "prize:t1:p1:3" }
    ],
    "settled_at": 12,
    ...
  }
}

$ # settle again with the same key: replayed; with a new key: rejected, nothing posted
$ curl -s -X POST $BASE/v1/tournaments/t1/settle -H 'Idempotency-Key: settle-t1' ... -d '{"exclusions":{"version":8,"jurisdictions":["XX","YY"]}}'
{ "code": "ok", "replayed": true, "slot": 12, ... }
$ curl -s -i -X POST $BASE/v1/tournaments/t1/settle -H 'Idempotency-Key: settle-t1-again' ... -d '{"exclusions":{"version":8,"jurisdictions":["XX","YY"]}}'
HTTP/1.1 409 Conflict
{"type":"about:blank","title":"Conflict","status":409,"detail":"tournament \"t1\" is settled","code":"not_closed"}

$ curl -s $BASE/v1/tournaments/t1/ledger
{
  "tournament": "t1",
  "postings": [
    { "seq": 1, "key": "fee:t1:p1",      "kind": "entry_fee", "debit": "player:p1", "credit": "pool:t1",   "amount": 500, "exclusion_version": 7, "slot": 3,  "ballot": { "round": 1, "node": 2 }, ... },
    { "seq": 2, "key": "fee:t1:p2",      "kind": "entry_fee", "debit": "player:p2", "credit": "pool:t1",   "amount": 500, "exclusion_version": 7, "slot": 4,  ... },
    { "seq": 3, "key": "fee:t1:p3",      "kind": "entry_fee", "debit": "player:p3", "credit": "pool:t1",   "amount": 500, "exclusion_version": 7, "slot": 5,  ... },
    { "seq": 4, "key": "rake:t1",        "kind": "rake",      "debit": "pool:t1",   "credit": "rake:t1",   "amount": 150, "exclusion_version": 8, "slot": 12, ... },
    { "seq": 5, "key": "prize:t1:p3:1",  "kind": "prize",     "debit": "pool:t1",   "credit": "player:p3", "amount": 675, "place": 1, "exclusion_version": 8, "slot": 12, ... },
    { "seq": 6, "key": "prize:t1:p2:2",  "kind": "prize",     "debit": "pool:t1",   "credit": "player:p2", "amount": 405, "place": 2, "exclusion_version": 8, "slot": 12, ... },
    { "seq": 7, "key": "prize:t1:p1:3",  "kind": "prize",     "debit": "pool:t1",   "credit": "player:p1", "amount": 270, "place": 3, "exclusion_version": 8, "slot": 12, ... }
  ],
  "applied_slot": 13,
  "consistent": true
}

$ curl -s -I "http://127.0.0.1:18083/v1/tournaments/t1?read=stale"   # any replica, from its own applied state
HTTP/1.1 200 OK
Content-Type: application/json
X-Arena-Applied-Slot: 13
X-Arena-Node: 3

$ curl -s -i -X POST $BASE/v1/tournaments/t1/close -H 'Content-Type: application/json' -d '{}'
HTTP/1.1 400 Bad Request
{"type":"about:blank","title":"Bad Request","status":400,"detail":"every POST requires an Idempotency-Key header","code":"missing_idempotency_key"}
```

The pool is 3 x 500 = 1500, the rake 10% = 150, the pool 1350, split 50/30/20
into 675/405/270; the pool account nets to zero and every posting names the
slot and the ballot under which this replica learned it.

To kill a replica independently, run one replica per process over HTTP. The
run below created and closed a tournament through node 2, killed the leader
(node 1) between `Close` and `Settle`, and settled through the survivors.

```
$ make build                         # bin/arena and bin/chaos
$ PEERS=1=http://127.0.0.1:19081,2=http://127.0.0.1:19082,3=http://127.0.0.1:19083
$ bin/arena -id 1 -listen 127.0.0.1:19081 -peers $PEERS -log-level warn &
$ bin/arena -id 2 -listen 127.0.0.1:19082 -peers $PEERS -log-level warn &
$ bin/arena -id 3 -listen 127.0.0.1:19083 -peers $PEERS -log-level warn &
arena: node 2 of [1 2 3] listening on http://127.0.0.1:19082
  node 1  http://127.0.0.1:19081
  node 2  http://127.0.0.1:19082
  node 3  http://127.0.0.1:19083
...

$ curl -s -i -X POST http://127.0.0.1:19082/v1/tournaments -H 'Idempotency-Key: create-t1' ... -d '{"id":"t1", ...}'
HTTP/1.1 201 Created
X-Arena-Node: 1                      # node 2 forwarded to the leader, node 1
...
$ # joins, scores and close through node 2 as above, then:
$ kill %1                            # node 1, the leader
$ curl -s -i -X POST http://127.0.0.1:19082/v1/tournaments/t1/settle -H 'Idempotency-Key: settle-t1' ... \
    -d '{"exclusions":{"version":8,"jurisdictions":["XX","YY"]}}'
HTTP/1.1 503 Service Unavailable
Content-Type: application/problem+json
Retry-After: 1
X-Arena-Node: 2
{"type":"about:blank","title":"Service Unavailable","status":503,"detail":"forward to node 1 failed: Post \"http://127.0.0.1:19081/v1/tournaments/t1/settle\": dial tcp 127.0.0.1:19081: connect: connection refused","code":"forward_failed","leader":1}

$ # about 300 ms later, the same request with the same key
$ curl -s -i -X POST http://127.0.0.1:19082/v1/tournaments/t1/settle -H 'Idempotency-Key: settle-t1' ... \
    -d '{"exclusions":{"version":8,"jurisdictions":["XX","YY"]}}'
HTTP/1.1 200 OK
X-Arena-Node: 2                      # node 2 is now the leader (ballot r2.n2)
{ "code": "ok", "replayed": false, "slot": 11, "tournament": { ..., "status": "settled", "payouts": [ ...675, 405, 270... ] } }

$ curl -s -i -X POST http://127.0.0.1:19083/v1/tournaments/t1/settle -H 'Idempotency-Key: settle-t1' ... -d '{...}'
HTTP/1.1 200 OK
X-Arena-Node: 3
{ "code": "ok", "replayed": true, "slot": 11, ... }

$ diff <(curl -s http://127.0.0.1:19082/v1/tournaments/t1 | grep -v consistent) \
       <(curl -s 'http://127.0.0.1:19083/v1/tournaments/t1?read=stale' | grep -v consistent) && echo identical
identical
```

`cmd/arena/main_test.go` (`TestThreeNodesOverHTTP`) performs this sequence
on `httptest` servers under `-race` and checks D1-D3 through the API.

### `cmd/chaos`: the simulator

`chaos` runs the deterministic simulation for one seed or a sweep, with the
random fault schedule or a named scenario, and prints one report line per
run, a summary per scenario and the result of every invariant the checker
evaluated. A violation prints the seed, the step and the exact command line
that replays it, and exits 1. Flags: `-seed`, `-seeds N` (sweep
seed..seed+N-1), `-nodes`, `-steps`, `-liveness-steps`, `-drop`, `-dup`,
`-scenario NAME|all`, `-trace FILE`, `-log-level`, `-no-summary`,
`-list-scenarios`.

```
$ go run ./cmd/chaos -seed 1 -nodes 5 -steps 200000 -log-level warn
seed=1 scenario="" nodes=5 lease=true steps=203073 sim_time=1m44.087712065s elections=62 leader_changes=328 crashes=67 torn_writes=5 partitions=104 net{sent=145839 delivered=132210 dropped=13900 duplicated=6225 blocked=5954} issued=6911 completed=6911 submits=7454 extra_retries=0 mutated=0 unexpected=0 settled=664 voided=28 keys=6911 applies=259296 replays=2530 key_reused=0 rejections=0 reads=4519 reads_ok=4268 reads_failed=237 commit=7043 applied=7043 participants=5 client.settled=664

scenario  runs  steps   elections  crashes  partitions  settled  keys  replays  key_reused  result
random    1     203073  62         67       104         664      6911  2530     0           ok

invariant  checks  result  description
S1         155689  ok      single chosen value per slot
S2         268598  ok      identical applied sequences and state hashes
S3         218212  ok      acceptor promise monotone; accepts at or above the promise
S4         180067  ok      ballots unique per node; one value per (ballot, slot)
S5         67      ok      durable state survives a crash; torn writes keep the old record
S6         6982    ok      a slot chosen after a higher slot is a no-op
S7         4268    ok      consistent reads reflect every command completed before them
S8         293851  ok      at most one non-replayed result per idempotency key
D1         81258   ok      prize pool equals entry fees minus rake; pool account nets to zero
D2         290733  ok      every payout appears exactly once, sums to the pool
D3         28117   ok      a settled tournament never changes
D4         53141   ok      standings are a function of the scores and the tie-break rule
D5         290733  ok      eligibility checked and its list version recorded at entry and payout
D6         259301  ok      money never appears or disappears
```

That run simulates 1 m 44 s of cluster time in 4.9 s of wall time: 62
elections, 67 crashes with 5 torn writes, 104 partitions, 145 839 messages
of which 13 900 were dropped and 6 225 duplicated, 664 tournaments settled
(28 voided for too few scores), 6 911 idempotency keys with 2 530 replays,
and every invariant evaluated between 67 and 293 851 times without a
violation.

Every scripted scenario, ten seeds each (4.5 s wall time):

```
$ go run ./cmd/chaos -scenario all -seeds 10 -log-level warn
seed=1 scenario="leader_crash_mid_settlement" nodes=5 lease=true steps=20000 ... crashes=1 ... settled=1 ... client.settled=1
...   (90 report lines)

scenario                           runs  steps   elections  crashes  partitions  settled  keys   replays  key_reused  result
leader_crash_mid_settlement        10    200000  20         10       0           10       2030   0        0           ok
dueling_leaders                    10    89403   78         0        68          354      3685   79       0           ok
partition_and_heal                 10    90244   41         0        39          272      2840   25       0           ok
duplicated_and_reordered_messages  10    74912   35         0        0           60       631    609      0           ok
client_retry_storm                 10    77991   15         10       12          124      1297   12773    5593        ok
crash_restart_storm                10    66025   76         192      14          326      3394   489      0           ok
clock_skew                         10    69545   28         9        21          310      3213   43       0           ok
late_learner                       10    294534  10         0        0           712      12691  0        0           ok
exclusion_change_at_settle         10    65259   33         11       41          402      4020   83       0           ok

invariant  checks   result  description
S1         829211   ok      single chosen value per slot
S2         230897   ok      identical applied sequences and state hashes
S3         1079483  ok      acceptor promise monotone; accepts at or above the promise
S4         838910   ok      ballots unique per node; one value per (ballot, slot)
S5         232      ok      durable state survives a crash; torn writes keep the old record
S6         39713    ok      a slot chosen after a higher slot is a no-op
S7         21520    ok      consistent reads reflect every command completed before them
S8         330495   ok      at most one non-replayed result per idempotency key
D1         79887    ok      prize pool equals entry fees minus rake; pool account nets to zero
D2         231440   ok      every payout appears exactly once, sums to the pool
D3         33010    ok      a settled tournament never changes
D4         46877    ok      standings are a function of the scores and the tie-break rule
D5         231440   ok      eligibility checked and its list version recorded at entry and payout
D6         189152   ok      money never appears or disappears
```

In `client_retry_storm` the clients re-sent every command up to ten times,
30% of them with a mutated payload under the same key; across the ten runs
the state machine applied each of the 1 297 keys once, answered 12 773
replays and rejected 5 593 mutations as `key_reused`. Heavier loss works the
same way: `go run ./cmd/chaos -seed 42 -seeds 3 -nodes 5 -drop 0.3 -dup 0.2
-steps 20000` dropped 27% of the messages sent, duplicated 13%, and settled
all 142 tournaments of the three runs. A failing seed is replayed with the
printed command; `-trace FILE` writes the event trace, and two runs of one
seed produce byte-identical traces (`sim.TestReplayDeterministic`).

## How it is tested

Three layers, all under the race detector in CI.

1. Table-driven unit tests per package, with property-style tests where the
   input space is large (1 000 random single-decree interleavings; 10 000
   random pools and prize tables for payout rounding) and fuzz targets for
   the two codecs and the simulator seed.
2. The deterministic simulation in `internal/sim`: whole clusters in one
   goroutine, a virtual clock, one event heap merged with the network's,
   per-node clock offset and rate, crashes that land between commit and
   apply, torn writes, partitions including directional and non-transitive
   ones, clients that follow leader hints, retry with the same key and issue
   consistent reads, and a `Checker` that evaluates S1-S8 and D1-D6 after
   every event and replays the chosen log into a fresh state at the end.
   `TestScenarios` runs the nine scenarios with 50 seeds each and
   `TestRandom` 200 seeds of the random schedule (`-seeds` and
   `-random-seeds` raise them; `-short` lowers them to 5 and 20;
   `make sim-long` runs 1 000 and 2 000). `testdata/seeds.txt` is the corpus
   of seeds that once failed; it is rerun by `TestSeedCorpus` on every run and
   is empty at the time of writing.
3. Goroutine-level tests: `replica.Runner` under `testing/synctest`, the
   HTTP transport and API on `httptest` servers, and `TestThreeNodesOverHTTP`
   for the three-process demo.

The checker is itself tested: `sim.TestCheckerDetectsKnownBugs` turns on four
planted bugs (`replog.UnsafeKnobs`: accept below the promise, ignore Phase 1
reports, serve reads before the leadership no-op, forget accepted values on
restart) one at a time and asserts that the checker reports the expected
invariant (S3, S1, S7, S5) within a bounded number of seeds.

Invariants and where each is checked:

| # | Invariant | Unit tests | Simulation |
|---|---|---|---|
| S1 | One chosen value per slot | `paxos.TestSingleDecreeAgreement` | every scenario; also derived from quorum-accepted state |
| S2 | Identical applied sequences and state hashes on every node, including a node rebuilt by replay | `tournament.TestReplayDeterministic`, `replica.TestReplayFromStore` | hash compared per (node, applied slot); full replay at the end |
| S3 | Promise never decreases; accepts only at or above the promise | `paxos.TestAcceptorRules`, `replog.TestAcceptorMonotonic` | every `Prepare`/`Accept` step |
| S4 | Ballots unique per node; one value per (ballot, slot) | `replog.TestBallotIsUniquePerNode` | registry over `Accept`s in transit |
| S5 | Durable state survives a crash; a torn write keeps the old record | `replog.TestRestartRestoresDurableState`, `replog.TestStoreFailureStopsNodeWithoutReplying` | every restart in `crash_restart_storm` and the random schedule |
| S6 | A slot proposed after a higher slot was chosen is a no-op | `replog.TestTakeoverFillsGapsWithNoOps` | every scenario ([ADR 0007](docs/adr/0007-checker-definitions-under-pipelining.md)) |
| S7 | A consistent read reflects every command completed before it | `replog.TestReadIndexNeedsMajorityAtOwnBallot`, `replog.TestReadIndexRefusedBeforeLeadershipNoOp`, `replica.TestReadConsistentSeesCompletedSubmit` | every consistent read, on both sides of partitions |
| S8 | One non-replayed result per idempotency key per node; replays equal the record | `tournament.TestApplyReplaysRecordedResult`, `tournament.TestKeyReusedWithDifferentPayload` | every apply; `client_retry_storm` |
| D1 | Pool = fees - rake; pool account nets to zero after settle | `tournament.TestComputePool`, `tournament.TestSettlePostings`, `ledger.TestBalancesSumZero` | every apply, every tournament, every node |
| D2 | Every payout exactly once, summing to the pool | `tournament.TestPayoutRoundingSumsToPool` (10 000 cases), `TestSplitTieBreak`, `TestVoidRefunds`, `ledger.TestPostDuplicateKeyIsNoop` | every apply |
| D3 | A settled tournament never changes | `tournament.TestSettledTournamentRejectsAllCommands` | canonical encoding compared against the snapshot at settle |
| D4 | Standings are a function of the scores and the tie-break rule | `tournament.TestStandingsTable` | recomputed on every closed tournament |
| D5 | Eligibility checked and its list version recorded at entry and payout | `tournament.TestJoinEligibility`, `tournament.TestSettleWithholdsNewlyExcluded` | every apply; `exclusion_change_at_settle` |
| D6 | Money never appears or disappears | `ledger.TestCheck` | `Ledger().Check()` on every node after every apply |

Liveness is not an invariant (no protocol guarantees it under unbounded
faults). `sim.TestLivenessAfterHeal` runs the fault schedule, heals
everything, and requires every workflow to settle and every node to catch up
within a bound; `sim.TestLivenessWithFrozenMinority` heals only a majority
core and freezes the faults outside it. Both fail if fewer than `Nodes`
replicas took part.

The scenarios (`docs/DESIGN.md` section 7 gives the schedules):

| Scenario | Fault schedule | Checked |
|---|---|---|
| `leader_crash_mid_settlement` | 5 nodes, 100-entrant tournament; the leader is crashed at one of 12 points of the `Settle` command chosen by seed: after `Submit` returned, on the first `Accept`, after the `Accept` reached k of 4 peers, after a quorum of `Accepted` with the `Learn`s discarded, after the `Learn` reached j of 4 peers, after the leader applied it. The client retries with the same key. `TestMidSettlementVariantsAllCrash` covers all 12. | S1, S2, S8, D1-D3; one settlement, same record on every node |
| `dueling_leaders` | asymmetric heartbeat blocks so a follower elects while the leader keeps accepting commands; lease on or off by seed | S1, S3, S4, S8; nothing from the deposed leader is chosen; elections bounded |
| `partition_and_heal` | {leader, one follower} split from three followers, also a non-transitive split, clients on both sides | S1, S2, S7; minority reads fail or wait; convergence after heal |
| `duplicated_and_reordered_messages` | 30% duplication, 10% loss, delays up to 20 heartbeat intervals | S1, S3, S4, S6, S7; stale messages never count |
| `client_retry_storm` | every command re-sent up to 10 times, 30% mutated under the same key, 20% loss | S8, D2; one apply per key, `key_reused` for mutations |
| `crash_restart_storm` | high crash rate, 20% torn writes, crashes as candidate, leader with proposals in flight and between commit and apply | S5 on every restart, S1-S4 throughout, replay reproduces the hash |
| `clock_skew` | per-node clock offset and rate in [0.5, 2] with the lease on | S1-S8 hold; skew costs only liveness |
| `late_learner` | one follower loses every `Learn` for 500 slots, then reconnects | catch-up through `LearnRequest`; S2; commit index monotone |
| `exclusion_change_at_settle` | list version 7 at creation, version 8 at settle adds an entrant's jurisdiction | D5: that payout is withheld with version 8; D2: totals unchanged |

The full run, on go1.27.1, darwin/arm64:

```
$ gofmt -l .

$ go vet ./...

$ go mod tidy -diff

$ go test -race -count=1 ./...
ok  	github.com/oguzhanozfe/paxos-arena/cmd/arena	2.039s
ok  	github.com/oguzhanozfe/paxos-arena/cmd/chaos	11.329s
ok  	github.com/oguzhanozfe/paxos-arena/internal/api	2.044s
ok  	github.com/oguzhanozfe/paxos-arena/internal/ledger	1.697s
ok  	github.com/oguzhanozfe/paxos-arena/internal/paxos	3.257s
ok  	github.com/oguzhanozfe/paxos-arena/internal/replica	3.534s
ok  	github.com/oguzhanozfe/paxos-arena/internal/replog	2.686s
ok  	github.com/oguzhanozfe/paxos-arena/internal/sim	80.213s
ok  	github.com/oguzhanozfe/paxos-arena/internal/tournament	3.994s
ok  	github.com/oguzhanozfe/paxos-arena/internal/transport	6.733s
```

The same suite passes with `-shuffle=on`; without `-race` the longest
package (`internal/sim`) takes about 11 s. `make check` runs gofmt, vet,
build, `go mod tidy -diff` and the
race run, which is what [`.github/workflows/go.yml`](.github/workflows/go.yml)
runs on `ubuntu-latest` for Go 1.26.x and 1.27.x with `GOTOOLCHAIN=local`.
There are no third-party linters: the standard-library constraint applies to
tooling too, and `gofmt`, `go vet` and `-race` cover the bug classes this code
can have. Fuzzing: `make fuzz` (30 s per target).

## Trade-offs

Each decision with the alternative it displaced. The ADRs in
[`docs/adr/`](docs/README.md) hold the reasoning.

- Multi-Paxos with a stable leader, not Raft. A new leader may have any log;
  it learns what was chosen from the `Promise`s of its Phase 1 majority and
  re-proposes it, filling gaps with no-ops. Raft's log-matching rule avoids
  that reconstruction at the cost of restricting who may lead. The two
  differ mainly in election, and Paxos was chosen because the case study is
  about the Paxos rules and their invariants.
- Read index, not lease reads. One heartbeat round per consistent read, in
  exchange for correctness under any clock behaviour. Each read pays its own
  round; batching reads between heartbeats is not implemented.
- Ledger inside the state machine, not an external wallet. "Paid exactly
  once" becomes a checkable invariant; the price is that no real money moves.
- Idempotency in the replicated results table, never pruned, rather than a
  process-local cache with a TTL. Survives every crash; grows without bound.
- Ballots on records are per-replica audit data, excluded from hashes and
  comparisons, because two correct replicas can learn one slot under
  different ballots ([ADR 0004](docs/adr/0004-ballot-is-audit-metadata.md)).
- A store error halts the node rather than being logged and skipped. A silent
  replica is tolerated by the majority; a replica that acknowledged an
  unpersisted promise is not.
- A hand-written event heap and virtual clock for the simulator, and
  `testing/synctest` only for the goroutine layer. The core packages are
  pure, so no runtime support is needed to make them deterministic.
- JSON over HTTP, `encoding/json` v1 with unknown fields, trailing data and
  oversized bodies rejected by hand, rather than a binary protocol or
  `encoding/json/v2` (which would raise the `go` line to 1.27).
- `go 1.26.0` in `go.mod`: the oldest supported release line, so the
  two-version CI matrix means something.
- `cmd/arena` starts N replicas in one process by default, one HTTP port
  each, so the first run needs one command; the per-process mode over HTTP
  is a flag away ([ADR 0008](docs/adr/0008-arena-runs-in-one-process-by-default.md)).

## What is not done

Each item is a boundary of the system as built, not an oversight.

- Persistence. The only `Store` is in memory. A restarted `arena` process
  starts empty, and a replica that lost its store must not rejoin under its
  old identity. The simulator models durable state by keeping each node's
  store across simulated crashes, so S5 is tested, but the append-only file
  store with a truncated torn tail described in the design
  (`internal/replog/wal`) is not built.
- Membership changes. `Peers` is fixed at start; adding or replacing a
  replica is a restart of the cluster with a new configuration and an empty
  log.
- Snapshots and log compaction. The log and the results table grow for the
  life of the process; a restart replays from slot 1.
- Real payments. The ledger is the wallet. Deposits, withdrawals, balance
  floors, an external payment provider and reconciliation are out of scope.
- Authentication, authorisation, TLS. Player identifiers, jurisdiction and
  age are claims taken as given. Anyone who can reach the API can create,
  close and settle tournaments. The inter-replica transport is plain HTTP
  and does not authenticate its peers.
- Transport. The one-process demo uses an in-memory bus. The HTTP transport
  is best effort: `Send` never blocks, a full per-peer queue drops the
  message, and the protocol's retransmission covers the loss. No connection
  pooling tuning, no backpressure beyond the queue.
- The game. No rules engine, no deal generation beyond a 64-bit seed, no
  replay validation of input logs, no anomaly detection; `input_digest` is
  stored, not checked.
- Lease-based reads, display leaderboards, matchmaking, ratings, tax
  reporting, multiple fund types.
- Byzantine faults. Replicas crash, restart, partition and see delayed,
  dropped, duplicated and reordered messages; they do not lie. The codec
  rejects malformed input but does not authenticate it.
- Liveness guarantees. Liveness is tested under a stated fairness
  assumption (faults stop, or stop in a majority core), not proven. With the
  lease on, a node returning from a long partition with an inflated round
  still deposes a working leader once; the lease only stops it from winning
  the election early.
- A linearizability checker over client histories. S7 and S8 are checked
  directly with the checker's global view; client histories are not exported.
- Performance. No benchmarks beyond what the simulation needs, no load
  testing, no tuning of `Window` or timeouts beyond what makes the simulation
  and the demo work. `internal/sim` takes about 80 s under `-race`.

## Further work

In the order they would add the most.

1. `internal/replog/wal`: an append-only file store behind the existing
   `Store` interface, one length-prefixed record per save, fsync per write,
   torn tail truncated on load, with a `TestTornTail`; then a `-wal` flag on
   `arena`.
2. Batching the read-index barrier: reads issued between two heartbeats
   share one round.
3. Snapshots of `tournament.State` at a slot, and truncation of the log and
   results table below it.
4. Membership changes through the log (a configuration entry chosen like any
   other, with the joint-majority rule during the transition).
5. An external payout path: `PayoutIssued` and `PayoutConfirmed` commands, an
   outbox driven by the leader with the ballot as a fencing token, and
   reconciliation against the provider.
6. Exporting client histories from the simulator and checking them with a
   linearizability checker, as a second opinion on S7 and S8.
7. TLS and peer authentication on the inter-replica transport; request
   authentication on the API.

## Sources

The research notes in [`docs/research/`](docs/README.md) cite everything in
detail; this is the short list.

Consensus

- Leslie Lamport, "Paxos Made Simple", 2001. https://lamport.azurewebsites.net/pubs/paxos-simple.pdf
- Leslie Lamport, "The Part-Time Parliament", ACM TOCS 16(2), 1998. https://lamport.azurewebsites.net/pubs/lamport-paxos.pdf
- Tushar Chandra, Robert Griesemer, Joshua Redstone, "Paxos Made Live: An Engineering Perspective", PODC 2007. https://www.cs.utexas.edu/users/lorenzo/corsi/cs380d/papers/paper2-1.pdf
- Robbert van Renesse, Deniz Altinbuken, "Paxos Made Moderately Complex", ACM Computing Surveys 47(3), 2015. https://www.cs.cornell.edu/courses/cs7412/2011sp/paxos.pdf
- Diego Ongaro, John Ousterhout, "In Search of an Understandable Consensus Algorithm (Extended Version)", USENIX ATC 2014. https://raft.github.io/raft.pdf
- Diego Ongaro, "Consensus: Bridging Theory and Practice", PhD dissertation, 2014 (client sessions, read-only queries). https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf
- Heidi Howard, Richard Mortier, "Paxos vs Raft: Have we reached consensus on distributed consensus?", PaPoC 2020. https://arxiv.org/abs/2004.05074
- Michael Fischer, Nancy Lynch, Michael Paterson, "Impossibility of Distributed Consensus with One Faulty Process", JACM 32(2), 1985. https://groups.csail.mit.edu/tds/papers/Lynch/jacm85.pdf
- Maurice Herlihy, Jeannette Wing, "Linearizability: A Correctness Condition for Concurrent Objects", ACM TOPLAS 12(3), 1990. https://cs.brown.edu/~mph/HerlihyW90/p463-herlihy.pdf
- Leslie Lamport, TLA+ specification of Paxos. https://github.com/tlaplus/Examples/blob/master/specifications/Paxos/Paxos.tla

Testing

- "Simulation and Testing", documentation of a database built around deterministic simulation. https://apple.github.io/foundationdb/testing.html
- "VOPR", internals documentation of a ledger database's simulator, and "Simulation Testing For Liveness" (2023) on healing a core to test liveness. https://github.com/tigerbeetle/tigerbeetle/blob/main/docs/internals/vopr.md , https://tigerbeetle.com/blog/2023-07-06-simulation-testing-for-liveness/
- Jepsen: consistency models and the analysis method. https://jepsen.io/consistency/models/linearizable , https://github.com/jepsen-io/jepsen
- Porcupine, a linearizability checker in Go. https://github.com/anishathalye/porcupine

Go and HTTP

- Go toolchains and the `go` directive. https://go.dev/doc/toolchain
- Testing concurrent code with `testing/synctest`. https://go.dev/blog/synctest
- `math/rand/v2` (seedable PCG). https://go.dev/blog/randv2
- Routing enhancements for Go 1.22 (method patterns). https://go.dev/blog/routing-enhancements
- Structured logging with `log/slog`. https://go.dev/blog/slog
- RFC 9457, Problem Details for HTTP APIs. https://www.rfc-editor.org/rfc/rfc9457
- The Idempotency-Key HTTP header field (IETF draft). https://www.ietf.org/archive/id/draft-ietf-httpapi-idempotency-key-header-07.txt

## Layout

```
paxos-arena/
  go.mod                        module github.com/oguzhanozfe/paxos-arena, go 1.26.0, no dependencies
  README.md                     this file
  LICENSE                       MIT
  Makefile                      build, test, race, lint, check, sim-long, fuzz
  .github/workflows/go.yml      gofmt, vet, build, tidy -diff, test -race on Go 1.26.x and 1.27.x
  cmd/arena/                    the service: N replicas in one process, or one per process
  cmd/chaos/                    the simulator's command line
  internal/paxos/               single-decree Paxos rules, reference acceptor and proposer
  internal/replog/              Multi-Paxos log: Node, Config, Store, codec, UnsafeKnobs
  internal/transport/           Network (simulation), Local (in-process), HTTP (inter-process)
  internal/ledger/              double-entry Book with idempotent postings
  internal/tournament/          deterministic state machine, codec, standings/pool/payout functions
  internal/replica/             Core (pure) and Runner (event loop)
  internal/api/                 HTTP handlers
  internal/sim/                 deterministic simulation and invariant checker
  docs/DESIGN.md                the specification
  docs/adr/                     decisions taken during implementation
  docs/research/                consensus, domain and Go practice notes with sources
  testdata/seeds.txt            seeds that once failed (empty)
```

## License

MIT. See [`LICENSE`](LICENSE).
