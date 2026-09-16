# Design: a replicated settlement log for paid card-game tournaments

Status: implemented (milestones 1 to 3), written 2026-09-16 and corrected
after review on 2026-09-17. This document is the specification the
implementation follows. Where the implementation deviates from a paragraph
below, the deviation is recorded in `docs/adr/` and the ADR takes precedence;
`docs/README.md` lists them. It supersedes the draft problem statement in
`docs/research/domain.md` section 9. Protocol claims rest on the sources
collected in `docs/research/consensus.md`; Go conventions follow
`docs/research/go-practice.md`. Neither research note is repeated here beyond
what an implementer needs. Milestone 4, server-authoritative play from a
mobile game client, is specified in `docs/UNITY-INTEGRATION.md`, which
takes precedence over this document for everything it covers.

Module: `github.com/oguzhanozfe/paxos-arena`. Standard library only.
`go 1.26.0` in `go.mod`, no `toolchain` line.

Contents

1. Problem statement
2. Why a replicated log
3. Architecture
4. Package layout
5. Messages and state
6. Invariants and how each is tested
7. Failure scenarios the chaos simulation must cover
8. Milestones
9. Non-goals

---

## 1. Problem statement

A small studio runs a skill-based competitive mobile card game with paid
tournaments. Each tournament has a fixed entry fee, a rake expressed in basis
points, a prize table expressed as shares of the prize pool by placement, a
minimum and maximum number of entrants, a minimum age, a versioned list of
excluded jurisdictions, and one server-generated deal seed shared by every
entrant. An entrant pays the fee, receives the seed, plays the deal once and
uploads a score. At close the entrants are ranked, the prize pool is computed
as entry fees minus rake, and prizes are credited to player accounts in a
ledger.

Settlement runs today as one process against one database. When that process
has died during payout, prizes have been paid twice or not at all, and support
staff have reconciled by hand. The studio wants a settlement service that runs
on three to five nodes and stays correct when any node crashes at any point,
when the network partitions, when messages are lost, duplicated or reordered,
and when two nodes both believe they lead.

The service must provide:

1. One agreed order of tournament commands (create, join, submit score,
   close, settle) on every node, applied by a deterministic state machine, so
   that every node holds the same tournament records and the same ledger.
2. At-most-once execution of every client command under arbitrary client
   retries, identified by a client-supplied idempotency key.
3. Settlement that either happens once in full or not at all: one prize
   record per (tournament, player, placement), prizes summing exactly to the
   pool, and a settled tournament that never changes afterwards.
4. Rejection of a node that has lost leadership: nothing it proposes after
   losing leadership can be chosen, and nothing it reports as done can be
   undone.
5. Eligibility checks (jurisdiction, age) at entry and again at payout, with
   the list version used recorded on the entry and on the payout.
6. Reads that reflect every command completed before the read was issued.
7. A reviewer's ability to reconstruct from the log, for any tournament, who
   entered, which seed they played, which score was accepted, who was paid
   what, and under which leader ballot.

The service does not implement the card game, the rules engine or score
validation by replay; it consumes an already validated score and checks only
that the score references the tournament's seed. It does not move money
outside its own ledger. Section 9 lists the other boundaries.

---

## 2. Why a replicated log

### 2.1 What breaks with one database and naive retries

The single-process design fails in three ways that are worth naming precisely,
because the design below is shaped by them.

Double payout. A settlement worker computes standings, credits the first
prize, and dies before recording that it did. The restarted worker, or a second
worker that took over the job, computes the same standings and credits the
first prize again. Retrying the whole job is at-least-once execution; the
credit is not idempotent; the combination pays twice. A variant with the same
outcome: a job lock with a time-to-live expires while the first worker is
paused (garbage collection, a slow disk, a stalled network call); a second
worker takes the lock; both finish. A lock with a timeout is not mutual
exclusion, it is a hint.

Split-brain settlement. The database fails over to a replica. Asynchronous
replication means writes acknowledged by the old primary may not exist on the
new one. The old primary keeps accepting writes from a worker that has not yet
noticed the failover. Two settlement records for one tournament now exist on
two machines that each believe they hold the truth, and the one that survives
is decided by which machine an operator trusts later, not by any rule.

Leaderboard divergence. Standings are computed from the scores a node can see.
A node reading from a lagging replica sees fewer scores than the primary and
ranks the entrants differently. If two nodes each compute standings and each
pay from what it computed, the ledger contains payouts for two different
rankings of one tournament. The display leaderboard being stale is tolerable;
the settlement leaderboard being computed twice from two different inputs is
not.

Naive retries make each of these worse. A mobile client that did not receive a
response retries. Without an idempotency key the server cannot tell a retry
from a second request; a joined player is charged twice, a submitted score is
recorded twice. With an idempotency key kept only in one process's memory, the
key is lost with the process, and the retry after a crash is again a new
request.

### 2.2 What a replicated log gives

A replicated log agreed by a majority of nodes gives one total order of
commands. A deterministic state machine applying that order on every node
gives identical state on every node without any node reading another's state.
Concretely:

- One order. Every tournament command occupies exactly one slot; every node
  applies slots in order; there is no second copy of the standings because
  standings are computed once, by the Close command, and the result is part
  of the agreed state.
- Stale leader rejection. A leader proposes with a ballot number. Acceptors
  that have promised a higher ballot refuse lower ones. A node that lost
  leadership cannot get anything chosen, no matter how long it believes it
  still leads. Fencing is built into the protocol rather than added to the
  payout path.
- Idempotency in the state, not in a process. The state machine records, for
  every idempotency key it has applied, the result it produced. A retry that
  reaches any leader after any number of crashes replays the recorded result
  and moves no money. The record is replicated with everything else.
- Crash at any point. A command is either chosen or it is not. If the Settle
  command was chosen, every node applies it exactly once, and a restarted node
  replays the log and arrives at the same ledger. If it was not chosen, no node
  applied it, and the client's retry proposes it again.

### 2.3 What it does not give

The log does not make an external payment idempotent, does not validate a
score, does not decide which jurisdictions are permitted, and does not make a
display leaderboard consistent. This design keeps the ledger inside the
replicated state so that "paid exactly once" is a property of replicated state
that a test can check directly. Section 9 describes what an external wallet
would add.

---

## 3. Architecture

### 3.1 Overview

N replicas (3 or 5 in the demo; any N >= 1 works, quorum is N/2+1). Each
replica runs the same binary and holds:

- a Multi-Paxos participant (`internal/replog`) that is acceptor, learner and
  potential leader for every slot of one replicated log;
- the deterministic tournament state machine (`internal/tournament`) with its
  ledger (`internal/ledger`), fed with chosen log entries in slot order;
- an HTTP API (`internal/api`) that accepts commands with idempotency keys,
  forwards them to the leader if this replica is not the leader, waits for
  the command to be applied, and serves reads;
- a transport (`internal/transport`) that carries protocol messages between
  replicas: an in-memory fault-injecting network for simulation, and HTTP
  between processes for the demo.

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

Every replica applies every chosen entry. Every replica can answer stale
reads from its own applied state. Only the leader proposes and only the leader
answers consistent reads.

### 3.2 The log

The log is a sequence of slots numbered from 1. Each slot is one
single-decree Paxos instance. A slot is chosen when a majority of acceptors
have accepted the same (ballot, value) for it. Slots may be chosen out of
order; they are applied strictly in order, and only when every lower slot is
chosen. The commit index is the largest slot s such that every slot in 1..s is
chosen.

A value is the encoded bytes of one tournament command, or the empty value,
which is the no-op used to fill gaps. The state machine skips no-ops.

### 3.3 Leadership, ballots and the lease

A ballot is (Round, Node). Ballots are totally ordered by Round, then Node.
Node identifiers are unique by configuration, so two nodes never use the same
ballot.

A node is Follower, Candidate or Leader.

- A Follower that hears nothing from a leader for an election timeout (drawn
  uniformly from [ElectionTimeoutMin, ElectionTimeoutMax], redrawn each time)
  becomes Candidate with ballot (maxRound+1, self), persists maxRound, and
  sends one Prepare for all slots from its commit index + 1.
- A Candidate that collects Promises from a majority becomes Leader. It
  re-proposes every value reported in the promises at its own ballot,
  proposes no-ops for the gaps, proposes one leadership no-op after the
  highest reported slot, and only then accepts client commands. This is the
  takeover of Paxos Made Simple section 3 with the per-slot state reduction
  of Paxos Made Moderately Complex section 4.1.
- A Leader sends a Heartbeat every HeartbeatInterval. It steps down to
  Follower when any message shows a higher promised ballot, or when it has
  not heard from a majority for ElectionTimeoutMax.

The lease. Each acceptor records the time it last heard from the leader of its
promised ballot (any Prepare, Accept or Heartbeat). While less than
LeaseDuration has passed since then, the acceptor refuses a Prepare from any
other node with a Nack carrying the remaining lease time, without changing its
promised ballot. Refusing a request never affects safety (an acceptor may
ignore any message). The lease exists for liveness only: it stops a node that
was partitioned and returns with an inflated round from deposing a working
leader, and it makes leadership stable enough that Phase 1 runs once per
leadership rather than once per command. LeaseDuration <= ElectionTimeoutMin
so that a follower whose own lease has expired can start an election; other
acceptors that heard from the leader more recently make it wait, which is the
intended behaviour.

The lease is not used to serve reads. See 3.5.

### 3.4 Write path

1. The API handler validates the request shape, requires an
   `Idempotency-Key` header, builds a `tournament.Command`, and calls
   `Submit`.
2. If this replica is not the leader, the handler forwards the whole request
   to the leader it knows, once, with `X-Arena-Forwarded: 1`. A request that
   already carries the header is not forwarded again; if no leader is known,
   the handler answers 503 with `Retry-After`.
3. The leader's replica encodes the command as the value of the next free
   slot and runs Phase 2 for that slot (Accept to all, wait for a majority of
   Accepted). Up to Window slots may be in flight.
4. When the slot is chosen the leader records it, advances the commit index
   if the prefix is complete, and sends Learn to the other replicas.
5. Every replica applies chosen slots in order. Applying a command produces a
   `tournament.Result`, recorded in the state under the command's idempotency
   key.
6. The handler that submitted the command is waiting on that key. When the
   local state machine applies a command with that key, from whichever slot,
   the handler returns the result if the applied command has the same
   fingerprint as its own; if it has a different one (two pending
   submissions under one key with different payloads), the handler's
   command is answered as its own application will be, `key_reused`. A wait
   whose context ends is forgotten at the runner's next tick. If leadership is lost first, the wait ends
   with `ErrLeadershipLost` and the handler forwards or answers 503. If the wait
   exceeds the request deadline, the handler answers 504 with code
   `outcome_unknown`; the client retries with the same key and receives the
   recorded result if the command was applied in the meantime.

A command that the leader proposed but that lost its slot to another leader's
value is not re-proposed by the log. The client's retry, carrying the same key,
proposes it again. If both the original and the retry are eventually chosen in
two different slots, the state machine applies the first and replays the
recorded result for the second. This is the client-session rule of the Raft
dissertation section 6.3 placed in the state machine.

### 3.5 Read path: read index, not lease

Consistent reads (`GET` without `?read=stale`) are served by the leader with a
read-index barrier:

1. The leader must have applied its own leadership no-op (its commit index is
   at least the slot of that no-op). Until then it answers 503; this
   guarantees its commit index covers everything chosen under earlier
   ballots, by quorum intersection with its Phase 1 majority.
2. It records readIndex = commit index and a read sequence number, and sends
   a Heartbeat carrying the sequence number.
3. When HeartbeatAcks carrying a sequence number at least that high, and
   reporting the leader's own ballot as their promised ballot, arrive from a
   majority (counting itself), the barrier is satisfied. Any ack reporting a
   higher promised ballot makes the leader step down and the read fails with
   `NotLeaderError`.
4. The handler waits until the local applied index reaches readIndex and
   answers from local state.

Reads issued between two heartbeats share one heartbeat round.

Why read index and not lease reads: a lease read is correct only if clock
drift between replicas stays under a bound; the chaos simulation in section 7
injects clock skew and the design should stay correct under it. The read index
adds one message round per batch of reads and depends on no clock. The
research notes record that lease-based reads are where independent
implementations have failed; this project does not implement them.

Stale reads (`?read=stale`) are answered by any replica from its own applied
state, with the response header `X-Arena-Applied-Slot` so the caller can tell
how far behind it is. They are for dashboards, never for settlement.

### 3.6 Durability

Each replica persists, before answering, its promised ballot, the highest
round it has used as a proposer, every accepted (ballot, slot, value), and
every chosen (slot, value). The simulator uses the in-memory store and keeps
the store object across a simulated crash so a restart recovers exactly what
a disk would have. The append-only file store `replog/wal` provides the same
interface for replicas that run in their own process, and `cmd/arena`
requires it in that mode. A node that has lost its store must not rejoin
under its old identity (section 9); the file records the node and membership
it belongs to, so a file cannot be started as another replica.

The state machine is not persisted. On restart a replica replays chosen
entries from slot 1. Replay is deterministic, so a replayed node's state hash
equals every other node's hash at the same applied slot.

### 3.7 Threads

`internal/paxos`, `internal/replog`, `internal/tournament`, `internal/ledger`
and `internal/replica.Core` contain no goroutines, no channels, no locks, and
never call `time.Now` or the global random functions. Time enters as a
`time.Duration` argument (`now`), randomness as an injected `*rand.Rand`. The
simulator drives all of them from one goroutine.

`internal/replica.Runner` is the one goroutine per process that owns a `Core`.
Transport receive goroutines and HTTP handler goroutines hand it work through
channels and wait on reply channels or `ctx.Done()`. The HTTP transport uses
two sending goroutines per peer, one for ordinary messages and one for large
ones. Every goroutine is bound to a context; the `Runner` tests use
`testing/synctest`.

---

## 4. Package layout

```
paxos-arena/
  go.mod                        module github.com/oguzhanozfe/paxos-arena, go 1.26.0
  README.md
  LICENSE
  Makefile                      check: gofmt, vet, build, tidy -diff, test -race
  .github/workflows/go.yml
  cmd/
    arena/main.go               one replica: HTTP API + Paxos participant
    chaos/main.go               deterministic simulation CLI
  internal/
    jsonx/                      strict JSON decoding shared by every codec and the API
    paxos/                      single-decree Paxos: ballots, rules, reference acceptor/proposer
    replog/                     Multi-Paxos log with leader, lease, read index; wire codec; MemStore
    replog/wal/                 append-only file store implementing replog.Store
    transport/                  in-memory fault-injecting network; HTTP transport
    tournament/                 deterministic tournament state machine and command codec
    game/                       Ladder card puzzle rules, deals and scores (milestone 4)
    ledger/                     append-only double-entry book with idempotent postings
    replica/                    Core (log + state machine, pure) and Runner (event loop)
    api/                        net/http handlers, idempotency handling, forwarding
    session/                    HMAC session tokens and device verifiers (milestone 4)
    intent/                     the play API for game clients (milestone 4)
    sim/                        virtual clock, event heap, fault schedule, clients, invariant checker
  unity-client/                 C# client SDK layout (milestone 4, README only)
  docs/
    DESIGN.md                   this document
    UNITY-INTEGRATION.md        the milestone 4 contract
    research/
    adr/                        one short file per decision taken during implementation
  testdata/
    seeds.txt                   seeds that once failed; rerun by every go test
```

Dependency direction. `TestNoForbiddenImports` in `internal/replog` (for
`paxos` and `replog`) and in `internal/tournament` (for `game`, `ledger`
and `tournament`) inspects `go list -deps`; the edges above the core are kept by
review:

```
paxos  <- ledger     <- tournament <- replica <- api <- cmd/arena
paxos  <- replog     <- replica    <- sim <- cmd/chaos
replog <- transport  <- sim, cmd/arena
replog <- replog/wal <- cmd/arena
replog, ledger, tournament <- api        ledger, tournament <- sim
jsonx  <- replog, replog/wal, tournament, api
game   <- tournament
game, ledger, paxos, replica, replog, session, tournament <- intent <- cmd/arena
```

`internal/paxos`, `internal/replog`, `internal/tournament`, `internal/ledger`
import only `encoding/json`, `crypto/sha256`, `errors`, `fmt`, `sort`,
`strconv`, `strings`, `time` (for `time.Duration` only), `math/rand/v2` (for
the injected `*rand.Rand` type only), `encoding/hex` (in `tournament`, for
the input digest rendered as hex), `internal/jsonx` and each other;
`tournament` also imports `internal/game`, which imports no other package of
the module and, for deals, may use `crypto/hmac` and `encoding/binary`. `internal/replog/wal` is
the one store that touches the file system and is kept out of `replog` so the
rule stays mechanical.

### 4.1 `internal/paxos`

Single-decree Paxos: the types shared by everything above it, the rules
stated once, and a reference acceptor and proposer that run the two-phase
protocol for one value. `replog` does not embed `paxos.Acceptor` per slot,
because in Multi-Paxos the promise is per acceptor and the accepted value is
per slot; it calls the same rule functions.

```go
package paxos

type NodeID uint32          // 0 is invalid
type Slot uint64            // slots start at 1; 0 means none
type Value []byte           // len 0 is the no-op

type Ballot struct {
    Round uint64
    Node  NodeID
}
func (b Ballot) Compare(o Ballot) int       // by Round, then Node
func (b Ballot) Less(o Ballot) bool
func (b Ballot) IsZero() bool               // Round == 0 && Node == 0
func (b Ballot) String() string             // "r12.n3"

type PValue struct {
    Ballot Ballot
    Slot   Slot
    Value  Value
}

// Rules. Each is one line and has its own table test.
func Quorum(n int) int                              // n/2 + 1
func MayPromise(promised, b Ballot) bool            // promised.Less(b)
func MayAccept(promised, b Ballot) bool             // !b.Less(promised)
// Choose implements P2c: the value of the highest-ballot report, or fallback
// when reports is empty.
func Choose(reports []PValue, fallback Value) Value

// Reference single-decree acceptor.
type Acceptor struct {
    Promised Ballot
    Accepted Ballot          // IsZero() means nothing accepted
    Value    Value
}
type Promise struct{ Ballot, Accepted Ballot; Value Value }
type Nack struct{ Ballot, Promised Ballot }
func (a *Acceptor) OnPrepare(b Ballot) (Promise, Nack, bool)   // ok=false -> Nack
func (a *Acceptor) OnAccept(b Ballot, v Value) (Nack, bool)    // ok=true  -> accepted

// Reference single-decree proposer.
type Proposer struct { /* self, n, ballot, promises, accepts */ }
func NewProposer(self NodeID, n int) *Proposer
func (p *Proposer) Start(round uint64) Ballot                  // ballot for a new attempt
func (p *Proposer) OnPromise(from NodeID, m Promise) (ready bool)
func (p *Proposer) OnNack(m Nack)                              // abandons the attempt
func (p *Proposer) Propose(want Value) (Ballot, Value, error)  // ErrNoQuorum before quorum; the first value is kept for the ballot
func (p *Proposer) OnAccepted(from NodeID, b Ballot) (chosen bool)
```

### 4.2 `internal/replog`

Multi-Paxos over slots with a stable leader, lease, learners, catch-up and
read index. One `Node` per replica. Not safe for concurrent use.

```go
package replog

type Config struct {
    Self               paxos.NodeID
    Peers              []paxos.NodeID   // includes Self; Quorum = paxos.Quorum(len(Peers))
    HeartbeatInterval  time.Duration    // default 50ms
    ElectionTimeoutMin time.Duration    // default 150ms
    ElectionTimeoutMax time.Duration    // default 300ms
    LeaseDuration      time.Duration    // default 150ms; must be <= ElectionTimeoutMin
    Window             int              // default 64 slots in flight
    LearnBatch         int              // default 256 entries per LearnRequest reply
    QueueLimit         int              // default 1024 values queued beyond Window; then ErrBusy
    Unsafe             *UnsafeKnobs     // nil in production; see section 6.4
}
func (c Config) Validate() error

type Role uint8 // Follower, Candidate, Leader

type Entry struct {
    Slot   paxos.Slot
    Ballot paxos.Ballot
    Value  paxos.Value      // len 0: no-op
}
func (e Entry) NoOp() bool

type Envelope struct {
    From paxos.NodeID
    To   paxos.NodeID
    Msg  Message
}
type Message interface{ msg() }   // the nine structs in section 5.1

type Event interface{ event() }
type LeaderChanged struct{ Leader paxos.NodeID; Ballot paxos.Ballot; Self bool }
type ReadReady     struct{ Seq uint64; Index paxos.Slot }
type ReadFailed    struct{ Seq uint64; Err error }

type Node struct { /* section 5.3 */ }
func New(cfg Config, store Store, rng *rand.Rand) (*Node, error)   // loads durable state

// Driving the node. Every call returns the messages to send; messages to
// Self are processed inline and never returned.
func (n *Node) Step(now time.Duration, env Envelope) []Envelope
func (n *Node) Tick(now time.Duration) []Envelope
func (n *Node) Propose(now time.Duration, v paxos.Value) ([]Envelope, error) // NotLeaderError{Leader}
func (n *Node) ReadIndex(now time.Duration) (seq uint64, out []Envelope, err error)
func (n *Node) Events() []Event          // drains events since the last call

// Observation (also used by the simulator's checker).
func (n *Node) Role() Role
func (n *Node) Ballot() paxos.Ballot                     // current ballot when Candidate/Leader
func (n *Node) Leader() (paxos.NodeID, paxos.Ballot, bool)
func (n *Node) Promised() paxos.Ballot
func (n *Node) Accepted(s paxos.Slot) (paxos.PValue, bool)
func (n *Node) Chosen(s paxos.Slot) (Entry, bool)
func (n *Node) CommitIndex() paxos.Slot
func (n *Node) Ready() bool                              // Leader and leadership no-op chosen

type NotLeaderError struct{ Leader paxos.NodeID } // Leader may be 0 (unknown)
var ErrNotReady = errors.New("replog: leader has not committed its leadership no-op")
var ErrBusy = errors.New("replog: the leader's proposal queue is full")
var ErrValueTooLarge = errors.New("replog: value is too large to replicate") // wrapped
const MaxValueBytes = 1 << 20   // Propose refuses longer values; every transport carries one

// Durable state.
type Durable struct {
    Promised paxos.Ballot
    MaxRound uint64
    Accepted []paxos.PValue
    Chosen   []Entry
}
type Store interface {
    Load() (Durable, error)
    SavePromised(paxos.Ballot) error
    SaveMaxRound(uint64) error
    SaveAccepted(paxos.PValue) error
    SaveChosen(Entry) error
}
type MemStore struct{ /* Durable, guarded by nothing: single owner */ }
func NewMemStore() *MemStore

// Wire codec: JSON, {"from","to","type","body"}. Decode never panics.
func Encode(env Envelope) ([]byte, error)
func Decode(b []byte) (Envelope, error)
```

`internal/replog/wal` implements `replog.Store` on an append-only file: one
record per `Save*` call (length and CRC-32C header, JSON payload), `fsync`
after each write. `Open` truncates a torn tail (an incomplete last record, or
a last record whose checksum fails) and refuses a file damaged before its
last record. `Bind` records the replica and membership in an empty file and
refuses a file that belongs to another.

```go
package wal

type File struct{ /* *os.File, path, valid length, membership */ }
func Open(path string) (*File, error)   // creates when missing; cuts a torn tail
var ErrCorrupt error                     // damage before the last record
type Membership struct{ Self paxos.NodeID; Peers []paxos.NodeID }
func (f *File) Bind(self paxos.NodeID, peers []paxos.NodeID) error
func (f *File) Membership() (Membership, bool)
func (f *File) Load() (replog.Durable, error)
func (f *File) SavePromised(paxos.Ballot) error
func (f *File) SaveMaxRound(uint64) error
func (f *File) SaveAccepted(paxos.PValue) error
func (f *File) SaveChosen(replog.Entry) error
func (f *File) Close() error
```

### 4.3 `internal/transport`

```go
package transport

type Deliver func(replog.Envelope)

// Network is the in-memory fault-injecting transport. Single-threaded; the
// simulator owns the clock and pops messages when their delivery time comes.
type Faults struct {
    DropP    float64
    DupP     float64
    MinDelay time.Duration
    MaxDelay time.Duration
}
type Network struct { /* heap of (deliverAt, seq, env), blocked map[[2]NodeID]bool, rng, stats */ }
func NewNetwork(rng *rand.Rand, f Faults) *Network
func (n *Network) SetFaults(f Faults)
func (n *Network) Send(now time.Duration, env replog.Envelope)          // applies drop/dup/delay/partition
func (n *Network) Next() (at time.Duration, env replog.Envelope, ok bool) // pops the earliest
func (n *Network) PeekTime() (time.Duration, bool)
func (n *Network) Block(from, to paxos.NodeID, blocked bool)               // directional partition
func (n *Network) Split(groups ...[]paxos.NodeID)                          // block across groups
func (n *Network) Heal()                                                   // remove all blocks
func (n *Network) Purge(node paxos.NodeID)                                 // drop in-flight to node (crash)
type Stats struct{ Sent, Delivered, Dropped, Duplicated, Blocked uint64 }
func (n *Network) Stats() Stats

// HTTP is the inter-process transport for the demo: POST /internal/paxos with
// the replog wire encoding. Best effort: Send never blocks; a full per-peer
// queue drops the message, and the protocol's retransmission covers the loss.
// Messages are encoded by the sender goroutines. Messages carrying more than
// BulkBytes (64 KiB) of values use a second per-peer queue, and a
// retransmission of one still queued is dropped, so heartbeats never wait
// behind them. MaxMessageBytes (16 MiB) admits an Accept or Learn carrying a
// value of replog.MaxValueBytes.
type HTTP struct { /* peers map[NodeID]*peer (queue, bulk queue, pending), client, deliver, log */ }
func NewHTTP(self paxos.NodeID, peers map[paxos.NodeID]string, deliver Deliver, client *http.Client, log *slog.Logger) *HTTP
func NewClient(timeout time.Duration, perHost int) *http.Client // a connection pool of its own
func (t *HTTP) Send(env replog.Envelope)
func (t *HTTP) Handler() http.Handler      // mount at POST /internal/paxos
func (t *HTTP) Run(ctx context.Context)    // starts and stops the sender goroutines
```

### 4.4 `internal/ledger`

An append-only book. Every posting moves one amount from one account to
another and carries an idempotency key; posting a key that exists is a no-op
that returns the existing posting. Balances are derived from postings.

```go
package ledger

type Money int64            // minor units; never a float anywhere in the module
type Account string         // "player:<id>", "pool:<tid>", "rake:<tid>", "withheld:<tid>"
type PostingKey string
type Kind uint8             // EntryFee, Rake, Prize, Withheld, Refund

type Posting struct {
    Seq              uint64        // 1-based append order, assigned by Post
    Key              PostingKey
    Kind             Kind
    Debit            Account
    Credit           Account
    Amount           Money         // > 0
    Tournament       string
    Player           string        // empty for Rake
    Place            int           // 0 unless Prize or Withheld
    ExclusionVersion uint64        // list version checked for Prize/Withheld/EntryFee
    Slot             paxos.Slot    // log slot of the command that produced it
    Ballot           paxos.Ballot  // ballot under which that slot was chosen
}

type Book struct { /* postings []Posting; byKey map[PostingKey]int; balances map[Account]Money */ }
func NewBook() *Book
func (b *Book) Post(p Posting) (Posting, bool)        // ok=false: key existed, existing returned
func (b *Book) Has(k PostingKey) bool
func (b *Book) Balance(a Account) Money
func (b *Book) Postings() []Posting                    // copy, in Seq order
func (b *Book) ForTournament(id string) []Posting
func (b *Book) Check() error                           // amounts > 0, keys unique, balances == recomputed, sum == 0
func (b *Book) Hash() [32]byte                         // sha256 over postings in Seq order

func PlayerAccount(id string) Account
func PoolAccount(tid string) Account
func RakeAccount(tid string) Account
func WithheldAccount(tid string) Account
func FeeKey(tid, pid string) PostingKey                 // "fee:<tid>:<pid>"
func RakeKey(tid string) PostingKey                     // "rake:<tid>"
func PrizeKey(tid, pid string, place int) PostingKey    // "prize:<tid>:<pid>:<place>"
func WithheldKey(tid, pid string, place int) PostingKey // "withheld:<tid>:<pid>:<place>"
func RefundKey(tid, pid string) PostingKey              // "refund:<tid>:<pid>"
```

### 4.5 `internal/tournament`

The deterministic state machine. Section 5.4 defines commands and rules.

```go
package tournament

type TournamentID string
type PlayerID string
type IdempotencyKey string
type Status uint8      // Open, Closed, Voided, Settled
type TieBreak string   // "earliest_submission" | "split"

type Command struct {
    Key        IdempotencyKey  // 1..128 bytes, printable ASCII, required
    ReceivedAt int64           // unix milliseconds, stamped by the leader's API; audit only
    Op         Op
}
type Op interface{ op() }
// CreateTournament, Join, SubmitScore, Close, Settle: section 5.4.

type Result struct {
    Code     Code           // OK or a rejection code
    Replayed bool           // true when answered from the results table
    Detail   string
    Slot     paxos.Slot     // slot of the command that produced this result
    Seed     uint64         // set for OK Join results (the deal to play)
}
type Code string           // "ok", "key_reused", "unknown_tournament", ... (section 5.4)

type State struct { /* tournaments, order, results, book, applied */ }
func NewState() *State
// Apply is total: it never panics on a decoded Command and never leaves the
// state half-changed. It returns the recorded result for a repeated key.
func (s *State) Apply(slot paxos.Slot, ballot paxos.Ballot, cmd Command) Result
func (s *State) Applied() paxos.Slot
func (s *State) Hash() [32]byte                          // canonical encoding of everything
func (s *State) Tournament(id TournamentID) (Tournament, bool)   // deep copy
func (s *State) Tournaments() []TournamentID             // creation order
func (s *State) TournamentCount() int                    // without copying
func (s *State) Ledger() *ledger.Book                    // read-only use by callers
func (s *State) Result(k IdempotencyKey) (Result, bool)

// Pure functions, exported so the simulator's checker can recompute them.
func ComputeStandings(t *Tournament) []Standing
func ComputePool(t *Tournament) (fees, rake, pool ledger.Money)   // rake is 0 when the tournament voids
func ComputePayouts(t *Tournament, ex Exclusions) []Payout        // Closed: prizes; Voided: refunds

// Codec. Encode is canonical (struct field order, sorted slices where the
// order is not semantic). Decode never panics and rejects unknown fields.
func Encode(c Command) ([]byte, error)
func Decode(b []byte) (Command, error)
// Fingerprint covers the client-supplied part of the command: op type and
// payload, excluding Key, ReceivedAt and CreateTournament.Seed.
func Fingerprint(c Command) [32]byte
```

### 4.6 `internal/replica`

```go
package replica

// Core joins one replog.Node with one tournament.State. Pure: no goroutines,
// no clock. Used directly by the simulator and wrapped by Runner.
type Core struct { /* Log *replog.Node; State *tournament.State */ }
func NewCore(cfg replog.Config, store replog.Store, rng *rand.Rand) (*Core, error)
func (c *Core) Step(now time.Duration, env replog.Envelope) []replog.Envelope
func (c *Core) Tick(now time.Duration) []replog.Envelope
func (c *Core) Submit(now time.Duration, cmd tournament.Command) ([]replog.Envelope, error)
func (c *Core) ReadIndex(now time.Duration) (uint64, []replog.Envelope, error)
// ApplyCommitted decodes and applies every chosen slot above Applied() up to
// the commit index, in order, and returns what it applied.
func (c *Core) ApplyCommitted() []Applied
type Applied struct{ Slot paxos.Slot; Key tournament.IdempotencyKey; Command tournament.Command; Result tournament.Result; NoOp bool; Err error }
func (c *Core) Events() []replog.Event
func (c *Core) Log() *replog.Node
func (c *Core) State() *tournament.State

// Runner is the single event-loop goroutine around a Core.
type Runner struct { /* core, inbox, submits, reads, ticker, waiters map[Key][]*waiter (command, fingerprint, reply) */ }
func NewRunner(core *Core, send func(replog.Envelope), log *slog.Logger) *Runner
func (r *Runner) Run(ctx context.Context) error       // returns when ctx is done
func (r *Runner) Deliver(env replog.Envelope)          // transport callback; non-blocking enqueue
func (r *Runner) Submit(ctx context.Context, cmd tournament.Command) (tournament.Result, error)
func (r *Runner) Read(ctx context.Context, consistent bool, fn func(*tournament.State) error) error
func (r *Runner) Status() Status
func (r *Runner) Held() int                            // requests waiting on the event loop
type Status struct {
    Self, Leader paxos.NodeID
    Role         replog.Role
    Ballot       paxos.Ballot
    Ready        bool
    CommitIndex  paxos.Slot
    Applied      paxos.Slot
    StateHash    tournament.Digest   // hex in JSON
    Tournaments  int
}

// ErrLeadershipLost ends a pending Submit when this replica stops leading
// before the key was applied. The command may still be chosen later.
var ErrLeadershipLost = errors.New("replica: leadership lost while the command was pending")
```

`Submit` returns `replog.NotLeaderError` immediately when this replica is not
the leader, `replog.ErrBusy` when the leader's queue is full, and
`ErrLeadershipLost` when the key was pending and leadership changed before
it was applied. A pending `Submit` is answered with the result of the applied
command only when the fingerprints match; otherwise with `key_reused`.
`Read` with `consistent=true` runs the read-index barrier of section 3.5 and
then executes `fn` on the event-loop goroutine; with `consistent=false` it
executes `fn` immediately on that goroutine. When the context ends first,
`Read` returns at once while `fn` may still run, so a caller uses what `fn`
wrote only when `Read` returned nil. The runner refreshes `Status` before it
answers any request an event completed, so a caller woken by
`ErrLeadershipLost` already sees the new role.

### 4.7 `internal/api`

```go
package api

type Backend interface {
    Submit(ctx context.Context, cmd tournament.Command) (tournament.Result, error)
    Read(ctx context.Context, consistent bool, fn func(*tournament.State) error) error
    Status() replica.Status
}

type Config struct {
    Self           paxos.NodeID
    Peers          map[paxos.NodeID]string   // node -> base URL, for forwarding
    RequestTimeout time.Duration             // default 5s; bounds the wait for apply
    MaxBody        int64                     // default DefaultMaxBody, 64 KiB
    Seed           func() uint64             // default crypto/rand; injectable in tests
    Now            func() time.Time          // default time.Now; injectable in tests
    Client         *http.Client              // forwarding; default has its own pool of ForwardConnsPerHost (256)
}
type Server struct { /* cfg, backend, inflight set of keys under one mutex, log */ }
func New(cfg Config, b Backend, log *slog.Logger) *Server
func (s *Server) Handler() http.Handler
```

Routes (Go 1.22 method patterns), all under `http.NewServeMux()`:

| Route | Command | Success |
|---|---|---|
| `POST /v1/tournaments` | CreateTournament | 201 |
| `POST /v1/tournaments/{id}/entries` | Join | 201 |
| `POST /v1/tournaments/{id}/scores` | SubmitScore | 200 |
| `POST /v1/tournaments/{id}/close` | Close | 200 |
| `POST /v1/tournaments/{id}/settle` | Settle | 200 |
| `GET /v1/tournaments/{id}` | read: record, standings, payouts | 200 |
| `GET /v1/tournaments/{id}/ledger` | read: postings for the tournament | 200 |
| `GET /v1/node` | status | 200 |
| `GET /healthz` | | 200 |

Every POST requires `Idempotency-Key`. Handler flow: 400 if the header is
missing or the body is malformed (unknown fields, trailing data); 413
`body_too_large` if the body exceeds MaxBody or the canonical encoding of
the command exceeds `replog.MaxValueBytes`; under the mutex, an in-flight
entry with the same key answers 409 `in_flight`; otherwise mark in flight,
call `Submit`, and clear the mark when it returns. A successful response
carries the tournament record from a stale read under a context of its own,
taken after the command was applied; lists render as `[]`, never `null`. The state
machine, not the in-flight map, is the authority on repeated keys: a replayed
`Result` with `Code == "ok"` returns the original success status with
`"replayed": true`; a replayed rejection returns its original status;
`key_reused` (same key, different fingerprint) answers 422.

State-machine rejections answer 409 with `application/problem+json`
(RFC 9457) whose `code` member is the `tournament.Code`. `NotLeaderError` or
`ErrLeadershipLost` with a known leader forwards once, and the leader's
answer is relayed whole; otherwise 503 with `Retry-After: 1`. `ErrBusy`
answers 503 with `Retry-After: 1`. A wait that exceeds `RequestTimeout`
answers 504 with `code: "outcome_unknown"`.

Request bodies (JSON):

```
POST /v1/tournaments
  {"id":"t-2026-09-16-001","rules":{"entry_fee":500,"rake_bps":1000,
   "prize_bps":[5000,3000,2000],"min_entrants":3,"max_entrants":100,
   "max_score":100000,"min_age":18,"tie_break":"earliest_submission",
   "exclusions":{"version":7,"jurisdictions":["XX","YY"]}}}
POST /v1/tournaments/{id}/entries
  {"player":{"id":"p-17","jurisdiction":"TR","age":31}}
POST /v1/tournaments/{id}/scores
  {"player":"p-17","score":8120,"deal_seed":1234567890123,"input_digest":"<64 hex>"}
POST /v1/tournaments/{id}/close
  {}
POST /v1/tournaments/{id}/settle
  {"exclusions":{"version":8,"jurisdictions":["XX","YY","ZZ"]}}
```

Success body: `{"code":"ok","replayed":false,"slot":412,"seed":...,
"tournament":{...}}` where `tournament` is the record after the command.

### 4.8 `internal/sim`

```go
package sim

type Params struct {
    Seed         uint64
    Nodes        int              // 3 or 5
    Steps        int              // events in fault mode
    LivenessSteps int             // bound after Heal
    Faults       transport.Faults
    PartitionP   float64          // per tick probability of a random split
    HealP        float64          // per tick probability of healing
    CrashP       float64          // per tick probability of crashing a random node
    RestartAfter [2]time.Duration
    ClockSkewMax time.Duration    // per-node offset added to now before Tick
    TornWriteP   float64          // crash during Save leaves the previous durable state
    Clients      int
    Tournaments  int
    Entrants     int
    RetryP       float64          // client retries a command even after success
    Scenario     string           // "" or a name from section 7
}
type Run struct { /* clock, heap, nodes []*replica.Core, stores, net, clients, checker, trace */ }
func New(p Params, trace io.Writer) (*Run, error)
func (r *Run) Step() error              // one event; non-nil error is an invariant violation
func (r *Run) RunFaults() error         // Steps events with faults
func (r *Run) Heal()                    // stop faults, heal partitions, restart crashed nodes
func (r *Run) RunLiveness() error       // until all client workflows complete or LivenessSteps
func (r *Run) Report() Report
type Report struct{ Steps int; Elections, Crashes, Partitions int; Net transport.Stats; Settled int; Applied paxos.Slot }

type Checker struct { /* section 6 */ }
func (c *Checker) AfterEvent(r *Run) error
func (c *Checker) AtEnd(r *Run) error

func Scenarios() []string
```

`cmd/chaos` flags: `-seed`, `-seeds N` (sweep seed..seed+N-1), `-nodes`,
`-steps`, `-liveness-steps`, `-drop`, `-dup`, `-scenario`, `-trace` (write
the event trace to a file), `-log-level`, `-no-summary`, `-list-scenarios`. On a violation it prints `seed=<n> step=<k> scenario=<s>` and the exact
command line to replay, and exits 1.

### 4.9 `cmd/arena`

Flags: `-id 1`, `-peers 1=http://127.0.0.1:8081,2=http://127.0.0.1:8082,3=http://127.0.0.1:8083`,
`-listen :8081`, `-wal path` (required with `-peers`), `-log-level info`,
`-log-json`; without `-peers`, `-nodes N` starts N replicas in one process
(ADR 0008) and `-wal` is refused.
`main` parses flags and calls `run(ctx, args, stdout, stderr) error`, which
builds `replog.Config` from the peer list, opens the store with `wal.Open`
and binds it to the replica with `Bind`, constructs `replica.Core`,
`replica.Runner`, `transport.HTTP` and `api.Server`, mounts the API and
`/internal/paxos` on one `http.Server` with timeouts set, and
stops on SIGINT/SIGTERM through `signal.NotifyContext` and `Shutdown`.

---

## 5. Messages and state

### 5.1 Protocol messages (`internal/replog`)

All nine implement `Message`. Field names are the JSON names.

```go
// Phase 1, sent by a Candidate to every peer. One Prepare covers every slot
// >= FromSlot (the candidate's commit index + 1).
type Prepare struct {
    Ballot   paxos.Ballot `json:"ballot"`
    FromSlot paxos.Slot   `json:"from_slot"`
}

// Phase 1 reply. Accepted lists the acceptor's most recently accepted pvalue
// for every slot >= FromSlot, in slot order. CommitIndex lets the new leader
// see how far this acceptor has learned.
type Promise struct {
    Ballot      paxos.Ballot   `json:"ballot"`
    Accepted    []paxos.PValue `json:"accepted"`
    CommitIndex paxos.Slot     `json:"commit_index"`
}

// Phase 2 for one slot.
type Accept struct {
    Ballot paxos.Ballot `json:"ballot"`
    Slot   paxos.Slot   `json:"slot"`
    Value  paxos.Value  `json:"value"`
}

type Accepted struct {
    Ballot paxos.Ballot `json:"ballot"`
    Slot   paxos.Slot   `json:"slot"`
}

// Refusal of a Prepare (Slot == 0) or an Accept (Slot set). Promised is the
// acceptor's current promise; LeaseRemaining > 0 means the refusal came from
// the lease rule and the sender should wait that long before retrying.
type Nack struct {
    Ballot         paxos.Ballot  `json:"ballot"`
    Promised       paxos.Ballot  `json:"promised"`
    Slot           paxos.Slot    `json:"slot"`
    LeaseRemaining time.Duration `json:"lease_remaining"`
}

// Sent by the leader when a slot is chosen, and in reply to LearnRequest.
type Learn struct {
    Slot   paxos.Slot   `json:"slot"`
    Ballot paxos.Ballot `json:"ballot"`
    Value  paxos.Value  `json:"value"`
}

// Sent by a replica whose commit index is behind the leader's.
type LearnRequest struct {
    FromSlot paxos.Slot `json:"from_slot"`
    MaxCount int        `json:"max_count"`
}

// Sent by the leader every HeartbeatInterval. ReadSeq is the highest read
// sequence number the leader wants acknowledged (0 when none pending).
type Heartbeat struct {
    Ballot      paxos.Ballot `json:"ballot"`
    CommitIndex paxos.Slot   `json:"commit_index"`
    ReadSeq     uint64       `json:"read_seq"`
}

type HeartbeatAck struct {
    Ballot      paxos.Ballot `json:"ballot"`    // the heartbeat's ballot
    Promised    paxos.Ballot `json:"promised"`  // the acceptor's current promise
    CommitIndex paxos.Slot   `json:"commit_index"`
    ReadSeq     uint64       `json:"read_seq"`
}
```

Wire form: `{"from":1,"to":2,"type":"accept","body":{...}}`. `Value` is
base64 through `encoding/json`'s default `[]byte` handling. `Decode` rejects
unknown types and unknown fields and is a fuzz target.

### 5.2 Acceptor and learner rules

Per node, durable: `promised paxos.Ballot`, `maxRound uint64`,
`accepted map[Slot]PValue`, `chosen map[Slot]Entry`. Volatile:
`commitIndex`, `leaseHolder NodeID`, `leaseUntil time.Duration`,
`leaderHint NodeID`, `electionDeadline`.

```
on Prepare(b, from) at now:
    if !MayPromise(promised, b):                 reply Nack{b, promised, 0, 0}
    else if now < leaseUntil && b.Node != leaseHolder:
                                                 reply Nack{b, promised, 0, leaseUntil-now}
    else:
        promised = b; store.SavePromised(b)      // before replying
        leaseHolder = b.Node; leaseUntil = now + LeaseDuration
        leaderHint = b.Node; reset electionDeadline
        reply Promise{b, accepted[s] for s >= from in slot order, commitIndex}

on Accept(b, s, v):
    if !MayAccept(promised, b):                  reply Nack{b, promised, s, 0}
    else:
        if promised.Less(b): promised = b; store.SavePromised(b)
        accepted[s] = {b, s, v}; store.SaveAccepted(accepted[s])   // before replying
        refresh lease and leaderHint as above
        reply Accepted{b, s}

on Heartbeat(b, ci, rs):
    if b.Less(promised):                          reply HeartbeatAck{b, promised, commitIndex, rs}
    else:
        if promised.Less(b): promised = b; store.SavePromised(b)
        refresh lease and leaderHint; reset electionDeadline
        reply HeartbeatAck{b, promised, commitIndex, rs}
        if ci > commitIndex && no LearnRequest outstanding this interval:
            send LearnRequest{commitIndex+1, LearnBatch} to b.Node

on Learn(s, b, v):
    if chosen[s] exists && chosen[s].Value != v:  invariant violation (panic in tests, log and ignore in production)
    chosen[s] = {s, b, v}; store.SaveChosen(...)
    advance commitIndex while chosen[commitIndex+1] exists

on LearnRequest(from, max):
    reply Learn for each chosen slot in [from, from+max) that exists, in order
```

Raising `promised` on an Accept or Heartbeat with a higher ballot is the rule
of Paxos Made Simple section 2.2 (the Phase 2 acceptor set need not equal the
Phase 1 set). Refusing on the lease never lowers `promised` and is therefore
safe under any clock behaviour.

### 5.3 Leader rules

Volatile leader state: `role`, `ballot`, `phase1 struct{ fromSlot; promises map[NodeID]Promise; deadline }`,
`proposals map[Slot]*proposal{ value; accepts set[NodeID]; lastSent }`,
`nextSlot`, `firstOwnSlot`, `queue []Value`, `lastMajorityContact`,
`readSeq`, `reads map[uint64]*read{ index; acks set[NodeID] }`.

```
Tick(now), Follower:
    if now >= electionDeadline && now >= leaseUntil:
        maxRound++; store.SaveMaxRound(maxRound)
        ballot = {maxRound, Self}; role = Candidate
        phase1 = {deadline: now + randomElectionTimeout()}
        send Prepare{ballot, commitIndex+1} to all peers   (own copy processed inline)

Tick(now), Candidate:
    if now >= phase1.deadline: back to Follower with a fresh electionDeadline (retry later)
    else re-send Prepare to peers that have not answered

on Promise(m) for ballot, Candidate:
    phase1.promises[from] = m
    if len(promises) >= Quorum:
        role = Leader; emit LeaderChanged{Self, ballot, true}
        reports = union of m.Accepted over promises
        top = max slot in reports (or phase1.fromSlot-1 when reports is empty)
        for s in phase1.fromSlot .. top:
            v = paxos.Choose(reports for s, NoOp)       // reported value, else no-op fill
            propose(s, v)                               // skipped when chosen[s] already exists
        firstOwnSlot = top+1; propose(firstOwnSlot, NoOp)  // leadership no-op
        nextSlot = firstOwnSlot+1
        drain queue into proposals up to Window

on Nack(m) for ballot, Candidate or Leader:
    if ballot.Less(m.Promised):
        maxRound = max(maxRound, m.Promised.Round); store.SaveMaxRound
        step down: role = Follower; proposals, reads, queue cleared; emit LeaderChanged{m.Promised.Node, m.Promised, false}
        electionDeadline = now + m.LeaseRemaining + randomElectionTimeout()
        pending reads fail with NotLeaderError; the host fails pending submits with ErrLeadershipLost

Propose(now, v), Leader:
    if len(proposals) >= Window: queue = append(queue, v); return
    propose(nextSlot, v); nextSlot++

propose(s, v): proposals[s] = {v}; accepted[s] handled inline via own OnAccept; send Accept{ballot, s, v} to peers

on Accepted(m) for ballot, Leader:
    proposals[m.Slot].accepts.add(from)
    if len(accepts) >= Quorum && proposals[m.Slot] not yet chosen:
        Learn locally; send Learn{s, ballot, v} to peers; delete proposals[s]; drain queue

Tick(now), Leader:
    every HeartbeatInterval: send Heartbeat{ballot, commitIndex, readSeq} to peers
                             re-send Accept for every proposal not yet chosen
    if now - lastMajorityContact > ElectionTimeoutMax: step down as above (Leader may be 0)

on HeartbeatAck(m), Leader:
    if ballot.Less(m.Promised): step down as in Nack
    record contact; if contacts from Quorum within the interval: lastMajorityContact = now
    for each pending read r with r.seq <= m.ReadSeq: r.acks.add(from); if len(acks) >= Quorum: emit ReadReady{r.seq, r.index}

ReadIndex(now), Leader:
    if !Ready(): return ErrNotReady
    readSeq++; reads[readSeq] = {index: commitIndex, acks: {Self}}
    send Heartbeat{ballot, commitIndex, readSeq} to peers
```

`Ready()` is `role == Leader && commitIndex >= firstOwnSlot`. A leader
counts itself as an acceptor: its own Prepare, Accept and Heartbeat are
processed inline through the rules of 5.2, so `Quorum` is reached with
`Quorum-1` remote replies. `Promise.CommitIndex` is diagnostic: a new leader
that is behind learns the slots it is missing by re-proposing them, because
every chosen value is reported by at least one member of its Phase 1
majority.

### 5.4 Tournament commands and validation

Types shared by the commands:

```go
type Exclusions struct {
    Version       uint64   `json:"version"`
    Jurisdictions []string `json:"jurisdictions"` // at most 1000 upper-case codes; sorted and deduplicated by Encode
}
type Player struct {
    ID           PlayerID `json:"id"`           // 1..64 characters from A-Z a-z 0-9 . _ -
    Jurisdiction string   `json:"jurisdiction"` // 2..8 bytes, upper-case letters
    Age          int      `json:"age"`          // 0..150
}
type Rules struct {
    EntryFee    ledger.Money `json:"entry_fee"`    // > 0
    RakeBps     uint32       `json:"rake_bps"`     // 0..9999
    PrizeBps    []uint32     `json:"prize_bps"`    // 1..1000 places; each > 0; sum == 10000
    MinEntrants int          `json:"min_entrants"` // >= len(PrizeBps); minimum scored entrants at Close
    MaxEntrants int          `json:"max_entrants"` // >= MinEntrants
    MaxScore    int64        `json:"max_score"`    // >= 0
    MinAge      int          `json:"min_age"`      // >= 0
    TieBreak    TieBreak     `json:"tie_break"`
    Exclusions  Exclusions   `json:"exclusions"`
}
```

Commands, with the validation performed by `Apply`. Validation order is the
order listed; the first failing rule is the result code. A rejected command
has no effect other than recording its result under its key.

Identifiers (tournament and player) are restricted to 1..64 characters from
`A-Z`, `a-z`, `0-9`, `.`, `_` and `-`. Ledger account names and posting keys
join identifiers with `:` (section 5.5), so an identifier containing `:`
could give two tournaments one posting key, and the ledger would treat the
second posting as a retry. The restriction also keeps identifiers valid
UTF-8, so two identifiers never share a JSON encoding and a fingerprint. As
a second line of defence, a command whose postings would reuse an existing
key is rejected with `ledger_conflict` before anything changes.

```go
type CreateTournament struct {
    ID    TournamentID `json:"id"`    // 1..64 bytes
    Seed  uint64       `json:"seed"`  // server-generated by the leader's API; excluded from Fingerprint
    Rules Rules        `json:"rules"`
}
// invalid_rules     any Rules bound above violated, or ID not of the identifier shape
// tournament_exists ID already exists
// Effect: Tournament{ID, Seed, Rules, Status: Open}; no postings.

type Join struct {
    Tournament TournamentID `json:"tournament"`
    Player     Player       `json:"player"`
}
// invalid_player           Player.ID not of the identifier shape, Jurisdiction or Age malformed
// unknown_tournament
// not_open                 Status != Open
// tournament_full          len(Entries) == MaxEntrants
// already_joined           Player.ID present
// jurisdiction_excluded    Player.Jurisdiction in Rules.Exclusions.Jurisdictions
// underage                 Player.Age < Rules.MinAge
// ledger_conflict          the FeeKey posting exists already (unreachable with valid identifiers)
// Effect: Entry{Player, JoinSeq: len(Entries)+1, ExclusionVersion: Rules.Exclusions.Version};
//         posting EntryFee: Debit player:<pid>, Credit pool:<tid>, Amount EntryFee, Key FeeKey.
//         Result.Seed = Tournament.Seed.

type SubmitScore struct {
    Tournament  TournamentID `json:"tournament"`
    Player      PlayerID     `json:"player"`
    Score       int64        `json:"score"`
    DealSeed    uint64       `json:"deal_seed"`     // must equal Tournament.Seed
    InputDigest [32]byte     `json:"input_digest"`  // hex in JSON; recorded, not verified
}
// unknown_tournament
// not_open
// not_joined
// already_scored           one accepted score per entrant
// seed_mismatch            DealSeed != Seed: the client did not play this tournament's deal
// score_out_of_range       Score < 0 || Score > MaxScore
// Effect: Entry.Score, Scored = true, SubmitSeq = scoredCount+1, InputDigest recorded.

type Close struct {
    Tournament TournamentID `json:"tournament"`
}
// unknown_tournament
// not_open
// Effect: Standings = ComputeStandings(t); ClosedAt = slot;
//         Status = Voided if scoredCount < MinEntrants, else Closed;
//         Fees, Rake, Pool = ComputePool(t) (Rake 0 and Pool == Fees when Voided). No postings.

type Settle struct {
    Tournament TournamentID `json:"tournament"`
    Exclusions Exclusions   `json:"exclusions"`   // the list current at settlement
}
// invalid_exclusions       more than 1000 codes, or a code that is not 2..8 upper-case letters
// unknown_tournament
// not_closed               Status is Open or Settled (a second Settle with a new key is rejected;
//                          a second Settle with the same key is replayed)
// ledger_conflict          a rake, prize, withheld or refund posting key exists already
// Effect, Status == Closed: Payouts = ComputePayouts(t, Exclusions); postings in this order:
//         Rake (pool -> rake:<tid>, if rake > 0), then per payout in Place order:
//         Prize (pool -> player) or Withheld (pool -> withheld:<tid>);
//         Status = Settled; SettledAt = slot.
// Effect, Status == Voided: Payouts = one Payout{Player, Place: 0, Amount: EntryFee, Key: RefundKey}
//         per entry in JoinSeq order, each posted as Refund (pool -> player);
//         Status = Settled; SettledAt = slot.
```

Standings (`ComputeStandings`), deterministic in the state alone:

1. Scored entries sorted by Score descending, then SubmitSeq ascending.
2. Unscored entries after them, by JoinSeq ascending, with `Scored=false`.
3. `Place` is the 1-based position. Under `split`, entries with equal Score
   share the place of the first of them (standard competition ranking);
   under `earliest_submission` places are distinct.

Pool (`ComputePool`), integer arithmetic only:

```
fees = EntryFee * len(Entries)
if scoredCount < MinEntrants:           // the tournament voids at Close
    rake = 0
    pool = fees                         // everything is refunded
else:
    rake = fees * RakeBps / 10000       (integer division)
    pool = fees - rake
```

Payouts (`ComputePayouts`), for Status Closed:

```
n     = len(PrizeBps)                   // <= scoredCount, guaranteed by MinEntrants
share[i] = pool * PrizeBps[i] / 10000   for i in 0..n-1
share[0] += pool - sum(share)           // rounding remainder to first place
earliest_submission: payout for standing i (0-based, i < n) is share[i]
split: for each group of equal Score among the first n standings occupying
       positions a..b (clipped to n-1): total = sum(share[a..b]); each member
       gets total / len; the remainder total % len is given one unit each to
       the members in SubmitSeq order
withheld: a payout whose player's Jurisdiction is in Settle.Exclusions.Jurisdictions
       is marked Withheld with Reason "jurisdiction_excluded"; the amount is
       unchanged and posts to withheld:<tid> instead of the player
each Payout records ExclusionVersion = Settle.Exclusions.Version
```

These rules guarantee `sum(payout amounts) == pool` and, with the rake
posting, `Balance(pool:<tid>) == 0` after Settle.

Record types:

```go
type Entry struct {
    Player           Player
    JoinSeq          uint32
    ExclusionVersion uint64
    Scored           bool
    Score            int64
    SubmitSeq        uint32
    InputDigest      [32]byte
}
type Standing struct {
    Place     int
    Player    PlayerID
    Score     int64
    Scored    bool
    SubmitSeq uint32
}
type Payout struct {
    Player           PlayerID
    Place            int
    Amount           ledger.Money
    Withheld         bool
    Reason           string
    ExclusionVersion uint64
    Key              ledger.PostingKey
}
type Tournament struct {
    ID        TournamentID
    Seed      uint64
    Rules     Rules
    Status    Status
    Entries   []Entry           // JoinSeq order
    Fees      ledger.Money      // set at Close
    Rake      ledger.Money      // set at Close
    Pool      ledger.Money      // set at Close
    Standings []Standing        // set at Close
    Payouts   []Payout          // set at Settle
    CreatedAt paxos.Slot
    ClosedAt  paxos.Slot
    SettledAt paxos.Slot
    Ballot    paxos.Ballot      // ballot of the Settle slot; audit
}
```

Idempotency inside `Apply`:

```
r, seen = results[cmd.Key]
if seen:
    if r.fingerprint != Fingerprint(cmd): return Result{Code: key_reused}   // not recorded
    r.Result.Replayed = true; return r.Result
res = validate and apply cmd
res.Slot = slot
results[cmd.Key] = {fingerprint, res}                                        // success and rejection alike
return res
```

`results` is never pruned. Two nodes therefore hold the same table for the
same log prefix, and any leader can replay any past result.

### 5.5 The ledger record

One posting per money movement; the `Posting` struct of section 4.4 is the
record. The postings a tournament produces, in the order produced:

| When | Kind | Debit | Credit | Amount | Key |
|---|---|---|---|---|---|
| Join | EntryFee | player:<pid> | pool:<tid> | EntryFee | fee:<tid>:<pid> |
| Settle (Closed) | Rake | pool:<tid> | rake:<tid> | rake | rake:<tid> |
| Settle (Closed) | Prize | pool:<tid> | player:<pid> | share | prize:<tid>:<pid>:<place> |
| Settle (Closed) | Withheld | pool:<tid> | withheld:<tid> | share | withheld:<tid>:<pid>:<place> |
| Settle (Voided) | Refund | pool:<tid> | player:<pid> | EntryFee | refund:<tid>:<pid> |

Player accounts are counterparty accounts: a negative balance means the
player has paid more in fees than received in prizes. Funding, withdrawal and
balance floors are outside the service (section 9). Every posting carries the
slot and the ballot of the command that produced it, which is how a reviewer
answers "under which leader".

---

## 6. Invariants and how each is tested

Three layers of tests. Unit tests are table-driven per package. The
deterministic simulation (`internal/sim`) runs whole clusters in one goroutine
under a seeded fault schedule and runs `Checker.AfterEvent` after every event
and `Checker.AtEnd` at the end; every failure prints the seed, the step count,
the scenario and the replay command. Fuzz targets cover the two codecs. An
end-to-end test runs three `replica.Runner`s over `transport.HTTP` on
`httptest` servers.

The checker has a global view: it reads every node's `replog.Node` through the
observation methods of section 4.2 and every node's `tournament.State`
through `Hash`, `Tournament` and `Ledger`. It keeps its own record of every
chosen (slot, value) it has seen, every state hash per (node, applied slot),
every applied idempotency key with its slot, and a snapshot of every
tournament at the moment its Settle was applied.

### 6.1 Safety invariants of the log

| # | Invariant | Checked where | Tests |
|---|---|---|---|
| S1 | Single chosen value per slot: for every slot, every node's `Chosen(s)` and every Learn observed carry the same value. | `Checker.AfterEvent`: compare each node's `Chosen(s)` with the checker's record; record on first sight. | `sim.TestScenarios/*`, `sim.TestRandom`, `sim.TestSeedCorpus`; unit: `paxos.TestSingleDecreeAgreement` (random interleavings of one instance, 1000 seeds). |
| S2 | Identical applied sequences: every node's applied prefix equals the chosen sequence up to its applied slot, and `State.Hash()` is equal for equal applied slots on every node, including a node rebuilt by replay. | `Checker.AfterEvent`: after each `ApplyCommitted`, compare `Hash()` against the first hash recorded for that slot. `Checker.AtEnd`: replay the chosen log into a fresh `tournament.State` and compare. | `sim.*`; `tournament.TestReplayDeterministic`; `replica.TestReplayFromStore`. |
| S3 | Acceptor monotonicity: `Promised()` never decreases; an acceptor accepts only ballots `>= Promised()`; after promising b it never accepts below b. | `Checker.AfterEvent` on every `Step` that carried Prepare or Accept: compare `Promised()` with the previous value; check `Accepted(s).Ballot >= Promised()` was true at accept time (the checker observes the pre-step promise). | `paxos.TestAcceptorRules` (table over all orderings of two ballots); `replog.TestAcceptorMonotonic`. |
| S4 | Ballot uniqueness: no two nodes ever use the same ballot; at most one value proposed per (ballot, slot). | `Checker`: registry ballot -> node from `Ballot()` when `Role() != Follower`; registry (ballot, slot) -> value from Accept messages seen in transit. | `replog.TestBallotIsUniquePerNode`; `sim.*`. |
| S5 | Durability across crash: after restart from its store, a node's `Promised()` is at least its pre-crash value, every pre-crash accepted pvalue is present, and S1..S4 continue to hold. A torn write leaves the previous durable record, never a partial one. | The simulator keeps each node's `MemStore` across a crash; `Checker` compares post-restart state with the pre-crash snapshot. `TornWriteP` makes a `Save*` call fail after the crash decision so the volatile update is lost and the store keeps the old value. | `replog.TestRestartRestoresDurableState`; `wal.TestTornTail`; `wal.TestNodeRestartsFromFile`; `sim.TestScenarios/crash_restart_storm`; `cmd/arena.TestAttackRestartedReplicaForgetsAcknowledgedCreate`. |
| S6 | Gap rule: a slot chosen after a higher slot was chosen contains a no-op. | `Checker`: record the maximum chosen slot over time; when a lower slot becomes chosen later, assert `Entry.NoOp()`. | `replog.TestTakeoverFillsGapsWithNoOps`; `sim.*`. |
| S7 | Reads are not stale: a consistent read returns an `Index` at least the highest slot chosen anywhere before `ReadIndex` was called, and the state served was applied at least through that index. | `Checker` records the global maximum chosen slot at each `ReadIndex` call and compares with the `ReadReady.Index` later delivered for that seq. | `replog.TestReadIndexNeedsMajorityAtOwnBallot`; `replog.TestReadIndexRefusedBeforeLeadershipNoOp`; `sim.TestScenarios/partition_and_heal` (reads issued on both sides). |
| S8 | At-most-once per idempotency key: for every key, exactly one non-replayed `Result` is produced, on every node, regardless of how many slots carry a command with that key. | `Checker` counts `Applied` records with `Result.Replayed == false` per (node, key). | `tournament.TestApplyReplaysRecordedResult`; `tournament.TestKeyReusedWithDifferentPayload`; `sim.TestScenarios/client_retry_storm`. |

### 6.2 Domain invariants

| # | Invariant | Checked where | Tests |
|---|---|---|---|
| D1 | Prize pool equals entry fees minus rake: for every tournament with Status Closed, Voided or Settled, `Fees == EntryFee * len(Entries)`, `Pool == Fees - Rake`, and `Rake == Fees * RakeBps / 10000` (Closed path) or `Rake == 0` (Voided path); for a Settled tournament, `Balance(pool:<tid>) == 0` and `Balance(rake:<tid>) == Rake`. | `Checker.AfterEvent` after every apply, over every tournament on every node; `ledger.Book.Check()` on every node. | `tournament.TestComputePool` (table incl. rake 0, rake 9999, one entrant); `tournament.TestSettlePostings`; `ledger.TestBalancesSumZero`. |
| D2 | Every payout appears exactly once: for a Settled tournament, `Payouts` has exactly one element per (player, place) for places 1..len(PrizeBps) (Closed path) or one Refund per entry (Voided path); the book has exactly one posting per `Payout.Key`; `sum(Payout.Amount) == Pool` (Closed) or `== Fees` (Voided); no `prize:`/`withheld:`/`refund:` posting exists for a tournament that is not Settled. | `Checker.AfterEvent` recomputes from the tournament record and cross-checks `Ledger().ForTournament`. | `tournament.TestPayoutRoundingSumsToPool` (property test over random pools and prize tables, 10 000 cases); `tournament.TestSplitTieBreak`; `tournament.TestVoidRefunds`; `ledger.TestPostDuplicateKeyIsNoop`. |
| D3 | A settled tournament never changes: the record returned by `Tournament(id)` and the postings returned by `ForTournament(id)` after Settle are byte-identical at every later step, on every node. | `Checker` snapshots the canonical encoding at the apply that set Status Settled; `AfterEvent` compares on every later event. | `tournament.TestSettledTournamentRejectsAllCommands` (Join, SubmitScore, Close, Settle with new keys are rejected with no postings); `sim.*`. |
| D4 | Standings are a function of the accepted scores and the tie-break rule: `ComputeStandings` on the Closed record reproduces the stored `Standings`. | `Checker.AfterEvent` on every Closed/Settled tournament. | `tournament.TestStandingsTable` (ties, unscored entrants, single entrant, zero scores). |
| D5 | Eligibility recorded: no `Entry` has a jurisdiction in the creation-time list or an age below `MinAge`; every `Entry.ExclusionVersion` equals `Rules.Exclusions.Version`; every Withheld payout's player is in the Settle-time list and every Prize payout's player is not; every Payout carries the Settle-time version. | `Checker.AfterEvent`. | `tournament.TestJoinEligibility`; `tournament.TestSettleWithholdsNewlyExcluded`. |
| D6 | Money never appears or disappears inside the book: every amount positive, keys unique, every cached balance equal to the balance recomputed from the postings, and the balances sum to zero. It cannot tell a posting that should exist but does not; D1 and D2 cover that, and the state machine rejects a command whose posting key exists (`ledger_conflict`). | `Checker.AfterEvent`: `Ledger().CheckBalances()` after every apply, `Ledger().Check()` every 64 applies; `Checker.AtEnd`: `Check()` on every node. | `ledger.TestCheck`; `tournament.TestLedgerConflictIsARejection`. |

### 6.3 Liveness, under a stated fairness assumption

Liveness is not an invariant; FLP rules out a guarantee under unbounded
faults. The simulation states its assumption in the test name:
`sim.TestLivenessAfterHeal` runs the fault schedule for `Steps` events, calls
`Heal()` (no drops, no duplicates, delays within bounds, partitions removed,
crashed nodes restarted, faults stopped), and requires that within
`LivenessSteps` every client workflow reaches Settled and every node's applied
slot reaches the commit index. `sim.TestLivenessWithFrozenMinority` heals only
a majority core and freezes every fault outside it, then requires the same
progress from the core. Both count voting members, so a misconfigured
member is not masked: `TestLivenessAfterHeal` fails if fewer than `Nodes`
replicas participated in the final round, `TestLivenessWithFrozenMinority`
if fewer than the members of the healed core did.

### 6.4 Testing the tester

`replog.Config.Unsafe *UnsafeKnobs` is nil in production and settable only
by tests in `internal/sim`:

```go
type UnsafeKnobs struct {
    AcceptBelowPromise bool // acceptor ignores MayAccept
    IgnorePhase1Reports bool // leader proposes its own value instead of paxos.Choose
    SkipLeadershipNoOp bool  // leader serves reads before its no-op is chosen
    ForgetAcceptedOnRestart bool // store drops accepted pvalues on Load
}
```

`sim.TestCheckerDetectsKnownBugs` turns on each knob in turn and asserts that
the checker reports a violation of the expected invariant (S3, S1, S7, S5
respectively) within 200 seeds of `TestRandom` parameters. A knob that the
checker fails to catch is a checker bug and blocks the milestone.

### 6.5 Layer tests outside the simulation

- `internal/api`: table-driven handler tests with `httptest.NewRecorder` and
  a fake `Backend`: missing key 400, malformed body 400, unknown field 400,
  oversized body 413, in-flight key 409, key reused 422, state-machine
  rejection 409 with the right `code`, replayed success 200 with
  `"replayed": true`, not-leader forwarded once, forwarded request not
  re-forwarded, unknown leader 503, wait timeout 504, stale read header
  present.
- `internal/transport`: `Network` statistics match the configured
  probabilities within tolerance over 100 000 sends at a fixed seed; blocked
  pairs deliver nothing; `Purge` removes in-flight messages; `HTTP` delivers
  across two `httptest` servers, drops rather than blocks when a peer is
  down, coalesces queued retransmissions of large messages, and admits an
  `Accept` carrying `replog.MaxValueBytes` (real time, not synctest).
- `internal/replica`: `Runner` under `testing/synctest`: a submit resolves
  when the key is applied from a different slot than proposed; a submit fails
  with `ErrLeadershipLost` on step-down; `Read(consistent)` waits for the
  applied index; no goroutine outlives `Run`.
- `cmd/arena`: `TestThreeNodesOverHTTP` starts three runners on `httptest`
  servers with the HTTP transport, runs one full tournament through the API
  against a follower (forwarding), stops the leader's server, and completes
  the tournament through the remaining two; asserts D1-D3 through the API.
- Fuzz: `replog.FuzzDecode` and `tournament.FuzzDecode` (no panic, and
  `Decode(Encode(x)) == x` on the seed corpus); `sim.FuzzSeed` treats the
  input as a run seed for a short random run.

- Adversarial tests (`adversarial_test.go` and `*_review_test.go` in
  several packages) attack one guarantee each and sweep fault schedules the
  scenarios do not use; the ones that found defects run by default.

CI runs `go test -race -count=1 -shuffle=on -timeout 20m ./...` with the
seed sweep limited to 50 seeds per scenario; `make sim-long` runs 1000 seeds
per scenario locally.

---

## 7. Failure scenarios the chaos simulation must cover

Each scenario is a named `sim.Scenario` with a scripted fault schedule on top
of the random background faults; `sim.TestScenarios` runs each with 50 seeds
in CI. The random mode (`sim.TestRandom`) draws all of them from the seeded
generator with the probabilities in `Params`.

| Name | Setup and fault schedule | What must hold |
|---|---|---|
| `leader_crash_mid_settlement` | 5 nodes, one 100-entrant tournament through Close. Then Settle is submitted and the leader is crashed at each of these points in separate runs: after `Submit` returned, after Accept reached k of 4 peers for k in 0..4, after a quorum of Accepted arrived but before Learn was sent, after Learn reached j of 4 peers, after `ApplyCommitted` but before the host observed the result. The client retries Settle with the same key against the new leader. | S1, S2, D1-D3; exactly one non-replayed Settle result (S8); every node ends with the same Settled record; the crashed node, restarted, converges to the same hash. |
| `dueling_leaders` | 3 or 5 nodes. Node A leads. Block A -> B and A -> C heartbeats (asymmetric) so B's election fires while A still believes it leads and keeps accepting client commands; then unblock. Repeat with the lease disabled (`LeaseDuration = 0`) to exercise the pure protocol, and enabled to exercise the lease. | S1, S3, S4: no slot has two values; A's proposals after B's ballot was promised by a majority are never chosen; every client command submitted to A during the duel is eventually applied exactly once via retry (S8) or rejected as not-leader; with the lease enabled, the number of leader changes is bounded by the number of partitions. |
| `partition_and_heal` | 5 nodes. Split {leader, one follower} from {three followers} during a tournament with joins and score submissions in flight; clients on both sides keep submitting and reading; heal after a random interval; also a non-transitive split (A sees B and C, B does not see C). | S1, S2, S7: nothing proposed by the minority-side old leader after the split is chosen unless re-proposed by the majority leader; consistent reads on the minority side fail or wait rather than answer stale; after heal, the minority nodes converge to the majority's hash; D1-D3. |
| `duplicated_and_reordered_messages` | `DupP = 0.3`, `MaxDelay = 20 * HeartbeatInterval`, `DropP = 0.1`, no crashes. Old Promises, Accepts, Accepted, Learns and HeartbeatAcks arrive after newer ballots exist. | S1, S3, S4, S6; a stale Accepted for an abandoned ballot never counts toward a quorum of the current ballot; a duplicate Learn is idempotent; S7. |
| `client_retry_storm` | `RetryP = 0.8`: a client re-sends a completed command with probability 0.8, then up to 10 times at random intervals with the same key, including after receiving a success, and sometimes with a mutated payload under the same key. Combined with `DropP = 0.2`. | S8: one non-replayed result per key; mutated payloads answered `key_reused` with no effect; D2: no duplicate fee, prize or refund postings; the log may contain several entries per key and the state machine applies one. |
| `crash_restart_storm` | `CrashP` high, `TornWriteP = 0.2`, restart after a random delay; a node may crash while it is Candidate, Leader with proposals in flight, or in the middle of `ApplyCommitted`. | S5 on every restart; S1-S4 throughout; replay from the store reproduces the hash (S2). |
| `clock_skew` | Per-node offsets up to `ClockSkewMax = 10 * ElectionTimeoutMax` added to `now` before `Tick`, with lease enabled. | Safety (S1-S8) holds regardless; the test also records leader changes to show that skew only costs liveness. |
| `late_learner` | One follower loses every Learn for 500 slots (directional block of Learn only), then reconnects. | It catches up through LearnRequest in batches of `LearnBatch`; S2 at the end; `CommitIndex` monotone. |
| `exclusion_change_at_settle` | Tournament created with exclusions version 7; one entrant's jurisdiction is added to version 8, which Settle carries. | D5: that payout is Withheld with version 8; D2: totals unchanged; an entrant in the version 7 list was rejected at Join. |

Every scenario runs first in safety mode with faults, then calls `Heal()` and
runs in liveness mode (section 6.3). Safety violations fail immediately;
liveness failures fail at the bound.

---

## 8. Milestones

Each milestone ends with `make check` green and a commit. No milestone starts
before the previous one's acceptance holds.

### M1. Core consensus

Deliverables: `internal/paxos`, `internal/replog` (Node, MemStore, codec; no
`wal` yet), `internal/transport.Network`, `internal/sim` with a byte-value
state machine standing in for the tournament (values are opaque, S1-S7
checked), `cmd/chaos`, `testdata/seeds.txt` (empty file, wired in).

Done when:
- `paxos` and `replog` unit tests in section 6 pass, including
  `TestTakeoverFillsGapsWithNoOps`, `TestLeaseRefusesPrepare`,
  `TestReadIndexNeedsMajorityAtOwnBallot`, `TestRestartRestoresDurableState`.
- `sim.TestScenarios` passes `dueling_leaders`, `partition_and_heal`,
  `duplicated_and_reordered_messages`, `crash_restart_storm`, `clock_skew`,
  `late_learner` with 50 seeds each; `sim.TestRandom` passes 200 seeds;
  `sim.TestLivenessAfterHeal` passes.
- `sim.TestCheckerDetectsKnownBugs` catches `AcceptBelowPromise`,
  `IgnorePhase1Reports`, `SkipLeadershipNoOp`, `ForgetAcceptedOnRestart`.
- `go run ./cmd/chaos -seed 1 -nodes 5 -steps 200000` completes and prints a
  report.

### M2. Domain and API

Deliverables: `internal/ledger`, `internal/tournament`, `internal/replica`
(Core and Runner), `internal/api`, `internal/transport.HTTP`, `cmd/arena`;
the simulator switched to `replica.Core` with tournament clients and D1-D6
plus S8 in the checker; the three remaining scenarios
(`leader_crash_mid_settlement`, `client_retry_storm`,
`exclusion_change_at_settle`).

Done when:
- `tournament` and `ledger` unit tests of section 6.2 pass, including the
  10 000-case rounding property test.
- All nine scenarios pass with 50 seeds; `sim.TestRandom` passes 200 seeds
  with domain clients.
- `api` handler table passes; `replica` synctest tests pass;
  `cmd/arena.TestThreeNodesOverHTTP` passes under `-race`.
- A manual run of three `arena` processes completes a tournament from the
  README commands, survives `kill` of the leader between Close and Settle,
  and `GET /v1/tournaments/{id}` returns the same record from every node.

### M3. Documentation and CI

Deliverables: `README.md` following the outline in
`docs/research/go-practice.md` section 10 with real command output;
`docs/adr/` with one record per decision that changed during M1-M2 (at
least: read index over lease reads, ledger inside the state machine,
in-memory idempotency map as an optimization only); `.github/workflows/go.yml`
(gofmt, vet, build, tidy -diff, test -race, matrix oldstable/stable,
`GOTOOLCHAIN=local`, jobs skipped while the repository is private);
`Makefile` with `check` and `sim-long`; `LICENSE`; `internal/replog/wal` with
`TestTornTail` (added after review, required by `arena -peers`).

Done when:
- CI is green on the first push of `main`.
- Every command in the README was executed and its output pasted.
- `docs/DESIGN.md` is updated wherever the implementation deviated, with the
  deviation recorded in an ADR.
- The README's "What is not done" section lists every item of section 9.

---

## 9. Non-goals

Stated so that the boundary of the system is explicit.

- Persistence beyond a demonstration. The simulator's store is in memory.
  `wal.File` is an append-only file with an fsync and a CRC-32C per record
  and a truncated torn tail on open; it is never compacted, and there is no
  detection of a rolled-back or deleted file. A replica whose store is lost
  must be given a new `NodeID`; rejoining under the old identity can break
  promises.
- Membership changes. `Peers` is fixed at start. Adding, removing or
  replacing a replica is a restart of the whole cluster with a new
  configuration and an empty log.
- Snapshots and log compaction. The log and the results table grow without
  bound for the life of the process. Restart replays from slot 1.
- A real payment provider or external wallet. The ledger is the wallet. An
  external wallet would need two more commands (PayoutIssued,
  PayoutConfirmed), an outbox driven by the leader with the ballot as a
  fencing token, and reconciliation against the provider; the domain notes
  describe the pattern, and this project does not build it.
- Authentication and authorization. A request's player identifier and
  eligibility attributes (jurisdiction, age) are claims taken as given. There
  is no session, no signature, no TLS, no operator role separation; anyone
  who can reach the API can create, close and settle tournaments. Milestone
  4 adds device-bound sessions and signed tokens for game clients on the
  separate play listener (`docs/UNITY-INTEGRATION.md` section 2); the
  operator API described here stays unauthenticated.
- Lease-based reads. The lease affects only who may run Phase 1. Consistent
  reads always pay a heartbeat round.
- The game itself. No rules engine, no deal generation beyond a 64-bit seed,
  no replay validation of input logs, no anomaly detection. `InputDigest` is
  stored, not checked. Milestone 4 adds, for play tournaments only, the
  Ladder rules engine, deals from committed seeds and scores computed by the
  state machine (`docs/UNITY-INTEGRATION.md` sections 4 to 6).
- Display leaderboards, matchmaking, ratings, pairing of scores, tax
  reporting, deposits and withdrawals, multiple fund types.
- Byzantine faults. Replicas crash, restart, get partitioned and see
  delayed, dropped, duplicated and reordered messages; they do not lie or
  corrupt messages. The codec rejects malformed input but does not
  authenticate it.
- A linearizability checker over client histories. S7 and S8 are checked
  directly; a full history check is not built. Histories are not exported.
- Performance. No benchmarks beyond the codec and the event heap; no load
  testing; no tuning of `Window` or timeouts beyond what makes the
  simulation and the demo work.
