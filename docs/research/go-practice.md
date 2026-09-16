# Go engineering practice for a small distributed system, standard library only

Research notes for `github.com/oguzhanozfe/paxos-arena`: a Paxos-based
tournament settlement service for a skill-based competitive mobile card game
with paid tournaments. The module uses the Go standard library only.

Written 2026-09-16. Facts that depend on the local toolchain were checked with
Go 1.27.1 on darwin/arm64. Every other claim links to its source. Where a
source and an observation disagree, both are stated.

Contents

1. Go release line and the `go` directive
2. Repository layout
3. Concurrency
4. A deterministic in-memory transport for simulation tests
5. Tests: table-driven, property-style, `-race`, `-count`
6. `net/http` JSON API without a framework
7. Logging with `log/slog`
8. GitHub Actions workflow
9. `gofmt` and `go vet` as gates
10. README structure
11. Decisions taken for this repository
12. Sources

---

## 1. Go release line and the `go` directive

### Facts

- Go 1.27.0 was released 2026-08-19; the current patch release is 1.27.1.
  Go 1.26.0 was released 2026-02-10; its current patch release is 1.26.8.
  ([release history](https://go.dev/doc/devel/release))
- Support policy: "Each major Go release is supported until there are two
  newer major releases." Today that means 1.27 and 1.26 receive fixes;
  1.25 does not. ([release policy](https://go.dev/doc/devel/release#policy))
- The language specification in force is "Language version go1.27
  (May 26, 2026)". ([spec](https://go.dev/ref/spec))
- Since Go 1.21 the `go` line in `go.mod` is mandatory, not advisory: "The
  go line declares the minimum required Go version for using the module or
  workspace" and it "sets the language version the compiler enforces when
  compiling packages in that module." ([toolchain doc](https://go.dev/doc/toolchain))
- "Go toolchains refuse to use modules declaring newer Go versions." A
  missing `go` line is treated as `go 1.16`. ([go.mod reference](https://go.dev/ref/mod#go-mod-file-go))
- `1.N` without a patch number is a "language version" that "denotes the
  overall family of Go releases implementing that version"; a `go 1.21.0`
  line with no `toolchain` line is read as `toolchain go1.21.0`.
  ([toolchain doc](https://go.dev/doc/toolchain))
- With the default `GOTOOLCHAIN=auto`, when a module requires a newer Go
  than the installed one, "the go command chooses and switches to an
  appropriate newer toolchain to continue executing the current command",
  picking the minimum version that satisfies the requirement. With
  `GOTOOLCHAIN=local`, "the go command always runs the bundled Go
  toolchain." ([toolchain doc](https://go.dev/doc/toolchain))
- A `toolchain` line is only needed when the preferred toolchain differs
  from the `go` line; commands that raise the `go` line also record a
  `toolchain` line. ([toolchain doc](https://go.dev/doc/toolchain))
- The `go` line gates language semantics per module. Example: the Go 1.22
  per-iteration loop variable change applies to code in modules whose `go`
  line is 1.22 or newer. ([Go 1.22 notes](https://go.dev/doc/go1.22#language))
- Since Go 1.27 `go test` also runs the `stdversion` vet check by default,
  which "reports use of standard library symbols too new for the configured
  Go version". ([Go 1.27 notes](https://go.dev/doc/go1.27))

### What `go mod init` writes, documented versus observed

- The Go 1.26 release notes say `go mod init` "now defaults to a lower go
  version in new go.mod files": a `1.N.X` toolchain writes `go 1.(N-1).0`,
  "intended to encourage the creation of modules that are compatible with
  currently supported versions of Go".
  ([Go 1.26 notes](https://go.dev/doc/go1.26#go-command))
- That change was reverted. Proposal
  [golang/go#77653](https://github.com/golang/go/issues/77653) ("change
  `go mod init` default go directive back to 1.N", opened 2026-02-17) is
  marked Proposal-Accepted for the Go 1.27 milestone, on the grounds that
  "language features, and compatibility settings are gated on the go
  directive" and users saw "editor / vet warnings for using standard library
  packages and functions of the toolchain you have installed". A 1.26
  backport is tracked in
  [golang/go#77860](https://github.com/golang/go/issues/77860).
- Observed on this machine: `go mod init example.com/probe` with Go 1.27.1
  wrote `go 1.27.1`.

### Recommendation for this repository

Write the `go` line by hand rather than accepting the `go mod init` output:

```
module github.com/oguzhanozfe/paxos-arena

go 1.26.0
```

Reasoning:

- `go 1.26.0` is the oldest supported release line, so the module builds on
  every supported toolchain and a CI matrix over `oldstable` and `stable`
  tests two different compilers instead of one. The Go team's own argument
  for a lower default was that newly published modules then do not force
  dependents to raise their requirement.
  ([golang/go#77653](https://github.com/golang/go/issues/77653))
- `go 1.27.1` (what `go mod init` produced) would require the newest patch
  release for no benefit; a patch-level minimum should only be set when a
  specific fix is needed.
- Everything this project needs is in 1.25 or earlier: `testing/synctest`
  (stable since 1.25), `sync.WaitGroup.Go` (1.25), `ServeMux` method and
  wildcard patterns (1.22), `math/rand/v2` (1.22), `log/slog` (1.21),
  `T.Context` (1.24). ([Go 1.25 notes](https://go.dev/doc/go1.25),
  [Go 1.24 notes](https://go.dev/doc/go1.24), [Go 1.22 notes](https://go.dev/doc/go1.22))
- Raise to `go 1.27.0` only if a 1.27-only API is adopted: `encoding/json/v2`
  became a regular package in 1.27, `net/http/httptest.NewTestServer` (an
  in-memory server usable inside a `synctest` bubble) is new in 1.27, and
  the `goroutineleak` profile became generally available in 1.27.
  ([Go 1.27 notes](https://go.dev/doc/go1.27)) If that happens, the CI
  matrix collapses to `stable` and the `oldstable` job must be removed.
- Do not add a `toolchain` line. Set `GOTOOLCHAIN=local` in CI so a job
  running an older toolchain fails loudly instead of silently downloading a
  newer one. ([toolchain doc](https://go.dev/doc/toolchain))
- To change the line later, use `go mod edit -go=1.27.0` ("The -go=version
  flag sets the expected Go language version").
  ([go mod edit](https://go.dev/ref/mod#go-mod-edit))

---

## 2. Repository layout

### Sources

- The official "Organizing a Go module" page gives the layout for several
  commands: `go.mod` at the root, shared packages under `internal/`, and one
  directory per program under `cmd/`. It recommends "placing such packages
  into a directory named internal; this prevents other modules from
  depending on packages we don't necessarily want to expose and support for
  external uses" and "to keep packages in internal as much as possible". It
  does not mention a `pkg/` directory.
  ([go.dev/doc/modules/layout](https://go.dev/doc/modules/layout))
- The compiler enforces the rule: "Packages in directories named internal
  are importable only by code in the directory tree rooted at the parent of
  internal." ([cmd/go, Internal Directories](https://pkg.go.dev/cmd/go#hdr-Internal_packages))
- "The go tool will ignore a directory named 'testdata', making it available
  to hold ancillary data needed by the tests."
  ([cmd/go, Test packages](https://pkg.go.dev/cmd/go#hdr-Test_packages))
- Package names: "short and clear. They are lower case, with no under_scores
  or mixedCaps"; avoid `util`, `common`, `misc`; do not repeat the package
  name in exported identifiers (`http.Server`, not `http.HTTPServer`).
  ([Package names](https://go.dev/blog/package-names))
- "All top-level, exported names should have doc comments, as should
  non-trivial unexported type or function declarations."
  ([Code Review Comments](https://go.dev/wiki/CodeReviewComments))
  A package comment starts with `Package name ...`; `gofmt` since Go 1.19
  reformats doc comments to canonical style.
  ([Go Doc Comments](https://go.dev/doc/comment))

### Proposed tree

```
paxos-arena/
  go.mod                      module github.com/oguzhanozfe/paxos-arena, go 1.26.0
  README.md
  LICENSE
  Makefile                    thin wrapper around the go commands used in CI
  .github/workflows/ci.yml
  cmd/
    arena-node/main.go        one replica: HTTP API + Paxos participant
    arena-sim/main.go         deterministic simulator CLI (seed, faults, steps)
  internal/
    paxos/                    protocol state machine; no I/O, no time.Now, no goroutines
    transport/                Transport interface + memory implementation with faults
    sim/                      scheduler: event queue, virtual clock, fault model, invariants
    tournament/               domain: entries, results, settlement records
    api/                      net/http handlers, request/response types, idempotency store
    clock/                    Clock interface, real and virtual implementations
    logx/                     slog setup: handler choice, level flag, common attrs
  docs/
    design.md
    research/
    adr/                      one short file per decision
  testdata/                   golden traces, seed lists
```

Rules that follow from the sources:

- Nothing outside `cmd/` is importable by other modules, because every
  library package sits under `internal/`. The module is a program, not a
  library, so there is no public API surface to maintain.
- Each `cmd/*/main.go` stays small: parse flags, build dependencies, call a
  `run(ctx, args, stdout, stderr) error` function that tests can call. This
  is the "synchronous functions" advice applied to `main`.
  ([Code Review Comments](https://go.dev/wiki/CodeReviewComments))
- `internal/paxos` must not import `net`, `time` (except `time.Duration`
  types), `math/rand/v2`, or `log/slog`. The simulator relies on that; see
  section 4. Enforce it with a test that inspects `go list -deps` output,
  or simply by review.
- Tests live next to the code (`foo_test.go`). Black-box tests use package
  `paxos_test` so they only see the exported surface; white-box tests use
  package `paxos`. ([testing package](https://pkg.go.dev/testing))

---

## 3. Concurrency

### Principles with sources

- "Do not communicate by sharing memory; instead, share memory by
  communicating." ([Effective Go](https://go.dev/doc/effective_go#sharing))
- The `sync` package doc: "Higher-level synchronization is better done via
  channels and communication." Values of `sync` types must not be copied
  after first use. ([sync](https://pkg.go.dev/sync))
- Channels are for "passing ownership of data, distributing units of work,
  communicating async results"; a mutex is for "caches, state". "Use
  whichever is most expressive and/or most simple."
  ([Go wiki: MutexOrChannel](https://go.dev/wiki/MutexOrChannel))
- "When you spawn goroutines, make it clear when - or whether - they exit.
  Goroutines can leak by blocking on channel sends or receives."
  "Prefer synchronous functions ... over asynchronous ones."
  ([Code Review Comments](https://go.dev/wiki/CodeReviewComments))
- "Never start a goroutine without knowing how it will stop": "Every time
  you use the go keyword in your program to launch a goroutine, you must
  know how, and when, that goroutine will exit."
  ([Cheney, 2016](https://dave.cheney.net/2016/12/22/never-start-a-goroutine-without-knowing-how-it-will-stop))
- "Goroutines are not garbage collected; they must exit on their own."
  Pipeline stages "close their outbound channels when all the send
  operations are done" and cancellation is a broadcast via a closed channel.
  ([Go Concurrency Patterns: Pipelines](https://go.dev/blog/pipelines))
- The memory model's advice: "Programs that modify data being
  simultaneously accessed by multiple goroutines must serialize such
  access." "If you must read the rest of this document to understand the
  behavior of your program, you are being too clever. Don't be clever."
  ([Go memory model](https://go.dev/ref/mem))

### Context

- "A Context carries a deadline, cancellation signal, and request-scoped
  values across API boundaries." Pass it as the first parameter named
  `ctx`. ([Go Concurrency Patterns: Context](https://go.dev/blog/context))
- "Do not store Contexts inside a struct type; instead, pass a Context
  explicitly to each function that needs it." Use `Value` only for
  request-scoped data, never for optional parameters. "Failing to call the
  CancelFunc leaks the child and its children until the parent is canceled";
  `go vet` checks this (`lostcancel`). ([context](https://pkg.go.dev/context),
  [cmd/vet](https://pkg.go.dev/cmd/vet))
- The one accepted exception for a context in a struct is backwards
  compatibility of an existing API, and even then duplicating functions is
  preferred. ([Contexts and structs](https://go.dev/blog/context-and-structs))
- Useful newer functions: `context.WithCancelCause` and `context.Cause`
  (record why a cancel happened), `context.AfterFunc` (run cleanup on
  cancel), `context.WithoutCancel` (detach background work from a request).
  ([context](https://pkg.go.dev/context))
- `signal.NotifyContext(parent, os.Interrupt, syscall.SIGTERM)` returns a
  context that is done when a signal arrives; "code should call stop as soon
  as the operations running in this Context complete". Since Go 1.26,
  `context.Cause` on it "will return an error describing the signal".
  ([os/signal](https://pkg.go.dev/os/signal#NotifyContext))

### Goroutine bookkeeping

- `sync.WaitGroup.Go(f)` (Go 1.25) starts a goroutine and counts it in one
  call, removing the `Add`/`Done` pairing mistakes that the `waitgroup` vet
  analyzer reports. ([Go 1.25 notes](https://go.dev/doc/go1.25),
  [sync](https://pkg.go.dev/sync#WaitGroup.Go))
- `sync.Map` is for two narrow cases; "Most code should use a plain Go map
  instead, with separate locking or coordination." ([sync](https://pkg.go.dev/sync#Map))
- The Go 1.27 runtime exposes a `goroutineleak` profile: "stack traces of
  all leaked goroutines", meaning goroutines blocked on synchronization
  primitives no longer reachable, via `pprof.Lookup("goroutineleak")` or
  `/debug/pprof/goroutineleak`. ([runtime/pprof](https://pkg.go.dev/runtime/pprof),
  [Go 1.27 notes](https://go.dev/doc/go1.27)) It is a diagnostic, not a
  gate, and requires `go 1.27.0`; a test that counts `runtime.NumGoroutine()`
  before and after, with a short retry loop, works on every version.
- `testing/synctest.Test` fails the test "If the goroutines in the bubble
  become deadlocked" and waits for every goroutine in the bubble to exit
  before returning, which turns a leaked goroutine into a test failure.
  ([testing/synctest](https://pkg.go.dev/testing/synctest))

### Nondeterminism the language guarantees

These matter for section 4.

- `select`: "If multiple communications can proceed, a uniform pseudo-random
  selection is chosen to decide which single communication will execute."
  ([spec, Select statements](https://go.dev/ref/spec#Select_statements))
- `range` over a map: "The iteration order over maps is not specified".
  ([spec, For statements with range clause](https://go.dev/ref/spec#For_range))
- Top-level `math/rand/v2` functions are "unconditionally randomly seeded".
  ([Go 1.22 notes](https://go.dev/doc/go1.22))
- Goroutine scheduling order is not specified; the race detector only
  reports races that actually execute.
  ([race detector](https://go.dev/doc/articles/race_detector))

### How this maps onto the project

- `internal/paxos`: a pure state machine. Methods take a message or a tick
  and return outgoing messages and state changes. No goroutines, no
  channels, no locks, no `time.Now`. This is what makes the simulator in
  section 4 possible and what keeps the protocol testable with plain
  table-driven tests.
- `internal/transport` (production path): one goroutine per connection for
  reads, one for writes, both bound to a context; an `inbox` channel into
  the node's single event loop. The event loop is the only goroutine that
  touches protocol state, so no mutex is needed there.
- `internal/api`: handlers run on `net/http`'s goroutines. Anything they
  share (the idempotency store, a snapshot of the last decided value) is a
  small struct guarded by one `sync.Mutex`, following the "caches, state"
  rule. Handlers hand work to the event loop through a channel and wait on
  a reply channel or `ctx.Done()`.
- Timers: use `time.NewTimer` and `Stop` it on every exit path. In Go 1.27
  "channels from time package now always unbuffered" and the
  `asynctimerchan` compatibility setting is gone, so the Go 1.23 timer
  semantics are the only ones. ([Go 1.27 notes](https://go.dev/doc/go1.27))
  In simulation, timers are events in the scheduler, not `time.Timer`
  values.

---

## 4. A deterministic in-memory transport for simulation tests

### Why

- FoundationDB: "Simulation is able to conduct a deterministic simulation of
  an entire FoundationDB cluster within a single-threaded process."
  "Determinism is crucial in that it allows perfect repeatability of a
  simulated run, facilitating controlled experiments to home in on issues."
  Faults include "connection failures, degradation of machine performance,
  machine shutdowns or reboots, machines coming back from the dead".
  ([FoundationDB testing](https://apple.github.io/foundationdb/testing.html))
- TigerBeetle's simulator: "all non-deterministic parts of the system are
  stubbed out. This includes the clock, network, and disk operations." "The
  VOPR uses a random seed to tune parameters for injecting different types
  of faults ... it may drop and reorder packets, partition the network, or
  corrupt reads and writes". "When a simulation causes any type of failure,
  the seed and Git commit hash can be used to replay back the exact
  simulation and bug." "One minute of VOPR time is equivalent to days of
  real-world testing."
  ([tigerbeetle/docs/internals/vopr.md](https://github.com/tigerbeetle/tigerbeetle/blob/main/docs/internals/vopr.md))
- The sled simulation guide describes the same shape for small projects:
  "write your algorithm around a state machine that receives messages from
  other nodes, and responds with the set of outgoing messages", then "stuff
  all messages / events in the system into a priority queue keyed on next
  delivery time" and "iterate over the priority queue, delivering messages
  to the intended state machine". It assumes an asynchronous network where
  "any message can be arbitrarily delayed (maybe forever, dropped) or
  reordered with others". ([sled simulation guide](https://sled.rs/simulation.html))
- Jepsen names the fault injector a nemesis, "a special client, not bound to
  any particular node, which introduces failures across the cluster", e.g.
  `partition-random-halves`. ([Jepsen tutorial](https://github.com/jepsen-io/jepsen/blob/main/doc/tutorial/05-nemesis.md))

### Design for this repository

Interfaces (sketch, in `internal/paxos` and `internal/transport`):

```go
// Node is the protocol state machine. It is not safe for concurrent use;
// the caller serialises all calls.
type Node interface {
    Step(now time.Duration, msg Message) []Envelope   // handle one inbound message
    Tick(now time.Duration) []Envelope                 // timers fire here
    Decided(instance uint64) (Value, bool)             // read-only observation
}

// Envelope is an outbound message with a destination.
type Envelope struct {
    To  NodeID
    Msg Message
}

// Transport delivers envelopes. The memory implementation is the simulator;
// the TCP implementation is production.
type Transport interface {
    Send(ctx context.Context, e Envelope) error
}
```

The simulator in `internal/sim`:

- One goroutine. A priority queue of events `(at time.Duration, seq uint64,
  kind, payload)`. `seq` is a monotonically increasing counter that breaks
  ties so equal timestamps are ordered deterministically.
- A virtual clock: `now` is the `at` of the event being processed. Nothing
  reads `time.Now()`. Node timeouts are scheduled as `Tick` events.
- One `*rand.Rand` built from the run seed: `rand.New(rand.NewPCG(seed, 0))`.
  `*Rand` is documented as single-goroutine ("Both types should be used by a
  single goroutine at a time"), which is fine here because the simulator is
  single-threaded. The blog's guidance is that programs needing seeded
  repeatability construct a local generator rather than using the global
  one. ([math/rand/v2](https://pkg.go.dev/math/rand/v2),
  [randv2 blog](https://go.dev/blog/randv2))
- Fault model, all driven by the seeded generator and by per-run
  parameters:
  - drop: each envelope is discarded with probability `p_drop`;
  - delay: delivery time is `now + U(minLatency, maxLatency)`; because
    delays differ per message, reordering falls out of delay;
  - duplicate: with probability `p_dup`, enqueue a second copy with its own
    delay;
  - partition: a set of blocked `(from, to)` pairs, installed and healed
    by scheduled events; asymmetric partitions are allowed because the set
    is directional;
  - crash and restart: a `crash(node)` event drops the node's volatile
    state and in-flight inbound events; a `restart(node)` event rebuilds it
    from the simulated durable store (acceptor promises and accepted
    values must survive; that is the safety-critical part of Paxos);
  - clock skew: a per-node offset added to `now` before calling `Tick`,
    which only matters for timeouts, never for correctness.
- Every run logs its seed first. On failure the test prints
  `seed=<n> steps=<k>` and the replay command. The corpus of seeds that once
  failed lives in `testdata/seeds.txt` and runs on every `go test`.

Invariants checked after every event (safety) and at the end (liveness):

- Agreement: for every instance, all nodes that have decided hold the same
  value. Checked by comparing `Decided(i)` across nodes.
- Validity: every decided value was proposed by some client.
- Durability across crash: after `restart`, a node never accepts a ballot
  lower than one it promised before the crash.
- Liveness, only under a fairness assumption: once partitions are healed
  and drop probability set to zero for a bounded number of steps, every
  instance with a live proposer decides. Without that assumption a liveness
  check is meaningless (FLP), so the test states the assumption in its
  name.

### Two ways to virtualise time in Go

1. Own scheduler (above). Works on any Go version, gives total control,
   and forces the protocol core to be I/O-free. This is the primary
   mechanism for `internal/paxos` and `internal/sim`.
2. `testing/synctest.Test(t, f)` runs `f` in a "bubble" with a fake clock:
   "Time advances in the bubble when all goroutines are blocked"; the
   initial time is midnight UTC, 2000-01-01. `synctest.Wait()` "blocks until
   every goroutine within the current bubble, other than the current
   goroutine, is durably blocked." Durably blocking operations are channel
   operations on bubble channels, `select` on them, `sync.Cond.Wait`,
   `sync.WaitGroup.Wait`, `time.Sleep`; mutexes, network I/O and syscalls
   are not. The doc says "Avoid using the network. Use a fake network
   implementation as needed"; `net.Pipe` is acceptable. Stable since Go 1.25.
   ([synctest blog](https://go.dev/blog/synctest),
   [testing/synctest](https://pkg.go.dev/testing/synctest),
   [Go 1.25 notes](https://go.dev/doc/go1.25))
   Use this for the goroutine-based layers (`internal/transport` production
   path, `internal/api` timeouts and idempotency TTL expiry) where the code
   under test genuinely uses `time.After`, tickers and channels. In Go 1.27,
   `httptest.NewTestServer` provides an in-memory HTTP server that "is
   suitable for use with the testing/synctest package"; with `go 1.26.0`
   use `net.Pipe` plus `http.Server.Serve` on a fake listener instead.
   ([net/http/httptest](https://pkg.go.dev/net/http/httptest))

### Determinism checklist

Every item is a real source of nondeterminism in Go; each has a source in
section 3.

- Never `range` over a map when the order affects output; sort keys first,
  or keep a slice alongside the map.
- Never use `select` with more than one ready case inside the simulator;
  the scheduler owns ordering.
- Never read `time.Now()` or the global `rand` functions in code that runs
  under the simulator; inject a `Clock` and a `*rand.Rand`.
- Never start goroutines in `internal/paxos`; the simulator is single
  threaded so that the race detector has nothing to find there and every
  run replays exactly.
- Do not key anything on pointer addresses or on `fmt.Sprintf("%p")`.
- Keep the seed, the fault parameters and the commit hash in the failure
  message; without all three the replay is not guaranteed.

---

## 5. Tests: table-driven, property-style, `-race`, `-count`

### Table-driven and subtests

- The idiom: a slice (or map) of cases with a `name`, inputs and expected
  outputs; `t.Run(tc.name, ...)` per case so failures name the case; use
  `t.Errorf` to keep going and `t.Fatalf` to stop the case. Using a map
  gives randomised iteration order and so catches order dependence. For Go
  1.22 and later, the `tc := tc` copy before `t.Parallel()` is no longer
  needed. ([Go wiki: TableDrivenTests](https://go.dev/wiki/TableDrivenTests),
  [Go 1.22 notes](https://go.dev/doc/go1.22#language))
- "Tests should fail with helpful messages saying what was wrong, with what
  inputs, what was actually got, and what was expected."
  ([Code Review Comments](https://go.dev/wiki/CodeReviewComments))
- Helpers: `t.Helper()` hides the helper frame in failure locations;
  `t.Cleanup` runs in LIFO order after the test and its subtests;
  `t.TempDir()` is removed automatically; `t.Context()` (Go 1.24) "is
  canceled just before Cleanup-registered functions are called"; `t.Setenv`
  cannot be used in parallel tests. ([testing](https://pkg.go.dev/testing))
- `t.Parallel()` "signals that this test is to be run in parallel with (and
  only with) other parallel tests"; `-parallel n` caps the concurrency.
  ([testing](https://pkg.go.dev/testing))
- `go test` runs a vet subset before running tests: "atomic, bools,
  buildtag, directive, errorsas, ifaceassert, nilfunc, printf, stdversion,
  stringintconv, and tests". `-vet=off` disables it, `-vet=all` runs all
  analyzers. (`go help test`, [cmd/go](https://pkg.go.dev/cmd/go#hdr-Testing_flags))

### Property-style tests without external libraries

Three options exist in the standard library.

1. Hand-rolled: a seeded `*rand.Rand`, a generator for operation sequences,
   the simulator from section 4, and invariant checks. This is the main
   property test of the project. Report the seed on failure; shrink by
   re-running with a shorter prefix of the same seed's sequence (binary
   search on the step count) and print the shortest failing prefix.
2. `testing/quick`: "implements utility functions to help with black box
   testing". `quick.Check(f, cfg)` calls `f` with random arguments and
   returns a `*CheckError` holding `Count` and the failing inputs `In`;
   `Config` has `MaxCount`, `Rand` and `Values`; types can implement
   `Generator`. The package "is frozen and is not accepting new features"
   and it uses `math/rand` (v1). Acceptable for small pure functions such
   as ballot comparison and message encoding round-trips; do not build the
   simulator on it. ([testing/quick](https://pkg.go.dev/testing/quick))
3. Fuzzing: `func FuzzXxx(f *testing.F)` with `f.Add` seeds and
   `f.Fuzz(func(t *testing.T, ...))`; argument types are limited to
   strings, byte slices, integers, floats and bools; `go test` without
   `-fuzz` runs the seed corpus as ordinary tests; `go test -fuzz=FuzzXxx
   -fuzztime=30s` explores; failing inputs are minimised and written to
   `testdata/fuzz/FuzzXxx/<hash>` and rerun by every later `go test`.
   ([Go Fuzzing](https://go.dev/doc/security/fuzz/)) Good targets here:
   the wire codec (`Decode(Encode(m)) == m`, and `Decode` never panics on
   arbitrary bytes), the HTTP request validators, and a fuzz harness that
   treats the byte input as a seed for the simulator (one `uint64`
   argument), which lets the fuzzer drive fault schedules.

### Flags and how to use them

From `go help testflag` and `go help test`:

- `-race`: "enable data race detection". The detector "requires cgo to be
  enabled, and on non-Darwin systems requires an installed C compiler";
  supported on linux/amd64, linux/arm64, darwin/amd64, darwin/arm64,
  windows/amd64 and others; cost is "5-10x" memory and "2-20x" time; it
  "only finds races that happen at runtime".
  ([race detector](https://go.dev/doc/articles/race_detector))
- `-count n`: "Run each test, benchmark, and fuzz seed n times (default 1)."
  "The idiomatic way to disable test caching explicitly is to use
  -count=1." Use `-count=20` or more on the concurrency tests to shake out
  scheduling-dependent failures; with `-parallel`, repeated instances of the
  same test never run concurrently with each other.
  ([cmd/go](https://pkg.go.dev/cmd/go#hdr-Testing_flags), [testing](https://pkg.go.dev/testing))
- `-shuffle on|N`: "Randomize the execution order of tests and benchmarks";
  "the seed will be reported for reproducibility". Exposes tests that
  depend on package-level state.
- `-failfast`: "Do not start new tests after the first test failure."
  Useful locally; not in CI, where the full failure list is wanted.
- `-timeout d`: default 10 minutes; the binary panics with all goroutine
  stacks when exceeded, which is the fastest way to see a deadlock.
- `-run regexp`: `-run 'TestSim/seed=42'` selects a subtest by name.
- `-fuzz`, `-fuzztime`: see above; only one fuzz target at a time.

Recommended invocations:

```
go test ./...                                   # fast, cached, vet subset
go test -race -count=1 -shuffle=on ./...        # CI
go test -race -count=50 -run 'TestTransport' ./internal/transport/   # flake hunt
go test -run 'TestSim' -seeds=1000 ./internal/sim/                   # custom flag, long
go test -fuzz=FuzzCodec -fuzztime=60s ./internal/paxos/
```

`-seeds` is a package-level `flag.Int` defined in the test file; custom
flags are allowed in test binaries and are parsed by `testing.Init`.
([testing](https://pkg.go.dev/testing))

### Benchmarks

`for b.Loop() { ... }` (Go 1.24) runs setup once per `-count` and prevents
the compiler from optimising the loop body away. ([Go 1.24 notes](https://go.dev/doc/go1.24))
Only worth adding for the codec and the scheduler's heap; do not benchmark
the HTTP layer of a small service.

---

## 6. `net/http` JSON API without a framework

### Routing (Go 1.22 and later)

- Patterns have the form `"[METHOD ][HOST]/[PATH]"`. `"POST /items/create"`
  restricts to POST; `/items/{id}` captures a segment, read with
  `r.PathValue("id")`; `{path...}` matches the rest; a trailing `{$}`
  matches only the exact path; "If two patterns overlap in the requests that
  they match, then the more specific pattern takes precedence"; two
  patterns that conflict with equal specificity panic at registration.
  Registering `GET` also registers `HEAD`. ([Go 1.22 notes](https://go.dev/doc/go1.22),
  [Routing Enhancements](https://go.dev/blog/routing-enhancements),
  [net/http ServeMux](https://pkg.go.dev/net/http#ServeMux))
- Handlers as methods on a struct that holds dependencies, registered on a
  `http.NewServeMux()` (not `DefaultServeMux`, which any imported package
  can add to):

```go
type Server struct {
    log   *slog.Logger
    node  NodeClient
    idem  *idempotencyStore
}

func (s *Server) routes() http.Handler {
    mux := http.NewServeMux()
    mux.HandleFunc("POST /v1/tournaments/{id}/results", s.handleSubmitResult)
    mux.HandleFunc("GET /v1/tournaments/{id}/settlement", s.handleGetSettlement)
    mux.HandleFunc("GET /healthz", s.handleHealth)
    return s.requestID(s.logging(mux))
}
```

- Middleware is a `func(http.Handler) http.Handler`. `net/http` needs no
  additions for this. ([net/http Handler](https://pkg.go.dev/net/http#Handler))

### Request decoding and validation

- Limit the body: `http.MaxBytesReader(w, r.Body, 1<<20)` returns a reader
  that yields a `*http.MaxBytesError` past the limit and tells the server
  to close the connection. ([net/http MaxBytesReader](https://pkg.go.dev/net/http#MaxBytesReader))
- `dec := json.NewDecoder(body); dec.DisallowUnknownFields()` rejects
  unknown members; by default `encoding/json` ignores unknown fields and
  accepts duplicate keys silently. Call `dec.Decode(&req)` and then check
  that a second `dec.Decode(&struct{}{})` returns `io.EOF`, otherwise the
  body contained trailing data. ([encoding/json](https://pkg.go.dev/encoding/json))
- Validate after decoding in a `func (r submitResultRequest) validate()
  error` that returns a field-level error list; the handler maps that to a
  422 or 400 problem response (below).
- `encoding/json/v2` (Go 1.27) has stricter defaults: case-sensitive name
  matching, rejection of duplicate names and invalid UTF-8, and an explicit
  `RejectUnknownMembers` option; `encoding/json` v1 "is implemented in terms
  of the v2 package" since 1.27, with `GOEXPERIMENT=nojsonv2` as the escape
  hatch. ([encoding/json/v2](https://pkg.go.dev/encoding/json/v2),
  [encoding/json](https://pkg.go.dev/encoding/json), [Go 1.27 notes](https://go.dev/doc/go1.27))
  With `go 1.26.0` this project stays on v1 and gets the strictness by hand
  as above. This is a documented trade-off, not an oversight.

### Error responses

- Use RFC 9457 Problem Details, media type `application/problem+json`,
  members `type`, `status`, `title`, `detail`, `instance` plus extension
  members. "When this member is not present, its value is assumed to be
  'about:blank'", in which case "the problem has no additional semantics
  beyond that of the HTTP status code". `title` "SHOULD NOT change from
  occurrence to occurrence"; `detail` "ought to focus on helping the client
  correct the problem". RFC 9457 obsoletes RFC 7807.
  ([RFC 9457](https://www.rfc-editor.org/rfc/rfc9457))
- One helper writes every error:

```go
type problem struct {
    Type     string            `json:"type,omitempty"`
    Title    string            `json:"title"`
    Status   int               `json:"status"`
    Detail   string            `json:"detail,omitempty"`
    Instance string            `json:"instance,omitempty"`
    Errors   map[string]string `json:"errors,omitempty"` // extension: field -> message
}

func writeProblem(w http.ResponseWriter, p problem) {
    w.Header().Set("Content-Type", "application/problem+json")
    w.WriteHeader(p.Status)
    _ = json.NewEncoder(w).Encode(p) // nothing useful to do if the client is gone
}
```

- `http.Error` is fine for the rare non-JSON case; it sets
  `text/plain; charset=utf-8` and `X-Content-Type-Options: nosniff`.
  ([net/http Error](https://pkg.go.dev/net/http#Error))
- Internally, wrap with `fmt.Errorf("submit result: %w", err)` and branch
  with `errors.Is` / `errors.As`; "wrapping an error makes that error part
  of your API", so only wrap sentinel errors the caller is meant to test
  for. ([Working with Errors in Go 1.13](https://go.dev/blog/go1.13-errors))
  Error strings "should not be capitalized ... or end with punctuation".
  ([Code Review Comments](https://go.dev/wiki/CodeReviewComments))

### Idempotency keys

Result submission moves money, so a retried POST must not settle twice.

- The IETF draft (draft-ietf-httpapi-idempotency-key-header-07, dated
  2025-10-15, expired 2026-04-18, informative only) defines the
  `Idempotency-Key` request header for non-idempotent methods such as POST
  and PATCH. Same key, same payload: "The resource SHOULD respond with the
  result of the previously completed operation, success or an error." Same
  key, different payload: "The resource SHOULD reply with a HTTP 422 status
  code." Retried while the original is still running: "The resource SHOULD
  reply with an HTTP 409 status code." Missing key on an operation that
  requires it: 400. Servers "SHOULD define such expiration policy and
  publish it in the documentation"; a payload fingerprint "MAY be used in
  conjunction with an idempotency key".
  ([draft-ietf-httpapi-idempotency-key-header-07](https://www.ietf.org/archive/id/draft-ietf-httpapi-idempotency-key-header-07.txt))
- A widely used payments API documents the same model: it saves "the
  resulting status code and body of the first request made for any given
  idempotency key, regardless of whether it succeeds or fails", returns
  that for later requests with the same key, prunes keys after 24 hours,
  errors when the parameters differ, and recommends V4 UUIDs of at most
  255 characters. ([Idempotent requests](https://docs.stripe.com/api/idempotent_requests))

Implementation in `internal/api`:

```go
type idemEntry struct {
    fingerprint [32]byte      // sha256 of method + path + canonical body
    inflight    bool
    status      int
    body        []byte
    expires     time.Time     // from the injected Clock
}

type idempotencyStore struct {
    mu      sync.Mutex
    entries map[string]*idemEntry
    clock   Clock
    ttl     time.Duration
}
```

Handler flow: require the header (400 if absent); compute the fingerprint;
under the lock, look up the key. No entry: insert `inflight=true` and
proceed. Entry in flight: 409. Entry with a different fingerprint: 422.
Entry complete: replay `status` and `body` unchanged. After the operation,
record status and body under the lock; on panic or context cancellation
delete the in-flight entry so the client can retry. Keys expire after
`ttl` (24 h to match common practice), swept by a ticker goroutine that is
bound to the server context. The store is in memory, so it does not survive
a restart; the settlement itself is idempotent at the protocol level (one
Paxos instance per tournament), so a lost key costs a duplicate protocol
round, not a duplicate payout. State that limit in the README.

### Server lifecycle

- Set `ReadHeaderTimeout`, `ReadTimeout`, `WriteTimeout`, `IdleTimeout`
  and `MaxHeaderBytes` on `http.Server`; the zero values mean no timeout.
  ([net/http Server](https://pkg.go.dev/net/http#Server))
- Graceful stop: `srv.Shutdown(ctx)` "closes all idle connections and waits
  for in-flight requests to complete"; `ListenAndServe` returns
  `http.ErrServerClosed` after `Shutdown`, which is not an error.
  ([net/http Server.Shutdown](https://pkg.go.dev/net/http#Server.Shutdown))
- Wire it to signals with `signal.NotifyContext`, then a bounded
  `context.WithTimeout` for the shutdown itself.
- `http.CrossOriginProtection` (Go 1.25) rejects non-safe cross-origin
  browser requests using `Sec-Fetch-Site`; only needed if a browser front
  end ever calls the API directly. ([net/http CrossOriginProtection](https://pkg.go.dev/net/http#CrossOriginProtection))

### Testing handlers

- `httptest.NewRequest` builds an incoming server request and "panics on
  error for ease of use in testing"; `httptest.NewRecorder()` captures the
  response; `rec.Result()` "must only be called after the handler has
  finished running". `httptest.NewServer` binds a loopback listener when a
  real client is needed. ([net/http/httptest](https://pkg.go.dev/net/http/httptest))
- Table-driven cases per endpoint: valid body, unknown field, oversized
  body, missing idempotency key, replayed key, conflicting key, in-flight
  key. Assert status, `Content-Type`, and the decoded problem `title`.

---

## 7. Logging with `log/slog`

- `log/slog` was added in Go 1.21 to give "a common framework that all the
  other structured logging packages can share"; the API splits into a
  front end (`Logger`) and back end (`Handler`); `TextHandler` emits
  `key=value`, `JSONHandler` emits one JSON object per call.
  ([slog blog](https://go.dev/blog/slog), [log/slog](https://pkg.go.dev/log/slog))
- Setup for this project, in `internal/logx`:

```go
func New(w io.Writer, level *slog.LevelVar, json bool) *slog.Logger {
    opts := &slog.HandlerOptions{Level: level, AddSource: false}
    var h slog.Handler
    if json {
        h = slog.NewJSONHandler(w, opts)
    } else {
        h = slog.NewTextHandler(w, opts)
    }
    return slog.New(h)
}
```

  `slog.LevelVar` lets a `-log-level` flag or a SIGHUP handler change the
  level at runtime; `HandlerOptions.ReplaceAttr` can rename or drop
  attributes (drop `time` in tests for stable golden output).
  ([log/slog](https://pkg.go.dev/log/slog))
- Attach stable context once: `log = log.With("node", id)`; use
  `WithGroup("paxos")` to namespace protocol attributes. Use the `...Context`
  methods (`InfoContext(ctx, ...)`) so a handler can pull a request ID out
  of the context. ([log/slog](https://pkg.go.dev/log/slog))
- Hot paths (per-message logging in the transport) use `LogAttrs` with
  `slog.String`, `slog.Int` and so on, which "minimize memory allocations";
  or gate them behind `log.Enabled(ctx, slog.LevelDebug)`.
  ([slog blog](https://go.dev/blog/slog))
- Implement `slog.LogValuer` on any type that contains a player identifier
  or a payout amount that must not appear in logs; the value method returns
  a redacted form. ([log/slog LogValuer](https://pkg.go.dev/log/slog#LogValuer))
- Tests use `slog.New(slog.DiscardHandler)` (Go 1.24) or a `JSONHandler` on a
  `bytes.Buffer` when the test asserts on log output. ([log/slog](https://pkg.go.dev/log/slog),
  [Go 1.24 notes](https://go.dev/doc/go1.24))
- `slog.SetDefault(log)` also redirects the `log` package's default logger
  to the slog handler, so `log.Printf` calls from the standard library
  (`http.Server.ErrorLog` when nil) land in the same stream. `http.Server`
  also accepts an explicit `ErrorLog *log.Logger`, which `slog.NewLogLogger`
  produces. ([log/slog](https://pkg.go.dev/log/slog))
- `slog.NewMultiHandler` (Go 1.26) fans one record out to several handlers;
  only needed if the simulator wants both a human trace and a JSON file.
  ([Go 1.26 notes](https://go.dev/doc/go1.26))
- If a custom handler is ever written (for example, one that records into
  the simulator's trace), the handler guide sets the contract: `Enabled`,
  `Handle`, `WithAttrs`, `WithGroup`; `WithAttrs`/`WithGroup` return new
  handlers and must respect call order; `Handle` must be safe for
  concurrent use, so hold a pointer to a mutex so copies share it; validate
  with `testing/slogtest`. ([slog handler guide](https://github.com/golang/example/blob/master/slog-handler-guide/README.md))
- `go vet` has a `slog` analyzer that checks "for invalid structured
  logging calls" (odd key/value counts, non-string keys).
  ([cmd/vet](https://pkg.go.dev/cmd/vet))

The simulator does not use `slog` for its event trace; it writes a compact
line format to an `io.Writer` so traces of two runs with the same seed can
be diffed byte for byte.

---

## 8. GitHub Actions workflow

### Facts

- "GitHub Actions usage is free for self-hosted runners and for public
  repositories that use standard GitHub-hosted runners." The Free plan
  includes 2,000 minutes per month for private repositories; Linux minutes
  are the cheapest, macOS the most expensive.
  ([About billing for GitHub Actions](https://docs.github.com/en/billing/managing-billing-for-your-products/managing-billing-for-github-actions/about-billing-for-github-actions))
- `actions/setup-go` is at major version v7; `go-version-file: go.mod`
  "Supports both go and toolchain directives in go.mod. If the toolchain
  directive is present, its version is used; otherwise, the action falls
  back to the go directive." Aliases `stable` and `oldstable` exist; module
  and build caching is on by default (`cache: true`) keyed on `go.sum`, so
  point `cache-dependency-path` at `go.mod` when there is no `go.sum`.
  The README example uses `actions/checkout@v7`.
  ([actions/setup-go](https://github.com/actions/setup-go))
- "Pinning an action to a full-length commit SHA is currently the only way
  to use an action as an immutable release." Set the default `GITHUB_TOKEN`
  permission to read-only and raise per job if needed.
  ([Security hardening for GitHub Actions](https://docs.github.com/en/actions/security-for-github-actions/security-guides/security-hardening-for-github-actions))
- Workflow syntax: `on.push.branches`, `on.pull_request`, `permissions`,
  `concurrency` with `cancel-in-progress: true`, `strategy.matrix` with
  `fail-fast: false`, `timeout-minutes` (default 360).
  ([Workflow syntax](https://docs.github.com/en/actions/writing-workflows/workflow-syntax-for-github-actions))
- `go mod tidy -diff` "causes tidy not to modify go.mod or go.sum but
  instead print the necessary changes as a unified diff. It exits with a
  non-zero code if the diff is not empty." (Go 1.23 and later.)
  ([cmd/go](https://pkg.go.dev/cmd/go#hdr-Add_missing_and_remove_unused_modules))
- The race detector on Linux needs cgo and a C compiler
  ([race detector](https://go.dev/doc/articles/race_detector)); the
  `ubuntu-latest` image ships gcc, so no extra step is needed.

### Workflow

`.github/workflows/ci.yml`:

```yaml
name: ci

on:
  push:
    branches: [main]
  pull_request:

permissions:
  contents: read

concurrency:
  group: ci-${{ github.ref }}
  cancel-in-progress: true

env:
  GOTOOLCHAIN: local      # never download a newer toolchain; fail instead
  GOFLAGS: -mod=mod

jobs:
  test:
    runs-on: ubuntu-latest
    timeout-minutes: 15
    strategy:
      fail-fast: false
      matrix:
        go: [oldstable, stable]
    steps:
      - uses: actions/checkout@v7           # pin to a full SHA before publishing
      - uses: actions/setup-go@v7           # pin to a full SHA before publishing
        with:
          go-version: ${{ matrix.go }}
          cache-dependency-path: go.mod
      - run: go version
      - name: gofmt
        run: test -z "$(gofmt -l .)" || { gofmt -l .; exit 1; }
      - name: go vet
        run: go vet ./...
      - name: go build
        run: go build ./...
      - name: go mod tidy is a no-op
        run: go mod tidy -diff
      - name: go test (race)
        run: go test -race -count=1 -shuffle=on -timeout 10m ./...
```

Notes on the choices:

- The matrix is meaningful only while `go.mod` says `go 1.26.0`; with
  `GOTOOLCHAIN=local` the `oldstable` job fails if the `go` line is ever
  raised above 1.26, which is the intended signal to update the workflow.
- `@v7` tags are shown for readability; the hardening guide says to pin to
  the full SHA. Replace both before the repository is public and record the
  tag in a trailing comment.
- One Linux job per Go version keeps a full run to a few minutes. The
  long-running simulation sweep (`-seeds=1000`) is not part of CI; it runs
  locally via `make sim-long` and is described in the README. If a
  scheduled run is wanted later, `on.schedule` with a weekly cron and a
  separate job is the place, still on Linux.
- `pull_request` from forks gets a read-only token; nothing here needs
  more. No secrets are used anywhere in the workflow.

---

## 9. `gofmt` and `go vet` as gates

- `gofmt -l`: "If a file's formatting is different from gofmt's, print its
  name to standard output." It does not set a non-zero exit status by
  itself, hence the `test -z "$(gofmt -l .)"` pattern above. `gofmt -s`
  applies simplifications; `gofmt -d` prints diffs; `gofmt -w` rewrites.
  ([cmd/gofmt](https://pkg.go.dev/cmd/gofmt))
- `go vet ./...` "runs the Go vet tool (cmd/vet) on the named packages and
  reports diagnostics" and exits non-zero when it reports.
  ([cmd/go](https://pkg.go.dev/cmd/go#hdr-Report_likely_mistakes_in_packages))
- Analyzers registered in Go 1.27.1 (from `go tool vet help`) that matter
  most for this codebase: `copylocks` (locks passed by value), `lostcancel`
  (uncalled `CancelFunc`), `waitgroup` (misplaced `WaitGroup.Add`),
  `testinggoroutine` (`t.Fatal` from a goroutine the test started), `slog`
  (malformed logging calls), `httpresponse` (using a response before
  checking the error), `hostport` (`fmt.Sprintf("%s:%d")` instead of
  `net.JoinHostPort`), `printf`, `stdversion`, `unusedresult`,
  `loopclosure`, `sigchanyzer` (unbuffered `os.Signal` channel).
  ([cmd/vet](https://pkg.go.dev/cmd/vet), [Go 1.25 notes](https://go.dev/doc/go1.25))
- `go test` already runs the high-confidence subset; the explicit `go vet
  ./...` step adds the rest. Keep both; they are cheap.
- A `Makefile` target mirrors CI so the two never drift:

```make
.PHONY: check
check:
	test -z "$$(gofmt -l .)" || { gofmt -l .; exit 1; }
	go vet ./...
	go build ./...
	go mod tidy -diff
	go test -race -count=1 -shuffle=on ./...
```

- No third-party linters. The task constraint is standard library only,
  and the two built-in tools plus `-race` cover the classes of bugs this
  project can realistically have. Say so in the README rather than leaving
  the reader to wonder why there is no linter config.

---

## 10. README structure

### What the sources say

- A README typically covers "What the project does, Why the project is
  useful, How users can get started with the project, Where users can get
  help with your project, Who maintains and contributes to the project".
  ([About READMEs](https://docs.github.com/en/repositories/managing-your-repositorys-settings-and-features/customizing-your-repository/about-readmes))
- A design document's anatomy: context and scope, goals and non-goals, the
  design (system-context diagram, API sketch, data storage), alternatives
  considered, cross-cutting concerns; "the design doc is the place to write
  down the trade-offs you made in designing your software".
  ([Design Docs at Google, Ubl 2020](https://www.industrialempathy.com/posts/design-docs-at-google/))
- An Architectural Decision Record "captures a single AD and its
  rationale" so a reader can "understand the reasons for a chosen
  architectural decision, along with its trade-offs and consequences".
  ([adr.github.io](https://adr.github.io/))

### Outline for this repository

```
# paxos-arena

One paragraph: what it is (Paxos-based settlement for paid tournaments in a
skill-based competitive mobile card game), what it is for (a case study in
consensus, fault injection and testing), what it is not (a production
service).

## Problem
The concrete failure the system prevents: two replicas settling the same
tournament differently, or a retried request paying twice. State the
consistency requirement in one sentence and the fault model in one list.

## Design
- Components: node, transport, simulator, HTTP API. One ASCII diagram.
- Protocol: which Paxos variant, one instance per tournament, what is
  durable, what leader election does and does not guarantee.
- API: the three endpoints, the idempotency-key contract, the error format.
- Link to docs/design.md and docs/adr/ for the long form.

## How to run
Exact commands, copy-pasteable, each with its expected output:
  go run ./cmd/arena-sim -seed 42 -nodes 5 -drop 0.1 -steps 20000
  go run ./cmd/arena-node -id 1 -peers ... -listen :8081
  curl -X POST -H 'Idempotency-Key: ...' ...

## How it is tested
- Unit: table-driven tests per package.
- Simulation: seeded, single-threaded, faults listed; how to replay a seed;
  which invariants are checked; where the failing-seed corpus lives.
- Concurrency: which tests run under -race and -count.
- Fuzz: which targets, how to run them.
- CI: link to the workflow, what it runs, on which Go versions.

## Trade-offs
Bullets, one per decision, each with the alternative rejected:
in-memory idempotency store vs persistent; single-decree vs multi-Paxos;
JSON over HTTP vs a binary protocol; encoding/json v1 vs v2; own scheduler
vs testing/synctest; go 1.26.0 vs 1.27.0.

## What is not done
An honest list: no persistence, no TLS, no authentication, no membership
changes, no log compaction, no liveness guarantee under unbounded faults,
no load testing. Each item says whether it is out of scope or future work.

## Layout
The tree from section 2 with one line per directory.

## License
```

Writing rules for the README, all of them checkable:

- Every command in "How to run" was executed before commit; paste real
  output, trimmed.
- Every claim in "How it is tested" points to a test file name.
- "What is not done" is as long as it needs to be. Readers who evaluate
  backend work look for that section first because it shows the author
  knows the boundary of the system.
- No adjectives about quality. Numbers (seeds run, faults injected, test
  runtime) replace them.
- Keep design decisions in `docs/adr/NNNN-title.md` with context, decision,
  consequences, so the README stays short and the reasoning survives edits.

---

## 11. Decisions taken for this repository

| Topic | Decision | Reason |
|---|---|---|
| `go` line | `go 1.26.0`, no `toolchain` line | oldest supported release; makes the two-version CI matrix real; nothing needs 1.27 |
| Layout | `cmd/arena-node`, `cmd/arena-sim`, everything else under `internal/` | official layout guidance; no public API to support |
| Protocol core | pure state machine, no goroutines, no time, no rand | required for deterministic simulation; testable with tables |
| Production transport | goroutine per connection, single event loop owns state | channels for ownership transfer, mutex only for HTTP-side caches |
| Simulation time | own event queue with virtual clock | works on any Go version; total control over ordering |
| Goroutine-layer tests | `testing/synctest` where the code uses timers/channels | stable since 1.25; turns leaks and deadlocks into failures |
| Randomness | `rand.New(rand.NewPCG(seed, 0))` per run, seed logged | reproducibility; documented single-goroutine use |
| Property tests | hand-rolled generators plus fuzz targets for codecs | `testing/quick` is frozen; fuzzing is maintained |
| JSON | `encoding/json` v1 with `DisallowUnknownFields` and trailing-data check | v2 needs `go 1.27.0`; strictness reproduced by hand |
| Errors | RFC 9457 `application/problem+json` | one format for every error; standard |
| Idempotency | header required; fingerprint; 400/409/422; 24 h TTL; in-memory | IETF draft semantics; limit documented |
| Logging | `slog.JSONHandler` to stderr, `LevelVar`, `LogValuer` redaction | standard library; runtime level change; no PII in logs |
| CI | one Linux job per Go version; gofmt, vet, build, tidy -diff, test -race | free for public repositories; fast; full failure list |
| Lint | `gofmt`, `go vet`, `-race` only | standard library constraint; stated in README |

---

## 12. Sources

Go project

- Release history and policy: https://go.dev/doc/devel/release
- Go 1.27 release notes: https://go.dev/doc/go1.27
- Go 1.26 release notes: https://go.dev/doc/go1.26
- Go 1.25 release notes: https://go.dev/doc/go1.25
- Go 1.24 release notes: https://go.dev/doc/go1.24
- Go 1.22 release notes: https://go.dev/doc/go1.22
- Go toolchains: https://go.dev/doc/toolchain
- go.mod reference (`go` directive, `go mod edit`, `go mod init`): https://go.dev/ref/mod
- Organizing a Go module: https://go.dev/doc/modules/layout
- cmd/go documentation (internal directories, testdata, test flags, vet subset, tidy -diff): https://pkg.go.dev/cmd/go
- cmd/vet analyzers: https://pkg.go.dev/cmd/vet
- cmd/gofmt: https://pkg.go.dev/cmd/gofmt
- Go specification (select, map iteration): https://go.dev/ref/spec
- Go memory model: https://go.dev/ref/mem
- Data race detector: https://go.dev/doc/articles/race_detector
- Effective Go: https://go.dev/doc/effective_go
- Go Code Review Comments: https://go.dev/wiki/CodeReviewComments
- Go wiki, MutexOrChannel: https://go.dev/wiki/MutexOrChannel
- Go wiki, TableDrivenTests: https://go.dev/wiki/TableDrivenTests
- Go Doc Comments: https://go.dev/doc/comment
- Blog, Package names: https://go.dev/blog/package-names
- Blog, Go Concurrency Patterns: Pipelines and cancellation: https://go.dev/blog/pipelines
- Blog, Go Concurrency Patterns: Context: https://go.dev/blog/context
- Blog, Contexts and structs: https://go.dev/blog/context-and-structs
- Blog, Working with Errors in Go 1.13: https://go.dev/blog/go1.13-errors
- Blog, Routing Enhancements for Go 1.22: https://go.dev/blog/routing-enhancements
- Blog, Structured Logging with slog: https://go.dev/blog/slog
- Blog, Evolving the Go Standard Library with math/rand/v2: https://go.dev/blog/randv2
- Blog, Testing concurrent code with testing/synctest: https://go.dev/blog/synctest
- Go Fuzzing: https://go.dev/doc/security/fuzz/
- Package context: https://pkg.go.dev/context
- Package sync: https://pkg.go.dev/sync
- Package testing: https://pkg.go.dev/testing
- Package testing/synctest: https://pkg.go.dev/testing/synctest
- Package testing/quick: https://pkg.go.dev/testing/quick
- Package math/rand/v2: https://pkg.go.dev/math/rand/v2
- Package net/http: https://pkg.go.dev/net/http
- Package net/http/httptest: https://pkg.go.dev/net/http/httptest
- Package encoding/json: https://pkg.go.dev/encoding/json
- Package encoding/json/v2: https://pkg.go.dev/encoding/json/v2
- Package log/slog: https://pkg.go.dev/log/slog
- Package os/signal: https://pkg.go.dev/os/signal
- Package runtime/pprof: https://pkg.go.dev/runtime/pprof
- slog handler guide: https://github.com/golang/example/blob/master/slog-handler-guide/README.md
- Proposal: change `go mod init` default go directive back to 1.N: https://github.com/golang/go/issues/77653
- Backport tracking for the above (1.26): https://github.com/golang/go/issues/77860

Standards

- RFC 9457, Problem Details for HTTP APIs: https://www.rfc-editor.org/rfc/rfc9457
- draft-ietf-httpapi-idempotency-key-header-07 (expired, informative): https://www.ietf.org/archive/id/draft-ietf-httpapi-idempotency-key-header-07.txt

Prior art on deterministic simulation and fault injection

- FoundationDB, Simulation and Testing: https://apple.github.io/foundationdb/testing.html
- TigerBeetle, The VOPR: https://github.com/tigerbeetle/tigerbeetle/blob/main/docs/internals/vopr.md
- sled, Simulation guide: https://sled.rs/simulation.html
- Jepsen tutorial, Nemesis: https://github.com/jepsen-io/jepsen/blob/main/doc/tutorial/05-nemesis.md
- Idempotent requests in a public payments API: https://docs.stripe.com/api/idempotent_requests

GitHub

- actions/setup-go: https://github.com/actions/setup-go
- Workflow syntax: https://docs.github.com/en/actions/writing-workflows/workflow-syntax-for-github-actions
- Security hardening for GitHub Actions: https://docs.github.com/en/actions/security-for-github-actions/security-guides/security-hardening-for-github-actions
- About billing for GitHub Actions: https://docs.github.com/en/billing/managing-billing-for-your-products/managing-billing-for-github-actions/about-billing-for-github-actions
- About READMEs: https://docs.github.com/en/repositories/managing-your-repositorys-settings-and-features/customizing-your-repository/about-readmes

Writing

- Design Docs at Google (Malte Ubl, 2020): https://www.industrialempathy.com/posts/design-docs-at-google/
- Architectural Decision Records: https://adr.github.io/
- Never start a goroutine without knowing how it will stop (Dave Cheney, 2016): https://dave.cheney.net/2016/12/22/never-start-a-goroutine-without-knowing-how-it-will-stop
