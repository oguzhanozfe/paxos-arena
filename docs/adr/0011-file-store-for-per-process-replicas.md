# ADR 0011: Per-process replicas keep their log state in an append-only file

Status: accepted, 2026-09-17. Design reference: `DESIGN.md` sections 3.6,
4.2, 4.9 and 9. Supersedes the consequence of ADR 0008 that the `-wal` flag
is absent, and the consequence of ADR 0006 that only `MemStore` ships.

## Context

`arena -id -peers` advertised running replicas as separate processes so
that one can be killed and restarted, but every start opened an empty
`MemStore`. A restarted process rejoined under its old node identity with no
promise, no accepted values and no chosen entries, which section 9 of the
design forbids. A review reproduced the consequence through the command's
own flag handling: a create acknowledged with `201` and applied by two of
three replicas disappeared after the leader died and the other replica's
process was restarted, and the same identifier could be created again with
other rules (`cmd/arena.TestAttackRestartedReplicaForgetsAcknowledgedCreate`).

## Decision

- Build `internal/replog/wal` as the design describes: `replog.Store` on an
  append-only file, one record per `Save*` call, an fsync before the call
  returns. A record is an 8-byte header (payload length and CRC-32C) and a
  JSON payload. `Open` cuts a torn tail: an incomplete last record, a zero-
  filled tail, or a last record whose checksum fails. A bad record followed
  by more data is `ErrCorrupt`, and the file is refused, because truncating
  there would silently drop durable state. A failed write or sync poisons
  the file; the node halts (ADR 0006) and is rebuilt from `Open`.
- `Bind(self, peers)` records the replica and the membership in an empty
  file and refuses a file that belongs to another replica or membership, or
  that holds records but no membership.
- `arena` requires `-wal` whenever `-peers` is given, and refuses `-wal`
  in the one-process mode, whose replicas share a process and its fate.

## Consequences

- A killed replica restarted with the same file rejoins with its promise,
  its accepted values and its chosen entries, and replays the state machine
  from them; the review's test now passes, and the README's three-process
  run restarts the killed leader from its file.
- The file is never compacted and grows by one record per save for the life
  of the cluster; startup replays all of it. Snapshots are further work.
- An fsync per save bounds throughput by the disk. The simulator keeps
  using `MemStore`; the file store is tested on its own (`TestTornTail`,
  `TestCorruptionBeforeTheTailIsRefused`, `TestNodeRestartsFromFile`).
- Nothing detects a replica started with a new, empty file under an old
  identity: an empty file looks like a new replica. Operators must give such
  a replica a new identity, which requires a new cluster until membership
  changes exist.
