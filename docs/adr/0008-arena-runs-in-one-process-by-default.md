# ADR 0008: `cmd/arena` starts N replicas in one process by default

Status: accepted, 2026-09-16. Design reference: `DESIGN.md` section 4.9.

## Context

The design specifies one replica per process, configured with `-id` and
`-peers`. That is the right shape for killing and restarting a replica
independently, and the three-node test `TestThreeNodesOverHTTP` and the
manual demo use it. For a first run it means three terminals before the
first `curl`.

## Decision

Without `-peers`, `arena` starts `-nodes` replicas (default 3) in one
process, connected by the in-process bus `transport.Local`, each with its
own HTTP listener on consecutive ports from `-listen` (port 0 picks free
ports). It prints the `curl` commands for a complete tournament. With `-id`
and `-peers` it runs one replica over `transport.HTTP`, as designed. The
`-wal` flag of the design is not present because the file store is not
built.

## Consequences

- One command gives a cluster to talk to. Forwarding, replay of repeated
  keys, stale reads with `X-Arena-Applied-Slot` and consistent reads all
  behave as in the multi-process mode because the same `Runner` and
  `api.Server` run behind every port.
- Killing one replica requires the per-process mode; in the one-process
  mode Ctrl-C stops all of them.
- State is in memory in both modes and is lost when the process exits.
