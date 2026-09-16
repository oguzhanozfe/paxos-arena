# ADR 0014: Reads run off the event loop; events polls share one scan

Status: accepted, 2026-09-17. Design reference: `DESIGN.md` section 4.6;
`UNITY-INTEGRATION.md` sections 7.1.6, 7.3.8 and 7.3.10. Supersedes the
sentence of ADR 0013 that `Runner.Read`'s `fn` may still run after `Read`
returned an error.

## Context

An availability review of the play API found that every read ran its
function on the replica's single event loop, the goroutine that steps
Paxos, fires the leader's heartbeats and applies chosen slots. Nothing
bounded that work:

- Every open `GET /v1/events` long-poll re-ran a state read each time the
  applied slot advanced. With 300 open polls one applied slot cost 300
  event-loop reads; with 4000 the leader's `Submit` throughput fell from
  about 54 000 to 5 500 per second. A session is one cheap log slot and
  the per-player poll limit does not bound the number of players.
- A leaderboard read ranked every entry with a linear search per row:
  35 ms at 3000 entrants on the event loop. A burst of such reads kept a
  14 000-entrant cluster's leader off its heartbeats long enough to be
  deposed.

## Decision

- `Runner.Read` runs `fn` on the caller's goroutine holding a read lock on
  the state machine. The event loop, the only writer, applies chosen slots
  holding the write lock and takes it only with `TryLock`: when reads are in
  progress it leaves the committed slots unapplied, marks the apply as
  waiting and keeps stepping the log. Reads that start while an apply waits
  wait for it, and every read that ends nudges the loop, so the apply
  happens as soon as the reads in progress end. A consistent read runs its
  barrier on the loop as before and then reads the same way. `fn` never
  runs after `Read` returned.
- The open events requests of a replica share one scan per applied slot: a
  single read that checks, for every open request, whether events exist
  after its cursor (`tournament.State.HasEvents`). A request reads for
  itself only when it has events to answer. A replica holds at most
  `intent.Config.MaxEventPolls` (10 000) open events requests; one more
  answers `503 unavailable` with `Retry-After`.
- A leaderboard page ranks the entries once, in O(n log n) with a few map
  lookups per entry, and builds only the rows it returns and the caller's
  own row.

## Consequences

- No read, however slow, delays heartbeats, elections or the log; a slow
  read delays the answers to commands, which wait for their slot to be
  applied. `replica.TestSlowReadDoesNotStallTheEventLoop` holds a read open
  for ten election timeouts and requires the same leader and ballot.
- One applied slot costs one read whatever the number of open polls
  (`intent.TestReviewEventsPollFanOutIsCoalesced`); a leaderboard of 3000
  entrants takes about 0.2 ms (`intent.TestReviewLeaderboardReadCostIsNotQuadratic`,
  and `TestLeaderboardPageMatchesReference` against the previous build).
- A read still costs O(entrants) CPU per leaderboard request. An
  incrementally maintained ranking would make a page O(page); it is not
  needed at the bounds tested and is not built.
