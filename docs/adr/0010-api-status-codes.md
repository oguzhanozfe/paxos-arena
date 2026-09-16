# ADR 0010: HTTP status codes and forwarding rules the design left open

Status: accepted, 2026-09-16. Design reference: `DESIGN.md` section 4.7.

## Context

The design fixes the routes, the `Idempotency-Key` contract and the main
status codes (400, 409, 413, 422, 503, 504) but leaves some cases open, and
the implementation had to choose.

## Decisions

| Case | Answer |
|---|---|
| Unknown tournament on a read | `404`, code `not_found` |
| Consistent read or POST on a follower with a known leader | forwarded once with `X-Arena-Forwarded: 1`; the answer carries the leader's `X-Arena-Node` |
| Forwarded request that reaches a non-leader, or no leader known | `503`, code `not_leader`, `Retry-After: 1` |
| Leader unreachable while forwarding | `503`, code `forward_failed`, `Retry-After: 1`, `leader` member set |
| Leader that has not yet committed its leadership no-op | `503`, code `not_ready`, `Retry-After: 1` |
| Client-supplied `seed` on `POST /v1/tournaments` | `400`, unknown field; the seed is server-generated and excluded from the fingerprint |
| Same key, same command, already applied | original status, body with `"replayed": true` |
| Same key, different command | `422`, code `key_reused` |
| State-machine rejection | `409`, `application/problem+json` with the `tournament.Code` in `code` |
| Wait for apply exceeds `RequestTimeout` | `504`, code `outcome_unknown`; the client retries with the same key |
| Body over `MaxBody` (default 64 KiB), or a command whose canonical encoding exceeds `replog.MaxValueBytes` | `413`, code `body_too_large`; nothing proposed, nothing recorded |
| Leader's proposal queue full (`replog.ErrBusy`) | `503`, code `unavailable`, `Retry-After: 1` |
| Forwarded request answered by the leader | the leader's status, headers `Content-Type`, `Content-Length`, `Retry-After`, `X-Arena-Applied-Slot`, `X-Arena-Node`, and its whole body |

The design describes forwarding for POSTs; consistent GETs are forwarded
by the same rule because a follower cannot serve them. Stale GETs are never
forwarded. The forwarding client has a connection pool of its own
(`ForwardConnsPerHost`, 256 open and idle connections per leader) rather
than `http.DefaultTransport`, whose two idle connections per host made
every concurrent forward open and close a TCP connection and exhausted the
ephemeral ports under a few thousand requests a second. Until 2026-09-17 a
forwarded answer was cut off at `MaxBody` under its success status.

## Consequences

- A client needs one rule: retry any `503` or `504` with the same key after
  `Retry-After`; the state machine makes the retry safe.
- Every response carries `X-Arena-Node`, so a client can tell which replica
  answered; stale reads add `X-Arena-Applied-Slot`.
- Error bodies follow RFC 9457 (`type`, `title`, `status`, `detail`, plus
  `code`), one format for every error.
