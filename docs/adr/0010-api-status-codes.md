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

The design describes forwarding for POSTs; consistent GETs are forwarded
by the same rule because a follower cannot serve them. Stale GETs are never
forwarded.

## Consequences

- A client needs one rule: retry any `503` or `504` with the same key after
  `Retry-After`; the state machine makes the retry safe.
- Every response carries `X-Arena-Node`, so a client can tell which replica
  answered; stale reads add `X-Arena-Applied-Slot`.
- Error bodies follow RFC 9457 (`type`, `title`, `status`, `detail`, plus
  `code`), one format for every error.
