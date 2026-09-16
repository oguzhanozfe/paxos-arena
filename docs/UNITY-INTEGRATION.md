# Unity integration: the contract for server-authoritative play

Status: contract for milestone 4, written 2026-09-17. The server side
(sections 2 to 7 and 10) is implemented in `internal/game`,
`internal/session`, `internal/intent`, the play commands of
`internal/tournament`, the claim posting of `internal/ledger`,
`replica.Runner.WaitApplied` and the play flags of `cmd/arena`; the few
places where the implementation settled a detail this document left open
are recorded in section 13.1. The client SDK of section 11 is built
separately in `unity-client/`. The invariants and chaos scenarios of
section 12 are implemented in `internal/sim` and `cmd/chaos`, and
`scripts/e2e.sh` runs the harness of section 11.6 against three `arena`
processes, once with a leader killed mid-round.

This document is written for two implementers who work in parallel without
talking to each other:

- the server implementer, who builds packages `internal/session`,
  `internal/game` and `internal/intent`, the play commands in
  `internal/tournament`, the claim posting in `internal/ledger`,
  `replica.Runner.WaitApplied` and the new flags of `cmd/arena`;
- the client implementer, who builds the C# SDK in `unity-client/` for a
  mobile card game made with the Unity engine.

Each of them must be able to finish from this document alone. When a Go doc
comment and this document disagree, this document wins. A change to the
contract is a change to this document first, with the reason added to
section 13.1.

The play API sits beside the operator API of `DESIGN.md` section 4.7 and
reuses the replicated log, the tournament state machine, the ledger and the
rule that the state machine is the authority on idempotency keys
([ADR 0003](adr/0003-idempotency-in-the-state-machine.md)). It retires two
non-goals of `DESIGN.md` section 9: authentication, and the game itself.

Words used with one meaning throughout: the *client* is the game running on
a player's device together with the SDK; the *cluster* is the set of
replicas; an *intent* is a request asking the cluster to change state; a
*view* is what one player may see of a round; *recorded* means stored in
the replicated results table under an idempotency key.

Contents

1. Trust model
2. Identity and sessions
3. Intents: idempotency keys and sequence numbers
4. The game: Ladder v1
5. Deals: seed, shuffle, commitment and reveal
6. Play commands in the log
7. HTTP API
8. JSON rules for JsonUtility
9. Client rules
10. Server layout (Go)
11. Client SDK layout (C#)
12. Invariants, chaos scenarios and tests
13. Decisions, and what the contract does not cover

Appendix A. Test vectors

---

## 1. Trust model

The client is untrusted. It runs on a device the player controls, and a
modified client can send any request, with any body, at any time. The
replicas are trusted in the sense of `DESIGN.md` section 9: they crash,
restart and get partitioned, but they do not lie. Everything below follows
from one rule: **the client sends intents; the cluster decides every
outcome.**

| The client says | The cluster decides |
|---|---|
| its device id and device secret; a jurisdiction and an age at first launch | the player id, whether the secret matches, the session token and its expiry |
| "enter tournament T" | eligibility, the entry fee posting, the join order |
| "deal round r" | every card, from a seed the client cannot know, written to the log first |
| "play column c" or "draw", with the move count it saw | whether the move is legal on the authoritative board, the new board, whether the round is over |
| "finish round r" | the round's score and the entry's total |
| nothing about scores, times, cards or money | scores, standings, prizes, the claimable amount |
| "claim my payout" | whether there is anything to claim, how much, and that it is claimed once |

Threats and how the contract answers them:

| Threat | Answer |
|---|---|
| Reporting a better score | There is no score input on the play API. The state machine computes every score from the seed and the accepted moves (4.4, 6.9). |
| Illegal moves | Every move is validated by the state machine against the board rebuilt from the round's seed and moves, and rejected with `illegal_move` (6.6). |
| Predicting or choosing the deal | The seed is an HMAC under a secret only the replicas hold (5.1). A view shows the tableau and the waste card; the stock is revealed one card per draw. |
| Re-dealing a bad round | A round is dealt once (`round_already_started`), and its seed is a function of tournament, player and round, so no retry through any leader yields other cards. |
| The operator changing the stock after seeing the player's moves | The seed is in the log before the first card is shown; the client receives its commitment with the deal and the seed when the round ends, and can verify both (5.3). |
| Replaying a captured request | The same key returns the recorded response and changes nothing; the same sequence number under a new key is `stale_seq` (3.2). |
| Two intents delivered out of order | A sequence number that skips one is `seq_gap`; the SDK sends one intent at a time (9.2). |
| Acting as another player | Player ids are drawn by the server and bound to a device secret; tokens are signed; idempotency keys are namespaced by player (2, 3.1). |
| Taking more time than allowed | Round deadlines are on the state machine's clock (4.5); no client clock is ever read. |
| Flooding the log | Per-device and per-player rate limits run before anything is proposed (7.1.6). |
| A lagging replica showing an old board | Views carry the slot they were built at, and reads accept `min_slot` (7.1.5). |

Per-player deals. Every entrant plays different cards. One shared deal per
tournament would compare entrants on identical cards, but an entrant who
finished could publish the stock order to entrants still playing, and one
person with two devices could scout the deal with one of them. Per-player
deals remove both; the price is card luck, which three rounds per entry and
a fully visible tableau keep small.

What the contract does not defend against is listed in section 13.2: move
suggestions by software on the device, one person with several devices,
device attestation, account recovery, and transport security, which the
deployment provides with TLS in front of the play listener.

---

## 2. Identity and sessions

### 2.1 Device credentials

At first launch the SDK draws from a cryptographic random generator:

- `device_id`: 16 bytes, written as 32 lower-case hex characters;
- `device_secret`: 32 bytes, written as 64 lower-case hex characters.

It stores both (9.1), sends them only in `POST /v1/session`, and never
displays or logs the secret. The pair is the player's identity: the first
session of a device binds the device to a new player id drawn by the
server, and every later session of that device must present the same
secret. A reinstall that loses the store is a new device and a new player.

The log never holds a device secret, only its verifier:

```
verifier = SHA-256("paxos-arena/device/v1" || 0x00 || secret)
```

where `secret` is the 32 bytes the 64 hex characters encode
(`session.Verifier`).

### 2.2 Player ids

A player id is `p-` followed by 26 characters of lower-case base32 (`a` to
`z`, `2` to `7`) encoding 130 random bits, for example
`p-mzxw6ytboi4dqnbrgm2wqzlmn4`. The leader's play API draws one from
`crypto/rand` for every session request; the state machine uses it only if
the device is not bound yet (6.3). The shape satisfies the identifier rule
of [ADR 0012](adr/0012-bounds-on-commands.md).

### 2.3 Session tokens

`POST /v1/session` (7.3.1) issues a bearer token that every other route
requires as `Authorization: Bearer <token>`. The client treats the token as
an opaque string and reads the expiry from `expires_at_ms` in the response.

```
token     = "v1." kid "." payload "." mac
kid       = 1 to 16 characters from a-z, 0-9, "-"
payload   = base64url, no padding, of exactly
            {"pid":"<player id>","did":"<device id>","iat":<issued ms>,"exp":<expiry ms>}
            fields in this order, no whitespace, integers in decimal
mac       = base64url, no padding, of HMAC-SHA256(key[kid], ASCII("v1." kid "." payload))
```

Claims: player id, device id, issue time and expiry, both in Unix
milliseconds. The issue time is the `received_at` the leader stamped on the
session command; the expiry is the issue time plus the token lifetime
(`-session-ttl`, default 1 hour, at most 24 hours). Appendix A gives a token
computed from a test key.

Every replica verifies every token itself, in this order, and answers `401`:

| Check | Code |
|---|---|
| no `Authorization` header, or not `Bearer <token>` | `session_missing` |
| not four dot-separated parts, first part not `v1`, bad base64url, payload not exactly the four fields | `session_invalid` |
| `kid` not in the replica's keyring | `session_invalid` |
| mac differs (compared in constant time) | `session_invalid` |
| `exp` before `iat`, or `exp - iat` above 24 hours | `session_invalid` |
| `iat` more than 30 s after the replica's clock | `session_invalid` |
| the replica's clock more than 30 s after `exp` | `session_expired` |

The 30-second leeway covers clock differences between replicas. The server
stores no tokens; a token is valid on every replica that holds its key.
Revoking a single token is not supported; removing a key revokes every
token it signed (2.5).

### 2.4 Keys and secrets

No key or secret is compiled into the binary or the repository.

- Session keys: the file named by `-session-keys-file`, one `kid=hex` per
  line (blank lines and lines starting with `#` ignored), or, when the flag
  is not given, the environment variable `ARENA_SESSION_KEYS` as
  `kid=hex,kid=hex`. The first key signs; every key verifies. Each key is at
  least 32 bytes (64 hex characters). `arena -play-listen` refuses to start
  with no key, a short key, a malformed key id or a repeated key id
  (`session.ErrNoKeys`, `ErrWeakKey`, `ErrBadKeyID`, `ErrDuplicateKeyID`).
- The deal secret (5.1): the file named by `-deal-secret-file`, or the
  environment variable `ARENA_DEAL_SECRET`, as hex of at least 32 bytes. It
  is separate from the session keys and the same on every replica.
- Neither is ever logged, returned by `/v1/node`, or written to the log.
  Generate them with `openssl rand -hex 32` or an equivalent.

### 2.5 Rotation

Rotating the signing key without ending anyone's session (token lifetime T,
default 1 hour):

1. Add the new key second: restart the replicas one at a time with
   `k1=<old>,k2=<new>`. Afterwards every replica verifies both and signs
   with `k1`.
2. Make it first: restart the replicas one at a time with
   `k2=<new>,k1=<old>`. Afterwards every replica signs with `k2`.
3. After T plus 30 seconds from the end of step 2, restart the replicas one
   at a time with `k2=<new>` only.

Doing step 2 before step 1 has finished lets one replica sign with a key
another cannot verify; clients then see `session_invalid`, open new
sessions, and recover once the rollout ends, so the mistake costs requests
but no data. Restarting one replica at a time keeps a majority up.

A compromised key is removed at once instead: restart with the new key
first and the compromised one absent. Every token it signed becomes
`session_invalid`, and SDKs open new sessions from their device credentials
without the player noticing.

The deal secret may be rotated at any time by a rolling restart. A round
already dealt keeps the seed in its record; rounds dealt later use the new
secret. While replicas disagree, two leaders may derive different seeds for
the same round; the first StartRound applied fixes the round (the seed is
not part of the fingerprint, 6.1), so nothing breaks, but an auditor
recomputing seeds must know which secret was current at each deal.

A device secret cannot be rotated in this version (13.2).

---

## 3. Intents: idempotency keys and sequence numbers

### 3.1 Idempotency keys

Every POST carries `Idempotency-Key`: 16 to 64 characters from `A-Z`,
`a-z`, `0-9`, `_` and `-`. The SDK uses 32 lower-case hex characters from a
cryptographic random generator, drawn when the intent is created and stored
with it (9.2).

The server namespaces the key before the command enters the log:
`u.<player id>.<key>` for routes with a token and `d.<device id>.<key>` for
`POST /v1/session`. No client can collide with, or reserve, a key another
player will send.

A request repeated with the same key is answered with the recorded response:
the same status and the same body except `"replayed": true`, and nothing
changes. The same key with a different request, meaning a different route,
path parameter or body (the token is not part of it, so a resend after a
session refresh is the same request), is answered `422 key_reused`.
Responses are rebuilt from the recorded result and replicated state (7.2),
not stored, so they are the same on every replica and after any restart.

### 3.2 Sequence numbers

Every intent except `POST /v1/session` carries `seq`, a per-player integer.
For each player the state machine keeps `last_seq`, the last number
consumed (0 before the first). After the key check, in the state machine:

1. `seq <= last_seq`: rejected `stale_seq`; nothing consumed.
2. `seq > last_seq + 1`: rejected `seq_gap`; nothing consumed.
3. `seq == last_seq + 1`: the number is consumed (`last_seq = seq`), then
   the intent is validated and applied. A rule rejection such as
   `illegal_move` also consumes the number.

Both rejections are recorded under their key like every other rejection.
Every response built from a recorded result of a sequenced intent tells the
client the next number to use: `next_seq` in success bodies, and the
`X-Arena-Next-Seq` header on every such response, rejections included.

Only a command that reaches the state machine can consume a number. A
request refused before the log (`400`, `401`, `413`, `429`, `503`, a
`409 in_flight`; 7.1.6 lists the checks) consumes nothing, and neither does
`422 key_reused`. A `504 outcome_unknown` or a lost response says nothing
either way: the command may still be applied, which is why the client
resends it with the same key and `seq` until it receives a recorded result.

Client rule: the SDK gives an intent `seq = last_assigned + 1` when it
creates it, stores the number with the key, and sends intents strictly one
at a time, the next only after the previous received a definitive answer
(9.2). A rule rejection consumes its number, so the intents queued behind it
stay valid and need no renumbering. A correct client never receives
`stale_seq` or `seq_gap`; if it does, its store was lost, copied or
corrupted, and it resynchronises (9.2.4). Session responses also carry
`next_seq`, and the SDK sets `last_assigned = max(last_assigned,
next_seq - 1)`.

---

## 4. The game: Ladder v1

Ladder is a single-player draw-and-play card puzzle. The player clears a
tableau of face-up cards onto a waste pile, one rank up or down at a time,
and draws from a face-down stock when no card fits. Everything except the
order of the stock is visible, so the score rewards planning which column to
take so that runs stay long. There are no tricks, no opponents' hands and no
bidding. Its variant name in tournament rules is `ladder-v1`; any change to
this section is a new variant name.

### 4.1 Cards

A standard deck of 52 cards without jokers. Each card has a number from 0 to
51: its rank is `number / 4 + 1` (ace 1, 2 to 10, jack 11, queen 12, king 13)
and its suit `number % 4` (clubs, diamonds, hearts, spades). On the wire a
card is two characters, the rank from `A23456789TJQK` and the suit from
`cdhs`: card 0 is `Ac`, card 13 is `4d`, card 51 is `Ks`. Suits have no
effect on play.

Two cards are *adjacent* when their ranks differ by one, or one is an ace and
the other a king: `(rank_a - rank_b) mod 13` is 1 or 12.

### 4.2 Layout

The round's deck is the canonical order 0 to 51 shuffled with the round's
seed (5.2). Deck positions are dealt as follows:

- Tableau: seven columns. Column c (0 to 6) holds positions 5c to 5c+4,
  bottom card first; position 5c+4 is its *top card*. All 35 cards are face
  up.
- Waste: position 35 is the starting *waste card*.
- Stock: positions 36 to 51, face down, drawn in that order. Its count is
  visible.

### 4.3 Moves

| Move | Legal when | Effect |
|---|---|---|
| `play` column c | c is 0 to 6, column c is not empty, and its top card is adjacent to the waste card | the top card moves onto the waste and becomes the waste card |
| `draw` (column -1) | the stock is not empty | the next stock card moves onto the waste and becomes the waste card |

A move on a round that is over is illegal. There is no undo.

After every accepted move the round ends by itself when:

- the tableau is empty (finish reason `cleared`), or
- the stock is empty and no column's top card is adjacent to the waste card
  (`blocked`).

While the stock has cards a draw is legal, so a round is never blocked with
cards left to draw. A round also ends when the player finishes it (`resigned`
before its deadline, `expired` after it) and when the tournament closes
(`closed`, or `expired` when the deadline had passed).

### 4.4 Score

```
cleared = number of cards played from the tableau, 0 to 35
score   = 100 * cleared
          + (cleared == 35 ? 500 + 50 * cards left in the stock : 0)
```

A round scores at most 3 500 + 500 + 50 * 16 = 4 800. An entry's total is the
sum of its three rounds, at most 14 400, which is the `max_score` every
`ladder-v1` tournament declares. A finished round's score never changes. A
round in play has a provisional score by the same formula, shown in its
view; leaderboards count finished rounds only.

### 4.5 Rounds and time

An entry has three rounds, dealt and played in order: round r can be dealt
once round r-1 is finished. A round accepts moves for 300 000 ms after it is
dealt: `deadline_ms = started_at_ms + 300000`.

Time is the state machine's clock: the largest `received_at` (the leader's
receipt time in Unix milliseconds, stamped on every command) of all commands
applied so far. It is replicated, deterministic, and never goes backwards
when leadership moves to a replica whose clock is behind. A move applied
while the clock is past the deadline is rejected `round_expired`; the round
keeps the score of its accepted moves until it is finished. Clock skew
between replicas moves a deadline by at most the skew, which the operator
keeps small with time synchronisation. The device clock never matters.

An entry's score is fixed when its round 3 finishes; the entry becomes
*scored*, and its submission order is the order in which entries became
scored. When the tournament closes, rounds still in play are finished, and
every unscored entry with at least one dealt round becomes scored with its
rounds' sum (rounds never dealt count 0), in join order. An entry that never
dealt a round stays unscored and counts toward voiding the tournament
(`DESIGN.md` section 5.4). Final standings use the existing rule: score
descending, then submission order, under the tournament's tie-break.

### 4.6 Example

Round 1 of the test vector (Appendix A) deals these columns, bottom to top,
with waste card `4h` and 16 cards in the stock:

```
column 0: Ks 8h 7s 3s Jd        column 4: Ts 7c 5s 6d 2d
column 1: 2h 4c 5d 7h Qh        column 5: 5h Tc Ad 9s 9h
column 2: 3c 8s Ah 8d Th        column 6: 2c 6s 6c 3d As
column 3: 9d Td 3h Jh Qs
```

No top card (`Jd Qh Th Qs 2d 9h As`) is adjacent to a four, so the only
legal move is `draw`, which reveals `Kh`. Now `Qh`, `Qs` and `As` are
playable; playing column 1 makes `Qh` the waste card and leaves `Jd` as the
only playable top card. A player who always plays the lowest playable column
and draws otherwise makes 48 moves, clears 32 cards, runs out of stock and is
blocked: score 3 200. Appendix A lists that move sequence.

---

## 5. Deals: seed, shuffle, commitment and reveal

### 5.1 Seed

```
seed = HMAC-SHA256(key = deal secret,
                   "paxos-arena/deal/v1" || 0x00 || tournament id || 0x00 ||
                   player id || 0x00 || round as 4 bytes, big-endian)
```

Identifiers are ASCII without zero bytes (ADR 0012), so the input is
unambiguous. The leader's play API derives the seed when it builds a
`StartRound` command; the state machine never sees the secret.

### 5.2 Shuffle

A byte stream is expanded from the seed: block k, for k = 0, 1, 2 and so on,
is `SHA-256(seed || k as 8 bytes, big-endian)`, and the stream is block 0
followed by block 1 and so on. `next32()` reads the next four bytes of the
stream as an unsigned big-endian integer. The deck is shuffled from the top:

```
deck = [0, 1, ..., 51]
for i = 51 down to 1:
    n = i + 1
    limit = 2^32 - (2^32 mod n)
    repeat: x = next32()
    until x < limit
    j = x mod n
    swap deck[i] and deck[j]
```

Rejecting `x >= limit` makes `j` uniform. For n up to 52 a rejection has a
probability below 2^-26 per draw, so the test vectors never hit one (for
n = 52, `limit` is 4 294 967 248); implementations must still implement it.

### 5.3 Commitment and reveal

```
commitment = SHA-256("paxos-arena/commit/v1" || 0x00 || seed)
```

Seeds and commitments are written as 64 lower-case hex characters. A deal
happens in this order:

1. The client sends `POST .../rounds/{round}/deal` with its key and `seq`.
2. The leader derives the seed and proposes `StartRound` carrying it.
3. The command is chosen and applied. The round record, seed included, is
   now replicated state: this is the commitment in the log. From this slot
   on, every replica holds the same cards for the round and no leader can
   change them.
4. Only then does the leader build the view, from applied state, and answer
   `201` with the tableau, the waste card, the stock count and the
   `commitment`.
5. While the round is in play, no response contains its seed or a stock
   card not yet drawn. Round records are not part of the operator API's
   tournament record either. The seeds are in the log, so the replica
   network is as sensitive as the deal secret.
6. Once the round is finished, every view of it carries `seed`. The client
   can check that the commitment matches, reshuffle, and confirm that the
   deal equals the first view and that its moves reproduce the final board
   and score. The SDK's harness does exactly that (11.6).

Any replica answering a read builds the view from its own applied state,
which contains the `StartRound` slot, so a follower cannot reveal cards
before the commitment either.

---

## 6. Play commands in the log

Six new `tournament.Op` types, declared in `internal/tournament/play.go`,
with these wire names in the command codec (`DESIGN.md` section 5.4):

| Op | Wire name | Route |
|---|---|---|
| `OpenSession` | `open_session` | `POST /v1/session` |
| `Enter` | `enter` | `POST /v1/tournaments/{tournament_id}/join` |
| `StartRound` | `start_round` | `POST .../rounds/{round}/deal` |
| `PlayMove` | `play_move` | `POST .../rounds/{round}/moves` |
| `FinishRound` | `finish_round` | `POST .../rounds/{round}/finish` |
| `ClaimPayout` | `claim_payout` | `POST .../payout/claim` |

Validation runs in the order listed below, after the key check of `DESIGN.md`
section 5.4; the first failing rule is the result.

### 6.1 Changes to existing types and rules

- `tournament.Rules` gains `Game game.Name` (`json:"game,omitempty"`): empty
  for tournaments with client-reported scores, whose behaviour does not
  change, or `ladder-v1`. `CreateTournament` answers `invalid_rules` when
  `Game` is neither, or is `ladder-v1` with `max_score` other than 14 400.
- `tournament.Result` gains `Play PlayOutcome` (`json:"play,omitzero"`),
  set by play commands and zero otherwise, so existing encodings and state
  hashes do not change. It is a value, not a pointer, so a `Result` stays
  comparable and never shares memory with the results table (13.1).
- `tournament.State` gains: bindings by device, player records by player,
  round records by (tournament, player, round), the event records in slot
  order with an index per tournament and per player, and `clock` (4.5), which
  advances to `max(clock, received_at)` before every decodable command is
  applied. `Hash` covers all of them: the chain step of each slot also mixes
  in the canonical encoding of every binding, player record, round record
  and event the command changed or added.
- `Fingerprint` excludes `OpenSession.Player` and `StartRound.Seed`, which
  the server draws, as it excludes `CreateTournament.Seed`.
- `Join` and `SubmitScore` addressed to a play tournament are rejected with
  `play_intent_required`, checked right after `unknown_tournament`.
- `Close` of a play tournament first finishes every round in play (reason
  `expired` if the clock is past its deadline, else `closed`), then marks
  every unscored entry with at least one dealt round as scored, in join order
  (4.5), then computes standings, pool and status as before. It records
  `round_finished` and `entry_scored` for those players, one
  `tournament_status` and one `leaderboard_changed`.
- `Settle` of a play tournament works as before and records
  `tournament_status`, `leaderboard_changed`, and `payout_available` for
  every payout that is not withheld and has an amount above 0.
- `DESIGN.md` section 5.4 says a rejected command has no effect other than
  recording its result. Play commands add one exception: a rule rejection of
  a sequenced command consumes its sequence number (3.2).
- `ledger` gains kind `Claim` (`"claim"`), account `ClaimsAccount(tid)` =
  `claims:<tid>` and key `ClaimKey(tid, pid)` = `claim:<tid>:<pid>`.

### 6.2 Common rules of sequenced commands

`Enter`, `StartRound`, `PlayMove`, `FinishRound` and `ClaimPayout` begin with:

1. `unknown_player`: no player record for `Player`.
2. `stale_seq`: `Seq <= LastSeq`.
3. `seq_gap`: `Seq > LastSeq + 1`.
4. From here on the number is consumed: `LastSeq = Seq`, whatever follows.
5. `unknown_tournament`.
6. `not_play_tournament`: the tournament's `Rules.Game` is empty.

Every result sets `PlayOutcome.Player` and `PlayOutcome.NextSeq` (`LastSeq +
1` after these rules).

### 6.3 OpenSession

Fields: `Device`, `Verifier`, `Player` (drawn by the API), `Jurisdiction`,
`Age`. No sequence number.

1. `invalid_device`: `Device` is not 32 lower-case hex characters, or
   `Verifier` is all zero.
2. If the device is bound: `device_mismatch` when `Verifier` differs from
   the binding's; otherwise OK with no change.
3. If it is not bound: `invalid_player` when `Player` is not of the
   identifier shape, `Jurisdiction` is not 2 to 8 upper-case letters or
   `Age` is outside 0 to 150; `player_exists` when `Player` is bound to
   another device. Effect: a `Binding` and a `PlayerRecord` with `LastSeq`
   0.

Outcome: `Player` (the bound player), `NewPlayer`, `IssuedAtMs` = the
command's `received_at`, `NextSeq`.

### 6.4 Enter

Fields: `Tournament`, `Player`, `Seq`. After 6.2:

7. `not_open`
8. `tournament_full`
9. `already_joined`
10. `jurisdiction_excluded`, against the binding's jurisdiction
11. `underage`, against the binding's age
12. `ledger_conflict`

Effect: exactly `Join` with `Player{ID, Jurisdiction, Age}` taken from the
binding. Outcome: `JoinSeq`.

### 6.5 StartRound

Fields: `Tournament`, `Player`, `Seq`, `Round`, `Seed` (derived by the API).
After 6.2:

7. `not_open`
8. `not_joined`
9. `invalid_round`: `Round` outside 1 to 3
10. `round_already_started`
11. `previous_round_unfinished`: `Round > 1` and round `Round - 1` is not
    finished or was never dealt

Effect: a `RoundRecord` with the seed, no moves, status `playing`,
`StartedAt` = slot, `StartedAtMs` = clock, `DeadlineMs` = clock + 300 000,
`UpdatedAt` = slot; event `round_started`. Outcome: `Round`, `MoveIndex` 0.

### 6.6 PlayMove

Fields: `Tournament`, `Player`, `Seq`, `Round`, `MoveIndex`, `Move`. After
6.2:

7. `not_open`
8. `not_joined`
9. `invalid_round`
10. `round_not_started`
11. `round_finished`
12. `round_expired`: clock > `DeadlineMs`
13. `move_index_mismatch`: `MoveIndex` differs from the number of accepted
    moves
14. `illegal_move`: the board rebuilt from the seed and the accepted moves
    refuses the move; the detail is the `game` error's message

Effect: the move is appended and `UpdatedAt` = slot; if the board is now
cleared or blocked, the round finishes (6.9). Outcome: `Round`, `MoveIndex`
= the number of accepted moves.

### 6.7 FinishRound

Fields: `Tournament`, `Player`, `Seq`, `Round`. After 6.2:

7. `not_joined`
8. `invalid_round`
9. `round_not_started`
10. If the round is finished: OK, with no change beyond the sequence number.
11. `not_open`: unreachable, since `Close` finishes every round in play; kept
    so that `Apply` stays total.

Effect: the round finishes (6.9) with reason `expired` if the clock is past
`DeadlineMs`, else `resigned`. Outcome: `Round`, `MoveIndex`.

### 6.8 ClaimPayout

Fields: `Tournament`, `Player`, `Seq`. After 6.2:

7. `not_joined`
8. `not_settled`: status is not settled
9. `no_payout`: the player's payouts that are not withheld sum to 0
10. `already_claimed`: the key `claim:<tid>:<pid>` exists in the ledger

Effect: one posting of kind `claim`, debit `player:<pid>`, credit
`claims:<tid>`, amount = that sum, key `claim:<tid>:<pid>`, with slot and
ballot; event `payout_claimed`. Outcome: `Amount`. Refunds of a voided
tournament are claimable; withheld prizes are not.

### 6.9 Finishing a round

Status `finished`, the reason, `Cleared`, `Score` (4.4), `FinishedAt` =
`UpdatedAt` = slot; event `round_finished`; one event `leaderboard_changed`
for the slot. If the round is round 3 and the entry is unscored, the entry
becomes scored with the sum of its three rounds and `SubmitSeq` = the number
of scored entries; event `entry_scored`.

### 6.10 Events

Every event is an `EventRecord` written by the command that caused it, so
every replica holds the same events at the same slot.

| Type | Recorded by | Seen by | Fields set besides slot, type, tournament, time |
|---|---|---|---|
| `tournament_status` | `Close`, `Settle` | every entrant | `status`: `closed`, `voided` or `settled` |
| `leaderboard_changed` | a round finishing, `Close`, `Settle` | every entrant | none; at most one per tournament per slot |
| `round_started` | `StartRound` | the player | `round`, `status` `playing` |
| `round_finished` | a round finishing (move, finish or `Close`) | the player | `round`, `status` `finished`, `score`, `total_score`, `rounds_finished` |
| `entry_scored` | round 3 finishing, `Close` | the player | `total_score`, `rounds_finished` |
| `payout_available` | `Settle` | the player | `amount` |
| `payout_claimed` | `ClaimPayout` | the player | `amount` |

`total_score` and `rounds_finished` count finished rounds of the entry.

---

## 7. HTTP API

### 7.1 Conventions

#### 7.1.1 Listener and base URLs

`arena -play-listen ADDR` serves the play routes and nothing else. The
operator routes of `DESIGN.md` section 4.7 (creating, closing and settling
tournaments, ledger reads, `/v1/node`) and `/internal/paxos` stay on
`-listen`, inside the replica network: they are unauthenticated. With
`-nodes N`, node i's play listener uses the port of `-play-listen` plus i-1,
as `-listen` does.

`-play-urls 1=https://n1.play.example,2=...` gives the public base URL of
every replica's play listener, used in `Location` and `X-Arena-Leader`;
without the flag each is `http://` plus the listen address. In any
deployment beyond a laptop, TLS terminates in front of the play listener.
The listener's write timeout is at least 40 seconds, since an events request
waits up to 25.

All paths are under `/v1`. An incompatible change would add `/v2` beside it.

#### 7.1.2 Request headers

| Header | Sent on | Value |
|---|---|---|
| `Authorization` | every route except `POST /v1/session` | `Bearer <session_token>` |
| `Idempotency-Key` | every POST | 16 to 64 characters from `A-Z a-z 0-9 _ -` |
| `Content-Type` | every POST | `application/json` |

A POST body is at most 4 KiB and is exactly one JSON object of the
documented shape (section 8).

#### 7.1.3 Response headers

| Header | Present on | Value |
|---|---|---|
| `Content-Type` | every response | `application/json`, errors included |
| `X-Arena-Server-Time-Ms` | every response | the answering replica's clock in Unix ms when it wrote the response |
| `X-Arena-Node` | every response | the answering node id |
| `X-Arena-Leader` | every response | public base URL of the leader as the answering replica knows it; empty when it knows none |
| `X-Arena-Applied-Slot` | every response read from state: 2xx, recorded rejections, 404 | the answering replica's applied slot |
| `X-Arena-Slot` | every response built from a recorded result | slot of the command that produced the result |
| `X-Arena-Next-Seq` | every response built from a recorded result of a sequenced intent | the player's next sequence number as of that result |
| `Retry-After` | `409 in_flight`, `429`, `503` | whole seconds, at least 1 |
| `Location` | `307` | absolute URL: the leader's public base URL plus the request's path and query |

#### 7.1.4 Status codes and the error body

Every response whose status is not 2xx has this body:

```json
{"code":"illegal_move","message":"game: the column's top card is not one rank above or below the waste card","retryable":false}
```

`code` is from section 7.4. `message` is for developers and logs; clients
never parse it. `retryable: true` means the identical request, with the same
key and the same `seq`, may succeed later, and the client must not change
it; `retryable: false` means the answer is final for that key.

| Status | Meaning | `retryable` |
|---|---|---|
| 200 | a read, or an intent applied or replayed | |
| 201 | a join or a deal applied or replayed | |
| 307 | a POST reached a follower that knows the leader | true |
| 400 | missing or malformed key, body, path or query | false |
| 401 | missing, invalid or expired session | true, after a new session |
| 403 | `device_mismatch` | false |
| 404 | unknown route; unknown tournament, entry or round on a read | false |
| 405 | method not allowed on the route | false |
| 409 | a recorded rejection (false), or `in_flight` (true) | per code |
| 413 | body over 4 KiB | false |
| 422 | `key_reused` | false |
| 429 | `rate_limited` | true |
| 500 | `internal` | true |
| 503 | `no_leader`, `unavailable`, `replica_behind` | true |
| 504 | `outcome_unknown` | true |

#### 7.1.5 Leader redirect, and reads from any replica

Every POST is served by the leader. A follower that knows the leader answers
without looking at the token, the key or the body:

```
HTTP/1.1 307 Temporary Redirect
Location: https://n2.play.example/v1/tournaments/daily-2026-09-17/rounds/1/moves
X-Arena-Leader: https://n2.play.example
X-Arena-Node: 1
Content-Type: application/json

{"code":"not_leader","message":"node 2 leads; send this request there","retryable":true}
```

A follower that knows no leader answers `503 no_leader` with
`Retry-After: 1`. A leader that has not yet committed its leadership no-op,
or whose proposal queue is full, answers `503 unavailable` with
`Retry-After: 1`.

The client follows a redirect once and caches the leader:

1. Resend the identical request (method, path, query, body,
   `Idempotency-Key`, `Authorization`) to `Location`.
2. Cache the origin of `Location` (scheme, host, port) as the leader; send
   every later request there.
3. If the resent request is answered `307` again, do not follow it: cache
   its `Location` origin and treat the attempt as a retryable failure
   (backoff, 9.3).
4. Drop the cached leader when a request to it fails without an HTTP
   response or is answered `503 no_leader`; then use `X-Arena-Leader` of the
   most recent response if it is not empty, and otherwise the next base URL
   of the SDK's configuration, round robin.

The SDK never lets the HTTP stack follow redirects itself
(`UnityWebRequest.redirectLimit = 0`): stacks differ in whether they keep the
`Authorization` header and the body across hosts. Because every response
carries `X-Arena-Leader`, a transport that cannot surface a 307 still reaches
the leader on its next attempt.

GET routes are answered by any replica from its applied state. They are
display data (the stale reads of [ADR 0001](adr/0001-read-index-not-lease-reads.md)),
never input to a decision. A GET with `min_slot=S` waits up to 1 second for
the replica to apply slot S; if it has not by then, a follower that knows
the leader answers `307` to it (the client rule above applies) and any other
replica answers `503 replica_behind` with `Retry-After: 1`. The SDK sends
the `X-Arena-Slot` of the player's last definitive intent as `min_slot` on
round reads, so a player never sees a board older than their own last move.
The events route uses its cursor instead (7.3.10).

#### 7.1.6 Rate limits and the order of checks

Token buckets, per replica, not replicated. Since every POST reaches the
leader, the leader's buckets bound writes.

| Bucket | Keyed by | Refill | Burst | Routes |
|---|---|---|---|---|
| session-device | `device_id` | 1 per 10 s | 6 | `POST /v1/session` |
| session-address | client IP address | 1 per s | 30 | `POST /v1/session` |
| intents | player | 1 per 100 ms | 20 | join, deal, moves, finish, claim |
| reads | player | 1 per 200 ms | 10 | tournaments, round, leaderboard |
| event-polls | player | 1 per 500 ms | 4 | events |

A player has at most one open events request per replica: a new one ends the
previous one at once with `200` and no events. The client IP address is the
TCP peer's, or the first `X-Forwarded-For` address when the peer is listed in
`-play-trusted-proxies`. Over a limit the answer is `429 rate_limited` with
`Retry-After` set to the seconds until a token is available; nothing is
recorded, and the client resends the same request after that time. Human play
stays far below the limits: a round has at most 51 moves.

Order of checks, which fixes the status a request with several problems
receives:

| Step | POST | GET, on any replica |
|---|---|---|
| 1 | route and method (`404`, `405`) | route and method (`404`, `405`) |
| 2 | on a follower: `307` or `503 no_leader` | token (`401`) |
| 3 | `Idempotency-Key` present and well formed (`400`) | rate limit (`429`) |
| 4 | token, except for the session route (`401`) | path and query (`400`) |
| 5 | rate limit (`429`); the session route checks its address bucket here and its device bucket right after step 6 | `min_slot` wait (`307`, `503`) |
| 6 | body at most 4 KiB (`413`), strict decode and value shapes, path parameters (`400`) | read (`404` or `200`) |
| 7 | session route: a binding of the device in applied state whose verifier differs (`403`, not recorded) | |
| 8 | the same namespaced key in flight on this replica (`409 in_flight`) | |
| 9 | propose and wait: a result of the state machine (`2xx`, `403`, `409`, `422`), or `503`, `504` | |

Value shapes checked in step 6, before the log: `device_id`, `device_secret`,
`jurisdiction` and `age` formats; `tournament_id` of the identifier shape;
`round` a decimal integer from 1 to 3; `seq` at least 1; `move_index` at
least 0; `kind` `play` with `column` 0 to 6, or `draw` with `column` -1.
A request failing them is a client bug. Every rule that depends on state is
checked in the log and recorded (13.1).

### 7.2 Route table

| # | Route | Leader | Auth | Key | `seq` | Command | Success | Response body | Bucket |
|---|---|---|---|---|---|---|---|---|---|
| R1 | `POST /v1/session` | yes | device secret | yes | no | `OpenSession` | 200 | `SessionResponse` | session-device, session-address |
| R2 | `GET /v1/tournaments` | any | token | no | no | read | 200 | `TournamentListResponse` | reads |
| R3 | `POST /v1/tournaments/{tournament_id}/join` | yes | token | yes | yes | `Enter` | 201 | `JoinResponse` | intents |
| R4 | `POST /v1/tournaments/{tournament_id}/rounds/{round}/deal` | yes | token | yes | yes | `StartRound` | 201 | `RoundResponse` | intents |
| R5 | `GET /v1/tournaments/{tournament_id}/rounds/{round}` | any | token | no | no | read | 200 | `RoundResponse` | reads |
| R6 | `POST /v1/tournaments/{tournament_id}/rounds/{round}/moves` | yes | token | yes | yes | `PlayMove` | 200 | `RoundResponse` | intents |
| R7 | `POST /v1/tournaments/{tournament_id}/rounds/{round}/finish` | yes | token | yes | yes | `FinishRound` | 200 | `RoundResponse` | intents |
| R8 | `GET /v1/tournaments/{tournament_id}/leaderboard` | any | token | no | no | read | 200 | `LeaderboardResponse` | reads |
| R9 | `POST /v1/tournaments/{tournament_id}/payout/claim` | yes | token | yes | yes | `ClaimPayout` | 200 | `ClaimResponse` | intents |
| R10 | `GET /v1/events` | any | token | no | no | read, long-poll | 200 | `EventsResponse` | event-polls |

How a recorded result becomes a response, identically on every replica and
at any later time:

| Recorded result | Status | Body and headers |
|---|---|---|
| `ok`, first application or replay | the route's success status | the route's body below, `replayed` set accordingly; `X-Arena-Slot`, `X-Arena-Next-Seq` |
| `device_mismatch` | 403 | error body, `retryable` false; `X-Arena-Slot` |
| any other rejection | 409 | error body with the rejection's code and detail, `retryable` false; `X-Arena-Slot`, `X-Arena-Next-Seq` for sequenced intents (absent on `unknown_player`, which has no number) |
| `key_reused` (not recorded) | 422 | error body, `retryable` false |

Success bodies are rebuilt from the recorded `PlayOutcome` and replicated
state:

- R1: `player_id`, `new_player`, `issued_at_ms` and `next_seq` from the
  outcome; `jurisdiction` and `age` from the binding; `session_token` signed
  when the response is written, with the recorded claims and `exp` =
  `issued_at_ms` + the lifetime; `expires_at_ms` = `exp`. A replay is
  re-signed with the current signing key, and a replay long after the first
  application carries an expired token (9.2.3).
- R3: `join_seq` from the outcome, `entry_fee` from the rules, `rounds` 3.
- R4, R6, R7: the view of the round *as of the result*: the board replayed
  from the seed and the first `move_index` accepted moves; `status`
  `finished`, with `finish_reason`, final `score` and `seed`, only if the
  round's `finished_at` is not 0 and not above the result's slot.
- R9: `amount` from the outcome, `posting_key` `claim:<tid>:<pid>`.

### 7.3 Routes

Examples use the identifiers and cards of Appendix A. Integer types are
given as C# types: `int` fits 32 bits, `long` is 64 bits (section 8).

#### 7.3.1 R1 `POST /v1/session`

Opens a session, binding the device to a new player on first use.

Request (`SessionRequest`):

```json
{
  "device_id": "9f86d081884c7d659a2feaa0c55ad015",
  "device_secret": "2c26b46b68ffc68ff99b453c1d30413413422d706483bfa0f98a5e886266e7ae",
  "jurisdiction": "TR",
  "age": 31
}
```

| Field | Type | Rule |
|---|---|---|
| `device_id` | string | 32 lower-case hex characters |
| `device_secret` | string | 64 lower-case hex characters |
| `jurisdiction` | string | 2 to 8 upper-case letters; recorded only by the first session of the device |
| `age` | int | 0 to 150; recorded only by the first session of the device |

Response `200` (`SessionResponse`):

```json
{
  "replayed": false,
  "slot": 412,
  "next_seq": 1,
  "player_id": "p-mzxw6ytboi4dqnbrgm2wqzlmn4",
  "new_player": true,
  "session_token": "v1.k1.eyJwaWQiOiJwLW16eHc2eXRib2k0ZHFuYnJnbTJ3cXpsbW40IiwiZGlkIjoiOWY4NmQwODE4ODRjN2Q2NTlhMmZlYWEwYzU1YWQwMTUiLCJpYXQiOjE3ODkyMDAwMDAwMDAsImV4cCI6MTc4OTIwMzYwMDAwMH0.5CWAjViqDUMjFQmhATnN_JiC085QYywGbSPGTGBVjiE",
  "issued_at_ms": 1789200000000,
  "expires_at_ms": 1789203600000,
  "jurisdiction": "TR",
  "age": 31
}
```

| Field | Type | Meaning |
|---|---|---|
| `replayed` | bool | true when the key was applied before |
| `slot` | long | slot of the session command |
| `next_seq` | long | the player's next sequence number |
| `player_id` | string | the player bound to the device |
| `new_player` | bool | true when this session made the binding |
| `session_token` | string | the bearer token (2.3) |
| `issued_at_ms`, `expires_at_ms` | long | the token's claims |
| `jurisdiction`, `age` | string, int | the claims recorded in the binding, which later sessions do not change |

Errors: `400` `missing_idempotency_key`, `invalid_idempotency_key`,
`malformed_request`; `403 device_mismatch`; `409` `invalid_device`,
`invalid_player`, `player_exists` (unreachable with a well-formed request);
`422 key_reused`; `409 in_flight`; `429`; `307`; `503`; `504`.

#### 7.3.2 R2 `GET /v1/tournaments`

Lists play tournaments (those whose `game` is not empty) in creation order,
with this player's state in each.

Query: `status` = `open` (default), `closed`, `voided`, `settled` or `all`;
`offset` (default 0, at least 0); `limit` (default 50, 1 to 100).

Response `200` (`TournamentListResponse`):

```json
{
  "applied_slot": 1290,
  "offset": 0,
  "limit": 50,
  "total": 1,
  "tournaments": [
    {
      "tournament_id": "daily-2026-09-17",
      "status": "open",
      "game": "ladder-v1",
      "rounds": 3,
      "round_time_limit_ms": 300000,
      "entry_fee": 500,
      "rake_bps": 1000,
      "prize_bps": [5000, 3000, 2000],
      "min_entrants": 3,
      "max_entrants": 100,
      "min_age": 18,
      "max_score": 14400,
      "entrants": 42,
      "projected_pool": 18900,
      "joined": false,
      "eligible": true,
      "rounds_finished": 0,
      "round_in_play": 0,
      "next_round": 0,
      "created_slot": 402
    }
  ]
}
```

| Field | Type | Meaning |
|---|---|---|
| `applied_slot` | long | the answering replica's applied slot |
| `offset`, `limit` | int | the page returned |
| `total` | int | tournaments matching `status` |
| `tournament_id` | string | identifier |
| `status` | string | `open`, `closed`, `voided`, `settled` |
| `game` | string | `ladder-v1` |
| `rounds`, `round_time_limit_ms` | int, long | 3 and 300000 for `ladder-v1` |
| `entry_fee` | long | minor units |
| `rake_bps` | int | 0 to 9999 |
| `prize_bps` | int[] | shares by place, summing to 10000 |
| `min_entrants`, `max_entrants`, `min_age` | int | rules |
| `max_score` | long | 14400 |
| `entrants` | int | entries so far |
| `projected_pool` | long | before close: `entrants * entry_fee` minus the rake on it; after close: the pool |
| `joined` | bool | this player has entered |
| `eligible` | bool | open, not full, not joined, jurisdiction not excluded, age at least `min_age` |
| `rounds_finished` | int | this player's finished rounds |
| `round_in_play` | int | this player's round in play, 0 when none |
| `next_round` | int | the round this player may deal now, 0 when none |
| `created_slot` | long | slot of the creation |

Errors: `400 malformed_request`, `401`, `429`.

#### 7.3.3 R3 `POST /v1/tournaments/{tournament_id}/join`

Enters the tournament and charges the entry fee, using the jurisdiction and
age recorded in the player's binding.

Request (`SeqRequest`): `{"seq": 1}`. `seq` is a long, at least 1.

Response `201` (`JoinResponse`):

```json
{"replayed": false, "slot": 415, "next_seq": 2, "tournament_id": "daily-2026-09-17", "join_seq": 43, "entry_fee": 500, "rounds": 3}
```

| Field | Type | Meaning |
|---|---|---|
| `replayed`, `slot`, `next_seq` | bool, long, long | as in R1 |
| `tournament_id` | string | the tournament entered |
| `join_seq` | int | 1-based join order |
| `entry_fee` | long | charged, minor units |
| `rounds` | int | rounds of the entry |

Errors: `409` `unknown_player`, `stale_seq`, `seq_gap`, `unknown_tournament`,
`not_play_tournament`, `not_open`, `tournament_full`, `already_joined`,
`jurisdiction_excluded`, `underage`, `ledger_conflict`; the transport and
pre-log errors common to every POST: `307`, `400`, `401`, `409 in_flight`,
`413`, `422`, `429`, `500`, `503`, `504`.

#### 7.3.4 R4 `POST /v1/tournaments/{tournament_id}/rounds/{round}/deal`

Deals round `round` (1 to 3) after writing its seed to the log.

Request (`SeqRequest`): `{"seq": 2}`.

Response `201` (`RoundResponse`):

```json
{
  "replayed": false,
  "slot": 418,
  "next_seq": 3,
  "round": {
    "tournament_id": "daily-2026-09-17",
    "round": 1,
    "status": "playing",
    "finish_reason": "",
    "move_index": 0,
    "columns": [
      {"cards": ["Ks", "8h", "7s", "3s", "Jd"]},
      {"cards": ["2h", "4c", "5d", "7h", "Qh"]},
      {"cards": ["3c", "8s", "Ah", "8d", "Th"]},
      {"cards": ["9d", "Td", "3h", "Jh", "Qs"]},
      {"cards": ["Ts", "7c", "5s", "6d", "2d"]},
      {"cards": ["5h", "Tc", "Ad", "9s", "9h"]},
      {"cards": ["2c", "6s", "6c", "3d", "As"]}
    ],
    "waste_top": "4h",
    "waste_count": 1,
    "stock_count": 16,
    "cleared": 0,
    "score": 0,
    "playable_columns": [],
    "can_draw": true,
    "last_move": {"kind": "", "column": -1, "card": ""},
    "started_at_ms": 1789200012000,
    "deadline_ms": 1789200312000,
    "commitment": "5e188f4cb340f1c0e94c8821b8f72279b339f5698c66e659b22d32b3f9b29065",
    "seed": ""
  }
}
```

`RoundView` fields:

| Field | Type | Meaning |
|---|---|---|
| `tournament_id`, `round` | string, int | the round |
| `status` | string | `playing` or `finished` |
| `finish_reason` | string | empty while playing; `cleared`, `blocked`, `resigned`, `expired` or `closed` |
| `move_index` | int | accepted moves so far; the next move sends this value |
| `columns` | `ColumnView[]` | always 7 entries; `cards` bottom first, the last card is the top card; an empty column has `[]` |
| `waste_top` | string | the waste card |
| `waste_count` | int | cards on the waste |
| `stock_count` | int | cards left to draw |
| `cleared` | int | cards played from the tableau |
| `score` | long | the score by 4.4; provisional while playing |
| `playable_columns` | int[] | columns whose top card is adjacent to the waste card, ascending; empty when finished |
| `can_draw` | bool | `stock_count > 0` and the round is playing |
| `last_move` | `MoveView` | the last accepted move: `kind` (`play`, `draw`, or empty before the first), `column` (-1 for a draw or none), `card` (the card it put on the waste, or empty) |
| `started_at_ms`, `deadline_ms` | long | server times (4.5) |
| `commitment` | string | 64 hex characters (5.3) |
| `seed` | string | empty while playing; 64 hex characters once finished |

Errors: the codes of 6.2, then `not_open`, `not_joined`, `invalid_round`,
`round_already_started`, `previous_round_unfinished`; the common POST errors.

#### 7.3.5 R5 `GET /v1/tournaments/{tournament_id}/rounds/{round}`

The current view of one of the player's rounds, for resuming after a restart
or after a rejection.

Query: `min_slot` (long, optional, 7.1.5).

Response `200` (`RoundResponse`): as R4, with `replayed` false, `slot` = the
round record's `updated_at`, and `next_seq` = the player's next sequence
number on the answering replica.

Errors: `400 malformed_request`; `401`; `404` `unknown_tournament`,
`not_joined`, `round_not_started`; `429`; `307` and `503 replica_behind` for
`min_slot`.

#### 7.3.6 R6 `POST /v1/tournaments/{tournament_id}/rounds/{round}/moves`

Applies one move.

Request (`MoveRequest`):

```json
{"seq": 3, "move_index": 0, "kind": "draw", "column": -1}
```

| Field | Type | Rule |
|---|---|---|
| `seq` | long | at least 1 |
| `move_index` | int | the `move_index` of the view the move was chosen from |
| `kind` | string | `play` or `draw` |
| `column` | int | 0 to 6 for `play`, -1 for `draw` |

Response `200` (`RoundResponse`), the view after the move; with the deal
above:

```json
{
  "replayed": false, "slot": 421, "next_seq": 4,
  "round": {
    "tournament_id": "daily-2026-09-17", "round": 1, "status": "playing", "finish_reason": "",
    "move_index": 1,
    "columns": [ "... unchanged, as in the deal ..." ],
    "waste_top": "Kh", "waste_count": 2, "stock_count": 15, "cleared": 0, "score": 0,
    "playable_columns": [1, 3, 6], "can_draw": true,
    "last_move": {"kind": "draw", "column": -1, "card": "Kh"},
    "started_at_ms": 1789200012000, "deadline_ms": 1789200312000,
    "commitment": "5e188f4cb340f1c0e94c8821b8f72279b339f5698c66e659b22d32b3f9b29065", "seed": ""
  }
}
```

(`columns` is abbreviated here; a response always carries the seven
objects.) A following `{"seq": 4, "move_index": 1, "kind": "play",
"column": 1}` answers `move_index` 2, column 1 `["2h","4c","5d","7h"]`,
`waste_top` `Qh`, `waste_count` 3, `cleared` 1, `score` 100,
`playable_columns` `[0]`. The move that ends a round answers
`status` `finished` with its `finish_reason` and `seed`; in the Appendix A
game that is move 48, a draw of `Qd` that leaves the round `blocked` with
`cleared` 32, `score` 3200, columns 4 and 6 holding `["Ts"]` and
`["2c","6s"]`, `waste_count` 49 and `stock_count` 0.

Errors: the codes of 6.2, then `not_open`, `not_joined`, `invalid_round`,
`round_not_started`, `round_finished`, `round_expired`,
`move_index_mismatch`, `illegal_move`; the common POST errors.

#### 7.3.7 R7 `POST /v1/tournaments/{tournament_id}/rounds/{round}/finish`

Ends the round and fixes its score. Finishing a round that is already
finished succeeds and returns its final view.

Request (`SeqRequest`): `{"seq": 51}`.

Response `200` (`RoundResponse`): the final view, `status` `finished`,
`seed` set.

Errors: the codes of 6.2, then `not_joined`, `invalid_round`,
`round_not_started`; the common POST errors.

#### 7.3.8 R8 `GET /v1/tournaments/{tournament_id}/leaderboard`

Standings of any play tournament; the player need not have entered.

Query: `offset` (default 0), `limit` (default 50, 1 to 100), `min_slot`.

Response `200` (`LeaderboardResponse`):

```json
{
  "tournament_id": "daily-2026-09-17",
  "status": "open",
  "final": false,
  "applied_slot": 1400,
  "entrants": 42,
  "offset": 0,
  "limit": 50,
  "rows": [
    {"place": 1, "player_id": "p-7kq2mzl4x5b3vw6yh2r4tnq5ea", "total_score": 9300, "rounds_finished": 2, "scored": false, "amount": 0, "withheld": false, "claimed": false},
    {"place": 2, "player_id": "p-mzxw6ytboi4dqnbrgm2wqzlmn4", "total_score": 3200, "rounds_finished": 1, "scored": false, "amount": 0, "withheld": false, "claimed": false}
  ],
  "me": {"place": 2, "player_id": "p-mzxw6ytboi4dqnbrgm2wqzlmn4", "total_score": 3200, "rounds_finished": 1, "scored": false, "amount": 0, "withheld": false, "claimed": false}
}
```

| Field | Type | Meaning |
|---|---|---|
| `status` | string | tournament status |
| `final` | bool | true once the tournament is closed, voided or settled |
| `entrants` | int | number of entries |
| `rows` | `LeaderboardRow[]` | the page, by place |
| `me` | `LeaderboardRow` | this player's row, also when outside the page; `place` 0 and empty `player_id` when not entered |
| `place` | int | 1-based |
| `total_score` | long | before close: sum of finished rounds; after close: the entry's score |
| `rounds_finished` | int | finished rounds |
| `scored` | bool | the entry's score is fixed |
| `amount` | long | after settle: the payout or refund; 0 before |
| `withheld` | bool | after settle: the payout was withheld |
| `claimed` | bool | the payout was claimed |

Ranking. Before close: `total_score` descending, then `rounds_finished`
descending, then the slot at which the entry reached its total ascending,
then join order; places are distinct. From close on: the stored standings
(`DESIGN.md` section 5.4), so equal scores share a place under the `split`
tie-break.

Errors: `400`, `401`, `404 unknown_tournament`, `429`, `307` and `503` for
`min_slot`.

#### 7.3.9 R9 `POST /v1/tournaments/{tournament_id}/payout/claim`

Claims the player's settled payout, once.

Request (`SeqRequest`): `{"seq": 142}`.

Response `200` (`ClaimResponse`):

```json
{"replayed": false, "slot": 610, "next_seq": 143, "tournament_id": "daily-2026-09-17", "amount": 675, "posting_key": "claim:daily-2026-09-17:p-mzxw6ytboi4dqnbrgm2wqzlmn4"}
```

(With the greedy games of Appendix A the player used numbers 1 to 141: the
join, then a deal, the moves and a finish for each of the three rounds.)

| Field | Type | Meaning |
|---|---|---|
| `amount` | long | moved to the claims account, minor units |
| `posting_key` | string | the ledger posting's key |

Errors: the codes of 6.2, then `not_joined`, `not_settled`, `no_payout`,
`already_claimed`; the common POST errors. A resend with the same key returns
the same body with `replayed` true; a new key after success is
`already_claimed`.

#### 7.3.10 R10 `GET /v1/events`

Long-poll for changes to the player's rounds, entries, payouts, and the
leaderboards and statuses of tournaments the player entered.

Query:

| Parameter | Type | Rule |
|---|---|---|
| `cursor` | long | default 0; the `cursor` of the previous response |
| `wait_ms` | int | 0 to 25000, default 25000 |
| `tournament_id` | string | optional; limits events to one entered tournament |

Response `200` (`EventsResponse`):

```json
{
  "cursor": 612,
  "applied_slot": 612,
  "has_more": false,
  "events": [
    {"slot": 610, "type": "payout_claimed", "tournament_id": "daily-2026-09-17", "player_id": "p-mzxw6ytboi4dqnbrgm2wqzlmn4", "round": 0, "status": "", "score": 0, "total_score": 0, "rounds_finished": 0, "amount": 675, "time_ms": 1789203000000}
  ]
}
```

`EventItem` has the fields of 6.10: `slot` (long), `type`, `tournament_id`,
`player_id` (empty for events every entrant sees), `round` (int), `status`,
`score` (long), `total_score` (long), `rounds_finished` (int), `amount`
(long), `time_ms` (long); fields a type does not use are zero or empty.

Semantics:

1. Visible events are those of tournaments the player has entered whose
   `player_id` is empty or the player's own, with `slot` above `cursor`.
2. If the replica has applied past `cursor` and visible events exist, it
   answers at once. Otherwise it waits, as new slots are applied, up to
   `wait_ms`.
3. Events come in slot order, and in recording order within a slot. A
   response holds at most 100 events but never splits one slot's events. The
   `leaderboard_changed` events of one tournament in a response are reduced
   to the last one.
4. `has_more` is true when visible events remain after the last one
   returned; `cursor` is then that event's slot. Otherwise `cursor` is the
   replica's applied slot.
5. A replica whose applied slot is below `cursor` (the client came from a
   replica further ahead) waits up to `wait_ms` to catch up and answers
   `200` with no events and `cursor` unchanged if it does not. A response's
   `cursor` is never below the request's.
6. Slots are the same on every replica, so a cursor from one replica is
   valid on any other.

Errors: `400 malformed_request`, `401`, `404` `unknown_tournament` or
`not_joined` for `tournament_id`, `429`.

### 7.4 Error codes

Codes answered outside the state machine (constants in `internal/intent`):

| Code | Status | Retryable | Client action |
|---|---|---|---|
| `not_leader` | 307 | true | follow once (7.1.5) |
| `no_leader` | 503 | true | backoff, retry the same request |
| `unavailable` | 503 | true | backoff, retry |
| `replica_behind` | 503 | true | backoff, retry |
| `outcome_unknown` | 504 | true | retry the same request; the command may have been applied |
| `internal` | 500 | true | backoff, retry |
| `in_flight` | 409 | true | retry the same request after `Retry-After` |
| `rate_limited` | 429 | true | retry after `Retry-After` |
| `session_missing`, `session_invalid`, `session_expired` | 401 | true | open a new session, then retry the same request with the new token |
| `missing_idempotency_key`, `invalid_idempotency_key`, `malformed_request` | 400 | false | client bug: resynchronise (9.2.4) |
| `body_too_large` | 413 | false | client bug: resynchronise |
| `key_reused` | 422 | false | client bug: resynchronise |
| `not_found` | 404 | false | unknown route |
| `method_not_allowed` | 405 | false | client bug |

Codes recorded by the state machine (constants in `internal/tournament`), all
`retryable: false`:

| Code | Status | Routes | Meaning |
|---|---|---|---|
| `invalid_device` | 409 | R1 | device id or verifier malformed |
| `device_mismatch` | 403 | R1 | the device is bound and the secret does not match |
| `invalid_player` | 409 | R1 | claims malformed |
| `player_exists` | 409 | R1 | the drawn player id is taken |
| `unknown_player` | 409 | R3, R4, R6, R7, R9 | no binding for the token's player |
| `stale_seq` | 409 | R3, R4, R6, R7, R9 | `seq` already used |
| `seq_gap` | 409 | R3, R4, R6, R7, R9 | `seq` skips a number |
| `unknown_tournament` | 409, 404 on reads | R3 to R10 | no such tournament |
| `not_play_tournament` | 409 | R3, R4, R6, R7, R9 | the tournament takes no play intents |
| `not_open` | 409 | R3, R4, R6 | the tournament is closed |
| `tournament_full` | 409 | R3 | `max_entrants` reached |
| `already_joined` | 409 | R3 | entered before |
| `jurisdiction_excluded` | 409 | R3 | the bound jurisdiction is excluded |
| `underage` | 409 | R3 | the bound age is below `min_age` |
| `ledger_conflict` | 409 | R3 | a posting key exists (unreachable with valid identifiers) |
| `not_joined` | 409, 404 on reads | R4 to R7, R9, R10 | the player has not entered |
| `invalid_round` | 409 | R4, R6, R7 | round outside 1 to 3 (unreachable through the API, 7.1.6) |
| `round_already_started` | 409 | R4 | the round was dealt before |
| `previous_round_unfinished` | 409 | R4 | the round before is not finished |
| `round_not_started` | 409, 404 on reads | R5, R6, R7 | the round was never dealt |
| `round_finished` | 409 | R6 | the round is over |
| `round_expired` | 409 | R6 | the deadline has passed; send finish |
| `move_index_mismatch` | 409 | R6 | the client's view is not the latest; read the round |
| `illegal_move` | 409 | R6 | the rules refuse the move |
| `not_settled` | 409 | R9 | not paid out yet |
| `no_payout` | 409 | R9 | nothing to claim |
| `already_claimed` | 409 | R9 | claimed before under another key |
| `play_intent_required` | 409 | operator `Join`, `SubmitScore` | a play tournament takes entries and scores from play intents only |

---

## 8. JSON rules for JsonUtility

The client parses and writes JSON with `UnityEngine.JsonUtility`, which maps
JSON objects onto `[Serializable]` C# classes with public fields. It supports
nested serializable classes, arrays and `List<T>` of them, and the primitive
types; it does not support dictionaries, properties, nullable value types,
polymorphic fields, jagged or multidimensional arrays, or a top-level array;
it writes enums as integers; and it leaves a field that is absent from the
JSON at its initial value. Every body of the play API therefore follows
these rules, and both implementations are held to them:

1. Every body is one JSON object. A list is always a field of an object
   (`tournaments`, `rows`, `events`, `columns`, `cards`); no route returns a
   top-level array.
2. Fixed fields. Every object of a type has exactly the fields its table
   lists, and always all of them. There are no maps and no dynamic keys. A
   field is never omitted and never `null`: strings are `""`, numbers `0`,
   booleans `false`, lists `[]`, objects present with zero fields. The Go
   types in `internal/intent/wire.go` have no `omitempty`, no pointers, and
   empty rather than nil slices.
3. No polymorphism. A field has one type. Variants (move kinds, event types)
   are flat objects with a string discriminator carrying the fields of every
   variant.
4. Enumerations are lower-case strings. The C# models hold them in `string`
   fields and name the values in constant classes (11.2).
5. No floating point anywhere. Money is a long in minor units of the
   operator's one currency. Timestamps are longs in Unix milliseconds and
   their names end in `_ms`. Slots, cursors and sequence numbers are longs.
   Counts, indices, rounds, places, ages and basis points are ints. The field
   tables give the type of every field; a C# model uses exactly that type.
6. Digests and seeds are 64 lower-case hex characters; cards are two
   characters (4.1).
7. Field names are snake_case, and the C# model fields carry exactly these
   names (`public long next_seq;`), which C# naming conventions yield to.
8. Requests: the server rejects unknown fields and trailing data. The SDK's
   request classes hold exactly the documented fields, since
   `JsonUtility.ToJson` writes every public field.
9. Responses: clients ignore fields they do not know, as `JsonUtility` does.
   Within `/v1` the server may add response fields; it never removes, renames
   or retypes one.
10. Strings are UTF-8; everything but `message` is ASCII.

A C# model, as the SDK declares it:

```csharp
[Serializable]
public sealed class RoundView
{
    public string tournament_id = "";
    public int round;
    public string status = "";
    public string finish_reason = "";
    public int move_index;
    public ColumnView[] columns = new ColumnView[0];
    public string waste_top = "";
    public int waste_count;
    public int stock_count;
    public int cleared;
    public long score;
    public int[] playable_columns = new int[0];
    public bool can_draw;
    public MoveView last_move = new MoveView();
    public long started_at_ms;
    public long deadline_ms;
    public string commitment = "";
    public string seed = "";
}

[Serializable]
public sealed class ColumnView
{
    public string[] cards = new string[0];
}
```

Every field has a non-null initial value, so a model is never null inside
and the harness's `System.Text.Json` adapter writes the same JSON as
`JsonUtility` (11.6).

---

## 9. Client rules

### 9.1 What the client may keep

| Item | Kept in | For how long | Rule |
|---|---|---|---|
| `device_id`, `device_secret` | the store | for the life of the installation | sent only in `POST /v1/session`; never logged or displayed |
| `player_id`, `session_token`, `expires_at_ms` | the store | until expiry | a new session 5 minutes before expiry by server time (9.4), and on any 401 |
| last assigned `seq` | the store | forever | raised to `next_seq - 1` of every session response |
| pending intents: key, `seq`, method, path, body, creation time | the store | until a definitive answer | written before the first send |
| last definitive intent slot | the store | forever | the `min_slot` of round reads |
| cached leader base URL | the store | until a failure | 7.1.5 |
| events cursor | the store, per player | forever | advanced only after the events were handed to the game |
| the deal view and commitment of each round | the store | until the round's seed was verified | for the audit of 5.3 |
| server clock samples | memory | until pause | 9.4 |
| tournament list, leaderboard | memory | for display | refetched on relevant events and on resume; shown as possibly out of date |
| the latest round view | memory, optionally the store for a cold start | for display | replaced by a read with `min_slot` on resume |

The client never treats as settled anything it did not receive from the
server: it does not compute scores, legality, standings, prizes or balances
for any purpose beyond display hints, never predicts stock cards, and never
changes a view locally in response to a move. A tap may start an animation;
the board changes when the view arrives.

### 9.2 Intents

#### 9.2.1 Queue

1. Creating an intent draws its key, assigns `seq = last_assigned + 1`,
   builds method, path and body, and persists all of it before anything is
   sent. With 64 intents pending, creation fails locally with
   `queue_full` and assigns nothing.
2. Only the head of the queue is sent. Nothing else from the queue is sent
   while it waits; reads and the events poll are independent of the queue.
3. The head is resent byte for byte (method, path, body, key); only
   `Authorization` may change, after a new session.
4. After a restart the stored intents are resent first. Their callbacks
   died with the process, so the SDK raises `IntentCompleted` for every
   definitive answer, whether or not a callback exists.

#### 9.2.2 Answers

| Answer | Class | Action |
|---|---|---|
| 2xx | definitive | remove the head, record its `X-Arena-Slot`, deliver the body |
| 409 with `retryable` false, 403 | definitive | remove the head, deliver the error; the number was consumed (except `stale_seq`, `seq_gap`: 9.2.4) |
| 400, 404, 405, 413, 422 | definitive, client bug | remove the head, deliver the error, resynchronise (9.2.4) |
| 307 | redirect | follow once (7.1.5) |
| 401 | session | open a new session (9.2.3), then resend the head with the new token |
| any other response with `retryable` true; no response; timeout; a 5xx without a readable body | retryable | keep the head, back off (9.3), resend |

Nothing is dropped because of time: an intent resent hours later receives
its recorded result, or a rule rejection such as `round_expired`. After
three consecutive retryable failures the SDK reports a connection problem
(9.6) and keeps retrying.

#### 9.2.3 Sessions

- The SDK opens a session at start when it has no token or the token expires
  within 5 minutes by server time, before resending after a `401`, and on
  resume under the same condition.
- A session request has its own key, kept in memory for its retries; it is
  not an intent in the queue and has no `seq`.
- A session response whose `expires_at_ms` is not after the server's time
  (a replay of an old key) is followed by another session request with a new
  key.
- `403 device_mismatch` means the stored credentials do not match the
  device's binding (a corrupted or copied store). The SDK stops sending and
  reports it; nothing automatic follows, since new credentials are a new
  player.
- A `player_id` different from the stored one means the store belonged to
  another binding: the SDK fails the stored intents with `resync_required`
  and adopts the new player.

#### 9.2.4 Resynchronisation

On `stale_seq`, `seq_gap`, or a client-bug answer (400, 404, 405, 413, 422)
to an intent, the SDK:

1. fails every pending intent with the local code `resync_required`,
   delivering each to its callback and to `IntentCompleted`;
2. sets `last_assigned` to `X-Arena-Next-Seq - 1` when the answer carries
   the header, and otherwise opens a new session and uses its `next_seq`;
3. raises `Resynced`, after which the game reads the round view and the
   tournament list again before offering input.

### 9.3 Backoff and timeouts

```
delay_ms = uniform random in [0, min(8000, 250 * 2^(failures - 1))]
delay_ms = max(delay_ms, 1000 * Retry-After)       when the header is present
```

`failures` counts consecutive retryable failures of the request being
retried and returns to 0 after any definitive answer. The events poll keeps
its own count; after a normal `200` it polls again at once. Timeouts:
10 seconds for every request except events, which get `wait_ms` plus 10
seconds. A resend after a `307` or a `401` is not delayed.

### 9.4 Clock

- The client never sends its clock and never compares a server time with
  the device clock.
- It estimates the server's clock from `X-Arena-Server-Time-Ms`. For each
  response, `sample = header + rtt / 2 - local_at_receive`, where `rtt` is
  the request's round trip and both it and `local_at_receive` come from a
  monotonic clock (`System.Diagnostics.Stopwatch`), which does not jump when
  the user changes the device time. The SDK keeps the sample with the
  smallest `rtt` among the last eight; `server_now = local_now + sample`.
- Uses: the round countdown `deadline_ms - server_now`, which is display
  only, because the server decides; the session refresh margin; times shown
  in lists.
- A suspended app's monotonic clock may or may not advance, so the samples
  are cleared on resume and the first response after it sets the estimate
  again.

### 9.5 Pause, resume and network loss

- `OnApplicationPause(true)`: stop starting requests, abort the events
  poll, persist the store. An intent request already on the wire may
  complete or fail; it is in the store either way.
- `OnApplicationPause(false)`: clear the clock samples; open a session if
  needed (9.2.3); resend the head of the queue at once, with its backoff
  reset; restart the events poll from the stored cursor; raise `Resumed`, on
  which the game reads the round in play with `min_slot` and refreshes the
  leaderboard.
- Network loss is seen as requests failing without a response. Those are
  retryable failures under 9.2.2 and 9.3; the SDK does not wait for the
  platform's reachability signal.
- A killed app needs nothing: the store is the write-ahead record of every
  intent.

### 9.6 While a leader election is in progress

An election on a healthy cluster takes a few hundred milliseconds (election
timeouts of 150 to 300 ms plus one round of Phase 1, `DESIGN.md` section
3.3). The client sees it as a `307` to a replica that just failed, requests
without a response, and `503 no_leader` or `unavailable` with
`Retry-After: 1`.

| Head intent unresolved for | Show |
|---|---|
| under 1 s | nothing new; the SDK's `Busy` is true and input on the round is disabled |
| 1 to 10 s | a small "reconnecting" indicator over the board; the last server view stays; the countdown keeps running on server time |
| over 10 s | a panel saying the connection is being restored, with a way back to the lobby; the queue keeps retrying in the background |

Lists and leaderboards keep their last data with its age. Final standings
and payouts are shown only from a response with `final` true or a settled
status. No score, rejection or finished round is shown unless a server
response said so. Time lost to an election counts against the round's
deadline; 300 seconds for at most 51 moves leaves room for it (13.2).

### 9.7 Sequence

```
 client (SDK)               node 1 (follower)         node 2 (leader)                 replicated log
    | POST /v1/session  key S     |                          |                                 |
    |---------------------------->|                          |                                 |
    |<----------------------------| 307 Location: node 2     |                                 |
    | same request, once; cache node 2 as leader             |                                 |
    |------------------------------------------------------->| OpenSession  d.<device>.S ----->| chosen, applied
    |<-------------------------------------------------------| 200 token, next_seq 1           |
    | POST .../join  key J  seq 1                            |                                 |
    |------------------------------------------------------->| Enter  u.<player>.J ----------->| fee posted
    |<-------------------------------------------------------| 201 join_seq, next_seq 2        |
    | POST .../rounds/1/deal  key D  seq 2                   | seed = HMAC(secret, t, p, 1)    |
    |------------------------------------------------------->| StartRound{seed} -------------->| seed in the log
    |<-------------------------------------------------------| 201 tableau, waste, commitment  |
    | POST .../moves  key M  seq 3  move_index 0  draw       |                                 |
    |------------------------------------------------------->| PlayMove ---------------------->| checked on the board
    |    x  response lost                                    |                                 |
    | same request, same key M                               |                                 |
    |------------------------------------------------------->| key recorded: replay            |
    |<-------------------------------------------------------| 200 replayed: true, card Kh     |
    |  ... moves ...                                         |                                 |
    | POST .../finish  key F  seq 51                         |                                 |
    |------------------------------------------------------->| FinishRound ------------------->| score fixed
    |<-------------------------------------------------------| 200 final view with seed        |
    |  ... rounds 2 and 3 ...                                |                                 |
    | GET /v1/events?cursor=..  (any node, long-poll)        |                                 |
    | GET .../leaderboard       (any node)                   |                                 |
    |                        operator on the internal network: Close, Settle ----------------->|
    | POST .../payout/claim  key C  seq 142                  |                                 |
    |------------------------------------------------------->| ClaimPayout ------------------->| claim posted once
    |<-------------------------------------------------------| 200 amount                      |
```

---

## 10. Server layout (Go)

### 10.1 Types declared with the contract

Package documentation and exported types, added together with this
contract and implemented by the functions of 10.3:

| File | Declares |
|---|---|
| `internal/game/game.go` | `Name`, `LadderV1`, the dimension and scoring constants, the domain strings, `Card`, `RankChars`, `SuitChars`, `Seed`, `Commitment`, `MoveKind`, `Play`, `Draw`, `NoColumn`, `Move`, `FinishReason` and its values, `Board`, the move errors |
| `internal/session/session.go` | token constants, device constants, `KeyID`, `Key`, `Keyring`, `Claims`, `Verifier`, the verification and keyring errors |
| `internal/tournament/play.go` | `DeviceID`, `Binding`, `PlayerRecord`, the six ops, the new codes, `PlayOutcome`, `RoundStatus`, `RoundRecord`, `EventType` and its values, `EventRecord` |
| `internal/intent/intent.go` | `Backend`, `Rate`, `Limits`, `DefaultLimits`, `Config`, `Server`, route patterns, header and query names, error codes, bounds |
| `internal/intent/wire.go` | every request and response body of section 7 |

`tournament`'s import test now covers `game`, which may import no other
package of the module.

### 10.2 Dependencies

```
game                                                              <- tournament, intent
session                                                           <- intent, cmd/arena
game, jsonx, ledger, paxos, replica, replog, session, tournament  <- intent <- cmd/arena
```

`intent` imports `ledger` for the claim posting key and `replog` for the
leader's errors and roles. `game` imports only `crypto/hmac`,
`crypto/sha256`, `encoding/binary`, `encoding/hex`, `errors` and `fmt`, and
stays pure. `session` imports only the standard library. `intent` does not
import `api`; the two HTTP packages share nothing but `jsonx`.

### 10.3 Functions

The functions below are implemented as listed. The implementation adds, in
the same packages: `game.Card.Suit`, `Card.Valid`, `Board.Clone` and
`Board.WasteCard`; `session.Keyring.Validate`, `ValidKeyID`,
`ValidDeviceSecret`, `ErrBadKeySpec`, `ErrBadDevice` and `MaxTokenLen`;
`tournament.State.Progress` (an entry's dealt, finished and in-play rounds),
`State.Header` and `State.Entry` (reads that do not copy every entry),
`State.EventCount` and `ClaimableAmount`; `intent.BuildRoundViewAt`, the
view as of a result with the move count its outcome recorded, which
`BuildRoundView` calls with every accepted move; `intent.RandomPlayerID`,
`intent.NoLimits` and `intent.Config.TrustedProxies`.

```go
package game

func (c Card) Rank() int                               // 1..13
func (c Card) String() string                          // "Ah"
func ParseCard(s string) (Card, error)
func Adjacent(a, b Card) bool
func (s Seed) MarshalText() ([]byte, error)            // hex; UnmarshalText and String too, and the same for Commitment
func DeriveSeed(secret []byte, tournament, player string, round int) Seed
func Commit(s Seed) Commitment
func Shuffle(s Seed) [DeckSize]Card
func Deal(s Seed) Board
func (b *Board) Check(m Move) error                    // nil or one of the Err values
func (b *Board) Apply(m Move) error                    // Check, then move; b unchanged on error
func (b *Board) Over() (FinishReason, bool)            // Cleared or Blocked
func (b *Board) Cleared() int
func (b *Board) Score() int64
func (b *Board) Playable() []int
func Replay(s Seed, moves []Move) (Board, error)

package session

func ParseKeyring(spec string) (Keyring, error)        // "kid=hex,kid=hex" or one per line
func (k Keyring) Sign(c Claims) (string, error)
func (k Keyring) Verify(token string, nowMs int64) (Claims, error)
func NewVerifier(secretHex string) (Verifier, error)
func ValidDeviceID(id string) bool

package tournament                                      // plus codec cases for the six ops

func (s *State) Binding(d DeviceID) (Binding, bool)
func (s *State) Player(id PlayerID) (PlayerRecord, bool)
func (s *State) Round(t TournamentID, p PlayerID, round int) (RoundRecord, bool)   // deep copy
func (s *State) Events(after paxos.Slot, t TournamentID, p PlayerID, max int) []EventRecord
func (s *State) Clock() int64

package ledger

const Claim Kind                                        // "claim"
func ClaimsAccount(tid string) Account                  // "claims:<tid>"
func ClaimKey(tid, pid string) PostingKey               // "claim:<tid>:<pid>"

package replica

func (r *Runner) WaitApplied(ctx context.Context, after paxos.Slot) (paxos.Slot, error)

package intent

func New(cfg Config, b Backend, log *slog.Logger) (*Server, error)   // refuses missing keys, a short secret, missing URLs
func (s *Server) Handler() http.Handler
// BuildRoundView is the view of section 7.2 as of slot asOf (0: current), or false when the
// round does not exist. Pure: the simulator's checker calls it for invariant P3.
func BuildRoundView(st *tournament.State, t tournament.TournamentID, p tournament.PlayerID, round int, asOf paxos.Slot) (RoundView, bool)
```

### 10.4 `cmd/arena` flags

| Flag | Default | Meaning |
|---|---|---|
| `-play-listen` | empty: play API off | address of the play listener; with `-nodes`, node i uses port + i - 1 |
| `-play-urls` | `http://` plus each listen address | public base URL of every replica's play listener, `id=url,...` |
| `-play-trusted-proxies` | empty | comma-separated proxy addresses whose `X-Forwarded-For` is believed |
| `-session-keys-file` | empty: `ARENA_SESSION_KEYS` | the keyring (2.4) |
| `-session-ttl` | `1h` | token lifetime, at most `24h` |
| `-deal-secret-file` | empty: `ARENA_DEAL_SECRET` | the deal secret (2.4) |

With `-play-listen` set and no keyring or no deal secret, `arena` exits with
an error naming the missing one. With `-peers` and more than one replica,
`-play-urls` must name every replica: a process knows only its own play
address, and a follower needs the leader's to redirect.

---

## 11. Client SDK layout (C#)

### 11.1 Package

```
unity-client/
  package.json                          UPM package com.paxosarena.client, unity 2022.3
  README.md
  Runtime/
    link.xml                            preserves both runtime assemblies under IL2CPP stripping
    Core/                               pure C#: no UnityEngine, netstandard2.1, C# 9
      PaxosArena.Client.Core.asmdef
      ArenaClient.cs                    the client: sessions, queue, redirects, reads
      ArenaClientOptions.cs
      IHttpTransport.cs                 HttpRequest, HttpResponse, HttpHeader
      IIntentStore.cs                   ClientState, PendingIntent
      IJson.cs
      IntentQueue.cs
      EventFollower.cs
      ServerClock.cs                    9.4
      Backoff.cs                        9.3
      Errors.cs                         ArenaError, ArenaResult<T>, ErrorCodes, ArenaException
      Models/
        Requests.cs                     SessionRequest, SeqRequest, MoveRequest
        Responses.cs                    SessionResponse, JoinResponse, RoundResponse, RoundView,
                                        ColumnView, MoveView, ClaimResponse, ErrorBody
        Reads.cs                        TournamentListResponse, TournamentSummary,
                                        LeaderboardResponse, LeaderboardRow, EventsResponse, EventItem
        Names.cs                        TournamentStatus, RoundStatus, FinishReason, MoveKind,
                                        EventType, Headers
    Unity/                              UnityEngine adapters
      PaxosArena.Client.Unity.asmdef
      UnityWebRequestTransport.cs
      PersistentDataIntentStore.cs      under Application.persistentDataPath
      JsonUtilityJson.cs
      ArenaClientBehaviour.cs           coroutine pump, OnApplicationPause
  Samples~/
    ArenaSample/
      PaxosArena.Client.Sample.asmdef
      ArenaSample.cs                    MonoBehaviour: session, join, deal, moves, finish, leaderboard, payout
      link.xml                          copy of Runtime/link.xml, imported into Assets with the sample
  Tests~/
    Harness/
      Harness.csproj                    dotnet console app compiling Runtime/Core sources
      Program.cs                        the whole flow against a live server
      HttpClientTransport.cs
      FileIntentStore.cs
      SystemTextJson.cs
      LadderAudit.cs                    5.2 and 4.3 in C#, to verify revealed seeds
      CoreCheck/
        CoreCheck.csproj                compiles Runtime/Core alone against netstandard2.1
```

`Samples~` and `Tests~` end in `~`, so the Unity editor imports neither;
the sample is copied into a project through the Package Manager's samples
list, and the harness never enters a Unity project. Namespaces:
`PaxosArena.Client` (Core), `PaxosArena.Client.Unity`,
`PaxosArena.Client.Sample`, `PaxosArena.Client.Harness`.

`package.json`:

```json
{
  "name": "com.paxosarena.client",
  "version": "0.1.0",
  "displayName": "Paxos Arena Client",
  "description": "Client for the paxos-arena play API: device sessions, idempotent sequenced intents that survive pause and restart, leader redirects and event long-polling.",
  "unity": "2022.3",
  "license": "MIT",
  "dependencies": {
    "com.unity.modules.jsonserialize": "1.0.0",
    "com.unity.modules.unitywebrequest": "1.0.0"
  },
  "samples": [
    {
      "displayName": "Arena Sample",
      "description": "One MonoBehaviour that opens a session, joins, deals, plays, finishes, reads the leaderboard and claims a payout.",
      "path": "Samples~/ArenaSample"
    }
  ]
}
```

The two dependencies are built-in engine modules, not downloads.

Assembly definitions (only the fields that differ from the editor's
defaults):

```json
{ "name": "PaxosArena.Client.Core", "rootNamespace": "PaxosArena.Client",
  "references": [], "noEngineReferences": true, "autoReferenced": true }

{ "name": "PaxosArena.Client.Unity", "rootNamespace": "PaxosArena.Client.Unity",
  "references": ["PaxosArena.Client.Core"], "autoReferenced": true }

{ "name": "PaxosArena.Client.Sample", "rootNamespace": "PaxosArena.Client.Sample",
  "references": ["PaxosArena.Client.Core", "PaxosArena.Client.Unity"], "autoReferenced": false }
```

### 11.2 Core

Language and library limits, so that the same sources compile in the Unity
editor, under IL2CPP and in the harness: C# 9 without records or init-only
setters, no default interface methods, no `async`/`await` (the Core is
callback-driven and single-threaded), `netstandard2.1` APIs only, and
`System.Security.Cryptography` for random bytes (`RandomNumberGenerator`) and,
in the harness, SHA-256 and HMAC. No reflection beyond what `IJson` does.

Public surface:

```csharp
namespace PaxosArena.Client
{
    public sealed class ArenaClientOptions
    {
        public string[] BaseUrls = new string[0];   // public play URLs of the replicas, or one balancer URL
        public string Jurisdiction = "";
        public int Age;
        public int RequestTimeoutMs = 10000;
        public int EventsWaitMs = 25000;
        public int BackoffBaseMs = 250;
        public int BackoffCapMs = 8000;
        public int MaxPendingIntents = 64;
        public int SessionRefreshMarginMs = 300000;
        public bool FollowEvents = true;
    }

    public sealed class HttpHeader { public string Name = ""; public string Value = ""; }

    public sealed class HttpRequest
    {
        public string Method = "GET";
        public string Url = "";
        public string Body = "";                    // empty for GET
        public HttpHeader[] Headers = new HttpHeader[0];
        public int TimeoutMs;
    }

    public sealed class HttpResponse
    {
        public int Status;                          // 0 when no HTTP response arrived
        public string Body = "";
        public HttpHeader[] Headers = new HttpHeader[0];
        public bool TimedOut;
        public string TransportError = "";          // set when Status is 0
        public string Header(string name) { /* case-insensitive lookup, "" when absent */ }
    }

    // Sends one request. Must not follow redirects. Calls done exactly once, on the
    // thread that calls ArenaClient.Update.
    public interface IHttpTransport
    {
        void Send(HttpRequest request, Action<HttpResponse> done);
        void CancelAll();
    }

    public interface IJson
    {
        string ToJson(object value);
        T FromJson<T>(string json);
    }

    // Loads and atomically replaces the client's whole persistent state.
    public interface IIntentStore
    {
        ClientState Load();                         // a fresh ClientState when nothing is stored
        void Save(ClientState state);
    }

    [Serializable] public sealed class ClientState
    {
        public int version = 1;
        public string device_id = "";
        public string device_secret = "";
        public string player_id = "";
        public string session_token = "";
        public long session_expires_at_ms;
        public long last_assigned_seq;
        public long last_intent_slot;
        public string leader_url = "";
        public long events_cursor;
        public PendingIntent[] pending = new PendingIntent[0];
        public RoundAudit[] audits = new RoundAudit[0];
    }

    [Serializable] public sealed class PendingIntent
    {
        public string idempotency_key = "";
        public long seq;
        public string method = "POST";
        public string path = "";                    // e.g. /v1/tournaments/t1/rounds/1/moves
        public string body = "";
        public string route = "";                   // "join", "deal", "move", "finish", "claim"
        public long created_at_ms;                  // server time estimate, for display only
    }

    [Serializable] public sealed class RoundAudit
    {
        public string tournament_id = "";
        public int round;
        public string commitment = "";
        public string deal_view_json = "";
    }

    public sealed class ArenaError
    {
        public int Status;                          // 0: no HTTP response, or a local error
        public string Code = "";                    // section 7.4, or a local code below
        public string Message = "";
        public bool Retryable;
    }

    public static class LocalErrorCodes
    {
        public const string QueueFull = "queue_full";
        public const string ResyncRequired = "resync_required";
        public const string NoResponse = "no_response";
        public const string BadResponse = "bad_response";
    }

    public sealed class ArenaResult<T>
    {
        public bool Ok;
        public T Value;
        public ArenaError Error;
        public long Slot;                           // X-Arena-Slot, 0 when absent
    }

    public sealed class IntentOutcome
    {
        public string Route = "";
        public string IdempotencyKey = "";
        public long Seq;
        public int Status;
        public string Body = "";
        public ArenaError Error;                    // null on success
    }

    public enum ConnectionState { Online, Retrying, Offline }

    public sealed class ArenaClient : IDisposable
    {
        public ArenaClient(ArenaClientOptions options, IHttpTransport transport, IIntentStore store,
                           IJson json, Func<long> monotonicMs);

        public string PlayerId { get; }
        public bool HasSession { get; }
        public bool Busy { get; }                   // an intent is pending
        public long ServerNowMs { get; }            // 9.4; 0 before the first response
        public ConnectionState Connection { get; }
        public EventFollower Events { get; }

        public event Action<IntentOutcome> IntentCompleted;
        public event Action Resynced;
        public event Action Resumed;
        public event Action<ConnectionState> ConnectionChanged;

        public void Update();                       // call every frame: sends, retries, timers, events
        public void Pause();
        public void Resume();
        public void Dispose();

        public void OpenSession(Action<ArenaResult<SessionResponse>> done);
        public void ListTournaments(string status, int offset, int limit, Action<ArenaResult<TournamentListResponse>> done);
        public void Join(string tournamentId, Action<ArenaResult<JoinResponse>> done);
        public void Deal(string tournamentId, int round, Action<ArenaResult<RoundResponse>> done);
        public void Play(string tournamentId, int round, int moveIndex, int column, Action<ArenaResult<RoundResponse>> done);
        public void Draw(string tournamentId, int round, int moveIndex, Action<ArenaResult<RoundResponse>> done);
        public void Finish(string tournamentId, int round, Action<ArenaResult<RoundResponse>> done);
        public void ClaimPayout(string tournamentId, Action<ArenaResult<ClaimResponse>> done);
        public void GetRound(string tournamentId, int round, Action<ArenaResult<RoundResponse>> done);
        public void GetLeaderboard(string tournamentId, int offset, int limit, Action<ArenaResult<LeaderboardResponse>> done);
    }

    public sealed class EventFollower
    {
        public long Cursor { get; }
        public string TournamentId { get; set; }    // "" for every entered tournament
        public event Action<EventItem> Received;
    }
}
```

`Join`, `Deal`, `Play`, `Draw`, `Finish` and `ClaimPayout` create intents
(9.2); the reads and `OpenSession` are plain requests with the same retry,
redirect and session handling. `ArenaClient` owns the queue, the session, the
leader cache, the clock and the follower, and persists `ClientState` through
the store after every change to it. `IntentQueue` is internal to the Core
apart from what `ArenaClient` exposes. The Core never blocks, starts no
threads and reads time only through `monotonicMs`.

### 11.3 Unity adapters

- `UnityWebRequestTransport`: `new UnityWebRequest(url, method)`; for a POST
  an `UploadHandlerRaw` of the UTF-8 body with `contentType`
  `application/json`; a `DownloadHandlerBuffer`; every header through
  `SetRequestHeader`; `timeout` = `TimeoutMs` rounded up to whole seconds;
  `redirectLimit = 0`. On completion it copies `responseCode`,
  `GetResponseHeaders()` and the text whenever `responseCode` is not 0, a
  protocol error included; otherwise `Status` 0 with `TransportError` and
  `TimedOut`; then disposes the request and calls `done`.
  `UnityWebRequest` completes on the main thread, which is the thread that
  pumps the client. Whether each target platform surfaces a 307 with
  `redirectLimit = 0` as a response is verified on device during
  integration; if one does not, `X-Arena-Leader` (7.1.5) still leads the
  next attempt to the leader.
- `PersistentDataIntentStore`: the file
  `Path.Combine(Application.persistentDataPath, "paxos-arena", "client-state.json")`.
  `Save` writes `client-state.json.tmp`, flushes it to disk, then replaces
  the file (`File.Replace` with a `.bak` backup where the platform supports
  it, otherwise delete and move). `Load` reads the file, falls back to the
  `.bak`, and returns a fresh state when neither parses. A game that keeps
  the device secret in the platform's keystore implements `IIntentStore`
  itself.
- `JsonUtilityJson`: `JsonUtility.ToJson(value)` and
  `JsonUtility.FromJson<T>(json)`.
- `ArenaClientBehaviour`: serialized fields `baseUrls`, `jurisdiction`,
  `age`, `followEvents`. `Awake` builds the client from the three adapters
  and a `Stopwatch`; `OnEnable` starts a coroutine that calls
  `Client.Update()` and yields `null` every frame, which drives sends,
  retries and the events long-poll; `OnApplicationPause(bool)` calls `Pause`
  or `Resume`; `OnDisable` stops the coroutine; `OnDestroy` disposes the
  client.

### 11.4 Sample

`ArenaSample` is one `MonoBehaviour` using `ArenaClientBehaviour` and IMGUI
(`OnGUI`), so it needs no UI package. Buttons in flow order: open session,
list tournaments, join the first eligible one, deal the next round, one
button per playable column and a draw button (both taken from the view, with
`move_index` from the view), finish, leaderboard, claim payout. It shows the
current view as text, the queue's `Busy` state, the connection state and
the last ten events, and on a finished round shows whether the commitment
matched the revealed seed. For a local server the player setting "Allow
downloads over HTTP" must permit `http://` in development builds.

### 11.5 IL2CPP stripping

`Runtime/link.xml`:

```xml
<linker>
  <assembly fullname="PaxosArena.Client.Core" preserve="all"/>
  <assembly fullname="PaxosArena.Client.Unity" preserve="all"/>
</linker>
```

Managed code stripping removes code it sees no static use of. The model
classes are created only by `JsonUtility`, so a high stripping level could
remove their constructors or fields. Unity documents `link.xml` files under
`Assets`; the sample carries a copy so importing it puts one there, and a
project that does not import the sample copies `Runtime/link.xml` to
`Assets/PaxosArena/link.xml`. The model classes are also marked with a
`PreserveAttribute` declared inside the Core, which the Unity linker
recognises by name without a reference to `UnityEngine`. Both must be
checked with a stripped IL2CPP build during integration.

### 11.6 Harness

`Tests~/Harness` is a console project (`net8.0` or later, `LangVersion` 9.0,
nullable disabled, `EnableDefaultCompileItems` false) that compiles its own
files and `../../Runtime/Core/**/*.cs`, and implements the three interfaces
with `HttpClient` (redirects disabled), files in a temporary directory, and
`System.Text.Json` with `IncludeFields`. `CoreCheck/CoreCheck.csproj` compiles
the Core sources alone as a `netstandard2.1` library with C# 9, which catches
an API or syntax Unity lacks.

```
dotnet run --project unity-client/Tests~/Harness/Harness.csproj -- \
    --play http://127.0.0.1:9081 --operator http://127.0.0.1:8081 --players 3
```

The flow, each step asserted, exit code 0 when all pass:

1. Operator API: create a `ladder-v1` tournament with a unique id,
   `min_entrants` equal to `--players`, `max_score` 14400.
2. For each player, with its own store directory: a session (`new_player`
   true); a second session with a new key (same `player_id`, `new_player`
   false); the tournament listed as `eligible`; join.
3. For rounds 1 to 3: deal (35 cards, `stock_count` 16, a commitment); play
   the lowest playable column or draw until the round finishes, checking
   `move_index` and `playable_columns` on every view; finish again
   (idempotent); verify the commitment against the revealed seed, reshuffle
   with `LadderAudit`, and compare the deal and the replayed final board and
   score with the server's views.
4. Replays and sequence errors: resend the last move with its key and compare
   the body (`replayed` true); send a used `seq` under a new key
   (`409 stale_seq`) and a skipped one (`409 seq_gap`), then check the
   client resynchronised.
5. Restart: dispose one player's client between creating an intent and its
   answer, build a new client on the same store, and check the intent is
   resent with the same key and completes through `IntentCompleted`.
6. Leaderboard ranks match the players' totals; operator close and settle.
7. Events from cursor 0 per player: three `round_started`, three
   `round_finished`, one `entry_scored`, `tournament_status` `closed` and
   `settled`, and `payout_available` for each paid player.
8. Claims: each paid player claims; the sum equals the pool minus withheld
   amounts; a new key is `already_claimed`; the same key is replayed.

Step 3 also plays one column the rules refuse (player 0, at the first view
that has one): `409 illegal_move`, `X-Arena-Next-Seq` one above the rejected
number, the board unchanged, no resynchronisation. Step 4 compares the
replayed response with the first one byte for byte except `replayed`,
including `X-Arena-Slot` and `X-Arena-Next-Seq`, and sends the last move's
body under a new key (`409 stale_seq`, answered identically when resent
with that key). Step 8 ends with the tournament's ledger read through the
operator API: one fee per entrant, one prize posting per paid place, every
posting key once, and exactly one claim posting per paid player for the
claimed amount.

With `--kill-leader-command CMD` the harness runs CMD (for example a
`kill` of the leader's process) in the middle of step 3 and requires the
flow to finish anyway. It does so while a move is unanswered: player 0's
client stores and sends the move, the leader applies it and answers, the
answer is not handed to the client, CMD kills the leader, and the client is
disposed as the app would die. A client rebuilt from the same store, with
the killed leader still cached and base URLs ordered killed leader,
follower, new leader, must resend the move with its key, get no response
from the killed leader, follow the follower's `307`, and receive the
recorded result from the new leader with `replayed` true and the first
answer's slot. This needs one `--operator` URL per `--play` URL, in node
order.

`scripts/e2e.sh` builds `arena` and the harness, starts three replicas as
separate processes on `127.0.0.1` with `-wal` and `-play-listen`, runs the
flow, runs it again with a CMD that kills the ready leader with `SIGKILL`,
restarts the killed replica from its wal file, and requires every replica to
report the same applied slot and state hash after each run. It prints a
`SKIP` line and exits 0 when no .NET SDK is installed.

---

## 12. Invariants, chaos scenarios and tests

New invariants for the simulator's checker, evaluated after every apply on
every replica, beside S1-S8 and D1-D6:

| # | Invariant |
|---|---|
| P1 | No double claim: at most one `claim` posting per (tournament, player); its amount equals the player's non-withheld payouts; claims exist only for settled tournaments. |
| P2 | Sequence discipline: for every player, the sequence numbers of commands that consumed one are 1, 2, 3 and so on in slot order, with no repeat and no gap; no command with `stale_seq` or `seq_gap` changed any state but the results table. |
| P3 | Commitment before reveal: every view built for a round was built from a state whose applied slot is at or above the round's `StartedAt`, shows no stock card that was not drawn, and carries a seed only if the round was finished as of the view's slot. The view builder (`intent.BuildRoundView`) is a pure function of the state, so the checker calls it for every applied round command and every simulated client read. |
| P4 | Server-computed scores: every finished round's `Score` equals the score of `game.Replay(Seed, Moves)`, every accepted move replays legally, and every scored play entry's score is the sum of its rounds. |
| P5 | Deterministic deals: every round's `Seed` equals `game.DeriveSeed` of the simulation's secret, tournament, player and round. |
| P6 | Deadlines: no move was accepted with the state clock past its round's `DeadlineMs`, and the state clock never decreases. |

New scenarios for `cmd/chaos` and `sim.TestScenarios`:

| Scenario | Schedule | Checked |
|---|---|---|
| `duplicate_intents_after_leader_change` | the leader crashes after proposing a move and a claim; the client resends both keys to the new leader, and the old proposals are later re-proposed in higher slots | S8, P1, P2: one application per key, replays identical, no extra claim |
| `stale_sequence_replay` | a client replays earlier intents under new keys and sends skipped numbers, during partitions | P2: all rejected, no state change beyond the results table |
| `partition_during_payout_claim` | the leader is partitioned from the majority while claims are in flight; clients retry on both sides | P1, D6: exactly one claim each; the minority leader's proposals are not chosen |
| `token_expiry_mid_round` | a player's app is suspended in the middle of a round, with a move stored and a session request in flight, for longer than the token lives; the leader crashes meanwhile | the resent move is refused `session_expired`, the replayed session carries an expired token, a session with a new key follows, and the move is then applied with its key and number (P2) |
| `deal_during_leader_change` | leadership changes between proposing a `StartRound` and applying it | P3, P5: no view before the apply; a retry deals the same cards |

How the simulator implements them. `internal/sim` has play clients beside
its operator clients; the play scenarios run play clients only. A play client creates a `ladder-v1` tournament, then
for each of its players opens a session, enters, deals round 1, sends two to
eight moves (about one in seven names a column the rules refuse and must
be answered `illegal_move`) and finishes the round; it closes and settles the
tournament and claims every player's payout, sometimes twice. Rounds 2 and 3
are never dealt, so `Close` scores each entry with round 1 (4.5). The
client talks to each node through a model of the play API: a follower
redirects, the leader verifies the token at its own clock, draws the player
id of a session, derives the seed of a deal from a simulation deal secret,
stamps `received_at`, proposes, and builds every round view with
`intent.BuildRoundViewAt` as of the result. Play clients also read round
views from random replicas, including ones that have not applied the deal.
The checker evaluates P1, P2, P4, P5 and P6 after every applied play
command on every node, P1, P4 and P5 again on every play tournament of
every live replica at the end of the run, P3 on every view built, and S2
once more at the end: every live replica at the same applied slot holds the
same state hash. In
`partition_during_payout_claim` it also fails the run when a replica cut off
with the old leader commits a slot that no majority acceptor had accepted
at the split. `sim.TestPlayInvariantsCatchPlantedFaults` plants a fault for
P1, P2, P3, P5 and P6 and requires the checker to report it.

Tests the implementations add:

- `game`: Appendix A vectors; adjacency including king and ace; every move
  error; auto-finish on clear and on block; a property test that replaying
  any legal move sequence reproduces `Apply`; the score bound.
- `session`: the Appendix A token; each verification failure of 2.3,
  including the 30-second leeway edges; keyring parsing errors; rotation
  (a token signed by the second key verifies).
- `tournament`: the validation order of every play command, one case per
  code; replays with the server-drawn fields changed; `Close` finishing
  rounds and scoring entries; events per command; hash determinism with play
  commands; `FuzzDecode` seeds for the six ops.
- `intent`: the route table as a handler table test with a fake `Backend`;
  the order of checks of 7.1.6; 307 on followers; recorded responses rebuilt
  byte for byte; every body with no `null` and no missing field (decoding
  with `DisallowUnknownFields` into the wire types and re-encoding); events
  cursor rules 1 to 6; rate limits under `testing/synctest`.
- `cmd/arena`: three processes with `-play-listen`, a full play flow over
  HTTP including a leader kill, and refusal to start without keys or secret.
- `unity-client`: the harness of 11.6 against the three-process cluster.

---

## 13. Decisions, and what the contract does not cover

### 13.1 Decisions

- Redirect instead of forwarding. The operator API forwards a follower's
  request to the leader ([ADR 0010](adr/0010-api-status-codes.md)); the play
  API answers `307`. A mobile client makes many small writes, and caching the
  leader removes a hop from each; the leader authenticates, rate-limits and
  answers once; followers hold no connections on behalf of clients.
- A separate listener. The operator API has no authentication and stays on
  the replica network; the play listener is the only surface exposed to
  devices.
- Shapes before the log, rules in it. Malformed values are refused before
  proposing, as malformed bodies already are. Every rule that depends on
  state, including the sequence rules, runs in the state machine and is
  recorded, which keeps [ADR 0012](adr/0012-bounds-on-commands.md)'s reason:
  a rejection outside the log would not be recorded under its key. The one
  exception, the `403` device pre-check, only repeats a check the log makes
  anyway and saves a slot per brute-force attempt.
- Rule rejections consume sequence numbers, so a client can queue intents
  without renumbering after a rejection.
- Sessions go through the log. A binding must be replicated, one command
  type gives sessions the same idempotency as every other intent, and the
  issue time comes from the log. It costs one slot per session, about one an
  hour per active player.
- Per-player deals from a deterministic HMAC seed (section 1), committed in
  the log and revealed at the end of the round.
- Responses rebuilt, not stored. The results table stays small, and a replay
  is identical on every replica because it is a function of replicated state.
- A three-field error body instead of RFC 9457 problem details, because the
  client parses it with `JsonUtility` into one fixed class.
- Events in replicated state with the slot as cursor, so any replica can
  continue a stream another replica started.
- Settled during implementation. `Result.Play` is a value with `omitzero`
  rather than a pointer: the JSON encoding is the same, a `Result` stays
  comparable (the simulator's checker compares results with `==`), and a
  caller can never write through a result into the results table. A token
  whose expiry precedes its issue time is `session_invalid`, like one whose
  lifetime is too long. A round view built for a recorded result replays
  the move count the result recorded (`BuildRoundViewAt`), because a round
  record keeps its moves but not the slot of each. `unknown_player` carries
  no `X-Arena-Next-Seq`, since the player has no number. In one replica per
  process mode `-play-urls` is required. The simulator's D3 check leaves
  claim postings out of a settled tournament's snapshot: a claim is the one
  posting that follows a settlement, and it moves money out of a player
  account, not out of the tournament.
- Settled with the simulator and the end-to-end run. The play invariants
  are evaluated for play commands only, so the operator scenarios and the
  random schedule, which send none, report the fourteen invariants they did
  before and `cmd/chaos` prints P1-P6 only when a run evaluated them. P5
  checks seeds against the simulation's own deal secret. A play scenario
  deals round 1 of each entry only; the rules of rounds 2 and 3 are the
  same code, which the unit tests and the end-to-end run cover. The
  scenario list adds `token_expiry_mid_round` to the four planned ones: a
  token that expires while a round is in play is the one session path the
  other scenarios never reach. The harness's `--kill-leader-command` step kills the
  leader while a move's answer is undelivered and rebuilds the client from
  its store, rather than killing it between two moves, so the resend is a
  replay of a result recorded by the killed leader.

### 13.2 Not covered

- Move suggestions by software on the device. The server enforces legality,
  not who chose the move; detecting assisted play is analysis of the
  recorded moves, not part of this contract.
- One person with several devices or accounts, device attestation, and
  account recovery or transfer after a reinstall; device secret rotation.
- Display names or any other personal data beyond the jurisdiction and age
  claims, which remain claims taken as given.
- TLS on the play listener (terminated in front of it) and on the replica
  network (`README.md`, further work).
- Push notifications and WebSockets; the events long-poll is the only push
  path.
- Leaving a tournament. An entry fee comes back only if the tournament voids.
- Refunding time lost to an outage or an election against a round's deadline.
- Pruning: the results table, round records and events grow for the life of
  the log, like the rest of the state (`DESIGN.md` section 9).
- Replicated rate limits; each replica counts on its own.
- A leaked deal secret makes every future deal predictable until it is
  rotated; there is no per-round randomness beyond it.
- More than one currency, and localised error messages.

---

## Appendix A. Test vectors

Computed from the definitions in sections 2, 4 and 5. The token, the
verifier, the seeds, the commitments and the decks were computed by two
independent programs, which agree; the greedy games by one of them. All hex
is lower case.

Session token (2.3):

```
key id     k1
key        000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f
payload    {"pid":"p-mzxw6ytboi4dqnbrgm2wqzlmn4","did":"9f86d081884c7d659a2feaa0c55ad015","iat":1789200000000,"exp":1789203600000}
token      v1.k1.eyJwaWQiOiJwLW16eHc2eXRib2k0ZHFuYnJnbTJ3cXpsbW40IiwiZGlkIjoiOWY4NmQwODE4ODRjN2Q2NTlhMmZlYWEwYzU1YWQwMTUiLCJpYXQiOjE3ODkyMDAwMDAwMDAsImV4cCI6MTc4OTIwMzYwMDAwMH0.5CWAjViqDUMjFQmhATnN_JiC085QYywGbSPGTGBVjiE
```

Device verifier (2.1):

```
secret     2c26b46b68ffc68ff99b453c1d30413413422d706483bfa0f98a5e886266e7ae
verifier   eefcb1443d2b7d069bf765df3584178eb2b4c9b5125242a75bf53c0c7e3da2b8
```

Deals (5.1 to 5.3), deal secret
`202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f`,
tournament `daily-2026-09-17`, player `p-mzxw6ytboi4dqnbrgm2wqzlmn4`:

```
round 1
  seed         1898590d493e4a523bd5ff782956ceeba5cd1cbcf09d5158ee54a01dc4588377
  commitment   5e188f4cb340f1c0e94c8821b8f72279b339f5698c66e659b22d32b3f9b29065
  block 0      969af5994aa697c0... (first 8 bytes)
  deck         Ks 8h 7s 3s Jd 2h 4c 5d 7h Qh 3c 8s Ah 8d Th 9d Td 3h Jh Qs Ts 7c 5s 6d 2d 5h
               Tc Ad 9s 9h 2c 6s 6c 3d As 4h Kh Js Jc 5c Kd Kc 8c Ac 7d 4s 9c Qc 4d 2s 6h Qd
  waste        4h
  stock        Kh Js Jc 5c Kd Kc 8c Ac 7d 4s 9c Qc 4d 2s 6h Qd

round 2
  seed         172b3ace53308aebc0a1219562d3f796f1f3e8a3bf443645a9d3439421be80c8
  commitment   e1662e3bf2f91bbfb5405a7198050114e24904d5bc77717c176c57cd4af16398
  deck         Qh Qs 6s 7c Ac 7h 8s Js 9h 3s Jc 5h 4c 4s 9c 3d Ah Jd 4d Ks 6c Kh Kd Qc Tc 2c
               8d 3c 7s 2d 4h 6d 8h Jh 7d 5s Td 8c Ad 2s 5d Th Kc Qd 2h Ts 6h 5c 3h 9d 9s As

round 3
  seed         995b407ebad8f37697d9db8c1b696976f821f3a4430b11d8e70eedb36750fcbc
  commitment   cd49d8cc8ab27eed2a5e78d5cf83f0173e96d74525faf9eb33f1d42767753578
  deck         Kc Ac 8s Ah Tc 5d 3h 4s 6c 8c Jd 4d Qc 5c 2h 6s 5h 8d 7s 2c 6d 7h 9h Qh 4c 6h
               Kd 5s As 3s Jc Qd Ks 9c 3c 8h 9d Ad Th Js 7d Td 4h Jh 2d Kh 2s 9s 3d Ts 7c Qs
```

Columns are dealt from the deck as in 4.2: in round 1, column 0 is
`Ks 8h 7s 3s Jd` and column 6 is `2c 6s 6c 3d As`.

Greedy play (always the lowest playable column, otherwise draw) as a check
of 4.3 and 4.4; `d` is a draw and `pN` a play of column N:

```
round 1  48 moves (32 plays, 16 draws), blocked, cleared 32, score 3200, waste card Qd
         d p1 p0 p2 p5 p2 p1 d p3 p3 d d d p2 p4 p0 d p6 d p0 p0 p5 p2 d p0 p5 d p4
         p1 p1 p2 p1 p3 d p4 d p3 p3 p5 d d p5 d p6 d p4 p6 d
round 2  42 moves (26 plays, 16 draws), blocked, cleared 26, score 2600, waste card Jh
         d p2 p4 d p6 d p3 p0 p5 p1 p2 d d p2 p2 p3 d p1 d p4 p1 d p2 d d p3 d p0 p0 p5
         p1 p1 d d d d d p4 p0 p4 p0 p6
round 3  44 moves (28 plays, 16 draws), blocked, cleared 28, score 2800, waste card Jc
         d p0 d p2 p0 p3 p5 p4 p2 d d p2 d p0 p3 p1 d d p6 p2 d p4 p2 d p0 p0 p5 d d d
         p3 p4 d d p6 d p1 p3 p1 p1 d p6 p6 p6
```

The entry total of that player is 8 600.
