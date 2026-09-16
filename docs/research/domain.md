# Domain research: backend requirements of paid, skill-based competitive card games

Status: research notes, written 2026-09-16.

Scenario: a small studio runs paid tournaments for a skill-based competitive
mobile card game. This note collects what public material says the backend of
such a game must do, separates the problems that a replicated log solves from
the problems it does not, and ends with a problem statement for a settlement
service.

Conventions. Sources are cited as `[n]` and listed at the end. In the prose,
publishers are described by role (a skill-gaming platform's developer
documentation, a payment processor's API reference, a national gambling
regulator's technical standards) rather than by name; the URL identifies them.
Quotations are verbatim from the cited page as fetched on 2026-09-16, with
names replaced by bracketed role words where needed. Where a claim rests on a
paraphrase rather than a quotation, the sentence says so or carries no
quotation marks.

Limits. Two operator support pages (eligibility list, match flow) refused
automated fetches, so the excluded-jurisdiction facts come from the operator's
annual report instead. The regulator's technical standards cited here govern
licensed chance-based gambling; they are used as the clearest written
statement of what a paying entrant expects, not as law that binds a skill game.

## 1. What "skill-based" commits the backend to

### 1.1 The legal frame

Gambling statutes in most US states treat an activity as gambling when three
elements are present: "(1) the award of a prize, (2) paid-in consideration
(meaning entrants pay to compete), and (3) an outcome determined on the basis
of chance" [1]. A paid tournament avoids the third element only if skill, not
chance, decides the outcome. Two tests are used. The predominance test is "the
most commonly used indicator of whether a game is skill- or chance-based": a
game qualifies if it sits "predominantly closer to the skill end of the
continuum" [1]. A minority of states, "8 states" according to one platform's
legal summary, apply a material element test that asks "whether chance plays a
material role in determining a game's outcome" even where skill predominates
[1][2]. The number of states that restrict cash skill gaming is around a dozen
and changes over time [3].

Backend consequences: (a) every element of chance in the game must be
neutralised across entrants, so that the difference in outcome between two
entrants is attributable to their play; (b) which jurisdictions are permitted is
not fixed and has to be treated as versioned data (section 7).

### 1.2 Identical deals: one shuffle seed for all entrants

The standard approach is to give every entrant the same sequence of random
values. One platform's developer documentation: "the numbers returned by
[the platform] are in the same sequence for each player in a match", which
"gives developers the means to ensure that each competitor in a match receives
the exact sequence of random values to maintain fairness" [4]. The same page
warns that "a game which uses this functionality for more than one logical
component may introduce a fairness issue" when the components are consumed at
different points by different players, and advises games to "request enough
random numbers to satisfy the theoretical maximum number needed" before play
begins [4]. On that platform, "To be eligible to host real prize tournaments,
all games that utilize random elements must have [the seeded generator]
integrated and functioning", and a game must be shown to be "more than 65%
skill versus chance" over at least 1,000 completed games before real prizes are
enabled [5].

A national gambling regulator's standard on random outcomes states the same
engineering rule from the other side: random numbers "are to be used in the
order in which they are received and they may not be discarded due to adaptive
behaviour", and "Adaptive behaviour (that is, a compensated game) is not
permitted" [6].

Backend consequence: the seed (equivalently, the shuffled deck) is generated
server-side per tournament, stored with the tournament record before the first
entrant plays, and delivered to each client on entry. The server must be able
to reproduce the deal from the stored seed at validation time. If the client
chooses the seed, or if the deck is derived by a floating-point or
platform-dependent procedure, "identical deal" cannot be proven.

### 1.3 No adaptive bots; real entrants only

The same developer documentation: "All [platform] real-money games, without
exception, are ones in which real players are in competition with other real
players for prizes. Not an AI taking the place of a player, and not a re-use of
a player score from an older game." [4]. Developer-built AI is allowed only as
"agents within the game experience that facilitate gameplay" [4].

A 2026 US jury verdict in a false-advertising suit between two skill-gaming
operators shows the cost of departing from this. The evidence described two
kinds of bots: liquidity bots that "fill in user slots" so a tournament "won't
stay open for too long", and tailored bots used where the operator "want[s] the
player to finish at some predefined rank", which required "control[ling] the
scores of the rest of the players" [7]. The court noted the operator "could
prevent players from winning–or allow them to win–no matter how they performed
in the game" [7]. The jury found the operator "used bots to post preselected
scores attributed to nonexistent players" and awarded $420 million in damages
[8]. At the pleading stage the court had already held that the words
"players", "individuals", "winners", "fair" and "skill-based" "may be found by
a jury to imply that [the operator's] games of competition are conducted among
human players only and not among humans and bots" [9]; legal commentary draws
the lesson that language implying human-only competition is actionable if bots
are present [10].

Backend consequences: every score that can enter a paid match references an
authenticated human account and one play session with its own input log; the
matchmaking rule is a documented function of stored ratings, not an
outcome-steering mechanism; and any automated opponents used in free practice
modes are structurally unable to appear in a paid-tournament score table. The
distinction between "AI that facilitates gameplay" and "AI that takes a
player's place" has to be enforced in data, not in policy text.

### 1.4 Matching against real players who played the same deal

In the asynchronous format, "players do not have to wait to be matched with
another person and compete in realtime. Instead, any time a player enters a
tournament they will immediately be put into a game. Their score will be
recorded and matched against another player with a similar rating. If a match
isn't immediately found, [the platform] will hold on to that player's score and
wait for another player with matching skill to enter the same tournament."
[11]. Because both play the same seed, a score can be paired with a later score
on the same deal.

Backend consequence: a submitted score is a durable, unpaired record that may
wait minutes or hours; pairing is a state transition (unpaired to paired to
resolved) that must happen exactly once per score; and the deal identity
(tournament id plus seed version) is part of the pairing key. Disconnects and
aborts are scored, not dropped: after a disconnect longer than the reconnect
timer "that user should be reported as an abort" and "their opponent should
submit whatever their score currently is, and so win" [12].

## 2. Money: entry fees, prize pools, rake

Rake is "the scaled commission fee taken by a cardroom operating a poker game";
for tournaments it "is collected as an entrance fee", shown as buy-in plus fee,
for example "$100+$20, with the $20 being the house fee", and the fee does not
enter the prize pool [13]. Skill-gaming platforms use the same structure. One
platform's annual report states that the platform "and its developers share in
the aggregate entry fees paid by end users", and that "Prior winnings
represented more than 84% of total paid-entry fees for the year ended December
31, 2024" [14]: most entry fees are paid from earlier winnings rather than
fresh deposits, so balances of different kinds (deposits, winnings, bonus
credit) circulate through the same tournaments. For developer-funded events
the developer receives "70% of the Entry Fees collected", must hold "at least
as much cash in their Wallet as the Prize Pool they want to specify" before
creating the event, and the developer's "effective revenue before taxes is 70%
of Entry Fees minus the Prize Pool paid out to the players" [15]. Prizes are
distributed by placement, for example "$5, $3, and $1, for a total Prize Pool
of $9" [15]. Tournament shapes include head-to-head, multi-player where "a lone
winner takes all", and brackets [11].

Backend consequences:

- Entry is a debit from a player balance of a specific fund type; the prize
  pool and the rake are separate accounts; the three must reconcile (for a
  fixed pool: sum of entry fees equals prize pool plus rake, or the pool is
  pre-funded and the rake is the residual).
- The prize pool is fully funded before the tournament opens; a payout can
  never exceed the pool.
- Ledger entries are double-entry and append-only, with the idempotency key
  enforced as a uniqueness constraint on the posting so that a replayed request
  cannot write a second set of entries; balances are derived from entries, not
  stored as a mutable field [16][17].
- Fund types carry different withdrawal rules (bonus credit is typically not
  withdrawable), so a payout record states which balance it credits.

## 3. Score submission and validation

### 3.1 The client cannot be the authority

A mobile client is under the player's control. The rule from multiplayer
networking: "the game state is managed by the server alone. Clients send their
actions to the server"; the server "knows" the true value even when "a hacked
client can modify its local copy of that value" [18]. Another engineering guide
puts the test as "if players can gain advantage by lying, it should run on the
server" and warns that otherwise "the client is effectively defining reality,
and the server is just logging it" [19]. The mobile application security
standard classifies client-side anti-tampering as defence in depth: "The
absence of these measures does not in itself constitute a vulnerability", and
"Security must rely on verifiable design, strong cryptography, and server-side
validation" [20].

The platform SDK model, in which the game calls a submit-score method and a
fallback if it fails [21], is client-reported by design. The platform
recommends that developers add their own measures, for example "copy key data
into separate variables and then compare the copy to the original" and
"obfuscate a variable or encrypt it" before writing to memory [22]. Those
measures raise the cost of the simplest memory edits; they do not make a
reported score trustworthy.

### 3.2 Replay: the seed plus the input log is the proof

A card game with a fixed deal is a deterministic system: "given the same
initial condition and the same set of inputs your simulation gives exactly the
same result" [23]. That makes input replay a practical validation method: "A
replay system records the complete sequence of inputs during a session and
stores them server-side. The server can then re-simulate the session
deterministically from those inputs to verify the resulting score." [24]. The
usual obstacle is non-determinism from "floating-point differences across
hardware, random number generators, or physics engines with platform-dependent
behavior" [24]; it is "incredibly naive to write arbitrary floating point code
in C or C++ and expect it to give exactly the same result across different
compilers or architectures" [25]. A card game's rules are integer and discrete,
so the studio can keep floating point out of the rules engine entirely and
share one rules implementation between client and server.

Backend consequence: the score the server accepts is the score it computes by
replaying (seed, input log) through the rules engine. The client's claimed
score is a hint used to detect disagreement. A checksum of the final state can
be exchanged as a cheap first check; a disagreement escalates to full replay
and review. Timing constraints (a session cannot exceed the tournament's
per-game clock; inputs cannot be timestamped in the future) are enforced
server-side. When two live players must not see each other's move first, the
classic remedy is a commit-reveal step in which each announces "a
cryptographically secure one-way hash of its decision as a commitment" before
revealing it [26]; in the asynchronous format this matters less, because the
deal is fixed and the opponent's score is hidden until pairing.

### 3.3 Anomaly detection, with care

Statistical flags are a second layer: "Flag any score that is more than 3
standard deviations above the mean for review", and a player who "goes from
rank 5,000 to rank 1 in a single session" with no history is more suspicious
than one who improved gradually [24]. The same source cautions that "Removing
a legitimate score from a competitive player without warning or explanation is
a community trust failure" and recommends pulling the session log and
validation results first [24]. A regulator's standard on collusion and
cheating requires that "Gambling systems must retain a record of relevant
activities to facilitate investigation and be capable of suspending or
disabling player accounts or player sessions" [27].

## 4. Settlement: paying each prize exactly once

### 4.1 "Exactly once" is a property of the record, not of the network

"Within the context of a distributed system, you cannot have exactly-once
message delivery"; of at-most-once, at-least-once and exactly-once, "the first
two are feasible and widely used", and "The way we achieve exactly-once
delivery in practice is by faking it. Either the messages themselves should be
idempotent, meaning they can be applied more than once without adverse effects,
or we remove the need for idempotency through deduplication." [28]. A payment
processor's API applies this directly: with a client-generated idempotency key
"you can safely repeat the request without risk of creating a second object or
performing the update twice"; the server saves "the resulting status code and
body of the first request made for any given idempotency key, regardless of
whether it succeeds or fails", replays it for later requests with the same key,
and "compares incoming parameters to those of the original request and errors
if they're not the same" [29].

### 4.2 Pattern for a settlement record

The transactional outbox pattern covers the step from a committed decision to
an external effect: store the outgoing message "in the database as part of the
transaction that updates the business entities" and let a separate relay send
it; because "The Message relay might publish a message more than once", "a
message consumer must be idempotent, perhaps by tracking the IDs of the
messages that it has already processed" [30]. Ledger design guidance lists the
same four properties: double-entry, idempotency per attempt, append-only logs,
reconciliation [17].

Backend consequence: settlement of a tournament is a small state machine
(open, closed, scored, settled) with one payout row per (tournament, entrant,
placement). The payout row's key is the idempotency key sent to the wallet or
payment system. A node that crashes after deciding a payout but before
recording the external result must, on recovery, re-issue the same call with
the same key and accept the replayed result. A node that has lost leadership
must be unable to write a payout row at all (section 8).

### 4.3 What a regulator expects of interrupted settlement

A regulator's standard on interrupted gambling: "Systems must be capable of
recovering from failures that cause interruptions to gambling, including where
appropriate, the capability to void gambles (with or without manual
intervention), the capability to suspend betting markets, and taking all
reasonable steps to retain sufficient information to be able to restore events
to their pre-failure state"; and where the customer "can have no further
influence on the outcome of the event or gamble the results of the gamble
should stand" [31]. On result determination: customers "should be notified
when errors that affect them, for example, incorrectly settled bets, have
occurred as soon as practicable after the event occurs" and "Steps should be
taken to rectify the error" [32]. Read as an engineering expectation: a crash
mid-settlement must not lose, duplicate or reorder a payout, and a wrong
settlement must be visible and correctable through a recorded adjustment, not
a silent edit.

## 5. Leaderboards across replicas

Sorted sets give "O(log N) inserts and updates" and "O(log N + M) range
queries" with atomic increments [33], which is why they are the usual store
for a live leaderboard. But the same store "uses by default asynchronous
replication"; its synchronous-acknowledgement command "does not turn a set of
[store] instances into a CP system with strong consistency: acknowledged writes
can still be lost during a failover" [34]. Reads from a replica can be stale,
and a replica promoted after failover starts a new replication history
because "it is possible that the old master is still working as a master
because of some network partition" [34].

Linearizability is the model under which "every operation appears to take
place atomically, in some order, consistent with the real-time ordering of
those operations"; it "cannot be totally or sticky available; in the event of a
network partition, some or all nodes will be unable to make progress" [35].

Backend consequence: there are two leaderboards. The display leaderboard may be
eventually consistent; a player briefly seeing rank 7 while they are at rank 6
is a presentation issue. The settlement leaderboard (final standings at close)
is computed from the committed, validated score records by the node that holds
leadership at close, and written back as one immutable standings entry.
Settlement never reads the display cache. Ties need a deterministic rule
recorded with the tournament before entries open (for example, earlier
server-recorded submission time wins, or the prize is split), because the rule
moves money.

## 6. Audit trails

Event sourcing: "Capture all changes to an application state as a sequence of
events", so that "we can also use the event log to reconstruct past states"
and can "rebuild it by re-running the events from the event log on an empty
application" [36]. A regulator's security requirements put in scope "electronic
systems that generate, transmit, or process random numbers used to determine
the outcome of games or virtual events" and "electronic systems that store
results or the current state of a customer's gamble", and reference the
ISO/IEC 27001 controls for logging (8.15) and clock synchronisation (8.17)
[37]. Customers "must have easy access to at least three months account and
gambling history", "A minimum of 12 months of gambling and account history
must be made available on request", and the history must include "entry fee
deductions" and "bets placed, the results of bets, winnings paid" [38].
Investigations of cheating need a retained record of relevant activities [27].

Backend consequence: the replicated log is the audit trail if, and only if,
entries are immutable, carry the acting principal and a server timestamp, and
are retained (with snapshots) for at least a regulator-style horizon rather than
compacted away. Per-player history views are projections of the log.
Corrections are new entries that reference the entry they correct.

## 7. Regulatory constraints as data

Eligibility rules published by operators: "All gamers must be at least 18
years old and their device location settings must be enabled" [1]; another
operator requires "at least 18, or any higher minimum age required by
applicable law", states that "Identity verification and current-location
verification are required for cash play", and that "Location checks may be
performed for deposits, cash-match entry, card purchases, and withdrawals"
[39]. One operator's annual report lists five US states in which it did not
enable cash prizes as of 31 December 2024 [14]; a legal explainer counts
roughly a dozen restricting states in 2026 [3]; the material element states
are a separate set [1]. Tax reporting thresholds also move: for US prizes, the
information-return threshold rises "from $600 to $2,000" starting in tax year
2026 [40].

Backend consequence: eligibility is a function of (account attributes,
jurisdiction list version, timestamp), evaluated at entry and again at payout,
with the list version recorded on the entry and on the payout row. The list is
configuration with history, not code. Age and identity verification results
are attributes with a verified-at time. Reporting thresholds are
per-jurisdiction parameters with effective dates. A generic case study needs
only "a list of excluded jurisdictions" and "a minimum age"; the point is that
both are inputs the settlement service checks and records, not assumptions.

## 8. Which of these are consistency problems

A replicated log (Paxos, Raft or another state-machine-replication protocol)
gives a set of servers one agreed order of commands: "Each log contains the
same commands in the same order, so each state machine processes the same
sequence of commands. Since the state machines are deterministic, each
computes the same state and the same sequence of outputs." [41]. The classical
statement of the requirement is Agreement ("Every nonfaulty state machine
replica receives every request") and Order ("Every nonfaulty state machine
replica processes the requests it receives in the same relative order") [42].

The part that matters most for settlement is stale-leader rejection. In Raft,
terms "act as a logical clock" that "allow servers to detect obsolete
information such as stale leaders", and "If a server receives a request with a
stale term number, it rejects the request" [41]; the safety properties include
"Election Safety: at most one leader can be elected in a given term" and
"Leader Completeness: if a log entry is committed in a given term, then that
entry will be present in the logs of the leaders for all higher-numbered
terms" [41]. A production Paxos system needed epoch numbers for the same
reason: between receiving a request and updating the database "the replica may
have lost its master status", so "all database operations are made conditional
on the value of the epoch number" [43]; and master leases guarantee that while
the master holds the lease "other replicas cannot successfully submit values
to Paxos", so it can serve reads locally [43]. Outside consensus systems the
same device is a fencing token: a lock holder paused past its lease has its
late write rejected because "the storage server remembers that it has already
processed a write with a higher token number" [44]; the advice for
correctness-critical locks is to "use a proper consensus system" [44].

| Problem | Does a replicated log solve it? | Notes |
|---|---|---|
| Two nodes both think they lead settlement | Yes | Election safety gives one leader per term; a stale leader's writes carry a stale term or epoch and are rejected [41][43]. The payout store must check the term, not only the log. |
| Node crashes mid-settlement | Yes, for internal state | Committed entries survive (Leader Completeness) [41]; the new leader replays the log and continues from the last committed transition. External side effects are not covered (next row). |
| A prize paid twice, or not at all | Partly | The log deduplicates the decision (one payout row per key). The external payment is at-least-once; idempotency keys and reconciliation finish the job [28][29][30]. |
| Final standings differ between nodes | Yes | Standings are a deterministic function of committed scores, computed once and committed as an entry [41][42]. |
| Live leaderboard display is stale | No, by choice | Acceptable to serve from an asynchronous replica [34]; never settle from it. |
| A submitted score is dishonest | No | Validation (replay, checksum, timing) happens before the command is proposed; the log only makes every node agree on the same validated result [24]. |
| The deal is identical for all entrants | No | A design rule: server-generated seed stored before play, consumed in order [4][6]. The log records the seed; it does not make the game deterministic. |
| Bots or steered matchmaking | No | A product-integrity and legal problem [7][8][9]; the log can make the matchmaking function auditable, nothing more. |
| Jurisdiction, age, tax thresholds | No | Versioned configuration and verification data [1][14][39][40]; the log records which version was applied. |
| Audit trail | Partly | An immutable log with principals and timestamps is a strong substrate [36]; retention, clock discipline and per-customer history views are separate obligations [37][38]. |
| Ties and tie-break rules | No | A rule fixed per tournament before entries open; the log applies it deterministically. |

## 9. Case-study problem statement

### A settlement service for paid card-game tournaments

A small studio runs a skill-based competitive mobile card game with paid
tournaments. Each tournament has a fixed entry fee, a pre-funded prize pool, a
rake, one server-generated deal shared by every entrant, and a close time.
Entrants play the deal once; a validation step replays the uploaded input log
through the rules engine and produces the accepted score. At close, entrants
are ranked and prizes are credited to their wallets.

Settlement currently runs as one process. When it has died during payout,
prizes have been paid twice or not at all, and support staff have reconciled
by hand. The studio wants a settlement service on three to five nodes that
stays correct when a node crashes at any point, when the network partitions,
and when two nodes both believe they are the leader.

Requirements

1. Every tournament state transition (opened, entry accepted, score accepted,
   closed, standings fixed, payout issued, payout confirmed, adjustment) is an
   entry in a replicated log agreed by a majority of nodes, applied by a
   deterministic state machine.
2. One node leads settlement per term. A node that has lost leadership cannot
   append entries or issue payouts. Each payout carries its term, and the
   wallet interface rejects a payout from a stale term.
3. Each payout has a key derived from (tournament, entrant, placement).
   Re-issuing it after a crash returns the original result and moves no money.
4. Final standings are computed from committed, validated scores with the
   tournament's recorded tie-break rule, committed as one entry, and are the
   only input to payout.
5. Entry and payout both check a versioned list of excluded jurisdictions and
   a minimum age, and record the list version used.
6. For any tournament a reviewer can reconstruct from the log who entered,
   which deal they played, which score was accepted, who was paid what, and in
   which term.

Non-goals

- The card game, the rules engine and the replay validator; the service
  consumes an "accepted score" event.
- A real payment provider; the wallet is an in-process interface with an
  idempotent API.
- Live leaderboard display, matchmaking, ratings, anti-cheat heuristics, tax
  reporting.
- Byzantine nodes; nodes crash or are partitioned, they do not lie.

Acceptance criteria

- Five nodes; kill the leader at each transition boundary while settling a
  100-entrant tournament. After recovery, each entrant has exactly one payout
  record, payouts sum to the prize pool, and standings match on all nodes.
- Partition the old leader from the majority mid-settlement. After a new
  leader is elected and the partition heals, no payout from the old term is
  accepted.
- Replaying the whole log on an empty node yields identical state.
- An entrant in an excluded jurisdiction is rejected at entry and at payout,
  with the list version recorded.
- All of the above run as automated tests without network access.

## Sources

Publishers are described by role; the URL identifies them.

1. Skill-gaming platform developer documentation, legality of skill gaming
   (three elements of gambling, predominance and material element tests,
   18+ and device location). https://docs.skillz.com/docs/legal-skillz/
2. Law-firm explainer, games of skill versus games of chance (predominance
   versus material element tests).
   https://kleinmoynihan.com/games-of-skill-v-games-of-chance-the-legal-analysis/
3. Plain-English state-by-state guide to skill-gaming legality (2026); count
   of restricting states. Commercial blog; used only for the order of
   magnitude. https://ataygames.com/blogs/is-skill-based-gaming-legal-in-your-state
4. Skill-gaming platform developer documentation, random numbers and fairness
   (same sequence per player, single-component warning, real players only).
   https://docs.skillz.com/docs/randomness/
5. Skill-gaming platform developer documentation, requirements to unlock real
   prizes (seeded generator required, 65% skill threshold).
   https://docs.skillz.com/docs/28.0.5/unlock-real-prizes/
6. National gambling regulator, remote technical standards, RTS 7 generation
   of random outcomes (use in order received, no adaptive behaviour).
   https://www.gamblingcommission.gov.uk/standards/remote-gambling-and-software-technical-standards/rts-7-generation-of-random-outcomes
7. Law professor's case note on the 2025 summary-judgment ruling in the
   skill-gaming bot suit (liquidity bots, tailored bots, court's remarks).
   https://tushnet.com/2025/10/30/claims-about-game-providers-bot-use-in-fair-and-skill-based-games-must-go-to-trial/
8. Legal trade journal report of the 2026 jury verdict (bots posting
   preselected scores, $420 million damages).
   https://ccbjournal.com/news/king-spalding-secures-record-false-advertising-win-for-skillz
9. Law professor's case note on the 2024 motion-to-dismiss ruling ("fair" and
   "skill-based" may imply human-only competition).
   https://tushnet.com/2024/07/29/fair-and-skill-based-may-falsely-imply-absence-of-bots-in-online-gaming/
10. Advertising-law blog, bots and false advertising (implicit representations).
    https://www.kelleydrye.com/viewpoints/blogs/ad-law-access/bots-and-false-advertising
11. Skill-gaming platform developer documentation, tournaments and gameplay
    parameters (asynchronous matching, score held until a similar-rating
    opponent enters, tournament shapes).
    https://docs.skillz.com/docs/tournaments-and-gameplay-parameters/
12. Skill-gaming platform developer documentation, aborted matches and
    forfeits. https://docs.skillz.com/docs/aborts/
13. Encyclopedia entry, rake in poker (definition; tournament buy-in plus fee).
    https://en.wikipedia.org/wiki/Rake_(poker)
14. Skill-gaming platform annual report for fiscal 2024 (entry-fee revenue
    share, prior winnings share of entry fees, states where cash prizes are not
    enabled, matching real players).
    https://www.sec.gov/Archives/edgar/data/1801661/000180166125000050/sklz-20241231.htm
15. Skill-gaming platform developer documentation, developer-funded live
    events (70% share, pre-funded prize pool, placement prizes).
    https://docs.skillz.com/docs/developer-funded-live-events/
16. Payments company engineering blog, idempotency keys in payment APIs
    (idempotency key as uniqueness constraint on ledger postings).
    https://dodopayments.com/blogs/idempotency-keys-payment-api
17. Engineering blog, designing a payment ledger (double-entry, idempotency
    per attempt, append-only, reconciliation).
    https://dev.to/gabrielanhaia/design-a-payment-ledger-idempotent-audit-compliant-reconciles-to-the-cent-59p7
18. Game-networking tutorial, client-server game architecture (authoritative
    server, clients send actions).
    https://www.gabrielgambetta.com/client-server-game-architecture.html
19. Game-backend vendor blog, server-authoritative game logic.
    https://accelbyte.io/blog/server-authoritative-logic-to-prevent-cheating
20. Mobile application security verification standard, resilience chapter
    (defence in depth; server-side validation).
    https://raw.githubusercontent.com/OWASP/owasp-masvs/master/Document/11-MASVS-RESILIENCE.md
    (rendered at https://mas.owasp.org/MASVS/11-MASVS-RESILIENCE/)
21. Skill-gaming platform developer documentation, core loop and score
    submission (submit score, fallback).
    https://docs.skillz.com/docs/next/play-and-compare-gameplay/
22. Skill-gaming platform developer documentation, anti-cheating techniques
    (duplicate-and-verify, obfuscation; platform measures plus developer
    measures). https://docs.skillz.com/docs/anti-cheating-techniques-overview/
23. Game-networking blog, deterministic lockstep (definition of determinism).
    https://gafferongames.com/post/deterministic_lockstep/
24. Game-tooling blog, debugging leaderboard score anomalies (input replay,
    determinism obstacles, statistical flags, false positives).
    https://bugnet.io/blog/debugging-leaderboard-anomalies
25. Game-networking blog, floating point determinism.
    https://gafferongames.com/post/floating_point_determinism/
26. Baughman and Levine, "Cheat-Proof Playout for Centralized and Peer-to-Peer
    Gaming", IEEE/ACM Transactions on Networking (lockstep protocol with hash
    commitments). http://forensics.umass.edu/pubs/baughman.ToN.pdf
27. National gambling regulator, RTS 11 limiting collusion and cheating.
    https://www.gamblingcommission.gov.uk/standards/remote-gambling-and-software-technical-standards/rts-11-limiting-collusion-and-cheating
28. Distributed-systems blog, "You Cannot Have Exactly-Once Delivery".
    https://bravenewgeek.com/you-cannot-have-exactly-once-delivery/
29. Payment processor API reference, idempotent requests.
    https://docs.stripe.com/api/idempotent_requests
30. Microservice pattern catalogue, transactional outbox.
    https://microservices.io/patterns/data/transactional-outbox.html
31. National gambling regulator, RTS 10 interrupted gambling.
    https://www.gamblingcommission.gov.uk/standards/remote-gambling-and-software-technical-standards/rts-10-interrupted-gambling
32. National gambling regulator, RTS 5 result determination.
    https://www.gamblingcommission.gov.uk/standards/remote-gambling-and-software-technical-standards/rts-5-result-determination
33. In-memory data store vendor tutorial, real-time leaderboard with sorted
    sets. https://redis.io/tutorials/howtos/leaderboard/
34. In-memory data store documentation, replication (asynchronous by default,
    WAIT semantics, replication history after failover).
    https://redis.io/docs/latest/operate/oss_and_stack/management/replication/
35. Distributed-systems testing project, consistency model reference,
    linearizability. https://jepsen.io/consistency/models/linearizable
36. Software-architecture pattern catalogue, Event Sourcing.
    https://martinfowler.com/eaaDev/EventSourcing.html
37. National gambling regulator, RTS security requirements (systems in scope;
    ISO/IEC 27001 logging and clock synchronisation controls).
    https://www.gamblingcommission.gov.uk/standards/remote-gambling-and-software-technical-standards/4-remote-gambling-and-software-technical-standards-rts-security-requirements
38. National gambling regulator, RTS 1 customer account information (3 and 12
    month history, entry fee deductions, winnings paid).
    https://www.gamblingcommission.gov.uk/standards/remote-gambling-and-software-technical-standards/rts-1-customer-account-information
39. Skill-gaming operator eligibility page (18 or higher local minimum,
    identity and location verification, location checks at deposit, entry and
    withdrawal). https://skillrcash.com/eligibility
40. Law-firm note on the 2026 change to the US prize information-return
    threshold ($600 to $2,000).
    https://www.reedsmith.com/our-insights/blogs/viewpoints/102ku4y/one-big-beautiful-bill-act-could-mean-more-valuable-prizes/
41. Ongaro and Ousterhout, "In Search of an Understandable Consensus Algorithm
    (Extended Version)" (replicated log definition, terms as logical clock,
    safety properties). https://raft.github.io/raft.pdf
42. Schneider, "Implementing Fault-Tolerant Services Using the State Machine
    Approach: A Tutorial", ACM Computing Surveys 1990 (Agreement and Order).
    https://www.cs.cornell.edu/fbs/publications/SMSurvey.pdf
43. Chandra, Griesemer and Redstone, "Paxos Made Live: An Engineering
    Perspective" (master leases, epoch numbers).
    https://research.google/pubs/paxos-made-live-an-engineering-perspective-2006-invited-talk/
    (PDF mirror: https://systems.cs.columbia.edu/ds2-class/papers/chandra-paxos.pdf)
44. Kleppmann, "How to do distributed locking" (fencing tokens; use a consensus
    system for correctness-critical locks).
    https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html
