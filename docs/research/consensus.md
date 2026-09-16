# Consensus research notes

Working notes for the implementer of the replicated log in this repository.
The scenario is a skill-based competitive mobile card game with paid
tournaments: tournament entries, match results and payouts must be applied in
one agreed order on every replica, and a replica that crashes must come back
without contradicting the others. These notes collect what the primary sources
say about how to do that with Paxos, what a production team learned doing it,
how Raft differs, which invariants a test suite must check, where liveness can
fail, and how to test all of it.

Conventions. Bracketed keys such as `[PMS]` refer to the source list in
section 10; every key resolves to a URL. Section numbers after a key (for
example `[PMS] §2.2`) point into the cited document. Text was read from the
PDFs or pages at those URLs; nothing here is quoted at length, and every
statement is a paraphrase unless it is a formula or an identifier.

## 1. Problem statement and system model

Consensus, as Lamport states it, is about a set of processes that may propose
values, of which exactly one must be chosen so that processes can learn it.
The safety requirements are three: only a proposed value may be chosen, only a
single value is chosen, and no process learns that a value was chosen unless it
really was. Liveness is left informal: some proposed value should eventually be
chosen, and once chosen it should be learnable ([PMS] §2.1).

The model is asynchronous and non-Byzantine. Processes run at arbitrary speed,
may crash and may restart; because every process may crash after a value was
chosen and then restart, a solution is impossible unless a process can remember
some state across a restart. Messages may be arbitrarily delayed, duplicated or
lost, but not corrupted ([PMS] §2.1). Raft states the same expectations for a
practical algorithm: safety under delays, partitions, loss, duplication and
reordering; availability whenever a majority is up and can talk to each other
and to clients; no dependence on timing for consistency of the logs; and a
normal-case commit after one round trip to a majority ([RAFT] §2).

The impossibility result that shapes every design decision below is FLP: in a
fully asynchronous system with reliable delivery, no deterministic consensus
protocol can guarantee that a decision is reached if even one process may stop
without announcement. The proof assumes no bound on process speed or message
delay, no synchronized clocks (so no timeouts), and no way to tell a dead
process from a slow one ([FLP] §1, Theorem 1). The authors say the result does
not make the problem unsolvable in practice; it shows that termination needs
additional assumptions about timing, or weaker requirements such as termination
with probability 1 ([FLP] §5). Lamport draws the practical conclusion: safety
is guaranteed unconditionally, and a reliable leader election needed for
progress must use either randomness or real time such as timeouts
([PMS] §2.4). Raft states the same split: safety must not depend on timing,
availability inevitably does ([RAFT] §5.6).

## 2. Single-decree Paxos

Single-decree Paxos chooses one value. Everything in section 3 is built by
running many independent instances of it, one per log slot, so the details here
are load-bearing.

### 2.1 Roles

Three roles: proposers propose values, acceptors accept them, learners find out
what was chosen. One process may play several roles ([PMS] §2.1). In the
replicated state machine, every server plays all three roles in every
instance, and the elected leader acts as the distinguished proposer and the
distinguished learner ([PMS] §2.5, §3).

### 2.2 Majorities and proposal numbers

A value is chosen when a single proposal carrying that value has been accepted
by a majority of the acceptors. Majorities work because any two of them share
at least one acceptor ([PMS] §2.2). The general form is any family of quorums
in which every pair intersects: condition B2 in the original paper
([PTP] §2.1), and the `QuorumAssumption` in Lamport's TLA+ specification
([PAXOS-TLA]).

Each proposal is a pair (proposal number, value). Different proposals must
have different numbers; how that is arranged is left to the implementation
([PMS] §2.2). Three workable schemes appear in the sources:

- Each proposer draws from a disjoint set of numbers and persists the highest
  number it has used, starting each new attempt above it ([PMS] §2.5).
- A number is a pair (round, proposer identifier) ordered lexicographically
  ([PTP] §2.2). This is also the ballot number used throughout Van Renesse and
  Altinbuken, whose leaders start at (0, own id) and go strictly upward
  ([PMMC] §2.2, §2.4).
- With n replicas numbered 0..n-1, replica r picks the smallest s greater than
  any number it has seen with s mod n = r ([PML] §4.1, footnote 1).

### 2.3 How the rules are derived

Lamport derives the algorithm from what safety needs, and the derivation is
the shortest route to understanding why each rule exists ([PMS] §2.2). Stated
in the notes' own words:

- P1: an acceptor must accept the first proposal it receives. Without it a
  single proposer in a failure-free run might never get a value chosen. But P1
  alone lets several values each be accepted by a minority, so acceptors must
  be allowed to accept more than one proposal; that is why proposals are
  numbered.
- P2: if a proposal with value v is chosen, every higher-numbered proposal that
  is chosen also has value v. Because numbers are totally ordered, P2 is what
  makes "only a single value is chosen" true.
- P2a strengthens P2 to accepted proposals: if a proposal with value v is
  chosen, every higher-numbered proposal accepted by any acceptor has value v.
- P2b moves it to the proposer: if a proposal with value v is chosen, every
  higher-numbered proposal issued by any proposer has value v. P2b implies P2a,
  which implies P2. The move is forced by asynchrony: an acceptor that never
  heard about the chosen value would otherwise be obliged by P1 to accept a
  later, different value.
- P2c is the invariant a proposer can actually maintain: to issue proposal
  (n, v) there must be a majority S such that either no acceptor in S has
  accepted any proposal numbered below n, or v is the value of the
  highest-numbered proposal below n accepted by any member of S. Maintaining
  P2c gives P2b by induction on n.
- P1a replaces P1 on the acceptor side: an acceptor may accept proposal n if
  and only if it has not responded to a prepare request numbered greater than
  n. P1a subsumes P1.

The trick that makes P2c enforceable is that a proposer cannot predict future
acceptances, so it asks acceptors for a promise not to accept anything numbered
below n. That promise is what the prepare request extracts ([PMS] §2.2).

### 2.4 The two phases

The resulting algorithm, following [PMS] §2.2 (Phase 1 and Phase 2) and the
TLA+ actions Phase1a/1b/2a/2b in [PAXOS-TLA]:

Phase 1 (Prepare / Promise)

- 1a. A proposer picks a proposal number n and sends Prepare(n) to a majority
  of acceptors (in practice, to all of them).
- 1b. An acceptor that receives Prepare(n) with n greater than every prepare
  number it has already responded to replies with a promise never to accept a
  proposal numbered below n, together with the highest-numbered proposal it has
  accepted so far, if any.

Phase 2 (Accept / Accepted)

- 2a. Once the proposer holds promises for n from a majority, it sends
  Accept(n, v) to acceptors, where v is the value of the highest-numbered
  proposal reported in the promises, or any value the proposer likes if no
  promise reported a proposal. The set of acceptors need not be the same set
  that answered the prepare.
- 2b. An acceptor that receives Accept(n, v) accepts it unless it has already
  responded to a prepare numbered greater than n. In the TLA+ specification the
  guard is `m.bal >= maxBal[a]` ([PAXOS-TLA]).

The value is chosen when Accepted(n) has been sent by a majority for the same
n. Because a value is fixed only in Phase 2, Phase 1 can be run before knowing
what value to propose; section 3 depends on that ([PMS] §3).

Two more rules from the same section matter for an implementation:

- An acceptor may ignore any request without harming safety. It may in
  particular ignore a Prepare(n) when it has already promised for a higher
  number, and ignore a prepare for a proposal it has already accepted
  ([PMS] §2.2). Telling the proposer why (a negative acknowledgement carrying
  the higher number) is a performance optimization, not a correctness
  requirement.
- A proposer may abandon a proposal at any time and may run several proposals
  as long as each follows the rules; late responses for an abandoned proposal
  are harmless. It should abandon when it learns of a higher-numbered proposal
  ([PMS] §2.2).

Minimal acceptor pseudocode consistent with the above:

```
durable:  promised  = none     // highest prepare number answered
          accepted  = none     // (n, v) of highest-numbered accepted proposal

on Prepare(n):
    if promised != none and n <= promised:
        reply Nack(promised)           // optional hint
        return
    promised = n
    persist(promised)                  // before replying
    reply Promise(n, accepted)

on Accept(n, v):
    if promised != none and n < promised:
        reply Nack(promised)           // optional hint
        return
    promised = n
    accepted = (n, v)
    persist(promised, accepted)        // before replying
    reply Accepted(n)
```

### 2.5 What an acceptor must persist

With the ignore rules in place an acceptor needs to remember only two things:
the highest-numbered proposal it has accepted and the number of the highest
prepare request it has responded to. Because P2c must hold across failures,
both must survive a crash and restart; Lamport's implementation note is that
the acceptor writes its intended response to stable storage before sending it
([PMS] §2.2, §2.5). A proposer must persist the highest proposal number it has
tried, so that it never reuses a number ([PMS] §2.5). The original paper makes
the same split for a priest: `lastTried`, `prevVote` and `nextBal` go in the
ledger (stable), everything about the ballot in progress goes on a slip of
paper that may be lost ([PTP] §2.3).

The engineering consequence is spelled out by Chandra, Griesemer and Redstone:
a naive implementation flushes to disk before every message, which is five
synchronous writes per instance (propose, promise, accept, acknowledge,
commit); a stable leader reduces that to one write per replica per instance,
done in parallel ([PML] §4.2). See section 3.3.

### 2.6 Why it is safe

Three presentations of the same argument, useful for cross-checking an
implementation and its tests:

1. Lamport's induction on proposal numbers. Assume (m, v) is chosen and every
   proposal numbered in m..n-1 has value v. The majority C that accepted m
   intersects any majority S a proposer for n consulted, so some member of S
   reports a proposal numbered in m..n-1, all of which carry v, and the
   proposer for n therefore adopts v. Hence P2c gives P2b, P2a and P2
   ([PMS] §2.2).
2. The original paper's three conditions on the set of ballots: B1, every
   ballot has a unique number; B2, any two ballots' quorums intersect; B3, if
   any quorum member of ballot B voted in an earlier ballot, B's decree equals
   the decree of the latest such earlier ballot. Theorem 1 states that under
   B1-B3 any two successful ballots carry the same decree; Theorem 2 states
   that a new ballot can always be constructed that preserves B1-B3, so the
   protocol never deadlocks by construction even though it does not guarantee
   progress ([PTP] §2.1).
3. Van Renesse and Altinbuken's invariant chain for the multi-slot version.
   Acceptor invariants: A1, an acceptor adopts strictly increasing ballot
   numbers; A2, it accepts (b, s, c) only when b equals its current ballot;
   A3, it never forgets accepted values (relaxed later); A4, for one (ballot,
   slot) at most one command is under consideration; A5, if a majority accepted
   (b, s, c) then any accepted (b', s, c') with b' > b has c' = c. Leader
   invariants C1 (one command and one commander per (ballot, slot)) and C2
   (the same condition as A5 stated for commanders) imply A4 and A5, and A5
   implies R1, the replica invariant that no two different commands are decided
   for one slot ([PMMC] §2.1-2.4). The paper walks through why the leader's
   choice after Phase 1 preserves C2: either no acceptor in the answering
   majority reported anything for slot s, in which case nothing can have been
   chosen for s below the current ballot, or the leader adopts the command with
   the highest reported ballot ([PMMC] §2.4).

Lamport's TLA+ specification states the safety property directly as the
invariant that any two chosen values are equal ([PAXOS-TLA]). Section 6 turns
these into runtime checks.

### 2.7 Learning a chosen value

A learner must find out that some proposal was accepted by a majority. The
options are: every acceptor informs every learner (messages proportional to
acceptors times learners); acceptors inform one distinguished learner that
relays (fewer messages, one extra hop, one extra failure point); or acceptors
inform a small set of distinguished learners ([PMS] §2.3). Because messages
may be lost, a value can be chosen without any learner knowing. A learner
cannot always settle the question by polling acceptors, because a crashed
acceptor may have been the deciding vote; the reliable way to find out is to
have a proposer run a new proposal, which by P2c will either re-choose the
same value or reveal it ([PMS] §2.3). In the log implementation the leader is
the distinguished learner and forwards decisions ([PMS] §2.5); in Van Renesse
and Altinbuken's formulation the commander that collects a majority of `p2b`
messages notifies all replicas ([PMMC] §2.3).

### 2.8 Progress and dueling proposers

Two proposers can starve each other forever: p completes Phase 1 with n1, q
completes Phase 1 with n2 > n1, p's accepts are ignored, p retries with
n3 > n2, q's accepts are ignored, and so on ([PMS] §2.4). Van Renesse and
Altinbuken draw the same ping-pong with two leaders and three acceptors and
note it continues indefinitely even when both propose the same command
([PMMC] §3). The remedy is a single distinguished proposer; if it can reach a
majority and uses a number larger than any already used, it will succeed, and
it can find a large enough number by abandoning and retrying when it hears of a
higher one ([PMS] §2.4). Multiple simultaneous leaders can only impede
progress, never break consistency ([PTP] §2.4; [PMS] §3). Section 7 covers how
to pick the single proposer.

## 3. Multi-Paxos with a stable leader and a replicated log

### 3.1 Log slots as consensus instances

The system is a deterministic state machine replicated on every server. The
servers agree on the sequence of commands by running a separate instance of the
consensus algorithm per position: the value chosen by instance i is the i-th
command. Every server is proposer, acceptor and learner in every instance. In
normal operation one server is elected leader and is the only proposer; clients
send commands to it and it decides which slot each command occupies. If it
guesses that a command should be slot 135 and another server also believes it
is leader with a different idea, the consensus instance for slot 135 still
chooses at most one command ([PMS] §3). The original paper describes the same
structure: one instance of the Synod protocol per decree number, one president
for all instances, who performs the first two steps only once ([PTP] §3.1).

### 3.2 Leader takeover: one Prepare for every slot

Because Phase 1 does not name a value, a new leader can run Phase 1 for
infinitely many instances with a single message carrying one proposal number.
An acceptor answers with more than a bare acknowledgement only for instances in
which it has already accepted something, so its reply is finite ([PMS] §3;
[PTP] §3.1). Lamport's worked example: the new leader, being a learner, knows
the chosen commands for slots 1-134, 138 and 139. It runs Phase 1 for 135-137
and for everything above 139. Suppose the replies constrain slots 135 and 140
and leave the rest free. It runs Phase 2 for 135 and 140 with the constrained
values, fills 136 and 137 with a no-op (section 3.4), and only then continues
with client commands from slot 141 ([PMS] §3).

Two refinements from the sources:

- The original paper's `NextBallot(b, n)` carries n, the highest decree number
  through which the new president's ledger is complete. A legislator replies
  with the decrees above n that it already has in its ledger, plus the usual
  last-vote information for decrees it does not have, and asks for any decrees
  at or below n that it is missing ([PTP] §3.1). This folds catch-up in both
  directions into the takeover message.
- In Van Renesse and Altinbuken's structure the leader spawns a scout that
  sends `p1a` for ballot b, collects `p1b` replies from a majority, and returns
  the union of all accepted pvalues (ballot, slot, command). The leader then
  computes `pmax`, which keeps for every slot the command with the highest
  ballot, and merges it into its proposals with the update operator `◁`, which
  replaces existing proposals for slots present in `pmax` and keeps the rest.
  It then spawns a commander (Phase 2) for every slot it has a proposal for
  ([PMMC] §2.4). With the state reduction of §4.1, acceptors keep and report
  only the most recently accepted pvalue per slot, which is all the leader
  needs to enforce C2 ([PMMC] §4.1).

Ongaro's comparison notes the cost: a new Paxos leader runs both phases for
every slot whose committed value it does not know, until it reaches an index
for which no reachable server has seen a proposal, which can delay the takeover
([PHD] §11.2.2). Raft avoids this by only electing a server whose log is
already complete (section 5).

### 3.3 Skipping Phase 1 for subsequent slots

After takeover, each new command costs only Phase 2: the leader assigns the
next free slot and sends Accept for that instance ([PMS] §3). The original
paper counts the steady state as steps 3-5 only, three message delays and
about 3N messages per decree, and notes that the president combines the
BeginBallot for one decree with the Success for the previous one when busy
([PTP] §3.2.2). Chandra et al. describe the same optimization as omitting
propose messages while the coordinator identity does not change, and point out
why it is safe: any replica can still broadcast a propose with a higher
sequence number at any time. The coordinator kept for a long time is what they
call the master. With it, each instance costs one disk write per replica in
parallel: the master writes after sending its accept, the others write before
acknowledging ([PML] §4.2). They also batch values submitted concurrently by
different threads into one instance ([PML] §4.2).

The leader may propose slot i+1 before slot i is known to be chosen; Lamport
calls the allowed lead alpha. Van Renesse and Altinbuken bound the same thing
with `WINDOW`, the number of slots a replica may have proposals pending, which
also fixes when a reconfiguration decided in slot s takes effect (slot s +
WINDOW) ([PMS] §3; [PMMC] §2.1, invariant R5).

### 3.4 Gaps and no-op fills

A leader that is alpha commands ahead and fails can leave up to alpha-1 slots
without a chosen value while later slots are chosen. Later commands cannot be
executed until the gap is filled, so the new leader fills each gap with a
special no-op command by running Phase 2 for those instances, instead of
placing a fresh client command there ([PMS] §3). The original paper explains
why a real command must not go into a gap: it would appear in the ledger before
a decree that was in fact passed earlier, which could confuse a citizen who
proposed it because he already knew the later decree had passed; presidents
therefore fill gaps with the meaningless "olive-day" decree. The resulting
decree-ordering property is that if A and B are both important and A was passed
before B was proposed, A has the lower number ([PTP] §3.1, §3.2.1).

### 3.5 Tracking what is chosen and applying it

Van Renesse and Altinbuken give the replica the state that an implementer
needs: `slot_in`, the next slot it has not proposed in; `slot_out`, the next
slot for which it needs a decision before it can apply anything further;
`requests`, `proposals` and `decisions` sets; and the application state
([PMMC] §2.1). Decisions may arrive out of order and more than once. When the
decision for `slot_out` is present the replica applies it and advances; if the
replica had itself proposed a different command for that slot, it puts that
command back into `requests` so it is proposed again for a later slot
([PMMC] §2.1). The invariants are R1 (no two different commands decided for
one slot), R2 (every slot below `slot_out` has a decision), R3 (state equals
the initial state with decisions applied in slot order up to `slot_out`), R4
(`slot_out` never decreases) and R5 (`slot_in` stays within `WINDOW` of
`slot_out`) ([PMMC] §2.1). The paper stresses that slots need not be decided
in order, only applied in order, and that there is no rollback ([PMMC] §2.1).

Raft's equivalents are `commitIndex` and `lastApplied`, both volatile, with the
rule that whenever `commitIndex` exceeds `lastApplied` the next entry is
applied ([RAFT] Figure 2). Ongaro notes the commit index may safely restart at
zero, because a new leader will re-establish and propagate it once it commits
an entry ([PHD] §3.8).

Because a command that lost its slot is re-proposed, the same command can be
decided in two slots. Replicas therefore keep the set of decisions to skip
duplicates, and in practice keep it only for a bounded period ([PMMC] §4.2).
The stronger approach is at the client boundary: each client gets a unique
identifier and numbers its commands; the state machine keeps a session per
client recording the last serial number applied and the response, and answers a
repeated serial number from the saved response without re-executing. Session
expiry must itself be deterministic across replicas, for example driven by a
leader timestamp committed with each entry, or the state machines diverge
([RAFT] §8; [PHD] §6.3). Duplicate execution is not a corner case: a retried
lock acquisition fails because the first attempt already succeeded, and a
retried increment counts twice ([PHD] §6.3).

Applying commands in order on every replica, combined with agreement per slot,
is what gives Raft's State Machine Safety property; the argument is that a
server applies an entry only when its log matches the leader's up to that entry
and the entry is committed, and the leaders of all later terms hold the same
entry ([RAFT] §5.4.3).

### 3.6 Leaders, ballots and identities

Each ballot has exactly one leader; a leader may work on many ballots but
mostly on one at a time; to tolerate f failures there must be at least f+1
leaders and 2f+1 acceptors, and f+1 replicas ([PMMC] §2.1, §2.2). Chandra et
al. describe the ordering of coordinators by sequence numbers as the mechanism
that lets replicas reject messages from old coordinators, so a stale master
cannot disrupt agreement once reached ([PML] §4.1). The leader switches between
a passive mode, waiting for its scout to report the ballot adopted, and an
active mode in which it runs commanders; a `preempted` message from any scout
or commander that saw a higher ballot returns it to passive mode with a new,
higher ballot ([PMMC] §2.4).

### 3.7 Persistence and recovery

Beyond the acceptor state of section 2.5, Chandra et al. log every Paxos action
to a local persistent log and replay it on restart; the same log serves
catch-up for lagging replicas ([PML] §4.2). Van Renesse and Altinbuken list
keeping acceptor and leader state on disk as the way to let processes recover
rather than count as crashed, with the reminder that a process may crash
part-way through saving its state ([PMMC] §3, §4.3, §4.6). Raft persists
`currentTerm`, `votedFor` and the log before answering RPCs; a server that
loses any persistent state may not rejoin under its old identity ([RAFT]
Figure 2; [PHD] §3.8). Section 4.2 covers what to do when a disk is lost.

### 3.8 Read-only requests

Serving a read from the leader's local state without going through the log is
unsafe in the plain protocol: another majority may have elected a new leader
and applied writes the old leader has not seen ([PML] §5.2). Three safe
approaches appear in the sources:

- Run the read through the log like any command, which serializes it with
  writes at full cost ([PML] §5.2; [PMMC] §4.5).
- Determine a slot number s that is at least as high as every replica's
  `slot_out`, by asking a majority of acceptors for the highest slot they have
  accepted a value in; propose no-ops for any unfilled slots below s; wait until
  `slot_out` exceeds s; then evaluate the read locally. Leaders are not
  involved, and any replica may answer ([PMMC] §4.5). Raft's version: the
  leader must first commit an entry of its own term (it commits a blank no-op at
  the start of the term for this), records its commit index as `readIndex`,
  exchanges heartbeats with a majority to confirm it has not been deposed, waits
  until its state machine has applied up to `readIndex`, then answers. One
  heartbeat round can be amortized across many pending reads ([RAFT] §8;
  [PHD] §6.4).
- Leases, which trade a clock-drift assumption for zero messages per read.
  Chandra et al.'s master lease: while it holds, other replicas refuse Paxos
  messages from anyone else, so the master's local state is current; the master
  uses a shorter lease timeout than the replicas to tolerate drift and refreshes
  the lease by committing a heartbeat value ([PML] §5.2). Van Renesse and
  Altinbuken's acceptor-granted lease works the same way at the replica
  ([PMMC] §4.5). Ongaro describes the heartbeat-based lease as an option, does
  not recommend it unless performance requires it, and states the failure mode:
  if the drift bound is violated the system can return arbitrarily stale data
  ([PHD] §6.4.1). Stale reads were found in two Raft implementations by external
  testing ([PHD] §6.4, §8.3), so this path deserves its own tests.

## 4. Engineering lessons from Paxos Made Live

Chandra, Griesemer and Redstone describe replacing a third-party replicated
database under a lock service with their own Paxos-based fault-tolerant log and
database, in cells of five replicas where one master serves all client traffic
([PML] §2, §3). The paper is organized as algorithmic gaps, software
engineering, and unexpected failures ([PML] §1).

### 4.1 Size and the limits of proofs

A one-page algorithm became several thousand lines of C++, not from verbosity
but from the features and optimizations a production system needs. Proofs of
one-page algorithms do not scale to that code, so other methods were needed to
gain confidence; the real world adds failure modes outside the algorithm's
model, including implementation bugs and operator error; and the specification
kept changing ([PML] §1). Their closing assessment is that there are
significant gaps between the algorithm as described and what a real system
needs, that filling them requires many ideas scattered across the literature
plus small protocol extensions, and that the result is an unproven protocol
([PML] §9). The Raft authors quote this passage as typical ([RAFT] §3).

### 4.2 Disk corruption and rejoining

A replica whose disk is corrupted or wiped may break promises it made earlier,
which violates a core assumption. Detection: a checksum per file catches
changed contents; a marker left in a shared file system at first start catches
a disk that came back empty, which is otherwise indistinguishable from a brand
new replica. Recovery: the replica rejoins as a non-voting member, catching up
but not sending promises or acknowledgements, until it has observed one full
Paxos instance that started after it began rebuilding. Waiting for that extra
instance guarantees it cannot have reneged on an earlier promise ([PML] §5.1).
The checksum does not detect a file rolled back to an old state; they accepted
that risk and rely on the cross-replica checksum of section 4.7 to notice it
([PML] §5.1, footnote 2).

### 4.3 Master leases and master churn

Leases are covered in section 3.8. The churn problem is separate: with a fixed
sequence number across instances, a master that temporarily disconnects may
raise its sequence number while partially connected, come back with a number
higher than the new master's, depose it, disconnect again, and repeat. The fix
is for the master to periodically boost its own sequence number by running a
full round including propose messages, at a frequency that avoids churn; under
load fewer than one percent of instances run the full protocol ([PML] §5.2 and
footnote 4). Masters held leases for days at a time ([PML] §5.2).

### 4.4 Epoch numbers

A request received by the master may be executed after the replica has lost
and possibly regained mastership. The service required such requests to fail.
The solution is a global epoch number stored as an entry in the replicated
database, with the guarantee that two reads of it at the master return the
same value if and only if the replica was master continuously in between, and
every database operation made conditional on it ([PML] §5.3). This requirement
was discovered months after release, when it turned out the database's
semantics differed from what the service expected ([PML] §7).

### 4.5 Group membership

The literature says Paxos itself can implement membership changes, but the
details with Multi-Paxos, disk corruption and the other extensions were not
spelled out and had no published proof; they had to fill the gaps
([PML] §5.4). Their membership state machine had to be reworked late in the
project from a three-state model (join once, leave forever) to a two-state
model (in or out, switching often), because intermittent failure of normal
replicas was far more common than expected ([PML] §6.1).

### 4.6 Snapshots and log truncation

An unbounded log costs disk and recovery time. The application owns the
snapshot because only it knows the data structure; the framework provides a
snapshot handle that records the Paxos instance number the snapshot covers and
the group membership at that point. The application requests a handle, takes
the snapshot (possibly in a thread, with care to capture the state as of the
handle), then hands the handle back; only then does the framework truncate the
log. A snapshot that fails or fails verification simply never triggers
truncation. Snapshots are not coordinated across replicas. A lagging replica
that cannot obtain old log entries is told to fetch a snapshot from another
replica and then the remaining log; the provider may move ahead or fail in the
meantime, so the mechanism must retry with another replica or a newer snapshot,
and a way to locate snapshots is needed ([PML] §5.5). Raft's design has the
same shape: each server snapshots independently, the snapshot records the last
included index and term plus the latest configuration, and a leader sends a
snapshot to a follower only when it has already discarded the entries that
follower needs ([RAFT] §7).

### 4.7 Runtime consistency checking

Beyond assertions, the master periodically submits a checksum request through
the log; every replica checksums its database on receiving it, and the master
distributes its own result for comparison. Three inconsistencies were found
this way: one operator error, one never explained (the replayed log was
consistent, so probably memory corruption), and one suspected illegal memory
access from unrelated code in the same binary, after which they added a second
database of checksums checked on every access ([PML] §6.2, §7). The same idea
appears in TigerBeetle, whose replicas' data files are designed to be
byte-identical across caught-up nodes so that a simulator can compare them
([VOPR]).

### 4.8 Expressing the algorithm as explicit state machines

The core algorithm was coded as two explicit state machines in a small
specification language compiled to C++, terse enough for a whole algorithm to
fit on one screen; the compiler also generated transition logging and coverage
measurement. The payoff was isolation: the membership rework above took one
hour to change in the state machine and three days to update tests
([PML] §6.1). Ongaro recommends a comparable structure for Raft: keep all
state transitions in one module with assertions on their preconditions and no
system code mixed in, and drive them from elsewhere, so the code resembles the
specification and invariants can be asserted at runtime ([PHD] §8.3).

### 4.9 Testing

The testing section is the most directly reusable part of the paper
([PML] §6.3):

- Two modes. Safety mode checks consistency only; operations may fail or report
  unavailability. Liveness mode additionally requires every operation to
  complete. A run starts in safety mode with random fault injection, stops
  injecting after a set period, lets the system recover, then switches to
  liveness mode to detect deadlock after a fault sequence.
- The log test simulates a distributed system with a random number of replicas
  and drives it through random network outages, message delays, timeouts,
  process crashes and recoveries, file corruptions and schedule interleavings.
- Repeatability comes from a seeded random number generator that determines the
  fault schedule, and from running the test in a single thread; this is possible
  because the log creates no threads of its own. A failing seed is rerun with
  detailed logging under a debugger.
- The test found subtle protocol errors in membership and corrupted-disk
  handling. To measure the test's strength they left some bugs found in review
  in place and confirmed the test caught them. Running on a farm of several
  hundred machines found more bugs, some needing weeks of simulated time at
  extreme failure rates.
- A second test injected failures through hooks in the log (crash a replica,
  disconnect it, force it to believe it lost mastership) and checked that the
  higher layers coped; it found five master-failover bugs in the service in two
  weeks.
- Fault tolerance masks bugs and misconfiguration: a cell started with a
  misspelled fifth replica name ran normally on four replicas, with the fifth
  in permanent catch-up, and tolerated one failure instead of two. They added a
  check for that case and note they have no general solution.

Repeatability was later lost as the product grew: the database and the local
log handling had to become multi-threaded, and the service itself was
multi-threaded from the start ([PML] §6.4).

### 4.10 Unexpected failures

Over more than a hundred machine-years of production they recorded: a
tenfold increase in worker threads starved key threads, causing timeouts,
rapid master failover, client migration storms and further failovers; a
rollback performed with an undocumented procedure by an operator without the
developers present used an old snapshot and lost fifteen hours of data; an
upgrade script that did not clean up the earlier failed upgrade ran a
months-old snapshot for minutes and lost thirty minutes of data; the semantics
mismatch that led to epoch numbers; the three checksum-detected
inconsistencies; and a kernel behaviour where flushing a small log write could
hang for seconds right after a large snapshot write, worked around by writing
large files in small flushed chunks ([PML] §7). Their conclusions: scripted,
tested rollouts with minimal operator involvement; a log-structured design that
let them replay the database to the exact point of a fault; and more checksums
with a policy of crashing a replica that detects corruption ([PML] §7).

## 5. How Raft differs, and why some teams prefer it

### 5.1 Structure

Raft manages a replicated log with a strong leader. It is decomposed into
leader election, log replication, safety and membership changes, and it reduces
the state space by forbidding holes in logs and limiting how logs may differ;
it uses randomization only where that simplifies things, in elections
([RAFT] §4, §5). A server is a follower, candidate or leader ([RAFT] §5.1).
Time is divided into terms numbered by consecutive integers; each term begins
with an election and has at most one leader; a term may end with no leader
after a split vote. Terms act as a logical clock: servers exchange their
current term on every message, adopt a larger term when they see one, revert
to follower if they were leader or candidate, and reject requests carrying a
stale term ([RAFT] §5.1). Two RPCs suffice for the core algorithm,
RequestVote and AppendEntries (the latter doubles as heartbeat), plus
InstallSnapshot for compaction ([RAFT] §5.1, §7).

Persistent state on every server: `currentTerm`, `votedFor`, and the log,
updated on stable storage before responding to RPCs. Volatile: `commitIndex`
and `lastApplied`. Leader-only, reinitialized after election: `nextIndex[]`
and `matchIndex[]` ([RAFT] Figure 2). Ongaro explains why term and vote must
be durable: otherwise a restarted server could vote twice in one term or let
entries from a deposed leader replace those of a newer one ([PHD] §3.8).

### 5.2 Leader election

Followers become candidates when they hear nothing from a leader for an
election timeout. A candidate increments its term, votes for itself and sends
RequestVote to all others in parallel; it wins with votes from a majority (each
server votes once per term, first come first served); it steps down if it
receives an AppendEntries from a leader with a term at least its own; and after
a split vote every candidate times out and starts a new election with a higher
term ([RAFT] §5.2). Split votes are made rare and short by choosing each
election timeout at random from a fixed interval, for example 150-300 ms, both
initially and when restarting after a split vote ([RAFT] §5.2). The authors
report that they first tried a ranking scheme among candidates and abandoned it
after repeated availability corner cases ([RAFT] §5.2).

The election restriction is what makes the leader-based log safe: a voter
refuses its vote if its own log is more up-to-date than the candidate's, where
the comparison is by the term of the last entry, then by log length. Since the
candidate needs a majority and every committed entry is on at least one member
of any majority, the winner holds every committed entry ([RAFT] §5.4.1).

Measured behaviour on a five-server cluster with about 15 ms broadcast time:
without randomization elections took over ten seconds because of repeated
split votes; 5 ms of randomization gave a median downtime of 287 ms; 50 ms
gave a worst case of 513 ms over 1000 trials; timeouts of 12-24 ms elected a
leader in 35 ms on average but violated the timing requirement below. The
authors recommend a conservative range such as 150-300 ms ([RAFT] §9.3).

### 5.3 Log replication and commit index

Each entry stores a command and the term in which the leader received it, at an
integer index. The leader appends, sends AppendEntries in parallel, and retries
forever until every follower stores every entry ([RAFT] §5.3). AppendEntries
carries the index and term of the entry preceding the new ones; a follower
refuses if it lacks a matching entry. This consistency check is an induction
step that yields the Log Matching property: if two logs hold an entry with the
same index and term, they store the same command and are identical up to that
index ([RAFT] §5.3). After a leader change, the leader repairs a follower by
decrementing `nextIndex` until the check passes, then overwriting conflicting
follower entries with its own; a leader never overwrites or deletes its own
entries ([RAFT] §5.3).

An entry is committed once the leader that created it has replicated it on a
majority; this commits all earlier entries too. The leader includes its
`commitIndex` in AppendEntries so followers learn it and apply in log order
([RAFT] §5.3). The subtle rule, motivated by Figure 8 of the paper: an entry
from a previous term stored on a majority is not thereby committed, because a
later leader could still overwrite it. Raft therefore never commits an entry of
an earlier term by counting replicas; it commits only current-term entries by
counting, and earlier entries become committed indirectly through Log Matching
([RAFT] §5.4.2). In the rules of Figure 2: advance `commitIndex` to N only if
a majority of `matchIndex` is at least N and the entry at N has the current
term ([RAFT] Figure 2). Ongaro's TLA+ specification has the same guard in its
`AdvanceCommitIndex` action ([RAFT-TLA]). Entries keep their original term
when a new leader re-replicates them; the paper contrasts this with algorithms
that renumber entries under the new leader's number ([RAFT] §5.4.2).

### 5.4 Safety properties

Figure 3 of the paper lists what Raft guarantees at all times: Election Safety
(at most one leader per term); Leader Append-Only; Log Matching; Leader
Completeness (an entry committed in a term is present in the logs of all
leaders of higher terms); and State Machine Safety (if a server has applied an
entry at an index, no server applies a different entry at that index)
([RAFT] Figure 3). The paper's proof sketch derives Leader Completeness by
contradiction through a voter that both stored the committed entry and voted
for the first leader lacking it ([RAFT] §5.4.3).

### 5.5 Differences from Paxos, as the sources state them

- Foundation. Paxos starts from single-decree consensus and composes instances
  into a log; the Raft authors argue the single-decree protocol is hard to build
  intuition for, that there is no agreed-upon multi-Paxos, that Lamport's
  descriptions leave many details out, and that real implementations end up
  with substantially different architectures ([RAFT] §3). Ongaro adds that the
  elaborations of Paxos in the literature all differ from each other, and that
  multi-Paxos leaves the log with too little structure, for example holes
  ([PHD] §11.1.1).
- Leadership. In Paxos, leader election is orthogonal to consensus and serves
  performance; Raft makes electing a leader the first phase of consensus and
  concentrates function in the leader ([RAFT] §10). Log entries flow only from
  leader to followers; the leader is elected with a complete log rather than
  fetching entries afterwards ([RAFT] §1, §5.4.1).
- Ordering. Multi-Paxos may accept and commit entries in any order; Raft
  appends and commits in order so followers' logs stay consistent with the
  leader's. Ongaro argues the Paxos flexibility is not a real performance gain
  because commands must be applied in order anyway ([PHD] §11.3).
- Takeover. A new Paxos leader runs both phases for every uncommitted slot it
  finds, renumbering with its ballot; Raft's new leader just starts replicating
  ([PHD] §11.2.2, §11.3; [RAFT] §5.3).
- Leader preference. Paxos lets any server become leader and so can honour a
  preference during election; Raft cannot and needs a separate leadership
  transfer mechanism ([PHD] §11.2.2, §3.10).
- Mechanism count. The paper counts four message types in Raft versus ten each
  in Viewstamped Replication and ZooKeeper's protocol ([RAFT] §10).
- Howard and Mortier's later comparison concludes that the two algorithms take
  a very similar approach and differ materially only in leader election: Raft
  only allows servers with up-to-date logs to become leader, Paxos lets any
  server lead and then update its log. They find Raft's approach efficient for
  its simplicity because no log entries are exchanged during election, and they
  attribute much of Raft's understandability to the paper's presentation
  rather than to something fundamental in the algorithm ([HM20]).

### 5.6 Why teams choose Raft

- Understandability evidence. In a study with 43 students at two universities,
  33 scored higher on the Raft quiz than on the Paxos quiz; means were 25.7
  versus 20.8 out of 60; 33 of 41 said Raft would be easier to implement and
  to explain. The authors describe the steps taken against bias and the study's
  limits ([RAFT] §9.1).
- Completeness. The paper covers election, replication, safety, membership
  change, compaction and client interaction in one place, and a TLA+
  specification of roughly 400-450 lines exists with a mechanically checked
  proof of Leader Completeness (relying on unchecked invariants) and an
  informal proof of State Machine Safety ([RAFT] §9.2; [PHD] §8.1). About 25
  independent implementations existed when the paper was published
  ([RAFT] §9).
- Fewer decisions left to the implementer, in particular the takeover and the
  no-holes log ([RAFT] §3, §4; [PHD] §11.1.1).

Raft has its own hazards that an implementer should know: a server that was
partitioned returns with a higher term and deposes a working leader; the paper
mitigates removed servers by ignoring RequestVote within the minimum election
timeout of hearing from a leader ([RAFT] §6), and Ongaro proposes Pre-Vote,
where a candidate increments its term only after a majority indicates it would
grant the vote ([PHD] §9.6). Read-only handling is the part where third-party
implementations have failed ([PHD] §6.4, §8.3).

## 6. Safety invariants to test

Each invariant below is stated for a Paxos log, with its source and a concrete
way to check it inside a deterministic simulation (section 8.1) that has a
global view of every node's state. The Raft equivalent is given where it helps.

| # | Invariant | Source | Check |
|---|-----------|--------|-------|
| 1 | At most one value is chosen per slot: no two replicas decide different commands for the same slot, and no replica decides two different commands for one slot. | Only a single value is chosen ([PMS] §2.1); R1 ([PMMC] §2.1); the TLA+ invariant that two chosen values are equal ([PAXOS-TLA]); State Machine Safety ([RAFT] Figure 3). | A global observer records every decision event (slot, command, replica). Assert equality of commands per slot across all events, ever. |
| 2 | A value accepted by a majority is never overwritten: once a majority has accepted (b, s, c), every accepted (b', s, c') with b' > b has c' = c, and the set of chosen (slot, value) pairs only grows. | P2a and P2c ([PMS] §2.2); A5 and C2 ([PMMC] §2.2, §2.3); B3 and Theorem 1 ([PTP] §2.1); Leader Completeness ([RAFT] Figure 3). | After every step, compute the chosen set from acceptor states (any (b, s, c) held by a majority). Assert it is a superset of the previous chosen set with the same command per slot, and that no acceptor's current accepted value for a chosen slot differs from the chosen command at a higher ballot. |
| 3 | All state machines apply the same sequence: every replica's applied prefix is a prefix of one global sequence, applied in slot order, never rolled back. | R2, R3, R4 ([PMMC] §2.1); in-order application and State Machine Safety ([RAFT] §5.4.3); cross-replica checksums ([PML] §6.2); byte-identical replica files ([VOPR]). | Keep the global chosen sequence; assert every replica's applied list is a prefix of it; hash each replica's state after applying slot k and assert equal hashes for equal k. |
| 4 | Acceptor monotonicity and promise keeping: an acceptor's promised ballot never decreases; it accepts only at its current ballot; after promising for n it never accepts anything numbered below n. | P1a ([PMS] §2.2); A1, A2 ([PMMC] §2.2). | Per-acceptor assertions on every transition; the observer also matches every Accepted(m) against the acceptor's earlier Promise(n) messages and asserts m >= n. |
| 5 | Uniqueness: no two proposers use the same ballot; at most one command per (ballot, slot). | B1 ([PTP] §2.1); A4 and C1 ([PMMC] §2.2, §2.3); Election Safety ([RAFT] Figure 3). | Global registry ballot -> proposer and (ballot, slot) -> command; assert no conflicting registration. |
| 6 | Durable state survives crashes: after crash and restart, promised ballot, accepted values per slot and the proposer's highest ballot are restored, and invariants 1-5 keep holding. | What an acceptor must remember ([PMS] §2.2, §2.5); ledger versus slip of paper ([PTP] §2.3); persisted state ([PHD] §3.8). | The simulated crash discards volatile state only. Add a torn-write fault: a crash during persistence leaves the previous durable state, never a partial one ([PMMC] §4.6, exercise 7). Rerun all checks. |
| 7 | Gaps are filled only with no-ops; real commands are never placed before a slot that was chosen earlier. | Decree-ordering property ([PTP] §3.2.1); no-op fills ([PMS] §3). | Mark no-ops; assert that any command chosen in slot s after slot s' > s was chosen is a no-op. |
| 8 | Client-visible operations are linearizable and executed at most once. | Linearizability definition ([HW90] §2.2); sessions and serial numbers ([PHD] §6.3; [RAFT] §8). | Record every client operation with invocation and response times and check the history against a sequential model (section 8.2). Count applications per (client, serial) and assert at most one. |
| 9 | Reads are not stale: a read returns a state that includes every write completed before the read was invoked. | Read path requirements ([RAFT] §8; [PHD] §6.4; [PMMC] §4.5). | Covered by invariant 8 if reads are in the history; also assert directly that a read's returned slot index is at least the highest slot committed before its invocation. |

For a Raft variant, add: at most one leader per term; a leader never removes
its own entries; `commitIndex` is monotone; every committed entry is present on
every leader of a later term ([RAFT] Figure 3). Ongaro's own advice is to
simulate the whole cluster in one process precisely so that such invariants can
be asserted across virtual servers at runtime ([PHD] §8.3).

## 7. Liveness caveats

- No guarantee in pure asynchrony. FLP rules out guaranteed termination with
  even one crash and no timing assumptions ([FLP] §1, Theorem 1). Van Renesse
  and Altinbuken point out that the argument applies to the Synod protocol and
  that randomized transitions do not escape it either ([PMMC] §3).
- Dueling proposers. Two active proposers can preempt each other forever, even
  proposing the same value ([PMS] §2.4; [PMMC] §3). Failures, oddly, help: if
  all leaders but one fail, the protocol terminates ([PMMC] §3, footnote 4).
- A single active proposer is needed for progress and must be chosen with
  randomness or time ([PMS] §2.4). Choices in the sources:
  - Failure detection instead of immediate escalation: a leader that is
    preempted by a higher ballot pings that ballot's leader and waits while it
    responds, escalating only when it stops ([PMMC] §3).
  - Timeouts that grow with the competing ballot number so that eventually one
    correct leader always wins, tuned with an additive-increase,
    multiplicative-decrease scheme: multiply the timeout after each preemption,
    decrease it linearly after each success ([PMMC] §3). The assumptions needed
    are only that bounds on clock drift and on message handling time exist, not
    that they are known ([PMMC] §3).
  - Randomized election timeouts from a fixed interval, restarted at each
    election, with the requirement broadcastTime much less than electionTimeout
    much less than MTBF; typical broadcast 0.5-20 ms with disk writes, so
    election timeouts of 10-500 ms ([RAFT] §5.2, §5.6).
  - Leases that keep a leader in place and let it serve reads; the master uses
    a shorter timeout than its followers to absorb clock drift ([PML] §5.2). If
    the drift bound is violated the lease is unsafe, not merely slow
    ([PHD] §6.4.1).
- Leader churn. A reconnecting old leader with an inflated ballot deposes the
  working one; the mitigation is periodic full rounds by the current leader
  ([PML] §5.2). Raft's analogue is the partitioned server returning with a
  higher term, mitigated by ignoring votes shortly after hearing from a leader
  and by Pre-Vote ([RAFT] §6; [PHD] §9.6).
- Timing knobs interact with the test harness. A course harness that limits
  heartbeats to ten per second and demands a new leader within five seconds
  forces election timeouts well above the paper's 150-300 ms ([MIT-LAB]).
- Fair links. Liveness arguments assume that a message repeatedly sent between
  correct processes is eventually delivered, implemented by retransmission with
  growing intervals ([PMMC] §3). Clients likewise retransmit until answered
  ([PMMC] §3).
- Bounded pipelining. The leader may run ahead by alpha or `WINDOW` slots; that
  bounds gaps and defines when reconfiguration takes effect ([PMS] §3;
  [PMMC] §2.1).
- Liveness is testable only under an explicit fairness regime. Chandra et al.
  stop injecting faults, let the system heal, then require every operation to
  complete ([PML] §6.3). TigerBeetle's simulator adds a second mode because
  faults that heal randomly hide liveness bugs: it picks a core set of replicas,
  heals every fault inside the core, freezes every fault outside it permanently,
  and requires the cluster to keep making progress; this found a deadlock
  between a round-robin repair strategy and message routing that would have
  been masked by random healing ([TB-LIVE]).
- Availability regressions can be silent. A misconfigured member that never
  joins leaves the cluster running with less fault tolerance than intended
  ([PML] §6.3). Tests should count voting members, not just check progress.

## 8. Testing approaches

### 8.1 Deterministic simulation

The pattern is the same in every source that used it: the entire cluster runs
inside one process, all nondeterminism is drawn from one seeded generator, and
a failing seed reproduces the failure exactly.

Design elements, with the source that motivates each:

1. One process, one thread. The log library creates no threads; the simulator
   drives every node by delivering events, so a run with a given seed is
   identical every time ([PML] §6.3, §6.4). FoundationDB simulates a whole
   cluster deterministically in a single-threaded process ([FDB]). Ongaro
   cites a Raft implementation that simulates the cluster in one process to
   assert invariants across virtual servers ([PHD] §8.3).
2. Virtual time. The simulator advances a simulated clock; FoundationDB reports
   simulated time running about ten times faster than real time ([FDB]), and
   TigerBeetle reports minutes of simulation standing for days of real testing
   ([VOPR]). Timers in the implementation must be injectable so they read the
   virtual clock.
3. A fault-injecting in-memory network. Messages may be dropped, delayed by a
   random per-link latency, duplicated, reordered, and cut by partitions,
   including asymmetric ones ([PML] §6.3; [PHD] §8.3; [VOPR]; [TB-LIVE]). The
   Raft TLA+ specification models exactly drop, duplicate and reorder
   ([PHD] §8.1), and the course harness's RPC layer delays, reorders and
   discards messages ([MIT-LAB]).
4. Crash and disk faults. Crash a node and restart it from durable state only;
   corrupt durable state in ways the implementation is supposed to detect;
   model a crash in the middle of a write ([PML] §5.1, §6.3; [PMMC] §4.6;
   [VOPR]).
5. Seeded randomness everywhere. The seed selects cluster size, the fault
   schedule and client behaviour; print it on every run; rerun a failing seed
   with verbose logging ([PML] §6.3; [VOPR]; [FDB]). In Go, a
   `rand.New(rand.NewSource(seed))` generator is deterministic for a given seed
   ([GO-RAND]), and `math/rand/v2` provides seedable generators such as PCG
   ([GO-RAND2]). Map iteration order is unspecified in Go ([GO-SPEC]), so any
   loop whose order affects behaviour must iterate over sorted keys.
6. Invariant checks after every step. Run the checks of section 6 from the
   observer that can see every node's state, plus periodic state hashes across
   replicas ([PML] §6.2; [VOPR]).
7. Safety mode, then liveness mode. Inject faults, then stop or freeze them and
   require progress within a bound ([PML] §6.3; [TB-LIVE]).
8. Make rare events common. Very short election timeouts with long heartbeat
   intervals to force leader changes; frequent snapshots to force catch-up;
   frequent restarts and membership changes; varied drop rates per link
   ([PHD] §8.3). Extreme failure rates found bugs that took weeks of simulated
   time ([PML] §6.3).
9. Many seeds, in parallel, for a long time ([PML] §6.3; [PHD] §8.3).
   FoundationDB's team estimates on the order of a trillion CPU-hours of
   simulation and states they could not have built the system without it
   ([FDB]).
10. Test the tester. Reintroduce known bugs and confirm the simulator catches
    them ([PML] §6.3).
11. Run with the race detector as well, even though the simulator is
    single-threaded, because the production wiring is not ([MIT-LAB]).

### 8.2 Jepsen-style history checking

Linearizability is the correctness condition for the client-visible behaviour
of the replicated state machine. Herlihy and Wing define a history as a
sequence of invocation and response events; a history is linearizable if it can
be extended by appending responses to some of its pending invocations so that
the completed operations are equivalent to a legal sequential history (their
condition L1) whose order respects the real-time precedence of non-overlapping
operations in the original (L2) ([HW90] §2.2). Two properties matter for
testing: locality, a history is linearizable if and only if each object's
subhistory is, so checks can be done per object ([HW90] §3.1); and
nonblocking, a pending total operation always has some legal completion
([HW90] §3.2). Sequential consistency drops the real-time requirement;
serializability concerns multi-operation transactions, and linearizability is
the special case of strict serializability with single-operation transactions
([HW90] §3.3; [JEP-LIN]). Ongaro uses this definition for Raft and states its
consequence for reads: each read must reflect a state at or after its
invocation, at least the latest committed write; a system that allows stale
reads provides only serializability ([PHD] §6.3, §6.4).

The Jepsen method, as its README describes it: a control node runs a set of
logically single-threaded client processes, each with its own client to the
system under test; a generator produces operations; a nemesis process injects
faults such as partitions, clock changes and process kills; the start and end
of every operation are recorded in a history; checkers analyze the history
afterwards ([JEP-README]). A published analysis of a Raft-based key-value
store shows the scale of faults used: partitions isolating one node, majority
and minority splits, non-transitive partitions with overlapping majorities,
crashes and pauses of random subsets, clock skew of hundreds of seconds, and
membership changes; single-key register histories were checked with Knossos
against a compare-and-set register model. The consensus core behaved
correctly, while the lock API built on it let multiple clients hold one lock at
once ([JEP-ETCD]). The lesson for this project is that the layer above the log
(sessions, reads, leases) needs the same scrutiny as the log.

Checkers:

- Knossos (Clojure). Input is a history of operations, each an invoke followed
  by a completion of type ok (took effect), fail (did not), or info (unknown,
  for example a timeout); info operations are treated as possibly having taken
  effect. A process performs one operation at a time. It runs a graph search
  and a tree search in parallel, ships models such as a register, a
  compare-and-set register and a mutex, and returns true, false or unknown with
  a counterexample path when false ([KNOSSOS]).
- Porcupine (Go). The sequential specification is Go code: a model with `Init`
  and `Step` plus optional functions for hashing and for describing states and
  operations in visualizations. An operation records client id, input, call
  time, output and return time; check functions accept a timeout and return
  Ok, Illegal or Unknown, and a visualization shows linearization points. It
  implements the P-compositionality refinement of Lowe's algorithm and is used
  in a distributed systems course and by production systems ([PORCUPINE]).
- Maelstrom. A workbench that runs a toy implementation as a process speaking
  JSON over standard input and output, with fault injection for partitions and
  latency, and whose `lin-kv` workload is checked for linearizability
  ([MAELSTROM]).

Project constraint: this repository uses the standard library only, so neither
checker can be imported. Two options remain: implement a small linearizability
checker in-repo following the definition above (a search over legal
completions and orderings, per object, with info operations allowed to have
happened or not), or emit histories in the invoke/ok/fail/info format so they
can be checked offline by Knossos or Porcupine. Either way the history must be
recorded at the client boundary with invocation and response timestamps taken
from the simulator's clock, and an operation that timed out must be recorded as
indeterminate rather than dropped, because a timed-out write may have been
applied ([KNOSSOS]; [HW90] §2.2).

What to model for a replicated log: the sequential specification is the state
machine itself, for example a map from tournament id to its entry list and
result, with operations that append entries and read state. Locality lets the
check run per key ([HW90] §3.1). The history should mix writes, reads and
retried operations with the same client serial number so that invariant 8
(at most once) is exercised together with linearizability ([PHD] §6.3).

### 8.3 Specifications as a reference

Lamport's TLA+ specification of single-decree Paxos states the algorithm
without explicit leaders or learners, refines a voting specification, and
states the safety invariant ([PAXOS-TLA]). Ongaro's TLA+ specification of Raft
models drop, duplicate and reorder in the network, restarts from stable
storage, and deliberately splits actions into small atomic steps, for example
truncating one log entry at a time, to show what implementations may do safely;
it does not itself state invariants ([RAFT-TLA]; [PHD] §8.1). Both are useful
as the reference when writing the checks in section 6, even if this project
does not run a model checker.

## 9. Implications for this project

These are recommendations drawn from the notes above, not decisions.

1. Implement Multi-Paxos with a stable leader as described in section 3,
   with per-slot acceptor state reduced to the most recent accepted pvalue
   ([PMMC] §4.1), ballots as (round, node id) pairs ([PMMC] §2.2), and a
   takeover message that carries the leader's first unknown slot so that
   catch-up runs in both directions ([PTP] §3.1).
2. Persist promised ballot, accepted pvalues and the leader's highest ballot
   before replying, and treat a lost disk as a new identity that rejoins
   non-voting until one full instance has completed ([PMS] §2.5; [PML] §5.1).
3. Fill gaps with no-ops and commit a no-op at the start of each leadership to
   learn the commit frontier before serving reads ([PMS] §3; [RAFT] §8).
4. Put client sessions with serial numbers in the state machine, with
   deterministic expiry ([PHD] §6.3).
5. Choose a leader with a failure detector and randomized, growing timeouts
   rather than immediate ballot escalation ([PMMC] §3; [RAFT] §5.2). Treat
   leases as an optional, separately tested optimization ([PHD] §6.4.1).
6. Build the deterministic simulator of section 8.1 first, with the invariant
   checks of section 6, a safety mode and a liveness mode, and a seed printed
   on every failure. Add an in-repo linearizability check or a history export
   for offline checking.
7. Document what the implementation does not do: membership changes, snapshots
   and leases are each a project of their own in the sources ([PML] §5.4,
   §5.5; [RAFT] §6, §7).

## 10. Sources

| Key | Source | Used for |
|-----|--------|----------|
| [PMS] | Leslie Lamport, "Paxos Made Simple", 1 November 2001 (ACM SIGACT News 32(4), December 2001). https://lamport.azurewebsites.net/pubs/paxos-simple.pdf | Problem statement, P1-P2c and P1a, the two phases, acceptor state, learners, progress, state machine implementation with no-op fills and Phase 1 for all instances. |
| [PTP] | Leslie Lamport, "The Part-Time Parliament", ACM TOCS 16(2), May 1998. https://lamport.azurewebsites.net/pubs/lamport-paxos.pdf | Conditions B1-B3 and Theorems 1-2, ledger versus slip-of-paper state, the six protocol steps, president selection, the multi-decree parliament, NextBallot(b, n), olive-day decrees and the decree-ordering property. |
| [PML] | Tushar Chandra, Robert Griesemer, Joshua Redstone, "Paxos Made Live - An Engineering Perspective", PODC 2007 (20 June 2007 revision). https://www.cs.utexas.edu/users/lorenzo/corsi/cs380d/papers/paper2-1.pdf (publisher record: https://research.google/pubs/paxos-made-live-an-engineering-perspective-2006-invited-talk/) | Multi-Paxos with a master, disk writes, disk corruption, master leases and churn, epoch numbers, membership, snapshots, MultiOp, state machine specification language, checksums, testing in safety and liveness modes with seeds, unexpected failures, conclusions. |
| [RAFT] | Diego Ongaro, John Ousterhout, "In Search of an Understandable Consensus Algorithm (Extended Version)", 20 May 2014 (USENIX ATC 2014). https://raft.github.io/raft.pdf | Raft structure, terms, election with randomized timeouts, log matching, commit rule and Figure 8, Figure 2 and Figure 3, timing requirement, membership, snapshots, client interaction and reads, user study, TLA+ proof, election measurements, related work. |
| [PHD] | Diego Ongaro, "Consensus: Bridging Theory and Practice", PhD dissertation, Stanford, August 2014. https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf | Persisted state, timing, leadership transfer, linearizable sessions, read-only queries and leases, formal specification, building correct implementations and testing advice, Pre-Vote, comparison of leader election and commitment across algorithms. |
| [PMMC] | Robbert van Renesse, Deniz Altinbuken, "Paxos Made Moderately Complex", ACM Computing Surveys 47(3), Article 42, February 2015. https://www.cs.cornell.edu/courses/cs7412/2011sp/paxos.pdf | Replica, acceptor, leader state; invariants R1-R5, A1-A5, C1-C2; scouts, commanders, pmax and the update operator; liveness with failure detection and AIMD timeouts; state reduction; garbage collection; read-only commands and leases; exercises. |
| [HM20] | Heidi Howard, Richard Mortier, "Paxos vs Raft: Have we reached consensus on distributed consensus?", PaPoC 2020. https://arxiv.org/abs/2004.05074 | The assessment that the algorithms differ materially only in leader election. |
| [FLP] | Michael Fischer, Nancy Lynch, Michael Paterson, "Impossibility of Distributed Consensus with One Faulty Process", JACM 32(2), April 1985. https://groups.csail.mit.edu/tds/papers/Lynch/jacm85.pdf | The impossibility result, its assumptions, and its practical reading. |
| [HW90] | Maurice Herlihy, Jeannette Wing, "Linearizability: A Correctness Condition for Concurrent Objects", ACM TOPLAS 12(3), July 1990. https://cs.brown.edu/~mph/HerlihyW90/p463-herlihy.pdf | Histories, conditions L1 and L2, locality, nonblocking, comparison with sequential consistency and serializability. |
| [PAXOS-TLA] | Leslie Lamport, TLA+ specification of Paxos in the TLA+ examples repository. https://raw.githubusercontent.com/tlaplus/Examples/master/specifications/Paxos/Paxos.tla | Quorum assumption, Phase 1a-2b actions and guards, the safety invariant. |
| [RAFT-TLA] | Diego Ongaro, TLA+ specification of Raft. https://raw.githubusercontent.com/ongardie/raft.tla/master/raft.tla | Variables, message types, actions, the AdvanceCommitIndex guard, absence of stated invariants. |
| [JEP-LIN] | Jepsen, "Linearizability" (consistency models). https://jepsen.io/consistency/models/linearizable | Informal definition and relation to other models. |
| [JEP-README] | Jepsen library README. https://github.com/jepsen-io/jepsen | Control node, client processes, nemesis, history, checkers. |
| [JEP-ETCD] | Jepsen analysis of etcd 3.4.3. https://jepsen.io/analyses/etcd-3.4.3 | Fault regime and checkers used against a Raft-based system; consensus core sound, lock layer not. |
| [KNOSSOS] | Knossos linearizability checker README. https://github.com/jepsen-io/knossos | History format with ok/fail/info, treatment of indeterminate operations, algorithms, models, results. |
| [PORCUPINE] | Porcupine linearizability checker README. https://github.com/anishathalye/porcupine | Go model interface, operation format, check results, visualization, algorithm. |
| [MAELSTROM] | Maelstrom README. https://github.com/jepsen-io/maelstrom | JSON-over-stdio harness, fault flags, lin-kv linearizability workload. |
| [FDB] | FoundationDB documentation, "Simulation and Testing". https://apple.github.io/foundationdb/testing.html | Deterministic single-process simulation, time compression, fault classes, scale of testing. |
| [VOPR] | TigerBeetle, "VOPR" internals documentation. https://github.com/tigerbeetle/tigerbeetle/blob/main/docs/internals/vopr.md | Seed-plus-commit determinism, injected faults, state checkers including byte-identical files, time compression, reproduction. |
| [TB-LIVE] | TigerBeetle, "Simulation Testing For Liveness", 6 July 2023. https://tigerbeetle.com/blog/2023-07-06-simulation-testing-for-liveness/ | Why safety-mode fault injection hides liveness bugs; the core-healing liveness mode; the repair deadlock found. |
| [MIT-LAB] | MIT 6.824/6.5840 Lab 3 (Raft) handout. https://pdos.csail.mit.edu/6.824/labs/lab-raft1.html | In-memory RPC network that delays, reorders and discards; heartbeat and election-time limits; persistence via a Persister; running with the race detector. |
| [GO-RAND] | Go standard library, package math/rand. https://pkg.go.dev/math/rand | Seeded deterministic generators. |
| [GO-RAND2] | Go standard library, package math/rand/v2. https://pkg.go.dev/math/rand/v2 | Seedable PCG generator. |
| [GO-SPEC] | The Go Programming Language Specification, for statements. https://go.dev/ref/spec#For_statements | Map iteration order is not specified. |
