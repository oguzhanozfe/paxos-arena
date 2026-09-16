package tournament

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"

	"github.com/oguzhanozfe/paxos-arena/internal/ledger"
	"github.com/oguzhanozfe/paxos-arena/internal/paxos"
)

// record is one row of the results table.
type record struct {
	fingerprint [32]byte
	result      Result
}

// State is the replicated tournament state: every tournament record, the
// results table keyed by idempotency key, the ledger, and the play state of
// docs/UNITY-INTEGRATION.md section 6: device bindings, player records,
// round records, the event records and the state machine's clock. Apply
// feeds it chosen log entries in slot order. It is not safe for concurrent
// use.
type State struct {
	tournaments map[TournamentID]*Tournament
	order       []TournamentID
	results     map[IdempotencyKey]record
	book        *ledger.Book
	applied     paxos.Slot
	hash        [32]byte
	commands    int
	mutations   uint64

	play playState
	// clock is the largest ReceivedAt of every command applied so far.
	clock int64
}

// Mutations returns the number of successful, non-replayed commands: the
// commands that may change a tournament record, the ledger or the play
// state. A checker uses it to prove that a replay, a rejection or a
// key_reused answer changed nothing; the one exception is a rule rejection
// of a sequenced play command, which also consumes the player's sequence
// number (docs/UNITY-INTEGRATION.md section 3.2).
func (s *State) Mutations() uint64 { return s.mutations }

// Recorded returns the number of keys in the results table.
func (s *State) Recorded() int { return len(s.results) }

// NewState returns an empty state at applied slot 0.
func NewState() *State {
	return &State{
		tournaments: make(map[TournamentID]*Tournament),
		results:     make(map[IdempotencyKey]record),
		book:        ledger.NewBook(),
		play:        newPlayState(),
	}
}

// Applied returns the highest slot applied or skipped.
func (s *State) Applied() paxos.Slot { return s.applied }

// Commands returns the number of commands applied, replays and rejections
// included.
func (s *State) Commands() int { return s.commands }

// Hash returns a digest that identifies the applied prefix: a chain over
// the canonical encoding of every applied slot's command, result, affected
// tournament record and new postings, the bindings, player records and
// round records the command changed and the events it recorded, and over
// every skipped slot. Two states with the same applied prefix have the same
// hash; a fresh state that replays the same log reproduces it. A slot that
// touches no play state mixes in exactly what earlier versions did.
func (s *State) Hash() [32]byte { return s.hash }

// Tournament returns a deep copy of the record with id.
func (s *State) Tournament(id TournamentID) (Tournament, bool) {
	t, ok := s.tournaments[id]
	if !ok {
		return Tournament{}, false
	}
	return t.clone(), true
}

// Tournaments returns the identifiers in creation order.
func (s *State) Tournaments() []TournamentID {
	return append([]TournamentID(nil), s.order...)
}

// TournamentCount returns the number of tournaments without copying the
// identifiers.
func (s *State) TournamentCount() int { return len(s.order) }

// Ledger returns the book for read-only use by callers.
func (s *State) Ledger() *ledger.Book { return s.book }

// Result returns the recorded result for key k, without the Replayed flag
// set.
func (s *State) Result(k IdempotencyKey) (Result, bool) {
	r, ok := s.results[k]
	return r.result, ok
}

// Replay answers cmd from the results table without applying it: the
// recorded result with Replayed set when the key is recorded with the same
// fingerprint, KeyReused when the fingerprint differs, and ok=false when
// the key is unknown. It is what Apply would return for cmd, minus the
// slot, so a host may answer a retry with it instead of proposing again.
func (s *State) Replay(cmd Command) (Result, bool) {
	rec, seen := s.results[cmd.Key]
	if !seen {
		return Result{}, false
	}
	if rec.fingerprint != Fingerprint(cmd) {
		return Result{Code: KeyReused, Detail: "idempotency key was used with a different command"}, true
	}
	r := rec.result
	r.Replayed = true
	return r, true
}

// Skip advances the applied slot over a no-op or an undecodable entry and
// mixes the slot into the hash. It does nothing when slot is not above the
// applied slot.
func (s *State) Skip(slot paxos.Slot) {
	if slot <= s.applied {
		return
	}
	s.applied = slot
	h := sha256.New()
	h.Write(s.hash[:])
	h.Write([]byte(fmt.Sprintf("skip:%d", slot)))
	copy(s.hash[:], h.Sum(nil))
}

// Apply executes cmd as the command chosen in slot under ballot. It is
// total: a malformed or rejected command produces a Result with a rejection
// code and changes nothing but the results table, the applied slot and the
// hash. A command whose key was applied before returns the recorded result
// with Replayed set, or KeyReused when the payload differs. slot must be
// above Applied(); otherwise OutOfOrder is returned and nothing changes.
func (s *State) Apply(slot paxos.Slot, ballot paxos.Ballot, cmd Command) Result {
	if slot <= s.applied {
		return Result{Code: OutOfOrder, Detail: fmt.Sprintf("slot %d is not above the applied slot %d", slot, s.applied), Slot: slot}
	}
	s.applied = slot
	s.commands++
	if cmd.ReceivedAt > s.clock {
		s.clock = cmd.ReceivedAt
	}
	before := s.book.Len()
	s.play.begin()
	res := s.execute(slot, ballot, cmd)
	s.mix(slot, ballot, cmd, res, before)
	return res
}

// execute runs the idempotency check and the command.
func (s *State) execute(slot paxos.Slot, ballot paxos.Ballot, cmd Command) Result {
	if err := ValidateKey(cmd.Key); err != nil {
		return Result{Code: InvalidKey, Detail: err.Error(), Slot: slot}
	}
	fp := Fingerprint(cmd)
	if rec, seen := s.results[cmd.Key]; seen {
		if rec.fingerprint != fp {
			return Result{Code: KeyReused, Detail: "idempotency key was used with a different command", Slot: slot}
		}
		r := rec.result
		r.Replayed = true
		return r
	}
	var res Result
	switch op := cmd.Op.(type) {
	case CreateTournament:
		res = s.create(slot, op)
	case Join:
		res = s.join(slot, ballot, op)
	case SubmitScore:
		res = s.score(op)
	case Close:
		res = s.close(slot, op)
	case Settle:
		res = s.settle(slot, ballot, op)
	case OpenSession:
		res = s.openSession(slot, cmd.ReceivedAt, op)
	case Enter:
		res = s.enterPlay(slot, ballot, op)
	case StartRound:
		res = s.startRound(slot, op)
	case PlayMove:
		res = s.playMove(slot, op)
	case FinishRound:
		res = s.finishRoundCmd(slot, op)
	case ClaimPayout:
		res = s.claimPayout(slot, ballot, op)
	default:
		return Result{Code: InvalidOp, Detail: fmt.Sprintf("unknown op %T", cmd.Op), Slot: slot}
	}
	res.Slot = slot
	if res.Code == OK {
		s.mutations++
	}
	s.results[cmd.Key] = record{fingerprint: fp, result: res}
	return res
}

func reject(code Code, detail string) Result { return Result{Code: code, Detail: detail} }

func (s *State) create(slot paxos.Slot, op CreateTournament) Result {
	if err := ValidateTournamentID(op.ID); err != nil {
		return reject(InvalidRules, err.Error())
	}
	if err := ValidateRules(op.Rules); err != nil {
		return reject(InvalidRules, err.Error())
	}
	if _, exists := s.tournaments[op.ID]; exists {
		return reject(TournamentExists, fmt.Sprintf("tournament %q exists", op.ID))
	}
	c := Canonical(op).(CreateTournament)
	t := &Tournament{ID: c.ID, Seed: c.Seed, Rules: c.Rules, Status: Open, CreatedAt: slot}
	s.tournaments[t.ID] = t
	s.order = append(s.order, t.ID)
	return Result{Code: OK}
}

func (s *State) join(slot paxos.Slot, ballot paxos.Ballot, op Join) Result {
	if err := ValidatePlayer(op.Player); err != nil {
		return reject(InvalidPlayer, err.Error())
	}
	t, ok := s.tournaments[op.Tournament]
	if !ok {
		return reject(UnknownTournament, fmt.Sprintf("no tournament %q", op.Tournament))
	}
	if t.Rules.Game != "" {
		return reject(PlayIntentRequired, fmt.Sprintf("tournament %q plays %s: entries come from play intents", t.ID, t.Rules.Game))
	}
	res := s.admit(slot, ballot, t, op.Player)
	if res.Code == OK {
		res.Seed = t.Seed
	}
	return res
}

// admit runs the entry rules shared by Join and Enter, from not_open on,
// and on success enters player and posts the fee.
func (s *State) admit(slot paxos.Slot, ballot paxos.Ballot, t *Tournament, player Player) Result {
	op := Join{Tournament: t.ID, Player: player}
	if t.Status != Open {
		return reject(NotOpen, fmt.Sprintf("tournament %q is %s", t.ID, t.Status))
	}
	if len(t.Entries) >= t.Rules.MaxEntrants {
		return reject(TournamentFull, fmt.Sprintf("tournament %q has %d entrants", t.ID, len(t.Entries)))
	}
	if _, joined := t.Entry(op.Player.ID); joined {
		return reject(AlreadyJoined, fmt.Sprintf("player %q has entered", op.Player.ID))
	}
	if t.Rules.Exclusions.Contains(op.Player.Jurisdiction) {
		return reject(JurisdictionExcluded, fmt.Sprintf("jurisdiction %q is excluded by list version %d", op.Player.Jurisdiction, t.Rules.Exclusions.Version))
	}
	if op.Player.Age < t.Rules.MinAge {
		return reject(Underage, fmt.Sprintf("age %d is below the minimum %d", op.Player.Age, t.Rules.MinAge))
	}
	tid, pid := string(t.ID), string(op.Player.ID)
	fee := ledger.Posting{
		Key: ledger.FeeKey(tid, pid), Kind: ledger.EntryFee,
		Debit: ledger.PlayerAccount(pid), Credit: ledger.PoolAccount(tid),
		Amount: t.Rules.EntryFee, Tournament: tid, Player: pid,
		ExclusionVersion: t.Rules.Exclusions.Version, Slot: slot, Ballot: ballot,
	}
	if err := s.checkFresh([]ledger.Posting{fee}); err != nil {
		return reject(LedgerConflict, err.Error())
	}
	_, posted, err := s.book.Post(fee)
	if err != nil {
		// Unreachable with validated rules; kept so Apply stays total.
		return reject(InvalidRules, "entry fee posting rejected: "+err.Error())
	}
	if !posted {
		// Unreachable after checkFresh; a no-op here would record an entry
		// whose fee was never charged.
		return reject(LedgerConflict, fmt.Sprintf("posting key %q already exists", fee.Key))
	}
	t.Entries = append(t.Entries, Entry{
		Player: op.Player, JoinSeq: uint32(len(t.Entries) + 1),
		ExclusionVersion: t.Rules.Exclusions.Version,
	})
	return Result{Code: OK}
}

func (s *State) score(op SubmitScore) Result {
	t, ok := s.tournaments[op.Tournament]
	if !ok {
		return reject(UnknownTournament, fmt.Sprintf("no tournament %q", op.Tournament))
	}
	if t.Rules.Game != "" {
		return reject(PlayIntentRequired, fmt.Sprintf("tournament %q plays %s: scores come from the rounds played", t.ID, t.Rules.Game))
	}
	if t.Status != Open {
		return reject(NotOpen, fmt.Sprintf("tournament %q is %s", t.ID, t.Status))
	}
	idx := -1
	for i := range t.Entries {
		if t.Entries[i].Player.ID == op.Player {
			idx = i
			break
		}
	}
	if idx < 0 {
		return reject(NotJoined, fmt.Sprintf("player %q has not entered", op.Player))
	}
	e := &t.Entries[idx]
	if e.Scored {
		return reject(AlreadyScored, fmt.Sprintf("player %q already has a score", op.Player))
	}
	if op.DealSeed != t.Seed {
		return reject(SeedMismatch, "deal seed does not match the tournament's seed")
	}
	if op.Score < 0 || op.Score > t.Rules.MaxScore {
		return reject(ScoreOutOfRange, fmt.Sprintf("score %d is outside 0..%d", op.Score, t.Rules.MaxScore))
	}
	e.Scored = true
	e.Score = op.Score
	e.SubmitSeq = uint32(t.ScoredCount())
	e.InputDigest = op.InputDigest
	return Result{Code: OK}
}

func (s *State) close(slot paxos.Slot, op Close) Result {
	t, ok := s.tournaments[op.Tournament]
	if !ok {
		return reject(UnknownTournament, fmt.Sprintf("no tournament %q", op.Tournament))
	}
	if t.Status != Open {
		return reject(NotOpen, fmt.Sprintf("tournament %q is %s", t.ID, t.Status))
	}
	if t.Rules.Game != "" {
		s.closeRounds(slot, t)
	}
	t.Standings = ComputeStandings(t)
	t.Fees, t.Rake, t.Pool = ComputePool(t)
	t.ClosedAt = slot
	if t.Voids() {
		t.Status = Voided
	} else {
		t.Status = Closed
	}
	if t.Rules.Game != "" {
		s.recordStatus(slot, t)
	}
	return Result{Code: OK}
}

func (s *State) settle(slot paxos.Slot, ballot paxos.Ballot, op Settle) Result {
	if err := ValidateExclusions(op.Exclusions); err != nil {
		return reject(InvalidExclusions, err.Error())
	}
	t, ok := s.tournaments[op.Tournament]
	if !ok {
		return reject(UnknownTournament, fmt.Sprintf("no tournament %q", op.Tournament))
	}
	if t.Status != Closed && t.Status != Voided {
		return reject(NotClosed, fmt.Sprintf("tournament %q is %s", t.ID, t.Status))
	}
	ex := canonicalExclusions(op.Exclusions)
	payouts := ComputePayouts(t, ex)
	tid := string(t.ID)
	var postings []ledger.Posting
	if t.Status == Closed && t.Rake > 0 {
		postings = append(postings, ledger.Posting{
			Key: ledger.RakeKey(tid), Kind: ledger.Rake,
			Debit: ledger.PoolAccount(tid), Credit: ledger.RakeAccount(tid),
			Amount: t.Rake, Tournament: tid, ExclusionVersion: ex.Version, Slot: slot, Ballot: ballot,
		})
	}
	for _, p := range payouts {
		if p.Amount <= 0 {
			continue // a zero share is recorded in Payouts but moves no money
		}
		pid := string(p.Player)
		post := ledger.Posting{
			Key: p.Key, Debit: ledger.PoolAccount(tid), Amount: p.Amount,
			Tournament: tid, Player: pid, Place: p.Place,
			ExclusionVersion: ex.Version, Slot: slot, Ballot: ballot,
		}
		switch {
		case t.Status == Voided:
			post.Kind, post.Credit = ledger.Refund, ledger.PlayerAccount(pid)
		case p.Withheld:
			post.Kind, post.Credit = ledger.Withheld, ledger.WithheldAccount(tid)
		default:
			post.Kind, post.Credit = ledger.Prize, ledger.PlayerAccount(pid)
		}
		postings = append(postings, post)
	}
	// Validate every posting before posting any, so a rejected command
	// leaves the book untouched.
	for _, p := range postings {
		if err := p.Validate(); err != nil {
			return reject(InvalidRules, "payout posting rejected: "+err.Error())
		}
	}
	if err := s.checkFresh(postings); err != nil {
		return reject(LedgerConflict, err.Error())
	}
	for _, p := range postings {
		if _, posted, err := s.book.Post(p); err != nil || !posted {
			// Unreachable after Validate and checkFresh, which guarantee
			// every Post appends. Stop rather than record a payout that
			// moved no money.
			panic(fmt.Sprintf("tournament: settle %q: posting %q not appended (err %v)", tid, p.Key, err))
		}
	}
	t.Payouts = payouts
	t.Status = Settled
	t.SettledAt = slot
	t.Ballot = ballot
	if t.Rules.Game != "" {
		s.recordStatus(slot, t)
		for _, p := range payouts {
			if !p.Withheld && p.Amount > 0 {
				s.recordEvent(EventRecord{Slot: slot, Type: EventPayoutAvailable, Tournament: t.ID, Player: p.Player, Amount: p.Amount})
			}
		}
	}
	return Result{Code: OK}
}

// checkFresh reports an error when a posting a command is about to make has
// a key that is already in the book or repeated within ps. The ledger treats
// a repeated key as an idempotent retry and books nothing, so a command that
// posted onto an existing key would record money movements that never
// happened. Identifier validation makes this unreachable; the check keeps
// the invariant local instead of relying on it.
func (s *State) checkFresh(ps []ledger.Posting) error {
	seen := make(map[ledger.PostingKey]bool, len(ps))
	for _, p := range ps {
		if s.book.Has(p.Key) || seen[p.Key] {
			return fmt.Errorf("posting key %q already exists", p.Key)
		}
		seen[p.Key] = true
	}
	return nil
}

// mix extends the hash chain with everything the applied slot changed.
func (s *State) mix(slot paxos.Slot, ballot paxos.Ballot, cmd Command, res Result, postingsBefore int) {
	h := sha256.New()
	h.Write(s.hash[:])
	// The ballot is deliberately left out: see Tournament.Ballot.
	h.Write([]byte(fmt.Sprintf("slot:%d\n", slot)))
	if enc, err := Encode(cmd); err == nil {
		h.Write(enc)
	} else {
		h.Write([]byte(fmt.Sprintf("unencodable:%v", err)))
	}
	h.Write([]byte{'\n'})
	h.Write(mustMarshal(res))
	h.Write([]byte{'\n'})
	if t, ok := s.tournaments[TournamentOf(cmd.Op)]; ok {
		h.Write(EncodeTournament(*t))
		h.Write([]byte{'\n'})
	}
	h.Write(EncodePostings(s.book.PostingsSince(postingsBefore)))
	s.mixPlay(func(b []byte) { h.Write(b) })
	copy(s.hash[:], h.Sum(nil))
}

// EncodeTournament returns the canonical JSON of the replicated part of a
// tournament record: everything but Ballot, which is per-replica audit
// metadata (two replicas may learn the same slot under different ballots
// when a chosen value is re-proposed by a later leader). The hash chain and
// the simulator's cross-replica comparisons use it.
func EncodeTournament(t Tournament) []byte {
	t.Ballot = paxos.Ballot{}
	b, err := json.Marshal(t)
	if err != nil {
		panic("tournament: marshal record: " + err.Error())
	}
	return b
}

// EncodePostings returns the canonical JSON of the replicated part of a
// list of postings, one per line, with Ballot cleared for the same reason
// as in EncodeTournament.
func EncodePostings(ps []ledger.Posting) []byte {
	var out []byte
	for _, p := range ps {
		p.Ballot = paxos.Ballot{}
		out = append(out, mustMarshal(p)...)
		out = append(out, '\n')
	}
	return out
}
