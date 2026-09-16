package tournament

import (
	"github.com/oguzhanozfe/paxos-arena/internal/game"
	"github.com/oguzhanozfe/paxos-arena/internal/ledger"
	"github.com/oguzhanozfe/paxos-arena/internal/paxos"
)

// DeviceID identifies one installation of the game client: 32 lower-case
// hex characters generated on the device at first launch.
type DeviceID string

// Binding ties one device to the player the server created for it. The
// first OpenSession of a device makes it, and it never changes.
type Binding struct {
	// Device is the bound device.
	Device DeviceID `json:"device"`
	// Verifier is the session.Verifier of the device secret.
	Verifier Digest `json:"verifier"`
	// Player is the player created for the device.
	Player PlayerID `json:"player"`
	// Jurisdiction is the claim made at binding, 2 to 8 upper-case letters.
	Jurisdiction string `json:"jurisdiction"`
	// Age is the claim made at binding, 0 to 150.
	Age int `json:"age"`
	// BoundAt is the slot of the OpenSession that made the binding.
	BoundAt paxos.Slot `json:"bound_at"`
}

// PlayerRecord is the replicated state of one bound player.
type PlayerRecord struct {
	// Binding is the player's device binding.
	Binding Binding `json:"binding"`
	// LastSeq is the sequence number of the last intent that consumed one,
	// 0 before the first. The next intent carries LastSeq+1.
	LastSeq uint64 `json:"last_seq"`
}

// OpenSession binds a device to a new player on its first use and records
// every session issued to the device afterwards. The play API computes
// Verifier from the device secret, which never enters the log, and draws a
// fresh Player for the case that the device is not bound yet.
type OpenSession struct {
	// Device is the device asking for a session.
	Device DeviceID `json:"device"`
	// Verifier is the session.Verifier of the device secret sent.
	Verifier Digest `json:"verifier"`
	// Player is the identifier a new binding receives. It is excluded from
	// the fingerprint, like CreateTournament.Seed, so a retry through
	// another leader is the same command.
	Player PlayerID `json:"player"`
	// Jurisdiction is the claim recorded by a new binding.
	Jurisdiction string `json:"jurisdiction"`
	// Age is the claim recorded by a new binding.
	Age int `json:"age"`
}

// Enter is Join for a bound player: the eligibility claims are the ones
// recorded in the player's binding.
type Enter struct {
	// Tournament is the tournament to enter.
	Tournament TournamentID `json:"tournament"`
	// Player is the authenticated player.
	Player PlayerID `json:"player"`
	// Seq is the intent's sequence number.
	Seq uint64 `json:"seq"`
}

// StartRound deals one round of the player's entry.
type StartRound struct {
	// Tournament is the tournament entered.
	Tournament TournamentID `json:"tournament"`
	// Player is the authenticated player.
	Player PlayerID `json:"player"`
	// Seq is the intent's sequence number.
	Seq uint64 `json:"seq"`
	// Round is 1 to game.Rounds.
	Round int `json:"round"`
	// Seed is the deal seed the leader's play API derived from the deal
	// secret. It is excluded from the fingerprint, so a retry through a
	// leader holding a rotated secret is the same command.
	Seed game.Seed `json:"seed"`
}

// PlayMove applies one move to a round in play.
type PlayMove struct {
	// Tournament is the tournament entered.
	Tournament TournamentID `json:"tournament"`
	// Player is the authenticated player.
	Player PlayerID `json:"player"`
	// Seq is the intent's sequence number.
	Seq uint64 `json:"seq"`
	// Round is the round the move belongs to.
	Round int `json:"round"`
	// MoveIndex is the number of moves the client saw accepted in the round
	// before this one.
	MoveIndex int `json:"move_index"`
	// Move is the move.
	Move game.Move `json:"move"`
}

// FinishRound ends a round and fixes its score.
type FinishRound struct {
	// Tournament is the tournament entered.
	Tournament TournamentID `json:"tournament"`
	// Player is the authenticated player.
	Player PlayerID `json:"player"`
	// Seq is the intent's sequence number.
	Seq uint64 `json:"seq"`
	// Round is the round to finish.
	Round int `json:"round"`
}

// ClaimPayout moves the player's settled, non-withheld payouts of one
// tournament from the player's account to the tournament's claims account,
// once.
type ClaimPayout struct {
	// Tournament is the settled tournament.
	Tournament TournamentID `json:"tournament"`
	// Player is the authenticated player.
	Player PlayerID `json:"player"`
	// Seq is the intent's sequence number.
	Seq uint64 `json:"seq"`
}

func (OpenSession) op() {}
func (Enter) op()       {}
func (StartRound) op()  {}
func (PlayMove) op()    {}
func (FinishRound) op() {}
func (ClaimPayout) op() {}

// Result codes of the play commands, in the validation order of
// docs/UNITY-INTEGRATION.md section 6.
const (
	// InvalidDevice means the device id is not 32 lower-case hex characters
	// or the verifier is zero.
	InvalidDevice Code = "invalid_device"
	// DeviceMismatch means the device is bound and the secret sent does not
	// match its verifier.
	DeviceMismatch Code = "device_mismatch"
	// PlayerExists means the identifier drawn for a new binding is taken.
	PlayerExists Code = "player_exists"
	// UnknownPlayer means no device is bound to the player.
	UnknownPlayer Code = "unknown_player"
	// StaleSeq means the sequence number is not above the player's last one.
	StaleSeq Code = "stale_seq"
	// SeqGap means the sequence number skips at least one number.
	SeqGap Code = "seq_gap"
	// NotPlayTournament means the tournament takes client-reported scores
	// and no play intents.
	NotPlayTournament Code = "not_play_tournament"
	// PlayIntentRequired means a Join or SubmitScore addressed a tournament
	// whose entries and scores come only from play intents.
	PlayIntentRequired Code = "play_intent_required"
	// InvalidRound means the round number is outside 1 to game.Rounds.
	InvalidRound Code = "invalid_round"
	// RoundAlreadyStarted means the round was dealt before.
	RoundAlreadyStarted Code = "round_already_started"
	// PreviousRoundUnfinished means the round before this one is not
	// finished.
	PreviousRoundUnfinished Code = "previous_round_unfinished"
	// RoundNotStarted means the round was never dealt.
	RoundNotStarted Code = "round_not_started"
	// RoundFinished means the round is over and takes no more moves.
	RoundFinished Code = "round_finished"
	// RoundExpired means the round's deadline has passed.
	RoundExpired Code = "round_expired"
	// MoveIndexMismatch means the client's move count differs from the
	// round's.
	MoveIndexMismatch Code = "move_index_mismatch"
	// IllegalMove means the rules of package game refuse the move.
	IllegalMove Code = "illegal_move"
	// NotSettled means the tournament has not paid out yet.
	NotSettled Code = "not_settled"
	// NoPayout means the player has nothing to claim in the tournament.
	NoPayout Code = "no_payout"
	// AlreadyClaimed means the player's payout was claimed before under
	// another key.
	AlreadyClaimed Code = "already_claimed"
)

// PlayOutcome is the part of a Result that only play commands produce. The
// play API rebuilds a response from it, so a repeated key is answered with
// the response the first application produced.
type PlayOutcome struct {
	// Player is the player the command acted for.
	Player PlayerID `json:"player"`
	// NewPlayer is true when OpenSession made the binding.
	NewPlayer bool `json:"new_player"`
	// IssuedAtMs is the session issue time recorded by OpenSession.
	IssuedAtMs int64 `json:"issued_at_ms"`
	// NextSeq is the sequence number the player's next intent carries.
	NextSeq uint64 `json:"next_seq"`
	// Round is the round a round command addressed.
	Round int `json:"round"`
	// MoveIndex is the number of accepted moves of the round after the
	// command.
	MoveIndex int `json:"move_index"`
	// JoinSeq is the entry's join order, set by Enter.
	JoinSeq uint32 `json:"join_seq"`
	// Amount is the amount ClaimPayout moved.
	Amount ledger.Money `json:"amount"`
}

// RoundStatus is the life-cycle state of a round.
type RoundStatus string

const (
	// RoundInPlay accepts moves until its deadline.
	RoundInPlay RoundStatus = "playing"
	// RoundDone has a final score and a revealed seed.
	RoundDone RoundStatus = "finished"
)

// RoundRecord is the replicated record of one round of one entry. Its Seed
// and the order of its stock are secret while Status is RoundInPlay: the
// play API shows neither before the round is done.
type RoundRecord struct {
	// Tournament is the tournament entered.
	Tournament TournamentID `json:"tournament"`
	// Player is the entrant.
	Player PlayerID `json:"player"`
	// Round is 1 to game.Rounds.
	Round int `json:"round"`
	// Seed is the deal seed from the StartRound command.
	Seed game.Seed `json:"seed"`
	// Moves are the accepted moves in order.
	Moves []game.Move `json:"moves"`
	// Status is RoundInPlay or RoundDone.
	Status RoundStatus `json:"status"`
	// Reason says why a done round ended.
	Reason game.FinishReason `json:"reason"`
	// Cleared is the number of tableau cards played.
	Cleared int `json:"cleared"`
	// Score is the round's score under the rules of package game.
	Score int64 `json:"score"`
	// StartedAt is the slot of the StartRound command.
	StartedAt paxos.Slot `json:"started_at"`
	// StartedAtMs is the state machine's clock when the round was dealt.
	StartedAtMs int64 `json:"started_at_ms"`
	// DeadlineMs is StartedAtMs plus game.RoundTimeLimitMs.
	DeadlineMs int64 `json:"deadline_ms"`
	// FinishedAt is the slot that finished the round, 0 while in play.
	FinishedAt paxos.Slot `json:"finished_at"`
	// UpdatedAt is the slot of the last command that changed the record.
	UpdatedAt paxos.Slot `json:"updated_at"`
}

// EventType names a replicated event of the play API's event stream.
type EventType string

// Event types.
const (
	// EventTournamentStatus reports a tournament's new status.
	EventTournamentStatus EventType = "tournament_status"
	// EventLeaderboardChanged reports that a tournament's leaderboard
	// changed.
	EventLeaderboardChanged EventType = "leaderboard_changed"
	// EventRoundStarted reports a player's round dealt.
	EventRoundStarted EventType = "round_started"
	// EventRoundFinished reports a player's round finished.
	EventRoundFinished EventType = "round_finished"
	// EventEntryScored reports a player's entry score fixed.
	EventEntryScored EventType = "entry_scored"
	// EventPayoutAvailable reports a payout the player can claim.
	EventPayoutAvailable EventType = "payout_available"
	// EventPayoutClaimed reports a player's claim applied.
	EventPayoutClaimed EventType = "payout_claimed"
)

// EventRecord is one event, recorded by the command that caused it, so every
// replica holds the same events at the same slot.
type EventRecord struct {
	// Slot is the slot of the command that caused the event.
	Slot paxos.Slot `json:"slot"`
	// Type is the event type.
	Type EventType `json:"type"`
	// Tournament is the tournament the event belongs to.
	Tournament TournamentID `json:"tournament"`
	// Player is the only player who sees the event, or empty for an event
	// every entrant sees.
	Player PlayerID `json:"player"`
	// Round is the round of a round event, 0 otherwise.
	Round int `json:"round"`
	// Status is the tournament status of a status event, or the round
	// status of a round event.
	Status string `json:"status"`
	// Score is the round score of a round_finished event.
	Score int64 `json:"score"`
	// TotalScore is the entry's total of finished rounds.
	TotalScore int64 `json:"total_score"`
	// RoundsFinished is the entry's number of finished rounds.
	RoundsFinished int `json:"rounds_finished"`
	// Amount is the amount of a payout event.
	Amount ledger.Money `json:"amount"`
	// TimeMs is the state machine's clock when the event was recorded.
	TimeMs int64 `json:"time_ms"`
}
