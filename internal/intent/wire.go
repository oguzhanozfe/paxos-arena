package intent

// The request and response bodies of the play API. Field types mirror the
// C# models of the client: int32 is a C# int, int64 a C# long. Every field
// is always written: no omitempty, no pointers, and lists are empty rather
// than nil, because JsonUtility cannot represent a missing object or null.

// ErrorBody is the body of every response whose status is not 2xx.
type ErrorBody struct {
	// Code is the machine-readable reason.
	Code string `json:"code"`
	// Message explains this occurrence to a developer; clients do not parse
	// it.
	Message string `json:"message"`
	// Retryable is true when the same request, with the same key, may
	// succeed later.
	Retryable bool `json:"retryable"`
}

// SessionRequest is the body of POST /v1/session.
type SessionRequest struct {
	// DeviceID is 32 lower-case hex characters.
	DeviceID string `json:"device_id"`
	// DeviceSecret is 64 lower-case hex characters.
	DeviceSecret string `json:"device_secret"`
	// Jurisdiction is 2 to 8 upper-case letters; recorded at first binding.
	Jurisdiction string `json:"jurisdiction"`
	// Age is 0 to 150; recorded at first binding.
	Age int32 `json:"age"`
}

// SessionResponse is the success body of POST /v1/session.
type SessionResponse struct {
	// Replayed is true when the key was applied before.
	Replayed bool `json:"replayed"`
	// Slot is the slot of the command that produced the result.
	Slot int64 `json:"slot"`
	// NextSeq is the sequence number of the player's next intent.
	NextSeq int64 `json:"next_seq"`
	// PlayerID is the player bound to the device.
	PlayerID string `json:"player_id"`
	// NewPlayer is true when this session made the binding.
	NewPlayer bool `json:"new_player"`
	// SessionToken is the bearer token.
	SessionToken string `json:"session_token"`
	// IssuedAtMs is the token's issue time.
	IssuedAtMs int64 `json:"issued_at_ms"`
	// ExpiresAtMs is the token's expiry.
	ExpiresAtMs int64 `json:"expires_at_ms"`
	// Jurisdiction is the claim recorded in the binding.
	Jurisdiction string `json:"jurisdiction"`
	// Age is the claim recorded in the binding.
	Age int32 `json:"age"`
}

// SeqRequest is the body of the join, deal, finish and claim routes.
type SeqRequest struct {
	// Seq is the intent's sequence number, at least 1.
	Seq int64 `json:"seq"`
}

// MoveRequest is the body of the moves route.
type MoveRequest struct {
	// Seq is the intent's sequence number, at least 1.
	Seq int64 `json:"seq"`
	// MoveIndex is the round's move_index in the view the move was chosen
	// from.
	MoveIndex int32 `json:"move_index"`
	// Kind is "play" or "draw".
	Kind string `json:"kind"`
	// Column is 0 to 6 for a play and -1 for a draw.
	Column int32 `json:"column"`
}

// TournamentListResponse is the body of GET /v1/tournaments.
type TournamentListResponse struct {
	// AppliedSlot is the answering replica's applied slot.
	AppliedSlot int64 `json:"applied_slot"`
	// Offset is the index of the first summary returned.
	Offset int32 `json:"offset"`
	// Limit is the page size used.
	Limit int32 `json:"limit"`
	// Total is the number of tournaments matching the status filter.
	Total int32 `json:"total"`
	// Tournaments are in creation order.
	Tournaments []TournamentSummary `json:"tournaments"`
}

// TournamentSummary describes one play tournament to one player.
type TournamentSummary struct {
	// TournamentID identifies the tournament.
	TournamentID string `json:"tournament_id"`
	// Status is open, closed, voided or settled.
	Status string `json:"status"`
	// Game is the variant, ladder-v1.
	Game string `json:"game"`
	// Rounds is the number of rounds of an entry.
	Rounds int32 `json:"rounds"`
	// RoundTimeLimitMs is how long a dealt round accepts moves.
	RoundTimeLimitMs int64 `json:"round_time_limit_ms"`
	// EntryFee is the fee in minor units.
	EntryFee int64 `json:"entry_fee"`
	// RakeBps is the operator's share in basis points.
	RakeBps int32 `json:"rake_bps"`
	// PrizeBps lists the prize shares by place.
	PrizeBps []int32 `json:"prize_bps"`
	// MinEntrants is the minimum number of scored entries at close.
	MinEntrants int32 `json:"min_entrants"`
	// MaxEntrants bounds the entries.
	MaxEntrants int32 `json:"max_entrants"`
	// MinAge is the minimum age to enter.
	MinAge int32 `json:"min_age"`
	// MaxScore is the best possible entry score.
	MaxScore int64 `json:"max_score"`
	// Entrants is the number of entries so far.
	Entrants int32 `json:"entrants"`
	// ProjectedPool is the pool the current entries would make, before
	// close; the pool, after it.
	ProjectedPool int64 `json:"projected_pool"`
	// Joined reports whether the player has entered.
	Joined bool `json:"joined"`
	// Eligible reports whether the player may enter now.
	Eligible bool `json:"eligible"`
	// RoundsFinished counts the player's finished rounds.
	RoundsFinished int32 `json:"rounds_finished"`
	// RoundInPlay is the player's round in play, 0 when none.
	RoundInPlay int32 `json:"round_in_play"`
	// NextRound is the round the player may deal now, 0 when none.
	NextRound int32 `json:"next_round"`
	// CreatedSlot is the slot that created the tournament.
	CreatedSlot int64 `json:"created_slot"`
}

// JoinResponse is the success body of the join route.
type JoinResponse struct {
	// Replayed is true when the key was applied before.
	Replayed bool `json:"replayed"`
	// Slot is the slot of the command that produced the result.
	Slot int64 `json:"slot"`
	// NextSeq is the sequence number of the player's next intent.
	NextSeq int64 `json:"next_seq"`
	// TournamentID is the tournament entered.
	TournamentID string `json:"tournament_id"`
	// JoinSeq is the entry's 1-based join order.
	JoinSeq int32 `json:"join_seq"`
	// EntryFee is the fee charged, in minor units.
	EntryFee int64 `json:"entry_fee"`
	// Rounds is the number of rounds of the entry.
	Rounds int32 `json:"rounds"`
}

// RoundResponse is the success body of the deal, moves, finish and round
// routes.
type RoundResponse struct {
	// Replayed is true when the key was applied before; always false on a
	// read.
	Replayed bool `json:"replayed"`
	// Slot is the slot of the command that produced the result, or, on a
	// read, the slot that last changed the round.
	Slot int64 `json:"slot"`
	// NextSeq is the sequence number of the player's next intent.
	NextSeq int64 `json:"next_seq"`
	// Round is the view of the round as of Slot.
	Round RoundView `json:"round"`
}

// RoundView is what a player may see of one round.
type RoundView struct {
	// TournamentID is the tournament the round belongs to.
	TournamentID string `json:"tournament_id"`
	// Round is 1 to 3.
	Round int32 `json:"round"`
	// Status is playing or finished.
	Status string `json:"status"`
	// FinishReason is empty while playing, then cleared, blocked,
	// resigned, expired or closed.
	FinishReason string `json:"finish_reason"`
	// MoveIndex counts the accepted moves; the next move sends it.
	MoveIndex int32 `json:"move_index"`
	// Columns always holds seven columns.
	Columns []ColumnView `json:"columns"`
	// WasteTop is the waste card.
	WasteTop string `json:"waste_top"`
	// WasteCount is the number of cards on the waste.
	WasteCount int32 `json:"waste_count"`
	// StockCount is the number of cards left to draw.
	StockCount int32 `json:"stock_count"`
	// Cleared is the number of cards played from the tableau.
	Cleared int32 `json:"cleared"`
	// Score is the round's score, provisional while playing.
	Score int64 `json:"score"`
	// PlayableColumns lists, ascending, the columns whose top card may be
	// played.
	PlayableColumns []int32 `json:"playable_columns"`
	// CanDraw reports whether a draw is legal.
	CanDraw bool `json:"can_draw"`
	// LastMove is the last accepted move.
	LastMove MoveView `json:"last_move"`
	// StartedAtMs is the state machine's clock at the deal.
	StartedAtMs int64 `json:"started_at_ms"`
	// DeadlineMs is the last time a move is accepted.
	DeadlineMs int64 `json:"deadline_ms"`
	// Commitment is the hex SHA-256 commitment to the seed.
	Commitment string `json:"commitment"`
	// Seed is empty while playing and the hex deal seed once finished.
	Seed string `json:"seed"`
}

// ColumnView is one tableau column, bottom card first.
type ColumnView struct {
	// Cards holds the column's cards, bottom first; the last is the top card.
	Cards []string `json:"cards"`
}

// MoveView describes the last accepted move of a round.
type MoveView struct {
	// Kind is "play", "draw", or "" before the first move.
	Kind string `json:"kind"`
	// Column is the column played, or -1.
	Column int32 `json:"column"`
	// Card is the card the move put on the waste, or "".
	Card string `json:"card"`
}

// LeaderboardResponse is the body of the leaderboard route.
type LeaderboardResponse struct {
	// TournamentID is the tournament ranked.
	TournamentID string `json:"tournament_id"`
	// Status is the tournament's status.
	Status string `json:"status"`
	// Final is true once the tournament is closed, voided or settled.
	Final bool `json:"final"`
	// AppliedSlot is the answering replica's applied slot.
	AppliedSlot int64 `json:"applied_slot"`
	// Entrants is the number of entries.
	Entrants int32 `json:"entrants"`
	// Offset is the index of the first row returned.
	Offset int32 `json:"offset"`
	// Limit is the page size used.
	Limit int32 `json:"limit"`
	// Rows are the page, by place.
	Rows []LeaderboardRow `json:"rows"`
	// Me is the player's own row; its Place is 0 when the player has not entered.
	Me LeaderboardRow `json:"me"`
}

// LeaderboardRow is one entrant's line.
type LeaderboardRow struct {
	// Place is 1-based.
	Place int32 `json:"place"`
	// PlayerID is the entrant.
	PlayerID string `json:"player_id"`
	// TotalScore sums finished rounds before close and is the entry's
	// score after it.
	TotalScore int64 `json:"total_score"`
	// RoundsFinished counts finished rounds.
	RoundsFinished int32 `json:"rounds_finished"`
	// Scored reports whether the entry's score is fixed.
	Scored bool `json:"scored"`
	// Amount is the payout or refund after settle, 0 before.
	Amount int64 `json:"amount"`
	// Withheld reports whether the payout was withheld.
	Withheld bool `json:"withheld"`
	// Claimed reports whether the payout was claimed.
	Claimed bool `json:"claimed"`
}

// ClaimResponse is the success body of the claim route.
type ClaimResponse struct {
	// Replayed is true when the key was applied before.
	Replayed bool `json:"replayed"`
	// Slot is the slot of the command that produced the result.
	Slot int64 `json:"slot"`
	// NextSeq is the sequence number of the player's next intent.
	NextSeq int64 `json:"next_seq"`
	// TournamentID is the settled tournament.
	TournamentID string `json:"tournament_id"`
	// Amount is the amount claimed, in minor units.
	Amount int64 `json:"amount"`
	// PostingKey is the key of the claim posting.
	PostingKey string `json:"posting_key"`
}

// EventsResponse is the body of GET /v1/events.
type EventsResponse struct {
	// Cursor is the cursor of the next request.
	Cursor int64 `json:"cursor"`
	// AppliedSlot is the answering replica's applied slot.
	AppliedSlot int64 `json:"applied_slot"`
	// HasMore is true when more events wait after Cursor.
	HasMore bool `json:"has_more"`
	// Events are in slot order.
	Events []EventItem `json:"events"`
}

// EventItem is one event. Fields a type does not use are zero or empty.
type EventItem struct {
	// Slot is the slot of the command that caused the event.
	Slot int64 `json:"slot"`
	// Type is the event type of section 6.10.
	Type string `json:"type"`
	// TournamentID is the tournament the event belongs to.
	TournamentID string `json:"tournament_id"`
	// PlayerID is the player the event is for, or empty for every entrant.
	PlayerID string `json:"player_id"`
	// Round is the round of a round event.
	Round int32 `json:"round"`
	// Status is the tournament or round status the event reports.
	Status string `json:"status"`
	// Score is the round score of a round_finished event.
	Score int64 `json:"score"`
	// TotalScore is the entry's total of finished rounds.
	TotalScore int64 `json:"total_score"`
	// RoundsFinished counts the entry's finished rounds.
	RoundsFinished int32 `json:"rounds_finished"`
	// Amount is the amount of a payout event.
	Amount int64 `json:"amount"`
	// TimeMs is the state machine's clock when the event was recorded.
	TimeMs int64 `json:"time_ms"`
}
