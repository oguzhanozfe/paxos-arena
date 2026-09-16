namespace PaxosArena.Client
{
    // String values of the play API's enumerations (section 8, rule 4) and its
    // header names. The models keep these in string fields.

    public static class TournamentStatus
    {
        public const string Open = "open";
        public const string Closed = "closed";
        public const string Voided = "voided";
        public const string Settled = "settled";

        /// <summary>Only as the status filter of GET /v1/tournaments.</summary>
        public const string All = "all";
    }

    public static class RoundStatus
    {
        public const string Playing = "playing";
        public const string Finished = "finished";
    }

    public static class FinishReason
    {
        public const string Cleared = "cleared";
        public const string Blocked = "blocked";
        public const string Resigned = "resigned";
        public const string Expired = "expired";
        public const string Closed = "closed";
    }

    public static class MoveKind
    {
        public const string Play = "play";
        public const string Draw = "draw";

        /// <summary>The column of a draw, and of no move.</summary>
        public const int NoColumn = -1;
    }

    public static class EventType
    {
        public const string TournamentStatus = "tournament_status";
        public const string LeaderboardChanged = "leaderboard_changed";
        public const string RoundStarted = "round_started";
        public const string RoundFinished = "round_finished";
        public const string EntryScored = "entry_scored";
        public const string PayoutAvailable = "payout_available";
        public const string PayoutClaimed = "payout_claimed";
    }

    /// <summary>The route names stored in <see cref="PendingIntent.route"/> and <see cref="IntentOutcome.Route"/>.</summary>
    public static class IntentRoutes
    {
        public const string Join = "join";
        public const string Deal = "deal";
        public const string Move = "move";
        public const string Finish = "finish";
        public const string Claim = "claim";
    }

    public static class Headers
    {
        public const string Authorization = "Authorization";
        public const string IdempotencyKey = "Idempotency-Key";
        public const string ContentType = "Content-Type";
        public const string Location = "Location";
        public const string RetryAfter = "Retry-After";
        public const string ServerTimeMs = "X-Arena-Server-Time-Ms";
        public const string Node = "X-Arena-Node";
        public const string Leader = "X-Arena-Leader";
        public const string AppliedSlot = "X-Arena-Applied-Slot";
        public const string Slot = "X-Arena-Slot";
        public const string NextSeq = "X-Arena-Next-Seq";
    }
}
