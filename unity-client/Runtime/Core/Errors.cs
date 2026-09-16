using System;

namespace PaxosArena.Client
{
    /// <summary>A failed request or intent, as the server or the client reported it.</summary>
    public sealed class ArenaError
    {
        /// <summary>The HTTP status; 0 when no HTTP response arrived or the error is local.</summary>
        public int Status;

        /// <summary>A code of section 7.4 (<see cref="ErrorCodes"/>) or a local code (<see cref="LocalErrorCodes"/>).</summary>
        public string Code = "";

        /// <summary>For developers and logs; never parse it.</summary>
        public string Message = "";

        /// <summary>True when the same request may succeed later.</summary>
        public bool Retryable;

        public ArenaError()
        {
        }

        public ArenaError(int status, string code, string message, bool retryable)
        {
            Status = status;
            Code = code ?? "";
            Message = message ?? "";
            Retryable = retryable;
        }

        public override string ToString()
        {
            return Status == 0 ? Code + ": " + Message : Status + " " + Code + ": " + Message;
        }
    }

    /// <summary>Codes the client produces itself, never sent by the server.</summary>
    public static class LocalErrorCodes
    {
        /// <summary>MaxPendingIntents intents are already waiting; nothing was created.</summary>
        public const string QueueFull = "queue_full";

        /// <summary>The intent was dropped by a resynchronisation (section 9.2.4) and never answered.</summary>
        public const string ResyncRequired = "resync_required";

        /// <summary>No HTTP response arrived: network loss, timeout or cancellation.</summary>
        public const string NoResponse = "no_response";

        /// <summary>A response arrived that the client could not read.</summary>
        public const string BadResponse = "bad_response";
    }

    /// <summary>The error codes of section 7.4.</summary>
    public static class ErrorCodes
    {
        // Answered outside the state machine.
        public const string NotLeader = "not_leader";
        public const string NoLeader = "no_leader";
        public const string Unavailable = "unavailable";
        public const string ReplicaBehind = "replica_behind";
        public const string OutcomeUnknown = "outcome_unknown";
        public const string Internal = "internal";
        public const string InFlight = "in_flight";
        public const string RateLimited = "rate_limited";
        public const string SessionMissing = "session_missing";
        public const string SessionInvalid = "session_invalid";
        public const string SessionExpired = "session_expired";
        public const string MissingIdempotencyKey = "missing_idempotency_key";
        public const string InvalidIdempotencyKey = "invalid_idempotency_key";
        public const string MalformedRequest = "malformed_request";
        public const string BodyTooLarge = "body_too_large";
        public const string KeyReused = "key_reused";
        public const string NotFound = "not_found";
        public const string MethodNotAllowed = "method_not_allowed";

        // Recorded by the state machine.
        public const string InvalidDevice = "invalid_device";
        public const string DeviceMismatch = "device_mismatch";
        public const string InvalidPlayer = "invalid_player";
        public const string PlayerExists = "player_exists";
        public const string UnknownPlayer = "unknown_player";
        public const string StaleSeq = "stale_seq";
        public const string SeqGap = "seq_gap";
        public const string UnknownTournament = "unknown_tournament";
        public const string NotPlayTournament = "not_play_tournament";
        public const string NotOpen = "not_open";
        public const string TournamentFull = "tournament_full";
        public const string AlreadyJoined = "already_joined";
        public const string JurisdictionExcluded = "jurisdiction_excluded";
        public const string Underage = "underage";
        public const string LedgerConflict = "ledger_conflict";
        public const string NotJoined = "not_joined";
        public const string InvalidRound = "invalid_round";
        public const string RoundAlreadyStarted = "round_already_started";
        public const string PreviousRoundUnfinished = "previous_round_unfinished";
        public const string RoundNotStarted = "round_not_started";
        public const string RoundFinished = "round_finished";
        public const string RoundExpired = "round_expired";
        public const string MoveIndexMismatch = "move_index_mismatch";
        public const string IllegalMove = "illegal_move";
        public const string NotSettled = "not_settled";
        public const string NoPayout = "no_payout";
        public const string AlreadyClaimed = "already_claimed";
    }

    /// <summary>The answer to one call: a value on success, an error otherwise.</summary>
    public sealed class ArenaResult<T>
    {
        public bool Ok;
        public T Value;
        public ArenaError Error;

        /// <summary>X-Arena-Slot of the response, 0 when absent.</summary>
        public long Slot;

        public static ArenaResult<T> Success(T value, long slot)
        {
            return new ArenaResult<T> { Ok = true, Value = value, Slot = slot };
        }

        public static ArenaResult<T> Failure(ArenaError error, long slot)
        {
            return new ArenaResult<T> { Ok = false, Error = error, Slot = slot };
        }
    }

    /// <summary>
    /// The definitive answer to one intent, raised through
    /// <see cref="ArenaClient.IntentCompleted"/> whether or not the intent's
    /// callback survived a restart.
    /// </summary>
    public sealed class IntentOutcome
    {
        public string Route = "";
        public string IdempotencyKey = "";
        public long Seq;

        /// <summary>The HTTP status; 0 for an intent failed locally with resync_required.</summary>
        public int Status;

        /// <summary>The response body as received.</summary>
        public string Body = "";

        /// <summary>X-Arena-Slot of the response, 0 when absent.</summary>
        public long Slot;

        /// <summary>Null on success.</summary>
        public ArenaError Error;
    }

    /// <summary>The client's view of its connectivity (section 9.6).</summary>
    public enum ConnectionState
    {
        /// <summary>The last request received a definitive answer.</summary>
        Online,

        /// <summary>One or two consecutive retryable failures.</summary>
        Retrying,

        /// <summary>Three or more consecutive retryable failures, or the client halted.</summary>
        Offline,
    }

    /// <summary>Thrown for misuse of the client, never for a server answer.</summary>
    public sealed class ArenaException : Exception
    {
        public ArenaException(string message) : base(message)
        {
        }

        public ArenaException(string message, Exception inner) : base(message, inner)
        {
        }
    }
}
