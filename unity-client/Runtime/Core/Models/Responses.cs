using System;

namespace PaxosArena.Client
{
    // Response bodies of the intent routes (section 7.3). Every field has a
    // non-null initial value, so a model is never null inside.

    /// <summary>The body of every response whose status is not 2xx.</summary>
    [Serializable]
    [Preserve]
    public sealed class ErrorBody
    {
        public string code = "";
        public string message = "";
        public bool retryable;
    }

    /// <summary>Success body of POST /v1/session.</summary>
    [Serializable]
    [Preserve]
    public sealed class SessionResponse
    {
        public bool replayed;
        public long slot;
        public long next_seq;
        public string player_id = "";
        public bool new_player;
        public string session_token = "";
        public long issued_at_ms;
        public long expires_at_ms;
        public string jurisdiction = "";
        public int age;
    }

    /// <summary>Success body of the join route.</summary>
    [Serializable]
    [Preserve]
    public sealed class JoinResponse
    {
        public bool replayed;
        public long slot;
        public long next_seq;
        public string tournament_id = "";
        public int join_seq;
        public long entry_fee;
        public int rounds;
    }

    /// <summary>Success body of the deal, moves, finish and round routes.</summary>
    [Serializable]
    [Preserve]
    public sealed class RoundResponse
    {
        public bool replayed;
        public long slot;
        public long next_seq;
        public RoundView round = new RoundView();
    }

    /// <summary>What a player may see of one round.</summary>
    [Serializable]
    [Preserve]
    public sealed class RoundView
    {
        public string tournament_id = "";
        public int round;

        /// <summary>One of <see cref="RoundStatus"/>.</summary>
        public string status = "";

        /// <summary>Empty while playing; one of <see cref="FinishReason"/> once finished.</summary>
        public string finish_reason = "";

        /// <summary>Accepted moves so far; the next move sends this value.</summary>
        public int move_index;

        /// <summary>Always seven columns, bottom card first.</summary>
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

        /// <summary>Empty while playing; 64 hex characters once finished.</summary>
        public string seed = "";
    }

    /// <summary>One tableau column, bottom card first; the last card is the top card.</summary>
    [Serializable]
    [Preserve]
    public sealed class ColumnView
    {
        public string[] cards = new string[0];
    }

    /// <summary>The last accepted move of a round.</summary>
    [Serializable]
    [Preserve]
    public sealed class MoveView
    {
        /// <summary>"play", "draw", or "" before the first move.</summary>
        public string kind = "";

        /// <summary>The column played, or -1.</summary>
        public int column = -1;

        /// <summary>The card the move put on the waste, or "".</summary>
        public string card = "";
    }

    /// <summary>Success body of the claim route.</summary>
    [Serializable]
    [Preserve]
    public sealed class ClaimResponse
    {
        public bool replayed;
        public long slot;
        public long next_seq;
        public string tournament_id = "";
        public long amount;
        public string posting_key = "";
    }
}
