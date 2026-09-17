using System;

namespace PaxosArena.Client
{
    // Response bodies of the read routes (sections 7.3.2, 7.3.8 and 7.3.10).
    // They are display data from any replica, never input to a decision.

    /// <summary>Body of GET /v1/tournaments.</summary>
    [Serializable]
    [Preserve]
    public sealed class TournamentListResponse
    {
        public long applied_slot;
        public int offset;
        public int limit;
        public int total;
        public TournamentSummary[] tournaments = new TournamentSummary[0];
    }

    /// <summary>One play tournament as one player sees it.</summary>
    [Serializable]
    [Preserve]
    public sealed class TournamentSummary
    {
        public string tournament_id = "";
        public string status = "";
        public string game = "";
        public int rounds;
        public long round_time_limit_ms;
        public long entry_fee;
        public int rake_bps;
        public int[] prize_bps = new int[0];
        public int min_entrants;
        public int max_entrants;
        public int min_age;
        public long max_score;
        public int entrants;
        public long projected_pool;
        public bool joined;
        public bool eligible;
        public int rounds_finished;
        public int round_in_play;
        public int next_round;
        public long created_slot;
    }

    /// <summary>Body of GET /v1/tournaments/{tournament_id}/leaderboard.</summary>
    [Serializable]
    [Preserve]
    public sealed class LeaderboardResponse
    {
        public string tournament_id = "";
        public string status = "";
        public bool final;
        public long applied_slot;
        public int entrants;
        public int offset;
        public int limit;
        public LeaderboardRow[] rows = new LeaderboardRow[0];

        /// <summary>This player's row; place 0 and an empty player_id when not entered.</summary>
        public LeaderboardRow me = new LeaderboardRow();
    }

    /// <summary>One entrant's line.</summary>
    [Serializable]
    [Preserve]
    public sealed class LeaderboardRow
    {
        public int place;
        public string player_id = "";
        public long total_score;
        public int rounds_finished;
        public bool scored;
        public long amount;
        public bool withheld;
        public bool claimed;
    }

    /// <summary>Body of GET /v1/events.</summary>
    [Serializable]
    [Preserve]
    public sealed class EventsResponse
    {
        public long cursor;
        public long applied_slot;
        public bool has_more;
        public EventItem[] events = new EventItem[0];
    }

    /// <summary>One event; fields its type does not use are zero or empty.</summary>
    [Serializable]
    [Preserve]
    public sealed class EventItem
    {
        public long slot;

        /// <summary>One of <see cref="ArenaEventType"/>.</summary>
        public string type = "";

        public string tournament_id = "";

        /// <summary>Empty for events every entrant sees.</summary>
        public string player_id = "";

        public int round;
        public string status = "";
        public long score;
        public long total_score;
        public int rounds_finished;
        public long amount;
        public long time_ms;
    }
}
