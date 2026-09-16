using System;

namespace PaxosArena.Client
{
    /// <summary>
    /// Loads and atomically replaces the client's whole persistent state. The
    /// client calls <see cref="Save"/> after every change and before it sends
    /// an intent, so the store is the write-ahead record of every intent.
    /// </summary>
    public interface IIntentStore
    {
        /// <summary>Returns the stored state, or a fresh <see cref="ClientState"/> when nothing usable is stored.</summary>
        ClientState Load();

        /// <summary>Replaces the stored state; either the old or the new state survives a crash.</summary>
        void Save(ClientState state);
    }

    /// <summary>Everything the client keeps across restarts (section 9.1).</summary>
    [Serializable]
    [Preserve]
    public sealed class ClientState
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

    /// <summary>An intent written to the store before its first send and removed after a definitive answer.</summary>
    [Serializable]
    [Preserve]
    public sealed class PendingIntent
    {
        public string idempotency_key = "";
        public long seq;
        public string method = "POST";

        /// <summary>Path of the request, for example /v1/tournaments/t1/rounds/1/moves.</summary>
        public string path = "";

        public string body = "";

        /// <summary>One of <see cref="IntentRoutes"/>.</summary>
        public string route = "";

        /// <summary>Server time estimate at creation, for display only; 0 when unknown.</summary>
        public long created_at_ms;
    }

    /// <summary>The deal view and commitment of a round, kept until the game has checked the revealed seed.</summary>
    [Serializable]
    [Preserve]
    public sealed class RoundAudit
    {
        public string tournament_id = "";
        public int round;
        public string commitment = "";
        public string deal_view_json = "";
    }
}
