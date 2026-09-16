namespace PaxosArena.Client
{
    /// <summary>Configuration of an <see cref="ArenaClient"/>. Defaults follow section 9 of the contract.</summary>
    public sealed class ArenaClientOptions
    {
        /// <summary>Public play URLs of the replicas, or one balancer URL, without a trailing slash.</summary>
        public string[] BaseUrls = new string[0];

        /// <summary>2 to 8 upper-case letters; recorded by the first session of the device only.</summary>
        public string Jurisdiction = "";

        /// <summary>0 to 150; recorded by the first session of the device only.</summary>
        public int Age;

        /// <summary>Timeout of every request except the events long-poll.</summary>
        public int RequestTimeoutMs = 10000;

        /// <summary>The events long-poll's wait_ms, 0 to 25000; its timeout is this plus 10 seconds.</summary>
        public int EventsWaitMs = 25000;

        public int BackoffBaseMs = 250;
        public int BackoffCapMs = 8000;

        /// <summary>Intents that may wait in the store; creating one more fails with queue_full.</summary>
        public int MaxPendingIntents = 64;

        /// <summary>A new session is opened when the token expires within this margin of server time.</summary>
        public int SessionRefreshMarginMs = 300000;

        /// <summary>Whether the client long-polls GET /v1/events while it has a session.</summary>
        public bool FollowEvents = true;

        /// <summary>
        /// Consecutive retryable failures after which a read (tournaments, round,
        /// leaderboard) is given up and its callback receives the last error.
        /// Intents and sessions retry without limit.
        /// </summary>
        public int MaxReadFailures = 3;
    }
}
