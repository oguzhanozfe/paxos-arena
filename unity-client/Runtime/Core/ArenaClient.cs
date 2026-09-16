using System;
using System.Collections.Generic;
using System.Globalization;
using System.Security.Cryptography;
using System.Text;

namespace PaxosArena.Client
{
    /// <summary>
    /// The client of the play API (docs/UNITY-INTEGRATION.md). It owns the
    /// device credentials, the session, the intent queue, the leader cache,
    /// the server clock estimate and the events follower, and persists
    /// <see cref="ClientState"/> through the store after every change.
    ///
    /// Threading: the client is single-threaded and callback-driven. Every
    /// public member must be called from one thread, the one that calls
    /// <see cref="Update"/>, and every callback and event runs inside
    /// <see cref="Update"/> on that thread. The client never blocks, starts no
    /// threads and reads time only through the monotonic clock it is given.
    ///
    /// Intents (<see cref="Join"/>, <see cref="Deal"/>, <see cref="Play"/>,
    /// <see cref="Draw"/>, <see cref="Finish"/>, <see cref="ClaimPayout"/>) are
    /// stored before they are sent, sent one at a time, resent byte for byte
    /// with the same key until a definitive answer, and survive pause, restart
    /// and network loss. Reads and <see cref="OpenSession"/> are plain requests
    /// with the same retry, redirect and session handling.
    /// </summary>
    public sealed class ArenaClient : IDisposable
    {
        /// <summary>How long past its timeout a request may stay unanswered before the client stops waiting for the transport.</summary>
        const int WatchdogGraceMs = 5000;

        /// <summary>Deal audits kept at most; the oldest is dropped first.</summary>
        const int MaxAudits = 16;

        /// <summary>Consecutive retryable failures after which the connection is reported Offline (section 9.2.2).</summary>
        const int ConnectionProblemFailures = 3;

        readonly ArenaClientOptions options;
        readonly string[] baseUrls;
        readonly IHttpTransport transport;
        readonly IIntentStore store;
        readonly IJson json;
        readonly Func<long> monotonicMs;
        readonly ServerClock clock = new ServerClock();
        readonly Backoff backoff;
        readonly IntentQueue queue;
        readonly EventFollower events;
        readonly RandomNumberGenerator rng = RandomNumberGenerator.Create();
        readonly ClientState state;
        readonly List<Call> calls = new List<Call>();
        readonly List<Action> deferred = new List<Action>();
        readonly List<Action<ArenaResult<SessionResponse>>> sessionWaiters = new List<Action<ArenaResult<SessionResponse>>>();

        List<Completion> completions = new List<Completion>();
        List<Completion> draining = new List<Completion>();
        Call sessionCall;
        Call headCall;
        string headSinceKey = "";
        long headSinceMs;
        int generation;
        bool paused;
        bool disposed;
        bool resyncAwaitingSession;
        int baseIndex;
        string lastLeaderHint = "";
        int consecutiveFailures;
        int sessionFailures;
        long sessionNotBefore;
        long sessionLifetimeMs;
        ConnectionState connection = ConnectionState.Online;
        ArenaError haltError;

        /// <param name="options">Configuration; at least one base URL.</param>
        /// <param name="transport">Sends requests; must not follow redirects.</param>
        /// <param name="store">Persists the client state atomically.</param>
        /// <param name="json">The JSON codec of the models.</param>
        /// <param name="monotonicMs">Milliseconds from a clock that never jumps, such as a Stopwatch.</param>
        public ArenaClient(ArenaClientOptions options, IHttpTransport transport, IIntentStore store,
                           IJson json, Func<long> monotonicMs)
            : this(options, transport, store, json, monotonicMs, null)
        {
        }

        internal ArenaClient(ArenaClientOptions options, IHttpTransport transport, IIntentStore store,
                             IJson json, Func<long> monotonicMs, Func<double> random)
        {
            if (options == null)
            {
                throw new ArgumentNullException(nameof(options));
            }
            if (transport == null)
            {
                throw new ArgumentNullException(nameof(transport));
            }
            if (store == null)
            {
                throw new ArgumentNullException(nameof(store));
            }
            if (json == null)
            {
                throw new ArgumentNullException(nameof(json));
            }
            if (monotonicMs == null)
            {
                throw new ArgumentNullException(nameof(monotonicMs));
            }
            baseUrls = NormalizeBaseUrls(options.BaseUrls);
            if (baseUrls.Length == 0)
            {
                throw new ArgumentException("ArenaClientOptions.BaseUrls needs at least one absolute http or https URL", nameof(options));
            }
            if (!ValidJurisdiction(options.Jurisdiction))
            {
                throw new ArgumentException("ArenaClientOptions.Jurisdiction must be 2 to 8 upper-case letters", nameof(options));
            }
            if (options.Age < 0 || options.Age > 150)
            {
                throw new ArgumentException("ArenaClientOptions.Age must be 0 to 150", nameof(options));
            }
            this.options = options;
            this.transport = transport;
            this.store = store;
            this.json = json;
            this.monotonicMs = monotonicMs;
            backoff = new Backoff(options.BackoffBaseMs, options.BackoffCapMs, random);
            state = Normalize(store.Load());
            queue = new IntentQueue(state, Save, options.MaxPendingIntents);
            events = new EventFollower(this);
            if (!IsLowerHex(state.device_id, 32) || !IsLowerHex(state.device_secret, 64))
            {
                // First launch, or a store without usable credentials: a new
                // device, and so a new player. Nothing kept under another
                // identity can be sent any more.
                state.device_id = RandomHex(16);
                state.device_secret = RandomHex(32);
                state.player_id = "";
                state.session_token = "";
                state.session_expires_at_ms = 0;
                state.last_assigned_seq = 0;
                state.last_intent_slot = 0;
                state.events_cursor = 0;
                state.pending = new PendingIntent[0];
                state.audits = new RoundAudit[0];
                Save();
            }
        }

        /// <summary>The player bound to this device, or "" before the first session.</summary>
        public string PlayerId
        {
            get { return state.player_id; }
        }

        /// <summary>True while a token is held that is not known to be expired.</summary>
        public bool HasSession
        {
            get
            {
                if (state.session_token.Length == 0)
                {
                    return false;
                }
                return !clock.HasEstimate || state.session_expires_at_ms > clock.NowMs(monotonicMs());
            }
        }

        /// <summary>True while an intent is pending; the game disables round input meanwhile (section 9.6).</summary>
        public bool Busy
        {
            get { return state.pending.Length > 0; }
        }

        /// <summary>How long the current head intent has been unresolved; 0 when not busy.</summary>
        public long BusyForMs
        {
            get
            {
                PendingIntent head = queue.Head;
                if (head == null || head.idempotency_key != headSinceKey)
                {
                    return 0;
                }
                return monotonicMs() - headSinceMs;
            }
        }

        /// <summary>Intents waiting in the store.</summary>
        public int PendingCount
        {
            get { return state.pending.Length; }
        }

        /// <summary>The estimated server time in Unix milliseconds (section 9.4); 0 before the first response.</summary>
        public long ServerNowMs
        {
            get { return clock.NowMs(monotonicMs()); }
        }

        public ConnectionState Connection
        {
            get { return connection; }
        }

        public EventFollower Events
        {
            get { return events; }
        }

        public bool IsPaused
        {
            get { return paused; }
        }

        /// <summary>
        /// Non-null once the client stopped sending: the device's binding does
        /// not match the stored secret (device_mismatch), or the session route
        /// answered a client error such as a malformed jurisdiction. Nothing
        /// automatic follows; stored intents stay in the store.
        /// </summary>
        public ArenaError HaltError
        {
            get { return haltError; }
        }

        /// <summary>Raised for every definitive answer to an intent, including intents created before a restart.</summary>
        public event Action<IntentOutcome> IntentCompleted;

        /// <summary>Raised after a resynchronisation (section 9.2.4); read the round and the tournament list again before offering input.</summary>
        public event Action Resynced;

        /// <summary>Raised by the first <see cref="Update"/> after <see cref="Resume"/>; read the round in play and refresh the leaderboard.</summary>
        public event Action Resumed;

        public event Action<ConnectionState> ConnectionChanged;

        /// <summary>Raised once when the client halts; see <see cref="HaltError"/>.</summary>
        public event Action<ArenaError> Halted;

        /// <summary>Call every frame: delivers answers, sends and retries requests, and runs the events long-poll.</summary>
        public void Update()
        {
            if (disposed)
            {
                return;
            }
            DrainCompletions();
            RunDeferred();
            if (disposed || paused)
            {
                return;
            }
            long now = monotonicMs();
            if (haltError == null)
            {
                if (sessionCall == null && now >= sessionNotBefore && (resyncAwaitingSession || NeedsSession(now)))
                {
                    StartSession();
                }
                EnsureHeadCall(now);
                events.Tick(now);
            }
            for (int i = 0; i < calls.Count; i++)
            {
                Call c = calls[i];
                if (c.InFlight)
                {
                    if (!c.WatchdogFired && c.TimeoutMs > 0 && now - c.SentAt > (long)c.TimeoutMs + WatchdogGraceMs)
                    {
                        c.WatchdogFired = true;
                        completions.Add(new Completion(c, c.Attempt, generation, new HttpResponse
                        {
                            TimedOut = true,
                            TransportError = "the transport did not answer within the request timeout",
                        }));
                    }
                    continue;
                }
                if (now < c.NotBefore || (c.Auth && state.session_token.Length == 0))
                {
                    continue;
                }
                Send(c, now);
            }
        }

        /// <summary>
        /// Stops starting requests, aborts the requests in flight (an intent
        /// on the wire stays in the store and is resent on resume) and persists
        /// the store. Call from OnApplicationPause(true).
        /// </summary>
        public void Pause()
        {
            if (disposed || paused)
            {
                return;
            }
            paused = true;
            generation++;
            for (int i = 0; i < calls.Count; i++)
            {
                calls[i].InFlight = false;
                calls[i].RedirectUrl = "";
            }
            transport.CancelAll();
            Save();
        }

        /// <summary>
        /// Clears the clock samples and resets every backoff, so that the next
        /// <see cref="Update"/> resends the head intent and restarts the events
        /// poll at once, opens a session when needed, and raises
        /// <see cref="Resumed"/>. Does nothing unless paused.
        /// </summary>
        public void Resume()
        {
            if (disposed || !paused)
            {
                return;
            }
            paused = false;
            clock.Clear();
            for (int i = 0; i < calls.Count; i++)
            {
                calls[i].Failures = 0;
                calls[i].NotBefore = 0;
            }
            sessionFailures = 0;
            sessionNotBefore = 0;
            consecutiveFailures = 0;
            events.ResetBackoff();
            // Raised from the next Update, like every other callback.
            deferred.Add(RaiseResumed);
        }

        /// <summary>Writes the state to the store; the client already saves after every change.</summary>
        public void Flush()
        {
            if (!disposed)
            {
                Save();
            }
        }

        /// <summary>Aborts requests and releases the client. Pending callbacks are dropped; stored intents are resent by the next client on the same store.</summary>
        public void Dispose()
        {
            if (disposed)
            {
                return;
            }
            disposed = true;
            generation++;
            try
            {
                transport.CancelAll();
            }
            finally
            {
                rng.Dispose();
            }
        }

        /// <summary>
        /// Opens a new session with a new key, or joins the session request in
        /// flight. The client also opens sessions by itself: at start without a
        /// token, before the token expires, and after a 401.
        /// </summary>
        public void OpenSession(Action<ArenaResult<SessionResponse>> done)
        {
            ThrowIfDisposed();
            if (haltError != null)
            {
                DeferFailure(done, haltError);
                return;
            }
            if (done != null)
            {
                sessionWaiters.Add(done);
            }
            if (sessionCall == null)
            {
                sessionNotBefore = 0;
                StartSession();
            }
        }

        /// <param name="status">open (default when empty), closed, voided, settled or all.</param>
        public void ListTournaments(string status, int offset, int limit, Action<ArenaResult<TournamentListResponse>> done)
        {
            if (string.IsNullOrEmpty(status))
            {
                status = TournamentStatus.Open;
            }
            if (offset < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(offset));
            }
            if (limit < 1 || limit > 100)
            {
                throw new ArgumentOutOfRangeException(nameof(limit), "limit is 1 to 100");
            }
            StartRead("/v1/tournaments?status=" + Uri.EscapeDataString(status) +
                      "&offset=" + offset.ToString(CultureInfo.InvariantCulture) +
                      "&limit=" + limit.ToString(CultureInfo.InvariantCulture), done);
        }

        /// <summary>Enters a tournament (intent, R3).</summary>
        public void Join(string tournamentId, Action<ArenaResult<JoinResponse>> done)
        {
            CreateIntent(IntentRoutes.Join, TournamentPath(tournamentId) + "/join", SeqBody, done);
        }

        /// <summary>Deals a round, 1 to 3 (intent, R4). The deal view is kept as an audit until <see cref="RemoveAudit"/>.</summary>
        public void Deal(string tournamentId, int round, Action<ArenaResult<RoundResponse>> done)
        {
            CreateIntent(IntentRoutes.Deal, RoundPath(tournamentId, round) + "/deal", SeqBody, done);
        }

        /// <summary>Plays the top card of a column, 0 to 6, with the move_index of the view it was chosen from (intent, R6).</summary>
        public void Play(string tournamentId, int round, int moveIndex, int column, Action<ArenaResult<RoundResponse>> done)
        {
            if (column < 0 || column > 6)
            {
                throw new ArgumentOutOfRangeException(nameof(column), "column is 0 to 6");
            }
            CreateMove(tournamentId, round, moveIndex, MoveKind.Play, column, done);
        }

        /// <summary>Draws from the stock, with the move_index of the view it was chosen from (intent, R6).</summary>
        public void Draw(string tournamentId, int round, int moveIndex, Action<ArenaResult<RoundResponse>> done)
        {
            CreateMove(tournamentId, round, moveIndex, MoveKind.Draw, MoveKind.NoColumn, done);
        }

        /// <summary>Ends a round and fixes its score; finishing a finished round returns its final view (intent, R7).</summary>
        public void Finish(string tournamentId, int round, Action<ArenaResult<RoundResponse>> done)
        {
            CreateIntent(IntentRoutes.Finish, RoundPath(tournamentId, round) + "/finish", SeqBody, done);
        }

        /// <summary>Claims the settled payout once (intent, R9).</summary>
        public void ClaimPayout(string tournamentId, Action<ArenaResult<ClaimResponse>> done)
        {
            CreateIntent(IntentRoutes.Claim, TournamentPath(tournamentId) + "/payout/claim", SeqBody, done);
        }

        /// <summary>Reads a round (R5) with min_slot set to the slot of the last definitive intent.</summary>
        public void GetRound(string tournamentId, int round, Action<ArenaResult<RoundResponse>> done)
        {
            string path = RoundPath(tournamentId, round);
            if (state.last_intent_slot > 0)
            {
                path += "?min_slot=" + state.last_intent_slot.ToString(CultureInfo.InvariantCulture);
            }
            StartRead(path, done);
        }

        /// <summary>Reads standings (R8).</summary>
        public void GetLeaderboard(string tournamentId, int offset, int limit, Action<ArenaResult<LeaderboardResponse>> done)
        {
            if (offset < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(offset));
            }
            if (limit < 1 || limit > 100)
            {
                throw new ArgumentOutOfRangeException(nameof(limit), "limit is 1 to 100");
            }
            StartRead(TournamentPath(tournamentId) + "/leaderboard?offset=" + offset.ToString(CultureInfo.InvariantCulture) +
                      "&limit=" + limit.ToString(CultureInfo.InvariantCulture), done);
        }

        /// <summary>The stored deal audit of a round, or null.</summary>
        public RoundAudit FindAudit(string tournamentId, int round)
        {
            RoundAudit[] audits = state.audits;
            for (int i = 0; i < audits.Length; i++)
            {
                if (audits[i].tournament_id == tournamentId && audits[i].round == round)
                {
                    return audits[i];
                }
            }
            return null;
        }

        /// <summary>Forgets a round's deal audit once the game has checked the revealed seed.</summary>
        public void RemoveAudit(string tournamentId, int round)
        {
            ThrowIfDisposed();
            if (RemoveAuditEntry(tournamentId, round))
            {
                Save();
            }
        }

        // ---- internal surface for EventFollower and the harness ----

        internal ClientState State
        {
            get { return state; }
        }

        internal ArenaClientOptions Options
        {
            get { return options; }
        }

        internal Backoff BackoffPolicy
        {
            get { return backoff; }
        }

        internal bool CanFollowEvents
        {
            get
            {
                return options.FollowEvents && haltError == null && state.session_token.Length > 0 &&
                       state.player_id.Length > 0;
            }
        }

        internal long MonotonicNow()
        {
            return monotonicMs();
        }

        internal Call NewCall(CallKind kind, string method, string pathAndQuery, string body, string key, bool auth, int timeoutMs)
        {
            Call c = new Call
            {
                Kind = kind,
                Method = method,
                PathAndQuery = pathAndQuery,
                Body = body ?? "",
                Key = key ?? "",
                Auth = auth,
                TimeoutMs = timeoutMs,
            };
            calls.Add(c);
            return c;
        }

        internal void RemoveCall(Call c)
        {
            c.Removed = true;
            c.InFlight = false;
            calls.Remove(c);
        }

        internal T Parse<T>(string body) where T : class
        {
            if (string.IsNullOrEmpty(body))
            {
                return null;
            }
            try
            {
                return json.FromJson<T>(body);
            }
            catch (Exception)
            {
                return null;
            }
        }

        internal ArenaError ErrorFrom(HttpResponse response, ErrorBody error)
        {
            if (response.Status == 0)
            {
                string message = response.TransportError.Length > 0 ? response.TransportError : "no response";
                return new ArenaError(0, LocalErrorCodes.NoResponse, response.TimedOut ? "timed out: " + message : message, true);
            }
            if (error != null)
            {
                return new ArenaError(response.Status, error.code, error.message ?? "", error.retryable);
            }
            return new ArenaError(response.Status, LocalErrorCodes.BadResponse,
                "HTTP " + response.Status.ToString(CultureInfo.InvariantCulture) + " without a readable error body",
                response.Status >= 500 || response.Status == 429);
        }

        internal void AdvanceEventsCursor(long cursor)
        {
            if (cursor > state.events_cursor)
            {
                state.events_cursor = cursor;
                Save();
            }
        }

        // ---- request engine ----

        void DrainCompletions()
        {
            if (completions.Count == 0)
            {
                return;
            }
            List<Completion> batch = completions;
            completions = draining;
            draining = batch;
            int i = 0;
            try
            {
                for (; i < batch.Count && !disposed; i++)
                {
                    HandleCompletion(batch[i]);
                }
            }
            finally
            {
                // A callback that threw leaves the rest of the batch for the next Update.
                if (!disposed && i + 1 < batch.Count)
                {
                    completions.InsertRange(0, batch.GetRange(i + 1, batch.Count - i - 1));
                }
                batch.Clear();
            }
        }

        void RunDeferred()
        {
            if (deferred.Count == 0)
            {
                return;
            }
            Action[] batch = deferred.ToArray();
            deferred.Clear();
            int i = 0;
            try
            {
                for (; i < batch.Length && !disposed; i++)
                {
                    batch[i]();
                }
            }
            finally
            {
                if (!disposed && i + 1 < batch.Length)
                {
                    Action[] rest = new Action[batch.Length - i - 1];
                    Array.Copy(batch, i + 1, rest, 0, rest.Length);
                    deferred.InsertRange(0, rest);
                }
            }
        }

        void Send(Call c, long now)
        {
            string url;
            if (c.RedirectUrl.Length > 0)
            {
                url = c.RedirectUrl;
                c.RedirectUrl = "";
                c.Followed = true;
                c.BaseUsed = Origin(url);
            }
            else
            {
                c.BaseUsed = CurrentBaseUrl();
                url = c.BaseUsed + c.PathAndQuery;
                c.Followed = false;
            }
            bool post = c.Method == "POST";
            HttpHeader[] headers = new HttpHeader[(c.Auth ? 1 : 0) + (c.Key.Length > 0 ? 1 : 0) + (post ? 1 : 0)];
            int k = 0;
            if (c.Auth)
            {
                headers[k++] = new HttpHeader(Headers.Authorization, "Bearer " + state.session_token);
            }
            if (c.Key.Length > 0)
            {
                headers[k++] = new HttpHeader(Headers.IdempotencyKey, c.Key);
            }
            if (post)
            {
                headers[k] = new HttpHeader(Headers.ContentType, "application/json");
            }
            HttpRequest request = new HttpRequest
            {
                Method = c.Method,
                Url = url,
                Body = c.Body,
                Headers = headers,
                TimeoutMs = c.TimeoutMs,
            };
            c.TokenUsed = c.Auth ? state.session_token : "";
            c.InFlight = true;
            c.WatchdogFired = false;
            c.Attempt++;
            c.SentAt = now;
            int attempt = c.Attempt;
            int gen = generation;
            try
            {
                transport.Send(request, response => completions.Add(new Completion(c, attempt, gen, response)));
            }
            catch (Exception e)
            {
                completions.Add(new Completion(c, attempt, gen, new HttpResponse { TransportError = "the transport threw: " + e.Message }));
            }
        }

        void HandleCompletion(Completion done)
        {
            Call c = done.Call;
            if (done.Generation != generation || c.Removed || !c.InFlight || done.Attempt != c.Attempt)
            {
                return;
            }
            c.InFlight = false;
            HttpResponse r = Normalize(done.Response);
            long now = monotonicMs();
            if (r.Status == 0)
            {
                if (state.leader_url.Length > 0)
                {
                    if (c.BaseUsed == state.leader_url)
                    {
                        DropLeader();
                    }
                }
                else if (c.BaseUsed == CurrentBaseUrl())
                {
                    baseIndex = (baseIndex + 1) % baseUrls.Length;
                }
                Retry(c, ErrorFrom(r, null), 0, now);
                return;
            }

            long serverTime;
            if (TryParseLong(r.Header(Headers.ServerTimeMs), out serverTime))
            {
                clock.AddSample(serverTime, now - c.SentAt, now);
            }
            lastLeaderHint = TrimSlash(r.Header(Headers.Leader));

            if (r.Status >= 300 && r.Status < 400)
            {
                string location = r.Header(Headers.Location);
                if (location.Length == 0 && lastLeaderHint.Length > 0)
                {
                    location = lastLeaderHint + c.PathAndQuery;
                }
                else if (location.StartsWith("/", StringComparison.Ordinal))
                {
                    location = c.BaseUsed + location;
                }
                string origin = Origin(location);
                if (origin.Length == 0)
                {
                    Retry(c, ErrorFrom(r, ParseError(r.Body)), Backoff.ParseRetryAfter(r.Header(Headers.RetryAfter)), now);
                    return;
                }
                CacheLeader(origin);
                if (c.Followed)
                {
                    // A second redirect in a row is not followed (section 7.1.5, rule 3).
                    Retry(c, ErrorFrom(r, ParseError(r.Body)), Backoff.ParseRetryAfter(r.Header(Headers.RetryAfter)), now);
                }
                else
                {
                    c.RedirectUrl = location;
                    c.NotBefore = 0;
                }
                return;
            }

            bool ok = r.Status >= 200 && r.Status < 300;
            ErrorBody error = ok ? null : ParseError(r.Body);
            if (r.Status == 401 && c.Auth)
            {
                Unauthorized(c, r, error, now);
                return;
            }
            if (error != null && error.code == ErrorCodes.NoLeader && state.leader_url.Length > 0 && c.BaseUsed == state.leader_url)
            {
                DropLeader();
            }
            bool retryable = !ok && (error != null
                ? error.retryable
                : r.Status >= 500 || r.Status == 429 || r.Status == 409 || r.Status == 408);
            if (retryable)
            {
                Retry(c, ErrorFrom(r, error), Backoff.ParseRetryAfter(r.Header(Headers.RetryAfter)), now);
                return;
            }
            c.Failures = 0;
            c.Unauthorized = 0;
            consecutiveFailures = 0;
            if (haltError == null)
            {
                SetConnection(ConnectionState.Online);
            }
            c.Answered(r, error);
        }

        void Retry(Call c, ArenaError error, long retryAfterSeconds, long now)
        {
            c.Failures++;
            consecutiveFailures++;
            SetConnection(consecutiveFailures >= ConnectionProblemFailures ? ConnectionState.Offline : ConnectionState.Retrying);
            if (c.MaxFailures > 0 && c.Failures >= c.MaxFailures)
            {
                RemoveCall(c);
                if (c.Abandoned != null)
                {
                    c.Abandoned(error);
                }
                return;
            }
            c.NotBefore = now + backoff.DelayMs(c.Failures, retryAfterSeconds);
        }

        void Unauthorized(Call c, HttpResponse r, ErrorBody error, long now)
        {
            c.Unauthorized++;
            if (c.TokenUsed.Length > 0 && state.session_token == c.TokenUsed)
            {
                state.session_token = "";
                state.session_expires_at_ms = 0;
                Save();
            }
            if (c.Unauthorized > 1)
            {
                // A fresh token was refused again: back off instead of looping.
                Retry(c, ErrorFrom(r, error), 0, now);
            }
            else
            {
                // Resent without delay once a session exists (section 9.3).
                c.NotBefore = 0;
            }
        }

        void SetConnection(ConnectionState next)
        {
            if (connection == next)
            {
                return;
            }
            connection = next;
            Action<ConnectionState> handler = ConnectionChanged;
            if (handler != null)
            {
                handler(next);
            }
        }

        string CurrentBaseUrl()
        {
            return state.leader_url.Length > 0 ? state.leader_url : baseUrls[baseIndex % baseUrls.Length];
        }

        void CacheLeader(string origin)
        {
            if (state.leader_url != origin)
            {
                state.leader_url = origin;
                Save();
            }
        }

        void DropLeader()
        {
            string dropped = state.leader_url;
            if (lastLeaderHint.Length > 0 && lastLeaderHint != dropped)
            {
                state.leader_url = lastLeaderHint;
            }
            else
            {
                state.leader_url = "";
                baseIndex = (baseIndex + 1) % baseUrls.Length;
            }
            Save();
        }

        ErrorBody ParseError(string body)
        {
            ErrorBody error = Parse<ErrorBody>(body);
            if (error == null || string.IsNullOrEmpty(error.code))
            {
                return null;
            }
            return error;
        }

        // ---- sessions ----

        bool NeedsSession(long now)
        {
            if (state.session_token.Length == 0)
            {
                return true;
            }
            // A server whose token lifetime is shorter than twice the margin
            // would otherwise be asked for a session on every frame.
            long margin = options.SessionRefreshMarginMs;
            if (sessionLifetimeMs > 0 && margin > sessionLifetimeMs / 2)
            {
                margin = sessionLifetimeMs / 2;
            }
            return clock.HasEstimate && state.session_expires_at_ms - clock.NowMs(now) < margin;
        }

        void StartSession()
        {
            SessionRequest request = new SessionRequest
            {
                device_id = state.device_id,
                device_secret = state.device_secret,
                jurisdiction = options.Jurisdiction ?? "",
                age = options.Age,
            };
            Call c = NewCall(CallKind.Session, "POST", "/v1/session", json.ToJson(request), RandomHex(16), false, options.RequestTimeoutMs);
            c.ForResync = resyncAwaitingSession;
            c.NotBefore = sessionNotBefore;
            c.Answered = (response, error) => SessionAnswered(c, response, error);
            sessionCall = c;
        }

        void SessionAnswered(Call c, HttpResponse r, ErrorBody error)
        {
            RemoveCall(c);
            if (sessionCall == c)
            {
                sessionCall = null;
            }
            long now = monotonicMs();
            long slot;
            TryParseLong(r.Header(Headers.Slot), out slot);
            if (r.Status < 200 || r.Status >= 300)
            {
                ArenaError failure = ErrorFrom(r, error);
                if (r.Status == 403 || IsClientBugStatus(r.Status))
                {
                    Halt(failure);
                }
                else
                {
                    SessionFailed();
                }
                FlushSessionWaiters(ArenaResult<SessionResponse>.Failure(failure, slot));
                return;
            }
            SessionResponse s = Parse<SessionResponse>(r.Body);
            if (s == null || string.IsNullOrEmpty(s.session_token) || string.IsNullOrEmpty(s.player_id) || s.next_seq < 1)
            {
                SessionFailed();
                FlushSessionWaiters(ArenaResult<SessionResponse>.Failure(
                    new ArenaError(r.Status, LocalErrorCodes.BadResponse, "the session response could not be read", true), slot));
                return;
            }
            long serverTime;
            if (TryParseLong(r.Header(Headers.ServerTimeMs), out serverTime) && s.expires_at_ms <= serverTime)
            {
                // The replay of an old key carries an expired token: ask again
                // with a new key (section 9.2.3). Waiters keep waiting.
                SessionFailed();
                return;
            }
            sessionFailures = 0;
            sessionNotBefore = 0;
            if (s.expires_at_ms > s.issued_at_ms)
            {
                sessionLifetimeMs = s.expires_at_ms - s.issued_at_ms;
            }
            bool playerChanged = state.player_id.Length > 0 && state.player_id != s.player_id;
            bool resynced = playerChanged || (resyncAwaitingSession && c.ForResync);
            PendingIntent[] failed = null;
            state.player_id = s.player_id;
            state.session_token = s.session_token;
            state.session_expires_at_ms = s.expires_at_ms;
            if (playerChanged)
            {
                // The store belonged to another binding (section 9.2.3).
                failed = queue.RemoveAll();
                DropHeadCall();
                state.last_assigned_seq = s.next_seq - 1;
                state.last_intent_slot = 0;
                state.events_cursor = 0;
                state.audits = new RoundAudit[0];
                events.Detach();
                DropCalls(CallKind.Events);
            }
            else if (resynced)
            {
                state.last_assigned_seq = s.next_seq - 1;
            }
            else if (s.next_seq - 1 > state.last_assigned_seq)
            {
                state.last_assigned_seq = s.next_seq - 1;
            }
            if (resynced)
            {
                resyncAwaitingSession = false;
            }
            Save();
            FlushSessionWaiters(ArenaResult<SessionResponse>.Success(s, slot));
            if (failed != null)
            {
                FailIntents(failed);
            }
            if (resynced)
            {
                RaiseResynced();
            }
        }

        void SessionFailed()
        {
            sessionFailures++;
            sessionNotBefore = monotonicMs() + backoff.DelayMs(sessionFailures, 0);
        }

        void FlushSessionWaiters(ArenaResult<SessionResponse> result)
        {
            if (sessionWaiters.Count == 0)
            {
                return;
            }
            Action<ArenaResult<SessionResponse>>[] waiters = sessionWaiters.ToArray();
            sessionWaiters.Clear();
            for (int i = 0; i < waiters.Length; i++)
            {
                waiters[i](result);
            }
        }

        void Halt(ArenaError error)
        {
            if (haltError != null)
            {
                return;
            }
            haltError = error;
            Call[] dropped = calls.ToArray();
            for (int i = 0; i < dropped.Length; i++)
            {
                RemoveCall(dropped[i]);
            }
            sessionCall = null;
            headCall = null;
            generation++;
            transport.CancelAll();
            SetConnection(ConnectionState.Offline);
            for (int i = 0; i < dropped.Length; i++)
            {
                if (dropped[i].Abandoned != null)
                {
                    dropped[i].Abandoned(error);
                }
            }
            events.Detach();
            Action<ArenaError> handler = Halted;
            if (handler != null)
            {
                handler(error);
            }
        }

        // ---- intents ----

        void CreateMove(string tournamentId, int round, int moveIndex, string kind, int column, Action<ArenaResult<RoundResponse>> done)
        {
            if (moveIndex < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(moveIndex));
            }
            string path = RoundPath(tournamentId, round) + "/moves";
            CreateIntent(IntentRoutes.Move, path, seq => json.ToJson(new MoveRequest
            {
                seq = seq,
                move_index = moveIndex,
                kind = kind,
                column = column,
            }), done);
        }

        string SeqBody(long seq)
        {
            return json.ToJson(new SeqRequest { seq = seq });
        }

        void CreateIntent<T>(string route, string path, Func<long, string> body, Action<ArenaResult<T>> done) where T : class
        {
            ThrowIfDisposed();
            ArenaError error = null;
            if (haltError != null)
            {
                error = haltError;
            }
            else if (resyncAwaitingSession)
            {
                error = new ArenaError(0, LocalErrorCodes.ResyncRequired,
                    "the client is resynchronising its sequence numbers; wait for Resynced", true);
            }
            if (error == null)
            {
                Action<IntentOutcome> callback = null;
                if (done != null)
                {
                    callback = outcome => done(ToResult<T>(outcome));
                }
                queue.Create(route, path, body, RandomHex(16), clock.NowMs(monotonicMs()), callback, out error);
            }
            if (error != null)
            {
                DeferFailure(done, error);
            }
        }

        void EnsureHeadCall(long now)
        {
            PendingIntent head = queue.Head;
            if (headCall != null || head == null || resyncAwaitingSession)
            {
                return;
            }
            if (head.idempotency_key != headSinceKey)
            {
                headSinceKey = head.idempotency_key;
                headSinceMs = now;
            }
            Call c = NewCall(CallKind.Intent, head.method, head.path, head.body, head.idempotency_key, true, options.RequestTimeoutMs);
            c.Intent = head;
            c.Answered = (response, error) => IntentAnswered(c, response, error);
            headCall = c;
        }

        void IntentAnswered(Call c, HttpResponse r, ErrorBody error)
        {
            RemoveCall(c);
            if (headCall == c)
            {
                headCall = null;
            }
            PendingIntent intent = c.Intent;
            if (queue.Head != intent)
            {
                return;
            }
            bool ok = r.Status >= 200 && r.Status < 300;
            long slot;
            TryParseLong(r.Header(Headers.Slot), out slot);
            if (slot > state.last_intent_slot)
            {
                state.last_intent_slot = slot;
            }
            queue.RemoveHead();
            if (ok && intent.route == IntentRoutes.Deal)
            {
                RecordAudit(r.Body);
            }
            IntentOutcome outcome = new IntentOutcome
            {
                Route = intent.route,
                IdempotencyKey = intent.idempotency_key,
                Seq = intent.seq,
                Status = r.Status,
                Body = r.Body,
                Slot = slot,
                Error = ok ? null : ErrorFrom(r, error),
            };
            bool sequenceError = error != null && (error.code == ErrorCodes.StaleSeq || error.code == ErrorCodes.SeqGap);
            if (!sequenceError && (ok || !IsClientBugStatus(r.Status)))
            {
                Save();
                Deliver(outcome);
                return;
            }

            // Resynchronisation (section 9.2.4): the store was lost, copied or
            // corrupted, or the client sent something the server cannot read.
            PendingIntent[] failed = queue.RemoveAll();
            long next;
            bool known = TryParseLong(r.Header(Headers.NextSeq), out next) && next >= 1;
            if (known)
            {
                state.last_assigned_seq = next - 1;
            }
            else
            {
                resyncAwaitingSession = true;
            }
            Save();
            Deliver(outcome);
            FailIntents(failed);
            if (known)
            {
                RaiseResynced();
            }
        }

        void Deliver(IntentOutcome outcome)
        {
            Action<IntentOutcome> callback = queue.TakeCallback(outcome.IdempotencyKey);
            if (callback != null)
            {
                callback(outcome);
            }
            Action<IntentOutcome> handler = IntentCompleted;
            if (handler != null)
            {
                handler(outcome);
            }
        }

        void FailIntents(PendingIntent[] failed)
        {
            for (int i = 0; i < failed.Length; i++)
            {
                Deliver(new IntentOutcome
                {
                    Route = failed[i].route,
                    IdempotencyKey = failed[i].idempotency_key,
                    Seq = failed[i].seq,
                    Error = new ArenaError(0, LocalErrorCodes.ResyncRequired,
                        "dropped by a resynchronisation before it received an answer", false),
                });
            }
        }

        void RaiseResumed()
        {
            Action handler = Resumed;
            if (handler != null)
            {
                handler();
            }
        }

        void RaiseResynced()
        {
            Action handler = Resynced;
            if (handler != null)
            {
                handler();
            }
        }

        void DropHeadCall()
        {
            if (headCall != null)
            {
                RemoveCall(headCall);
                headCall = null;
            }
        }

        void DropCalls(CallKind kind)
        {
            for (int i = calls.Count - 1; i >= 0; i--)
            {
                if (calls[i].Kind == kind)
                {
                    calls[i].Removed = true;
                    calls.RemoveAt(i);
                }
            }
        }

        ArenaResult<T> ToResult<T>(IntentOutcome outcome) where T : class
        {
            if (outcome.Error != null)
            {
                return ArenaResult<T>.Failure(outcome.Error, outcome.Slot);
            }
            T value = Parse<T>(outcome.Body);
            if (value == null)
            {
                return ArenaResult<T>.Failure(
                    new ArenaError(outcome.Status, LocalErrorCodes.BadResponse, "the response body could not be read", false),
                    outcome.Slot);
            }
            return ArenaResult<T>.Success(value, outcome.Slot);
        }

        void RecordAudit(string body)
        {
            RoundResponse response = Parse<RoundResponse>(body);
            if (response == null || response.round == null || string.IsNullOrEmpty(response.round.commitment))
            {
                return;
            }
            string view;
            try
            {
                view = json.ToJson(response.round);
            }
            catch (Exception)
            {
                view = "";
            }
            RemoveAuditEntry(response.round.tournament_id, response.round.round);
            RoundAudit[] before = state.audits;
            int keep = before.Length < MaxAudits ? before.Length : MaxAudits - 1;
            RoundAudit[] after = new RoundAudit[keep + 1];
            Array.Copy(before, before.Length - keep, after, 0, keep);
            after[keep] = new RoundAudit
            {
                tournament_id = response.round.tournament_id ?? "",
                round = response.round.round,
                commitment = response.round.commitment,
                deal_view_json = view,
            };
            state.audits = after;
        }

        bool RemoveAuditEntry(string tournamentId, int round)
        {
            RoundAudit[] before = state.audits;
            for (int i = 0; i < before.Length; i++)
            {
                if (before[i].tournament_id == tournamentId && before[i].round == round)
                {
                    RoundAudit[] after = new RoundAudit[before.Length - 1];
                    Array.Copy(before, 0, after, 0, i);
                    Array.Copy(before, i + 1, after, i, before.Length - i - 1);
                    state.audits = after;
                    return true;
                }
            }
            return false;
        }

        // ---- reads ----

        void StartRead<T>(string path, Action<ArenaResult<T>> done) where T : class
        {
            ThrowIfDisposed();
            if (haltError != null)
            {
                DeferFailure(done, haltError);
                return;
            }
            Call c = NewCall(CallKind.Read, "GET", path, "", "", true, options.RequestTimeoutMs);
            c.MaxFailures = options.MaxReadFailures > 0 ? options.MaxReadFailures : 0;
            c.Answered = (response, error) =>
            {
                RemoveCall(c);
                if (done == null)
                {
                    return;
                }
                long slot;
                TryParseLong(response.Header(Headers.Slot), out slot);
                if (response.Status >= 200 && response.Status < 300)
                {
                    T value = Parse<T>(response.Body);
                    done(value != null
                        ? ArenaResult<T>.Success(value, slot)
                        : ArenaResult<T>.Failure(new ArenaError(response.Status, LocalErrorCodes.BadResponse,
                            "the response body could not be read", false), slot));
                    return;
                }
                done(ArenaResult<T>.Failure(ErrorFrom(response, error), slot));
            };
            c.Abandoned = error =>
            {
                if (done != null)
                {
                    done(ArenaResult<T>.Failure(error, 0));
                }
            };
        }

        void DeferFailure<T>(Action<ArenaResult<T>> done, ArenaError error)
        {
            if (done != null)
            {
                deferred.Add(() => done(ArenaResult<T>.Failure(error, 0)));
            }
        }

        // ---- helpers ----

        void Save()
        {
            store.Save(state);
        }

        void ThrowIfDisposed()
        {
            if (disposed)
            {
                throw new ObjectDisposedException(nameof(ArenaClient));
            }
        }

        static bool ValidJurisdiction(string jurisdiction)
        {
            if (jurisdiction == null || jurisdiction.Length < 2 || jurisdiction.Length > 8)
            {
                return false;
            }
            for (int i = 0; i < jurisdiction.Length; i++)
            {
                if (jurisdiction[i] < 'A' || jurisdiction[i] > 'Z')
                {
                    return false;
                }
            }
            return true;
        }

        static bool IsClientBugStatus(int status)
        {
            return status == 400 || status == 404 || status == 405 || status == 413 || status == 422;
        }

        static string TournamentPath(string tournamentId)
        {
            if (string.IsNullOrEmpty(tournamentId))
            {
                throw new ArgumentException("a tournament id is required", nameof(tournamentId));
            }
            return "/v1/tournaments/" + Uri.EscapeDataString(tournamentId);
        }

        static string RoundPath(string tournamentId, int round)
        {
            if (round < 1 || round > 3)
            {
                throw new ArgumentOutOfRangeException(nameof(round), "round is 1 to 3");
            }
            return TournamentPath(tournamentId) + "/rounds/" + round.ToString(CultureInfo.InvariantCulture);
        }

        string RandomHex(int bytes)
        {
            byte[] buffer = new byte[bytes];
            rng.GetBytes(buffer);
            StringBuilder sb = new StringBuilder(bytes * 2);
            for (int i = 0; i < buffer.Length; i++)
            {
                sb.Append(buffer[i].ToString("x2", CultureInfo.InvariantCulture));
            }
            return sb.ToString();
        }

        static bool IsLowerHex(string s, int length)
        {
            if (s == null || s.Length != length)
            {
                return false;
            }
            for (int i = 0; i < s.Length; i++)
            {
                char ch = s[i];
                if (!((ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f')))
                {
                    return false;
                }
            }
            return true;
        }

        static bool TryParseLong(string s, out long value)
        {
            value = 0;
            return !string.IsNullOrEmpty(s) &&
                   long.TryParse(s.Trim(), NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out value);
        }

        static string TrimSlash(string url)
        {
            if (string.IsNullOrEmpty(url))
            {
                return "";
            }
            url = url.Trim();
            while (url.EndsWith("/", StringComparison.Ordinal))
            {
                url = url.Substring(0, url.Length - 1);
            }
            return url;
        }

        static string Origin(string url)
        {
            Uri uri;
            if (string.IsNullOrEmpty(url) || !Uri.TryCreate(url, UriKind.Absolute, out uri) ||
                (uri.Scheme != "http" && uri.Scheme != "https"))
            {
                return "";
            }
            return uri.GetLeftPart(UriPartial.Authority);
        }

        static string[] NormalizeBaseUrls(string[] urls)
        {
            List<string> result = new List<string>();
            if (urls != null)
            {
                for (int i = 0; i < urls.Length; i++)
                {
                    string url = TrimSlash(urls[i]);
                    if (Origin(url).Length > 0)
                    {
                        result.Add(url);
                    }
                }
            }
            return result.ToArray();
        }

        static HttpResponse Normalize(HttpResponse r)
        {
            if (r == null)
            {
                return new HttpResponse { TransportError = "the transport returned no response" };
            }
            if (r.Body == null)
            {
                r.Body = "";
            }
            if (r.Headers == null)
            {
                r.Headers = new HttpHeader[0];
            }
            if (r.TransportError == null)
            {
                r.TransportError = "";
            }
            return r;
        }

        static ClientState Normalize(ClientState s)
        {
            if (s == null)
            {
                s = new ClientState();
            }
            s.device_id = s.device_id ?? "";
            s.device_secret = s.device_secret ?? "";
            s.player_id = s.player_id ?? "";
            s.session_token = s.session_token ?? "";
            s.leader_url = TrimSlash(s.leader_url);
            List<PendingIntent> pending = new List<PendingIntent>();
            if (s.pending != null)
            {
                for (int i = 0; i < s.pending.Length; i++)
                {
                    PendingIntent p = s.pending[i];
                    if (p != null && !string.IsNullOrEmpty(p.idempotency_key) && !string.IsNullOrEmpty(p.path))
                    {
                        p.method = string.IsNullOrEmpty(p.method) ? "POST" : p.method;
                        p.body = p.body ?? "";
                        p.route = p.route ?? "";
                        pending.Add(p);
                    }
                }
            }
            s.pending = pending.ToArray();
            List<RoundAudit> audits = new List<RoundAudit>();
            if (s.audits != null)
            {
                for (int i = 0; i < s.audits.Length; i++)
                {
                    if (s.audits[i] != null)
                    {
                        audits.Add(s.audits[i]);
                    }
                }
            }
            s.audits = audits.ToArray();
            return s;
        }
    }

    internal enum CallKind
    {
        Session,
        Intent,
        Read,
        Events,
    }

    /// <summary>One logical request and its retry state.</summary>
    internal sealed class Call
    {
        public CallKind Kind;
        public string Method = "GET";
        public string PathAndQuery = "";
        public string Body = "";
        public string Key = "";
        public bool Auth;
        public int TimeoutMs;

        /// <summary>Consecutive retryable failures after which the call is abandoned; 0 for none.</summary>
        public int MaxFailures;

        public int Failures;
        public int Unauthorized;
        public long NotBefore;
        public bool InFlight;
        public bool WatchdogFired;
        public bool Removed;
        public int Attempt;
        public long SentAt;

        /// <summary>The absolute URL of a redirect to follow on the next send.</summary>
        public string RedirectUrl = "";

        /// <summary>True when the current attempt was the one redirect follow.</summary>
        public bool Followed;

        public string BaseUsed = "";
        public string TokenUsed = "";

        /// <summary>A session started while a resynchronisation waited for next_seq.</summary>
        public bool ForResync;

        public PendingIntent Intent;
        public Action<HttpResponse, ErrorBody> Answered;
        public Action<ArenaError> Abandoned;
    }

    internal struct Completion
    {
        public readonly Call Call;
        public readonly int Attempt;
        public readonly int Generation;
        public readonly HttpResponse Response;

        public Completion(Call call, int attempt, int generation, HttpResponse response)
        {
            Call = call;
            Attempt = attempt;
            Generation = generation;
            Response = response;
        }
    }
}
