using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using PaxosArena.Client.UnityAdapters;

namespace PaxosArena.Client.Harness
{
    /// <summary>
    /// Unit tests of the Core with a fake transport, a memory store and a fake
    /// monotonic clock: the queue, backoff, redirects, sessions,
    /// resynchronisation, pause and resume, the events cursor, the JSON shapes,
    /// the file store, and the Appendix A vectors through LadderAudit.
    /// </summary>
    public static class UnitTests
    {
        const string ServerTime = "1789200000000";
        const long Expiry = 1789203600000;
        const string PlayerA = "p-mzxw6ytboi4dqnbrgm2wqzlmn4";
        const string PlayerB = "p-7kq2mzl4x5b3vw6yh2r4tnq5ea";

        public static int Run(string filter)
        {
            List<KeyValuePair<string, Action>> tests = new List<KeyValuePair<string, Action>>
            {
                T("backoff ceiling, jitter and Retry-After floor", BackoffCeilingJitterAndFloor),
                T("server clock keeps the lowest-RTT sample of eight", ServerClockBestSample),
                T("session at start; credentials persist", SessionAtStartAndCredentialsPersist),
                T("intents are stored before sending and sent one at a time", IntentsStoredFirstAndSentOneAtATime),
                T("retryable answers resend the same bytes with backoff", RetryableAnswersResendSameBytes),
                T("a rule rejection consumes its number and the queue continues", RuleRejectionKeepsQueue),
                T("stale_seq resynchronises from X-Arena-Next-Seq", StaleSeqResyncsFromHeader),
                T("seq_gap without the header resynchronises through a session", SeqGapResyncsThroughSession),
                T("401 opens a session and resends the head at once", UnauthorizedRefreshesAndResends),
                T("307 is followed once and the leader cached", RedirectFollowedOnceAndCached),
                T("the leader cache is dropped on no response and no_leader", LeaderDroppedOnFailure),
                T("a restarted client resends the stored intent with its key", RestartResendsStoredIntent),
                T("queue_full assigns nothing and answers inside Update", QueueFullAssignsNothing),
                T("pause aborts requests; resume resends at once", PauseAndResume),
                T("events cursor advances only after delivery", EventsCursorAdvancesAfterDelivery),
                T("an expired token from a session replay asks again with a new key", ExpiredSessionReplayAsksAgain),
                T("sessions refresh before expiry, at half a short lifetime", SessionRefreshMargin),
                T("options are validated when the client is built", OptionsValidated),
                T("device_mismatch halts the client and keeps the store", DeviceMismatchHalts),
                T("a different player fails the stored intents", PlayerChangeFailsStoredIntents),
                T("reads send min_slot and give up after MaxReadFailures", ReadsUseMinSlotAndGiveUp),
                T("the watchdog ends a request the transport never answers", WatchdogEndsSilentRequest),
                T("request and response JSON match the contract", JsonMatchesContract),
                T("Appendix A deals and greedy games", LadderAuditVectors),
                T("the persistent store writes atomically and recovers", PersistentStoreRecovers),
                T("a throwing callback during a resynchronisation stops no other notification", ThrowingCallbackDuringResync),
                T("a throwing callback or connection handler on a 2xx loses nothing", ThrowingCallbackOnSuccess),
                T("a 2xx without X-Arena-Slot or a readable body is resent with its key", UnreadableSuccessIsResent),
                T("long-poll responses are not clock samples", LongPollsAreNotClockSamples),
                T("after resume one authenticated request goes first; an expired token costs one 401", ResumeProbesBeforeSending),
                T("a redirect from https to http is neither followed nor cached", HttpsDowngradeRefused),
                T("an answer the store cannot save is resent, then delivered once", StoreFailureKeepsTheHead),
                T("an idle Update allocates nothing", IdleUpdateAllocatesNothing),
                T("the store recovers from its .tmp, keeps unreadable files and allows one client", PersistentStoreKeepsIdentity),
                T("real HTTP: a follower's 307, headers and timeouts through HttpClientTransport", HttpTransportTests.RoundTrip),
            };
            int failed = 0;
            int ran = 0;
            foreach (KeyValuePair<string, Action> test in tests)
            {
                if (!string.IsNullOrEmpty(filter) && test.Key.IndexOf(filter, StringComparison.OrdinalIgnoreCase) < 0)
                {
                    continue;
                }
                ran++;
                try
                {
                    test.Value();
                    Console.WriteLine("ok    " + test.Key);
                }
                catch (Exception e)
                {
                    failed++;
                    Console.WriteLine("FAIL  " + test.Key + ": " + e.Message);
                    Console.WriteLine(e.StackTrace);
                }
            }
            Console.WriteLine(failed == 0 ? "PASS  " + ran + " unit tests" : "FAIL  " + failed + " of " + ran + " unit tests");
            return failed;
        }

        static KeyValuePair<string, Action> T(string name, Action body)
        {
            return new KeyValuePair<string, Action>(name, body);
        }

        // ---- assertions ----

        static void Check(bool condition, string message)
        {
            if (!condition)
            {
                throw new Exception(message);
            }
        }

        static void Eq<TValue>(TValue expected, TValue actual, string what)
        {
            if (!EqualityComparer<TValue>.Default.Equals(expected, actual))
            {
                throw new Exception(what + ": expected <" + expected + ">, got <" + actual + ">");
            }
        }

        // ---- rig ----

        sealed class Rig
        {
            public long Now = 1000;
            public double Random = 0.5;
            public readonly FakeTransport Transport = new FakeTransport();
            public readonly SystemTextJson Json = new SystemTextJson();
            public readonly MemoryStore Store;
            public readonly ArenaClientOptions Options;
            public readonly ArenaClient Client;
            public readonly List<IntentOutcome> Outcomes = new List<IntentOutcome>();
            public int Resyncs;
            public int Resumes;

            public Rig(Action<ArenaClientOptions> configure = null, MemoryStore store = null)
            {
                Options = new ArenaClientOptions
                {
                    BaseUrls = new[] { "http://a.test", "http://b.test/" },
                    Jurisdiction = "TR",
                    Age = 30,
                    FollowEvents = false,
                };
                if (configure != null)
                {
                    configure(Options);
                }
                Store = store ?? new MemoryStore(Json);
                Client = new ArenaClient(Options, Transport, Store, Json, () => Now, () => Random);
                Client.IntentCompleted += o => Outcomes.Add(o);
                Client.Resynced += () => Resyncs++;
                Client.Resumed += () => Resumes++;
            }

            public ClientState Stored
            {
                get { return Json.FromJson<ClientState>(Store.Saved); }
            }

            public void Tick(long advanceMs = 0)
            {
                Now += advanceMs;
                Client.Update();
            }

            /// <summary>The only unanswered request, which must match method and path prefix.</summary>
            public FakeTransport.Exchange Single(string method, string pathPrefix)
            {
                List<FakeTransport.Exchange> open = Transport.Open();
                Check(open.Count == 1, "expected one open request for " + pathPrefix + ", have " + Describe(open));
                FakeTransport.Exchange e = open[0];
                Eq(method, e.Request.Method, "method");
                Check(e.Path.StartsWith(pathPrefix, StringComparison.Ordinal), "path " + e.Path + " does not start with " + pathPrefix);
                return e;
            }

            public FakeTransport.Exchange Find(string pathPrefix)
            {
                foreach (FakeTransport.Exchange e in Transport.Open())
                {
                    if (e.Path.StartsWith(pathPrefix, StringComparison.Ordinal))
                    {
                        return e;
                    }
                }
                throw new Exception("no open request for " + pathPrefix + "; open: " + Describe(Transport.Open()));
            }

            public void NoneOpen()
            {
                List<FakeTransport.Exchange> open = Transport.Open();
                Check(open.Count == 0, "expected no open request, have " + Describe(open));
            }

            public void Answer(FakeTransport.Exchange e, int status, string body, params string[] headers)
            {
                Transport.Answer(e, Response(status, body, headers));
                Client.Update();
            }

            public void AnswerSession(string player, string token, long nextSeq)
            {
                FakeTransport.Exchange e = Find("/v1/session");
                Answer(e, 200, SessionBody(player, token, nextSeq, Expiry), "X-Arena-Server-Time-Ms", ServerTime, "X-Arena-Slot", "5");
            }

            public string SessionBody(string player, string token, long nextSeq, long expires, long lifetimeMs = 3600000)
            {
                return Json.ToJson(new SessionResponse
                {
                    slot = 5,
                    next_seq = nextSeq,
                    player_id = player,
                    new_player = true,
                    session_token = token,
                    issued_at_ms = expires - lifetimeMs,
                    expires_at_ms = expires,
                    jurisdiction = "TR",
                    age = 30,
                });
            }

            /// <summary>Starts the client and completes its first session.</summary>
            public Rig Started(string token = "tok1")
            {
                Tick();
                AnswerSession(PlayerA, token, 1);
                return this;
            }
        }

        static HttpResponse Response(int status, string body, params string[] headers)
        {
            HttpHeader[] list = new HttpHeader[headers.Length / 2 + 1];
            for (int i = 0; i + 1 < headers.Length; i += 2)
            {
                list[i / 2] = new HttpHeader(headers[i], headers[i + 1]);
            }
            list[list.Length - 1] = new HttpHeader("Content-Type", "application/json");
            return new HttpResponse { Status = status, Body = body ?? "", Headers = list };
        }

        static string ErrorJson(string code, bool retryable)
        {
            return "{\"code\":\"" + code + "\",\"message\":\"test\",\"retryable\":" + (retryable ? "true" : "false") + "}";
        }

        static string Describe(List<FakeTransport.Exchange> open)
        {
            List<string> parts = new List<string>();
            foreach (FakeTransport.Exchange e in open)
            {
                parts.Add(e.Request.Method + " " + e.Request.Url);
            }
            return "[" + string.Join(", ", parts) + "]";
        }

        static string JoinBody(Rig rig, long nextSeq)
        {
            return rig.Json.ToJson(new JoinResponse { slot = 40, next_seq = nextSeq, tournament_id = "t1", join_seq = 1, entry_fee = 500, rounds = 3 });
        }

        static string RoundBody(Rig rig, long nextSeq, int moveIndex)
        {
            RoundResponse r = new RoundResponse { slot = 41, next_seq = nextSeq };
            r.round.tournament_id = "t1";
            r.round.round = 1;
            r.round.status = RoundStatus.Playing;
            r.round.move_index = moveIndex;
            r.round.commitment = "5e188f4cb340f1c0e94c8821b8f72279b339f5698c66e659b22d32b3f9b29065";
            return rig.Json.ToJson(r);
        }

        // ---- tests ----

        static void BackoffCeilingJitterAndFloor()
        {
            Backoff high = new Backoff(250, 8000, () => 0.999999);
            long[] ceilings = { 0, 250, 500, 1000, 2000, 4000, 8000, 8000 };
            for (int f = 0; f < ceilings.Length; f++)
            {
                Eq(ceilings[f], high.CeilingMs(f), "ceiling after " + f + " failures");
            }
            Eq(8000L, high.CeilingMs(200), "ceiling after 200 failures");
            Eq(250L, high.DelayMs(1, 0), "delay at the top of the range");
            Eq(8000L, high.DelayMs(60, 0), "delay at the cap");
            Backoff low = new Backoff(250, 8000, () => 0.0);
            Eq(0L, low.DelayMs(5, 0), "delay at the bottom of the range");
            Eq(2000L, low.DelayMs(1, 2), "Retry-After is a floor");
            Backoff mid = new Backoff(250, 8000, () => 0.5);
            Eq(4000L, mid.DelayMs(6, 1), "a larger jittered delay beats Retry-After");
            Eq(3L, Backoff.ParseRetryAfter("3"), "Retry-After seconds");
            Eq(0L, Backoff.ParseRetryAfter(""), "no Retry-After");
            Eq(0L, Backoff.ParseRetryAfter("-1"), "negative Retry-After");
            Eq(0L, Backoff.ParseRetryAfter("Wed, 21 Oct 2026 07:28:00 GMT"), "date Retry-After");
        }

        static void ServerClockBestSample()
        {
            ServerClock clock = new ServerClock();
            Check(!clock.HasEstimate && clock.NowMs(5) == 0, "a new clock has no estimate");
            clock.AddSample(10000, 400, 100);
            Eq(10100L, clock.OffsetMs, "offset of the first sample");
            clock.AddSample(20000, 50, 10000);
            Eq(10025L, clock.OffsetMs, "the lower round trip wins");
            Eq(21025L, clock.NowMs(11000), "server now");
            for (int i = 0; i < ServerClock.Window; i++)
            {
                clock.AddSample(30000 + i, 100, 20000);
            }
            Eq(10000L + ServerClock.Window - 1 + 50, clock.OffsetMs, "old samples leave the window; ties prefer the newest");
            clock.AddSample(0, 1, 1);
            Eq(10000L + ServerClock.Window - 1 + 50, clock.OffsetMs, "a zero header is ignored");
            clock.Clear();
            Check(!clock.HasEstimate && clock.NowMs(100) == 0, "cleared");
        }

        static void SessionAtStartAndCredentialsPersist()
        {
            Rig rig = new Rig();
            ClientState first = rig.Stored;
            Eq(32, first.device_id.Length, "device id length");
            Eq(64, first.device_secret.Length, "device secret length");
            rig.Tick();
            FakeTransport.Exchange e = rig.Single("POST", "/v1/session");
            Eq("http://a.test", e.Origin, "first base URL");
            Eq("", e.Request.Header("Authorization"), "no token on the session route");
            Eq(32, e.Request.Header("Idempotency-Key").Length, "session key");
            Eq("application/json", e.Request.Header("Content-Type"), "content type");
            SessionRequest body = rig.Json.FromJson<SessionRequest>(e.Request.Body);
            Eq(first.device_id, body.device_id, "device id sent");
            Eq(first.device_secret, body.device_secret, "device secret sent");
            Eq("TR", body.jurisdiction, "jurisdiction");
            Eq(30, body.age, "age");
            Eq(10000, e.Request.TimeoutMs, "request timeout");
            rig.AnswerSession(PlayerA, "tok1", 1);
            Eq(PlayerA, rig.Client.PlayerId, "player");
            Check(rig.Client.HasSession, "has a session");
            Eq("tok1", rig.Stored.session_token, "token stored");
            Eq(ConnectionState.Online, rig.Client.Connection, "online");
            Eq(1789200000000L, rig.Client.ServerNowMs, "server time estimate");

            FakeTransport transport = new FakeTransport();
            long now = 5000;
            ArenaClient again = new ArenaClient(rig.Options, transport, rig.Store, rig.Json, () => now);
            again.Update();
            Eq(first.device_id, rig.Stored.device_id, "credentials survive a restart");
            Eq(0, transport.Sent.Count, "a stored token needs no session at start");
            Eq(PlayerA, again.PlayerId, "player survives a restart");
        }

        static void IntentsStoredFirstAndSentOneAtATime()
        {
            Rig rig = new Rig().Started();
            int checkedSends = 0;
            rig.Transport.OnSend = request =>
            {
                string key = request.Header("Idempotency-Key");
                if (request.Url.Contains("/v1/tournaments/"))
                {
                    Check(rig.Store.Saved.Contains(key), "intent " + key + " was sent before it was stored");
                    checkedSends++;
                }
            };
            ArenaResult<JoinResponse> joined = null;
            ArenaResult<RoundResponse> dealt = null;
            ArenaResult<RoundResponse> drew = null;
            rig.Client.Join("t1", r => joined = r);
            rig.Client.Deal("t1", 1, r => dealt = r);
            rig.Client.Draw("t1", 1, 0, r => drew = r);
            Eq(3, rig.Client.PendingCount, "pending");
            Check(rig.Client.Busy, "busy");
            Eq(3, rig.Stored.pending.Length, "stored pending");
            rig.Tick();
            FakeTransport.Exchange join = rig.Single("POST", "/v1/tournaments/t1/join");
            Eq("{\"seq\":1}", join.Request.Body, "join body");
            Eq("Bearer tok1", join.Request.Header("Authorization"), "token");
            Eq(rig.Stored.pending[0].idempotency_key, join.Request.Header("Idempotency-Key"), "stored key");
            rig.Tick(50);
            rig.Single("POST", "/v1/tournaments/t1/join");

            rig.Answer(join, 201, JoinBody(rig, 2), "X-Arena-Slot", "40", "X-Arena-Next-Seq", "2");
            Check(joined != null && joined.Ok && joined.Value.join_seq == 1 && joined.Slot == 40, "join callback");
            FakeTransport.Exchange deal = rig.Single("POST", "/v1/tournaments/t1/rounds/1/deal");
            Eq("{\"seq\":2}", deal.Request.Body, "deal body");
            rig.Answer(deal, 201, RoundBody(rig, 3, 0), "X-Arena-Slot", "41");
            Check(dealt != null && dealt.Ok && dealt.Value.round.commitment.Length == 64, "deal callback");
            RoundAudit audit = rig.Client.FindAudit("t1", 1);
            Check(audit != null && audit.commitment == dealt.Value.round.commitment && audit.deal_view_json.Contains("\"move_index\":0"), "deal audit stored");
            FakeTransport.Exchange draw = rig.Single("POST", "/v1/tournaments/t1/rounds/1/moves");
            Eq("{\"seq\":3,\"move_index\":0,\"kind\":\"draw\",\"column\":-1}", draw.Request.Body, "draw body");
            rig.Answer(draw, 200, RoundBody(rig, 4, 1), "X-Arena-Slot", "43");
            Check(drew != null && drew.Ok && drew.Value.round.move_index == 1, "draw callback");
            Check(!rig.Client.Busy, "not busy");
            Eq(3, rig.Outcomes.Count, "IntentCompleted per answer");
            Eq(0, rig.Stored.pending.Length, "store emptied");
            Eq(43L, rig.Stored.last_intent_slot, "last intent slot");
            Eq(3L, rig.Stored.last_assigned_seq, "last assigned seq");
            Eq(3, checkedSends, "sends checked against the store");
            rig.Client.RemoveAudit("t1", 1);
            Check(rig.Client.FindAudit("t1", 1) == null && rig.Stored.audits.Length == 0, "audit removed");
        }

        static void RetryableAnswersResendSameBytes()
        {
            Rig rig = new Rig().Started();
            ArenaResult<JoinResponse> joined = null;
            List<ConnectionState> states = new List<ConnectionState>();
            rig.Client.ConnectionChanged += s => states.Add(s);
            rig.Client.Join("t1", r => joined = r);
            rig.Tick();
            FakeTransport.Exchange first = rig.Single("POST", "/v1/tournaments/t1/join");

            rig.Answer(first, 503, ErrorJson("unavailable", true), "Retry-After", "1");
            Eq(ConnectionState.Retrying, rig.Client.Connection, "retrying after one failure");
            rig.NoneOpen();
            rig.Tick(999);
            rig.NoneOpen();
            rig.Tick(2);
            FakeTransport.Exchange second = rig.Single("POST", "/v1/tournaments/t1/join");
            Eq(first.Request.Url, second.Request.Url, "same URL");
            Eq(first.Request.Body, second.Request.Body, "same body");
            Eq(first.Request.Header("Idempotency-Key"), second.Request.Header("Idempotency-Key"), "same key");

            rig.Transport.Answer(second, new HttpResponse { TimedOut = true, TransportError = "timeout" });
            rig.Tick();
            rig.NoneOpen();
            rig.Tick(251);
            FakeTransport.Exchange third = rig.Single("POST", "/v1/tournaments/t1/join");
            Eq("http://b.test", third.Origin, "no response moves to the next base URL");
            Eq(first.Request.Body, third.Request.Body, "same body after a timeout");
            Eq(first.Request.Header("Idempotency-Key"), third.Request.Header("Idempotency-Key"), "same key after a timeout");

            rig.Answer(third, 504, ErrorJson("outcome_unknown", true));
            Eq(ConnectionState.Offline, rig.Client.Connection, "offline after three failures");
            rig.Tick(8000);
            FakeTransport.Exchange fourth = rig.Single("POST", "/v1/tournaments/t1/join");
            rig.Answer(fourth, 409, ErrorJson("in_flight", true), "Retry-After", "1");
            rig.Tick(1000);
            FakeTransport.Exchange fifth = rig.Single("POST", "/v1/tournaments/t1/join");
            rig.Answer(fifth, 502, "<html>bad gateway</html>");
            rig.Tick(8000);
            FakeTransport.Exchange sixth = rig.Single("POST", "/v1/tournaments/t1/join");
            Check(joined == null, "no callback before a definitive answer");
            rig.Answer(sixth, 201, JoinBody(rig, 2), "X-Arena-Slot", "40");
            Check(joined != null && joined.Ok, "join completed");
            Eq(ConnectionState.Online, rig.Client.Connection, "online again");
            Check(states.Contains(ConnectionState.Offline) && states[states.Count - 1] == ConnectionState.Online, "connection events");
            Eq(1, rig.Outcomes.Count, "one outcome");
        }

        static void RuleRejectionKeepsQueue()
        {
            Rig rig = new Rig().Started();
            ArenaResult<RoundResponse> drew = null;
            rig.Client.Draw("t1", 1, 0, r => drew = r);
            rig.Client.Play("t1", 1, 1, 3, null);
            rig.Tick();
            FakeTransport.Exchange draw = rig.Single("POST", "/v1/tournaments/t1/rounds/1/moves");
            rig.Answer(draw, 409, ErrorJson("illegal_move", false), "X-Arena-Slot", "50", "X-Arena-Next-Seq", "2");
            Check(drew != null && !drew.Ok && drew.Error.Code == ErrorCodes.IllegalMove && drew.Error.Status == 409, "illegal_move delivered");
            Eq(50L, drew.Slot, "rejection slot");
            FakeTransport.Exchange play = rig.Single("POST", "/v1/tournaments/t1/rounds/1/moves");
            Eq("{\"seq\":2,\"move_index\":1,\"kind\":\"play\",\"column\":3}", play.Request.Body, "the next intent keeps its number");
            Eq(0, rig.Resyncs, "no resync");
            Eq(50L, rig.Stored.last_intent_slot, "rejection slot recorded");
        }

        static void StaleSeqResyncsFromHeader()
        {
            Rig rig = new Rig().Started();
            ArenaResult<RoundResponse> dealt = null;
            rig.Client.Join("t1", null);
            rig.Client.Deal("t1", 1, r => dealt = r);
            rig.Client.Draw("t1", 1, 0, null);
            rig.Tick();
            FakeTransport.Exchange join = rig.Single("POST", "/v1/tournaments/t1/join");
            rig.Answer(join, 409, ErrorJson("stale_seq", false), "X-Arena-Slot", "60", "X-Arena-Next-Seq", "7");
            Eq(3, rig.Outcomes.Count, "every pending intent completed");
            Eq(ErrorCodes.StaleSeq, rig.Outcomes[0].Error.Code, "head answer");
            Eq(LocalErrorCodes.ResyncRequired, rig.Outcomes[1].Error.Code, "second failed locally");
            Eq(LocalErrorCodes.ResyncRequired, rig.Outcomes[2].Error.Code, "third failed locally");
            Check(dealt != null && dealt.Error.Code == LocalErrorCodes.ResyncRequired, "deal callback got resync_required");
            Eq(1, rig.Resyncs, "Resynced raised");
            Eq(6L, rig.Stored.last_assigned_seq, "last assigned from the header");
            Eq(0, rig.Client.PendingCount, "queue empty");
            rig.NoneOpen();
            rig.Client.Join("t1", null);
            rig.Tick();
            Eq("{\"seq\":7}", rig.Single("POST", "/v1/tournaments/t1/join").Request.Body, "numbering continues from next_seq");
        }

        static void SeqGapResyncsThroughSession()
        {
            Rig rig = new Rig().Started();
            rig.Client.Join("t1", null);
            rig.Tick();
            FakeTransport.Exchange join = rig.Single("POST", "/v1/tournaments/t1/join");
            string sessionKey = rig.Transport.Sent[0].Request.Header("Idempotency-Key");
            rig.Answer(join, 409, ErrorJson("seq_gap", false));
            Eq(0, rig.Resyncs, "no Resynced before next_seq is known");
            ArenaResult<RoundResponse> early = null;
            rig.Client.Deal("t1", 1, r => early = r);
            Check(early == null, "local failures are delivered inside Update");
            FakeTransport.Exchange session = rig.Single("POST", "/v1/session");
            Check(session.Request.Header("Idempotency-Key") != sessionKey, "a new session key");
            rig.Tick();
            Check(early != null && early.Error.Code == LocalErrorCodes.ResyncRequired, "intents are refused while resynchronising");
            rig.AnswerSession(PlayerA, "tok2", 12);
            Eq(1, rig.Resyncs, "Resynced after the session");
            Eq(11L, rig.Stored.last_assigned_seq, "last assigned from the session");
            rig.Client.Join("t1", null);
            rig.Tick();
            Eq("{\"seq\":12}", rig.Single("POST", "/v1/tournaments/t1/join").Request.Body, "numbering from the session's next_seq");
        }

        static void UnauthorizedRefreshesAndResends()
        {
            Rig rig = new Rig().Started();
            rig.Client.Join("t1", null);
            rig.Tick();
            FakeTransport.Exchange first = rig.Single("POST", "/v1/tournaments/t1/join");
            rig.Answer(first, 401, ErrorJson("session_expired", true));
            Eq("", rig.Stored.session_token, "token dropped");
            FakeTransport.Exchange session = rig.Single("POST", "/v1/session");
            rig.AnswerSession(PlayerA, "tok2", 2);
            FakeTransport.Exchange again = rig.Single("POST", "/v1/tournaments/t1/join");
            Eq("Bearer tok2", again.Request.Header("Authorization"), "new token");
            Eq(first.Request.Body, again.Request.Body, "same body");
            Eq(first.Request.Header("Idempotency-Key"), again.Request.Header("Idempotency-Key"), "same key");
            Eq(1L, rig.Stored.last_assigned_seq, "a session's next_seq never lowers the assigned numbers");
            Check(session.Answered, "session answered");
        }

        static void RedirectFollowedOnceAndCached()
        {
            Rig rig = new Rig().Started();
            rig.Client.Join("t1", null);
            rig.Tick();
            FakeTransport.Exchange first = rig.Single("POST", "/v1/tournaments/t1/join");
            Eq("http://a.test", first.Origin, "first base");
            rig.Answer(first, 307, ErrorJson("not_leader", true),
                "Location", "http://c.test:9082/v1/tournaments/t1/join", "X-Arena-Leader", "http://c.test:9082");
            FakeTransport.Exchange follow = rig.Single("POST", "/v1/tournaments/t1/join");
            Eq("http://c.test:9082", follow.Origin, "followed at once");
            Eq(first.Request.Body, follow.Request.Body, "same body");
            Eq(first.Request.Header("Idempotency-Key"), follow.Request.Header("Idempotency-Key"), "same key");
            Eq(first.Request.Header("Authorization"), follow.Request.Header("Authorization"), "same token");
            Eq("http://c.test:9082", rig.Stored.leader_url, "leader cached");

            rig.Answer(follow, 307, ErrorJson("not_leader", true),
                "Location", "http://d.test/v1/tournaments/t1/join", "X-Arena-Leader", "http://d.test");
            rig.NoneOpen();
            Eq("http://d.test", rig.Stored.leader_url, "second Location cached, not followed");
            Eq(ConnectionState.Retrying, rig.Client.Connection, "a second redirect is a retryable failure");
            rig.Tick(126);
            FakeTransport.Exchange third = rig.Single("POST", "/v1/tournaments/t1/join");
            Eq("http://d.test", third.Origin, "retried at the cached leader");
            rig.Answer(third, 201, JoinBody(rig, 2), "X-Arena-Leader", "http://d.test", "X-Arena-Slot", "40");
            rig.Client.Deal("t1", 1, null);
            rig.Tick();
            Eq("http://d.test", rig.Single("POST", "/v1/tournaments/t1/rounds/1/deal").Origin, "later intents go to the leader");
        }

        static void LeaderDroppedOnFailure()
        {
            Rig rig = new Rig().Started();
            rig.Client.Join("t1", null);
            rig.Tick();
            rig.Answer(rig.Single("POST", "/v1/tournaments/t1/join"), 307, ErrorJson("not_leader", true),
                "Location", "http://c.test/v1/tournaments/t1/join", "X-Arena-Leader", "http://c.test");
            rig.Answer(rig.Single("POST", "/v1/tournaments/t1/join"), 201, JoinBody(rig, 2), "X-Arena-Leader", "http://e.test", "X-Arena-Slot", "40");

            rig.Client.Deal("t1", 1, null);
            rig.Tick();
            FakeTransport.Exchange deal = rig.Single("POST", "/v1/tournaments/t1/rounds/1/deal");
            Eq("http://c.test", deal.Origin, "sent to the cached leader");
            rig.Transport.Answer(deal, new HttpResponse { TransportError = "connection refused" });
            rig.Tick();
            Eq("http://e.test", rig.Stored.leader_url, "the most recent X-Arena-Leader replaces a failed leader");
            rig.Tick(126);
            deal = rig.Single("POST", "/v1/tournaments/t1/rounds/1/deal");
            Eq("http://e.test", deal.Origin, "retried at the hinted leader");
            rig.Answer(deal, 503, ErrorJson("no_leader", true), "Retry-After", "1", "X-Arena-Leader", "");
            Eq("", rig.Stored.leader_url, "no_leader drops the cache");
            rig.Tick(1000);
            deal = rig.Single("POST", "/v1/tournaments/t1/rounds/1/deal");
            Eq("http://b.test", deal.Origin, "next base URL, round robin");
        }

        static void RestartResendsStoredIntent()
        {
            Rig rig = new Rig().Started();
            bool callbackRan = false;
            rig.Client.Join("t1", r => callbackRan = true);
            rig.Tick();
            FakeTransport.Exchange first = rig.Single("POST", "/v1/tournaments/t1/join");
            rig.Client.Dispose();
            Eq(1, rig.Transport.CancelCalls, "dispose aborts requests");

            FakeTransport transport = new FakeTransport();
            long now = 90000;
            ArenaClient restarted = new ArenaClient(rig.Options, transport, rig.Store, rig.Json, () => now);
            List<IntentOutcome> outcomes = new List<IntentOutcome>();
            restarted.IntentCompleted += o => outcomes.Add(o);
            Check(restarted.Busy, "the stored intent is pending after a restart");
            restarted.Update();
            Eq(1, transport.Sent.Count, "only the stored intent is sent");
            HttpRequest again = transport.Sent[0].Request;
            Eq(first.Request.Url, again.Url, "same URL");
            Eq(first.Request.Body, again.Body, "same body");
            Eq(first.Request.Header("Idempotency-Key"), again.Header("Idempotency-Key"), "same key");
            transport.Answer(transport.Sent[0], Response(201, JoinBody(rig, 2), "X-Arena-Slot", "40"));
            restarted.Update();
            Eq(1, outcomes.Count, "IntentCompleted without a callback");
            Eq(first.Request.Header("Idempotency-Key"), outcomes[0].IdempotencyKey, "outcome key");
            Eq(201, outcomes[0].Status, "outcome status");
            Check(!restarted.Busy && !callbackRan, "completed; the old callback died with its process");
        }

        static void QueueFullAssignsNothing()
        {
            Rig rig = new Rig(o => o.MaxPendingIntents = 2).Started();
            ArenaResult<RoundResponse> third = null;
            rig.Client.Join("t1", null);
            rig.Client.Deal("t1", 1, null);
            rig.Client.Finish("t1", 1, r => third = r);
            Check(third == null, "not delivered inside the call");
            Eq(2L, rig.Stored.last_assigned_seq, "nothing assigned to the refused intent");
            rig.Tick();
            Check(third != null && !third.Ok && third.Error.Code == LocalErrorCodes.QueueFull, "queue_full delivered inside Update");
            Eq(2, rig.Client.PendingCount, "two pending");
        }

        static void PauseAndResume()
        {
            Rig rig = new Rig(o => o.FollowEvents = true).Started();
            rig.Tick();
            FakeTransport.Exchange poll = rig.Single("GET", "/v1/events?cursor=0&wait_ms=25000");
            Eq(35000, poll.Request.TimeoutMs, "events timeout is wait plus 10 s");
            rig.Client.Join("t1", null);
            rig.Tick();
            FakeTransport.Exchange join = rig.Find("/v1/tournaments/t1/join");
            rig.Answer(join, 503, ErrorJson("unavailable", true), "Retry-After", "5");
            int savesBefore = rig.Store.Saves;
            rig.Client.Pause();
            Check(rig.Client.IsPaused, "paused");
            Eq(1, rig.Transport.CancelCalls, "pause aborts requests");
            Check(rig.Store.Saves > savesBefore, "pause persists the store");
            rig.Tick(60000);
            rig.NoneOpen();
            Eq(1, rig.Client.PendingCount, "the intent stays stored");
            rig.Client.Resume();
            Eq(0, rig.Resumes, "Resumed waits for Update");
            Eq(0L, rig.Client.ServerNowMs, "clock samples cleared on resume");
            rig.Tick();
            Eq(1, rig.Resumes, "Resumed raised inside Update");
            Check(!rig.Client.HasSession, "no session is claimed before the clock estimate is back");
            // Without a clock estimate the token may have expired while paused:
            // the head goes at once, alone, and the events poll follows the
            // first response.
            FakeTransport.Exchange again = rig.Single("POST", "/v1/tournaments/t1/join");
            rig.Answer(again, 201, JoinBody(rig, 2), "X-Arena-Slot", "40", "X-Arena-Server-Time-Ms", ServerTime);
            Check(rig.Client.HasSession, "the estimate is back");
            FakeTransport.Exchange quick = rig.Single("GET", "/v1/events?cursor=0&wait_ms=0");
            rig.Answer(quick, 200, "{\"cursor\":0,\"applied_slot\":40,\"has_more\":false,\"events\":[]}", "X-Arena-Server-Time-Ms", ServerTime);
            rig.Single("GET", "/v1/events?cursor=0&wait_ms=25000");
            rig.Client.Resume();
            rig.Tick();
            Eq(1, rig.Resumes, "Resume without Pause does nothing");
        }

        static void EventsCursorAdvancesAfterDelivery()
        {
            Rig rig = new Rig(o => o.FollowEvents = true).Started();
            List<EventItem> received = new List<EventItem>();
            bool throwOnce = true;
            rig.Client.Events.Received += item =>
            {
                if (throwOnce)
                {
                    throwOnce = false;
                    throw new InvalidOperationException("game handler failed");
                }
                received.Add(item);
            };
            rig.Tick();
            FakeTransport.Exchange poll = rig.Single("GET", "/v1/events?cursor=0&wait_ms=25000");
            Check(poll.Request.Header("Authorization") == "Bearer tok1" && poll.Request.Header("Idempotency-Key") == "", "poll headers");
            string body = "{\"cursor\":612,\"applied_slot\":612,\"has_more\":false,\"events\":[" +
                          "{\"slot\":610,\"type\":\"payout_claimed\",\"tournament_id\":\"t1\",\"player_id\":\"" + PlayerA + "\",\"round\":0,\"status\":\"\",\"score\":0,\"total_score\":0,\"rounds_finished\":0,\"amount\":675,\"time_ms\":1789203000000}]}";
            rig.Transport.Answer(poll, Response(200, body));
            bool threw = false;
            try
            {
                rig.Tick();
            }
            catch (InvalidOperationException)
            {
                threw = true;
            }
            Check(threw, "the handler's exception reaches the caller of Update");
            Eq(0L, rig.Client.Events.Cursor, "cursor not advanced when delivery failed");
            rig.Tick();
            rig.NoneOpen();
            rig.Tick(125);
            poll = rig.Single("GET", "/v1/events?cursor=0");
            rig.Answer(poll, 200, body);
            Eq(1, received.Count, "event delivered");
            Eq(675L, received[0].amount, "event fields");
            Eq(612L, rig.Client.Events.Cursor, "cursor advanced");
            Eq(612L, rig.Stored.events_cursor, "cursor stored");
            poll = rig.Single("GET", "/v1/events?cursor=612&wait_ms=25000");

            rig.Client.Events.TournamentId = "t1";
            rig.Answer(poll, 200, body.Replace("612", "700"));
            Eq(1, received.Count, "a response for the old filter is discarded");
            Eq(612L, rig.Client.Events.Cursor, "cursor kept");
            poll = rig.Single("GET", "/v1/events?cursor=612&wait_ms=25000&tournament_id=t1");

            rig.Answer(poll, 404, ErrorJson("not_joined", false));
            Eq(ErrorCodes.NotJoined, rig.Client.Events.LastError.Code, "LastError");
            rig.NoneOpen();
            rig.Tick(250);
            rig.Single("GET", "/v1/events?cursor=612");
        }

        static void ExpiredSessionReplayAsksAgain()
        {
            Rig rig = new Rig();
            rig.Tick();
            FakeTransport.Exchange first = rig.Single("POST", "/v1/session");
            rig.Answer(first, 200, rig.SessionBody(PlayerA, "old", 1, 1789100000000), "X-Arena-Server-Time-Ms", ServerTime);
            Eq("", rig.Stored.session_token, "an expired token is not kept");
            rig.Tick(126);
            FakeTransport.Exchange second = rig.Single("POST", "/v1/session");
            Check(second.Request.Header("Idempotency-Key") != first.Request.Header("Idempotency-Key"), "a new key");
        }

        static void SessionRefreshMargin()
        {
            Rig rig = new Rig();
            rig.Tick();
            rig.AnswerSession(PlayerA, "tok1", 1);
            rig.Tick(3600000 - 300000 - 1000);
            rig.NoneOpen();
            rig.Tick(2000);
            FakeTransport.Exchange refresh = rig.Single("POST", "/v1/session");
            Eq("Bearer tok1", "Bearer " + rig.Stored.session_token, "the old token is kept until the new one arrives");

            // A 60-second lifetime is refreshed after 30 seconds, not on every frame.
            long issued = 1789200000000;
            rig.Answer(refresh, 200, rig.SessionBody(PlayerA, "short", 1, issued + 60000, 60000), "X-Arena-Server-Time-Ms", issued.ToString());
            rig.Tick(1000);
            rig.Tick(1000);
            rig.NoneOpen();
            rig.Tick(29000);
            rig.Single("POST", "/v1/session");
        }

        static void OptionsValidated()
        {
            SystemTextJson json = new SystemTextJson();
            Action<ArenaClientOptions>[] bad =
            {
                o => o.BaseUrls = new string[0],
                o => o.BaseUrls = new[] { "not a url", "ftp://x.test" },
                o => o.Jurisdiction = "",
                o => o.Jurisdiction = "tr",
                o => o.Jurisdiction = "ABCDEFGHI",
                o => o.Age = -1,
                o => o.Age = 151,
            };
            for (int i = 0; i < bad.Length; i++)
            {
                ArenaClientOptions options = new ArenaClientOptions { BaseUrls = new[] { "https://a.test" }, Jurisdiction = "TR", Age = 30 };
                bad[i](options);
                bool threw = false;
                try
                {
                    new ArenaClient(options, new FakeTransport(), new MemoryStore(json), json, () => 0).Dispose();
                }
                catch (ArgumentException)
                {
                    threw = true;
                }
                Check(threw, "bad options case " + i + " accepted");
            }
        }

        static void DeviceMismatchHalts()
        {
            Rig rig = new Rig();
            ArenaError halted = null;
            rig.Client.Halted += e => halted = e;
            rig.Client.Join("t1", null);
            rig.Tick();
            FakeTransport.Exchange session = rig.Single("POST", "/v1/session");
            rig.Answer(session, 403, ErrorJson("device_mismatch", false), "X-Arena-Slot", "9");
            Check(halted != null && halted.Code == ErrorCodes.DeviceMismatch, "Halted raised");
            Eq(ConnectionState.Offline, rig.Client.Connection, "offline");
            Eq(1, rig.Client.PendingCount, "stored intents are kept");
            rig.Tick(600000);
            rig.NoneOpen();
            ArenaResult<RoundResponse> refused = null;
            ArenaResult<TournamentListResponse> listed = null;
            rig.Client.Deal("t1", 1, r => refused = r);
            rig.Client.ListTournaments("", 0, 50, r => listed = r);
            rig.Tick();
            Check(refused != null && refused.Error.Code == ErrorCodes.DeviceMismatch, "intents refused");
            Check(listed != null && listed.Error.Code == ErrorCodes.DeviceMismatch, "reads refused");
            Eq(1, rig.Client.PendingCount, "nothing new stored");
        }

        static void PlayerChangeFailsStoredIntents()
        {
            SystemTextJson json = new SystemTextJson();
            MemoryStore store = new MemoryStore(json);
            ClientState old = new ClientState
            {
                device_id = "9f86d081884c7d659a2feaa0c55ad015",
                device_secret = "2c26b46b68ffc68ff99b453c1d30413413422d706483bfa0f98a5e886266e7ae",
                player_id = PlayerB,
                last_assigned_seq = 9,
                events_cursor = 300,
                pending = new[]
                {
                    new PendingIntent { idempotency_key = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", seq = 8, path = "/v1/tournaments/t1/join", body = "{\"seq\":8}", route = "join" },
                    new PendingIntent { idempotency_key = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", seq = 9, path = "/v1/tournaments/t1/rounds/1/deal", body = "{\"seq\":9}", route = "deal" },
                },
            };
            store.Save(old);
            Rig rig = new Rig(null, store);
            Eq(2, rig.Client.PendingCount, "stored intents loaded");
            rig.Tick();
            rig.AnswerSession(PlayerA, "tok1", 1);
            Eq(PlayerA, rig.Client.PlayerId, "new player adopted");
            Eq(2, rig.Outcomes.Count, "stored intents failed");
            Eq(LocalErrorCodes.ResyncRequired, rig.Outcomes[0].Error.Code, "resync_required");
            Eq("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", rig.Outcomes[0].IdempotencyKey, "in order");
            Eq(1, rig.Resyncs, "Resynced raised");
            Eq(0L, rig.Stored.last_assigned_seq, "numbering of the new player");
            Eq(0L, rig.Stored.events_cursor, "cursor of the new player");
            rig.NoneOpen();
        }

        static void ReadsUseMinSlotAndGiveUp()
        {
            Rig rig = new Rig().Started();
            rig.Client.Draw("t1", 1, 0, null);
            rig.Tick();
            rig.Answer(rig.Single("POST", "/v1/tournaments/t1/rounds/1/moves"), 200, RoundBody(rig, 2, 1), "X-Arena-Slot", "77");
            ArenaResult<RoundResponse> round = null;
            rig.Client.GetRound("t1", 1, r => round = r);
            rig.Tick();
            FakeTransport.Exchange read = rig.Single("GET", "/v1/tournaments/t1/rounds/1?min_slot=77");
            Eq("", read.Request.Header("Idempotency-Key"), "no key on reads");
            Eq("", read.Request.Header("Content-Type"), "no content type on reads");
            Eq("", read.Request.Body, "no body on reads");
            for (int i = 0; i < 3; i++)
            {
                Check(round == null, "no callback before giving up");
                rig.Answer(read, 503, ErrorJson("replica_behind", true), "Retry-After", "1");
                if (i < 2)
                {
                    rig.Tick(1000);
                    read = rig.Single("GET", "/v1/tournaments/t1/rounds/1?min_slot=77");
                }
            }
            Check(round != null && !round.Ok && round.Error.Code == ErrorCodes.ReplicaBehind && round.Error.Retryable, "gave up with the last error");
            rig.NoneOpen();

            ArenaResult<LeaderboardResponse> board = null;
            rig.Client.GetLeaderboard("t1", 0, 10, r => board = r);
            rig.Tick();
            string body = "{\"tournament_id\":\"t1\",\"status\":\"open\",\"final\":false,\"applied_slot\":1400,\"entrants\":2,\"offset\":0,\"limit\":10," +
                          "\"rows\":[{\"place\":1,\"player_id\":\"" + PlayerA + "\",\"total_score\":9300,\"rounds_finished\":2,\"scored\":false,\"amount\":0,\"withheld\":false,\"claimed\":false}]," +
                          "\"me\":{\"place\":1,\"player_id\":\"" + PlayerA + "\",\"total_score\":9300,\"rounds_finished\":2,\"scored\":false,\"amount\":0,\"withheld\":false,\"claimed\":false}}";
            rig.Answer(rig.Single("GET", "/v1/tournaments/t1/leaderboard?offset=0&limit=10"), 200, body);
            Check(board != null && board.Ok && board.Value.rows.Length == 1 && board.Value.me.total_score == 9300, "leaderboard parsed");

            ArenaResult<TournamentListResponse> missing = null;
            rig.Client.ListTournaments("all", 5, 20, r => missing = r);
            rig.Tick();
            rig.Answer(rig.Single("GET", "/v1/tournaments?status=all&offset=5&limit=20"), 400, ErrorJson("malformed_request", false));
            Check(missing != null && missing.Error.Code == ErrorCodes.MalformedRequest && missing.Error.Status == 400, "definitive read error");
            Eq(0, rig.Resyncs, "a read error does not resynchronise");
        }

        static void WatchdogEndsSilentRequest()
        {
            Rig rig = new Rig().Started();
            rig.Client.Join("t1", null);
            rig.Tick();
            FakeTransport.Exchange silent = rig.Single("POST", "/v1/tournaments/t1/join");
            rig.Tick(15001);
            rig.Tick();
            List<FakeTransport.Exchange> open = rig.Transport.Open();
            Eq(1, open.Count, "no resend before the backoff");
            rig.Tick(126);
            open = rig.Transport.Open();
            Eq(2, open.Count, "resent after the watchdog");
            rig.Answer(silent, 201, JoinBody(rig, 2), "X-Arena-Slot", "40");
            Eq(1, rig.Client.PendingCount, "a late answer to an abandoned attempt is ignored");
            rig.Answer(open[1], 201, JoinBody(rig, 2), "X-Arena-Slot", "40");
            Eq(0, rig.Client.PendingCount, "the current attempt completes the intent");
        }

        static void JsonMatchesContract()
        {
            SystemTextJson json = new SystemTextJson();
            Eq("{\"seq\":3,\"move_index\":0,\"kind\":\"draw\",\"column\":-1}",
                json.ToJson(new MoveRequest { seq = 3, move_index = 0, kind = "draw", column = -1 }), "MoveRequest");
            Eq("{\"seq\":1}", json.ToJson(new SeqRequest { seq = 1 }), "SeqRequest");
            Eq("{\"device_id\":\"9f86d081884c7d659a2feaa0c55ad015\",\"device_secret\":\"2c26b46b68ffc68ff99b453c1d30413413422d706483bfa0f98a5e886266e7ae\",\"jurisdiction\":\"TR\",\"age\":31}",
                json.ToJson(new SessionRequest
                {
                    device_id = "9f86d081884c7d659a2feaa0c55ad015",
                    device_secret = "2c26b46b68ffc68ff99b453c1d30413413422d706483bfa0f98a5e886266e7ae",
                    jurisdiction = "TR",
                    age = 31,
                }), "SessionRequest");

            string deal = "{\"replayed\":false,\"slot\":418,\"next_seq\":3,\"round\":{\"tournament_id\":\"daily-2026-09-17\",\"round\":1,\"status\":\"playing\",\"finish_reason\":\"\",\"move_index\":0," +
                          "\"columns\":[{\"cards\":[\"Ks\",\"8h\",\"7s\",\"3s\",\"Jd\"]},{\"cards\":[\"2h\",\"4c\",\"5d\",\"7h\",\"Qh\"]},{\"cards\":[\"3c\",\"8s\",\"Ah\",\"8d\",\"Th\"]}," +
                          "{\"cards\":[\"9d\",\"Td\",\"3h\",\"Jh\",\"Qs\"]},{\"cards\":[\"Ts\",\"7c\",\"5s\",\"6d\",\"2d\"]},{\"cards\":[\"5h\",\"Tc\",\"Ad\",\"9s\",\"9h\"]},{\"cards\":[\"2c\",\"6s\",\"6c\",\"3d\",\"As\"]}]," +
                          "\"waste_top\":\"4h\",\"waste_count\":1,\"stock_count\":16,\"cleared\":0,\"score\":0,\"playable_columns\":[],\"can_draw\":true," +
                          "\"last_move\":{\"kind\":\"\",\"column\":-1,\"card\":\"\"},\"started_at_ms\":1789200012000,\"deadline_ms\":1789200312000," +
                          "\"commitment\":\"5e188f4cb340f1c0e94c8821b8f72279b339f5698c66e659b22d32b3f9b29065\",\"seed\":\"\"}}";
            RoundResponse response = json.FromJson<RoundResponse>(deal);
            Eq(418L, response.slot, "slot");
            Eq(7, response.round.columns.Length, "columns");
            Eq("Jd", response.round.columns[0].cards[4], "top card of column 0");
            Eq(0, response.round.playable_columns.Length, "playable columns");
            Eq(-1, response.round.last_move.column, "last move column");
            Eq(1789200312000L, response.round.deadline_ms, "deadline");
            Eq(deal, json.ToJson(response), "a response re-encodes to the same bytes");

            string tournaments = "{\"applied_slot\":1290,\"offset\":0,\"limit\":50,\"total\":1,\"tournaments\":[{\"tournament_id\":\"daily-2026-09-17\",\"status\":\"open\",\"game\":\"ladder-v1\",\"rounds\":3,\"round_time_limit_ms\":300000,\"entry_fee\":500,\"rake_bps\":1000,\"prize_bps\":[5000,3000,2000],\"min_entrants\":3,\"max_entrants\":100,\"min_age\":18,\"max_score\":14400,\"entrants\":42,\"projected_pool\":18900,\"joined\":false,\"eligible\":true,\"rounds_finished\":0,\"round_in_play\":0,\"next_round\":0,\"created_slot\":402}]}";
            TournamentListResponse list = json.FromJson<TournamentListResponse>(tournaments);
            Eq(3, list.tournaments[0].prize_bps.Length, "prize_bps");
            Eq(tournaments, json.ToJson(list), "tournament list re-encodes to the same bytes");

            ClientState state = new ClientState { player_id = PlayerA, pending = new[] { new PendingIntent { idempotency_key = "k", path = "/p" } } };
            ClientState back = json.FromJson<ClientState>(json.ToJson(state));
            Eq("/p", back.pending[0].path, "client state round trip");
            Check(!json.ToJson(new ClientState()).Contains("null"), "no nulls in a fresh state");

            LadderAudit.Board board = new LadderAudit.Board(LadderAudit.Shuffle("1898590d493e4a523bd5ff782956ceeba5cd1cbcf09d5158ee54a01dc4588377"));
            Eq("", board.Compare(response.round), "the contract's deal view matches the Appendix A deal");
        }

        static void LadderAuditVectors()
        {
            byte[] secret = LadderAudit.FromHex("202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f");
            string[] seeds =
            {
                "1898590d493e4a523bd5ff782956ceeba5cd1cbcf09d5158ee54a01dc4588377",
                "172b3ace53308aebc0a1219562d3f796f1f3e8a3bf443645a9d3439421be80c8",
                "995b407ebad8f37697d9db8c1b696976f821f3a4430b11d8e70eedb36750fcbc",
            };
            string[] commitments =
            {
                "5e188f4cb340f1c0e94c8821b8f72279b339f5698c66e659b22d32b3f9b29065",
                "e1662e3bf2f91bbfb5405a7198050114e24904d5bc77717c176c57cd4af16398",
                "cd49d8cc8ab27eed2a5e78d5cf83f0173e96d74525faf9eb33f1d42767753578",
            };
            string[] decks =
            {
                "Ks 8h 7s 3s Jd 2h 4c 5d 7h Qh 3c 8s Ah 8d Th 9d Td 3h Jh Qs Ts 7c 5s 6d 2d 5h Tc Ad 9s 9h 2c 6s 6c 3d As 4h Kh Js Jc 5c Kd Kc 8c Ac 7d 4s 9c Qc 4d 2s 6h Qd",
                "Qh Qs 6s 7c Ac 7h 8s Js 9h 3s Jc 5h 4c 4s 9c 3d Ah Jd 4d Ks 6c Kh Kd Qc Tc 2c 8d 3c 7s 2d 4h 6d 8h Jh 7d 5s Td 8c Ad 2s 5d Th Kc Qd 2h Ts 6h 5c 3h 9d 9s As",
                "Kc Ac 8s Ah Tc 5d 3h 4s 6c 8c Jd 4d Qc 5c 2h 6s 5h 8d 7s 2c 6d 7h 9h Qh 4c 6h Kd 5s As 3s Jc Qd Ks 9c 3c 8h 9d Ad Th Js 7d Td 4h Jh 2d Kh 2s 9s 3d Ts 7c Qs",
            };
            string[] greedy =
            {
                "d p1 p0 p2 p5 p2 p1 d p3 p3 d d d p2 p4 p0 d p6 d p0 p0 p5 p2 d p0 p5 d p4 p1 p1 p2 p1 p3 d p4 d p3 p3 p5 d d p5 d p6 d p4 p6 d",
                "d p2 p4 d p6 d p3 p0 p5 p1 p2 d d p2 p2 p3 d p1 d p4 p1 d p2 d d p3 d p0 p0 p5 p1 p1 d d d d d p4 p0 p4 p0 p6",
                "d p0 d p2 p0 p3 p5 p4 p2 d d p2 d p0 p3 p1 d d p6 p2 d p4 p2 d p0 p0 p5 d d d p3 p4 d d p6 d p1 p3 p1 p1 d p6 p6 p6",
            };
            int[] cleared = { 32, 26, 28 };
            string[] wasteCards = { "Qd", "Jh", "Jc" };
            long total = 0;
            for (int r = 0; r < 3; r++)
            {
                string seed = LadderAudit.DeriveSeed(secret, "daily-2026-09-17", PlayerA, r + 1);
                Eq(seeds[r], seed, "seed of round " + (r + 1));
                Eq(commitments[r], LadderAudit.Commit(seed), "commitment of round " + (r + 1));
                int[] deck = LadderAudit.Shuffle(seed);
                List<string> names = new List<string>();
                foreach (int card in deck)
                {
                    names.Add(LadderAudit.CardName(card));
                }
                Eq(decks[r], string.Join(" ", names), "deck of round " + (r + 1));

                LadderAudit.Board board = new LadderAudit.Board(deck);
                List<string> moves = new List<string>();
                while (board.Over().Length == 0)
                {
                    int[] playable = board.Playable();
                    if (playable.Length > 0)
                    {
                        Check(board.Apply(MoveKind.Play, playable[0]), "greedy play is legal");
                        moves.Add("p" + playable[0]);
                    }
                    else
                    {
                        Check(board.Apply(MoveKind.Draw, -1), "greedy draw is legal");
                        moves.Add("d");
                    }
                }
                Eq(greedy[r], string.Join(" ", moves), "greedy moves of round " + (r + 1));
                Eq(FinishReason.Blocked, board.Over(), "finish reason of round " + (r + 1));
                Eq(cleared[r], board.Cleared, "cleared in round " + (r + 1));
                Eq(100L * cleared[r], board.Score, "score of round " + (r + 1));
                Eq(wasteCards[r], LadderAudit.CardName(board.WasteTop), "waste card of round " + (r + 1));
                Check(!board.Apply(MoveKind.Draw, -1), "no move after the round is over");
                total += board.Score;
            }
            Eq(8600L, total, "entry total");
            Check(LadderAudit.Adjacent(LadderAudit.ParseCard("Ks"), LadderAudit.ParseCard("Ah")), "king and ace are adjacent");
            Check(LadderAudit.Adjacent(LadderAudit.ParseCard("2c"), LadderAudit.ParseCard("Ad")), "two and ace are adjacent");
            Check(!LadderAudit.Adjacent(LadderAudit.ParseCard("3c"), LadderAudit.ParseCard("Ad")), "three and ace are not adjacent");
            Check(!LadderAudit.Adjacent(LadderAudit.ParseCard("Qc"), LadderAudit.ParseCard("Qd")), "equal ranks are not adjacent");
        }

        static void PersistentStoreRecovers()
        {
            SystemTextJson json = new SystemTextJson();
            string dir = Path.Combine(Path.GetTempPath(), "paxos-arena-store-" + Guid.NewGuid().ToString("N"));
            try
            {
                PersistentDataIntentStore store = new PersistentDataIntentStore(json, dir);
                ClientState fresh = store.Load();
                Check(fresh != null && fresh.version == 1 && fresh.player_id == "", "nothing stored gives a fresh state");

                store.Save(new ClientState { player_id = "p1" });
                Eq("p1", store.Load().player_id, "first save");
                Check(!File.Exists(store.FilePath + ".tmp"), "no temporary file left");
                store.Save(new ClientState { player_id = "p2" });
                Eq("p2", store.Load().player_id, "second save replaces the file");
                Check(File.Exists(store.FilePath + ".bak"), "backup kept");

                File.WriteAllText(store.FilePath, "{\"version\":1,\"player_id\":\"p");
                Eq("p1", store.Load().player_id, "a torn file falls back to the backup");

                File.Delete(store.FilePath);
                File.WriteAllText(store.FilePath + ".tmp", json.ToJson(new ClientState { player_id = "p3" }));
                Eq("p3", store.Load().player_id, "a complete temporary file wins when the file is missing");

                store.Save(new ClientState { player_id = "p4" });
                Eq("p4", store.Load().player_id, "save after recovery");

                File.Delete(store.FilePath);
                File.Delete(store.FilePath + ".bak");
                if (File.Exists(store.FilePath + ".tmp"))
                {
                    File.Delete(store.FilePath + ".tmp");
                }
                Eq("", store.Load().player_id, "nothing readable gives a fresh state");
            }
            finally
            {
                if (Directory.Exists(dir))
                {
                    Directory.Delete(dir, true);
                }
            }
        }
        static void ThrowingCallbackDuringResync()
        {
            Rig rig = new Rig().Started();
            int second = 0;
            int third = 0;
            rig.Client.Join("t1", r => { throw new InvalidOperationException("a destroyed UI object"); });
            rig.Client.Deal("t1", 1, r => second++);
            rig.Client.Draw("t1", 1, 0, r => third++);
            rig.Tick();
            FakeTransport.Exchange join = rig.Single("POST", "/v1/tournaments/t1/join");
            rig.Transport.Answer(join, Response(409, ErrorJson("stale_seq", false), "X-Arena-Slot", "60", "X-Arena-Next-Seq", "9"));
            bool threw = false;
            try
            {
                rig.Client.Update();
            }
            catch (InvalidOperationException)
            {
                threw = true;
            }
            Check(threw, "the callback's exception leaves Update");
            Eq(3, rig.Outcomes.Count, "IntentCompleted for the head and both dropped intents");
            Eq(1, second, "the second callback ran");
            Eq(1, third, "the third callback ran");
            Eq(1, rig.Resyncs, "Resynced raised");
            Eq(0, rig.Client.PendingCount, "queue empty");
            Eq(8L, rig.Stored.last_assigned_seq, "numbering from X-Arena-Next-Seq");
            rig.Tick();
            Eq(3, rig.Outcomes.Count, "nothing delivered twice");
        }

        static void ThrowingCallbackOnSuccess()
        {
            Rig rig = new Rig().Started();
            rig.Client.Deal("t1", 1, r => { throw new InvalidOperationException("deal callback"); });
            rig.Tick();
            FakeTransport.Exchange deal = rig.Single("POST", "/v1/tournaments/t1/rounds/1/deal");
            rig.Transport.Answer(deal, Response(201, RoundBody(rig, 2, 0), "X-Arena-Slot", "41"));
            bool threw = false;
            try
            {
                rig.Client.Update();
            }
            catch (InvalidOperationException)
            {
                threw = true;
            }
            Check(threw, "the callback's exception leaves Update");
            Eq(1, rig.Outcomes.Count, "IntentCompleted raised");
            Eq(0, rig.Client.PendingCount, "the intent completed");
            Check(rig.Client.FindAudit("t1", 1) != null, "the deal audit is kept");

            // A ConnectionChanged handler that throws must not skip the backoff.
            rig.Client.ConnectionChanged += state => { throw new InvalidOperationException("connection handler"); };
            ArenaResult<JoinResponse> joined = null;
            rig.Client.Join("t1", r => joined = r);
            rig.Tick();
            FakeTransport.Exchange join = rig.Single("POST", "/v1/tournaments/t1/join");
            rig.Transport.Answer(join, Response(503, ErrorJson("unavailable", true), "Retry-After", "2"));
            threw = false;
            try
            {
                rig.Client.Update();
            }
            catch (InvalidOperationException)
            {
                threw = true;
            }
            Check(threw, "the handler's exception leaves Update");
            rig.Tick(1000);
            rig.NoneOpen();
            rig.Tick(1001);
            join = rig.Single("POST", "/v1/tournaments/t1/join");
            rig.Transport.Answer(join, Response(201, JoinBody(rig, 3), "X-Arena-Slot", "42"));
            try
            {
                rig.Client.Update();
            }
            catch (InvalidOperationException)
            {
                // ConnectionChanged to Online throws again.
            }
            Check(joined != null && joined.Ok, "the answer was delivered although the connection handler threw");
            Eq(2, rig.Outcomes.Count, "two outcomes");
        }

        static void UnreadableSuccessIsResent()
        {
            Rig rig = new Rig().Started();
            ArenaResult<ClaimResponse> claimed = null;
            rig.Client.ClaimPayout("t1", r => claimed = r);
            rig.Tick();
            FakeTransport.Exchange first = rig.Single("POST", "/v1/tournaments/t1/payout/claim");
            rig.Answer(first, 200, "<html>Sign in to Wi-Fi</html>");
            Check(claimed == null, "no callback for a portal page");
            Eq(1, rig.Client.PendingCount, "the claim stays pending");
            Eq(ConnectionState.Retrying, rig.Client.Connection, "a retryable failure");
            rig.Tick(126);
            FakeTransport.Exchange second = rig.Single("POST", "/v1/tournaments/t1/payout/claim");
            Eq(first.Request.Header("Idempotency-Key"), second.Request.Header("Idempotency-Key"), "same key");
            Eq(first.Request.Body, second.Request.Body, "same body");
            string body = rig.Json.ToJson(new ClaimResponse { slot = 90, next_seq = 3, tournament_id = "t1", amount = 675, posting_key = "claim:t1:" + PlayerA });
            // A body cut short under a 200 that does carry the header.
            rig.Answer(second, 200, body.Substring(0, body.Length / 2), "X-Arena-Slot", "90");
            Check(claimed == null, "no callback for a truncated body");
            rig.Tick(8000);
            FakeTransport.Exchange third = rig.Single("POST", "/v1/tournaments/t1/payout/claim");
            rig.Answer(third, 200, body.Replace("\"replayed\":false", "\"replayed\":true"), "X-Arena-Slot", "90");
            Check(claimed != null && claimed.Ok && claimed.Value.amount == 675 && claimed.Value.replayed, "the recorded answer delivered");
            Eq(1, rig.Outcomes.Count, "one outcome");
            Eq(90L, rig.Stored.last_intent_slot, "slot recorded");
        }

        static void LongPollsAreNotClockSamples()
        {
            Rig rig = new Rig(o => o.FollowEvents = true);
            long offset = 1789200000000 - 1000;
            rig.Tick();
            FakeTransport.Exchange session = rig.Single("POST", "/v1/session");
            // The server writes its answer 20 ms after the request left, and it
            // arrives 20 ms later: the midpoint rule is exact here.
            rig.Now += 40;
            rig.Answer(session, 200, rig.SessionBody(PlayerA, "tok1", 1, Expiry), "X-Arena-Server-Time-Ms", (rig.Now - 20 + offset).ToString(), "X-Arena-Slot", "5");
            Eq(0L, rig.Client.ServerNowMs - (rig.Now + offset), "estimate after the session");
            string idle = "{\"cursor\":0,\"applied_slot\":5,\"has_more\":false,\"events\":[]}";
            for (int i = 0; i < 8; i++)
            {
                FakeTransport.Exchange poll = rig.Single("GET", "/v1/events?cursor=0&wait_ms=25000");
                // Held for the whole wait, stamped when the wait ends.
                rig.Now += 25020;
                rig.Answer(poll, 200, idle, "X-Arena-Server-Time-Ms", (rig.Now - 20 + offset).ToString());
                Eq(0L, rig.Client.ServerNowMs - (rig.Now + offset), "estimate after idle poll " + (i + 1));
            }
        }

        static void ResumeProbesBeforeSending()
        {
            Rig rig = new Rig(o => o.FollowEvents = true).Started();
            rig.Tick();
            rig.Single("GET", "/v1/events?cursor=0&wait_ms=25000");
            rig.Client.Pause();
            rig.Now += 2 * 3600000;
            rig.Client.Resume();
            rig.Client.Draw("t1", 1, 0, null);
            rig.Tick();
            Check(!rig.Client.HasSession, "HasSession is false until the clock estimate is back");
            FakeTransport.Exchange move = rig.Single("POST", "/v1/tournaments/t1/rounds/1/moves");
            Eq("Bearer tok1", move.Request.Header("Authorization"), "the stored token is tried once");
            rig.Tick(50);
            rig.Single("POST", "/v1/tournaments/t1/rounds/1/moves");
            long expired = Expiry + 2 * 3600000;
            rig.Answer(move, 401, ErrorJson("session_expired", true), "X-Arena-Server-Time-Ms", expired.ToString());
            FakeTransport.Exchange session = rig.Single("POST", "/v1/session");
            rig.Answer(session, 200, rig.SessionBody(PlayerA, "tok2", 2, expired + 3600000), "X-Arena-Server-Time-Ms", expired.ToString(), "X-Arena-Slot", "70");
            Check(rig.Client.HasSession, "a session after the refresh");
            List<FakeTransport.Exchange> open = rig.Transport.Open();
            Eq(2, open.Count, "the move and the events poll go with the new token");
            foreach (FakeTransport.Exchange e in open)
            {
                Eq("Bearer tok2", e.Request.Header("Authorization"), "token of " + e.Path);
            }
            int oldToken = 0;
            foreach (FakeTransport.Exchange e in rig.Transport.Sent)
            {
                if (e.Request.Header("Authorization") == "Bearer tok1" && e.Path.StartsWith("/v1/tournaments/", StringComparison.Ordinal))
                {
                    oldToken++;
                }
            }
            Eq(1, oldToken, "one request carried the expired token");
        }

        static void HttpsDowngradeRefused()
        {
            Rig rig = new Rig(o => o.BaseUrls = new[] { "https://a.test" });
            rig.Tick();
            FakeTransport.Exchange session = rig.Single("POST", "/v1/session");
            rig.Answer(session, 307, ErrorJson("not_leader", true),
                "Location", "http://evil.example:8080/v1/session", "X-Arena-Leader", "http://evil.example:8080");
            rig.NoneOpen();
            Eq("", rig.Stored.leader_url, "not cached");
            rig.Tick(126);
            FakeTransport.Exchange again = rig.Single("POST", "/v1/session");
            Eq("https://a.test", again.Origin, "retried at the https base URL");
            // An https leader is followed and cached; an http hint is not adopted when it fails.
            rig.Answer(again, 307, ErrorJson("not_leader", true), "Location", "https://c.test/v1/session", "X-Arena-Leader", "https://c.test");
            Eq("https://c.test", rig.Stored.leader_url, "an https leader is cached");
            FakeTransport.Exchange followed = rig.Single("POST", "/v1/session");
            Eq("https://c.test", followed.Origin, "followed");
            rig.Answer(followed, 503, ErrorJson("no_leader", true), "Retry-After", "1", "X-Arena-Leader", "http://evil.example:8080");
            Eq("", rig.Stored.leader_url, "an http X-Arena-Leader hint is not adopted");

            SystemTextJson json = new SystemTextJson();
            MemoryStore store = new MemoryStore(json);
            store.Save(new ClientState { leader_url = "http://evil.example:8080" });
            ArenaClient restarted = new ArenaClient(new ArenaClientOptions { BaseUrls = new[] { "https://a.test" }, Jurisdiction = "TR", Age = 30 },
                new FakeTransport(), store, json, () => 0);
            Eq("", json.FromJson<ClientState>(store.Saved).leader_url, "a stored http leader is dropped under https base URLs");
            restarted.Dispose();
        }

        static void StoreFailureKeepsTheHead()
        {
            Rig rig = new Rig().Started();
            int callbacks = 0;
            List<Exception> storeErrors = new List<Exception>();
            rig.Client.StoreFailed += e => storeErrors.Add(e);
            rig.Client.Draw("t1", 1, 0, r => callbacks++);
            rig.Tick();
            FakeTransport.Exchange move = rig.Single("POST", "/v1/tournaments/t1/rounds/1/moves");
            rig.Store.FailSaves = 1;
            rig.Transport.Answer(move, Response(200, RoundBody(rig, 2, 1), "X-Arena-Slot", "43"));
            bool threw = false;
            try
            {
                rig.Client.Update();
            }
            catch (IOException)
            {
                threw = true;
            }
            Check(threw, "the store's exception leaves Update");
            Eq(0, callbacks, "nothing delivered that was not saved");
            Eq(0, rig.Outcomes.Count, "no IntentCompleted");
            Eq(1, rig.Client.PendingCount, "the head is back in memory");
            rig.Tick();
            Eq(1, storeErrors.Count, "StoreFailed raised inside the next Update");
            rig.NoneOpen();
            rig.Tick(125);
            FakeTransport.Exchange again = rig.Single("POST", "/v1/tournaments/t1/rounds/1/moves");
            Eq(move.Request.Header("Idempotency-Key"), again.Request.Header("Idempotency-Key"), "resent with its key");
            rig.Answer(again, 200, RoundBody(rig, 2, 1).Replace("\"replayed\":false", "\"replayed\":true"), "X-Arena-Slot", "43");
            Eq(1, callbacks, "delivered once");
            Eq(1, rig.Outcomes.Count, "one outcome");
            Eq(0, rig.Stored.pending.Length, "saved");
        }

        static void IdleUpdateAllocatesNothing()
        {
            Rig rig = new Rig(o => o.FollowEvents = true).Started();
            rig.Client.Join("t1", null);
            rig.Tick();
            Eq(2, rig.Transport.Open().Count, "an intent and an events poll in flight");
            for (int i = 0; i < 1000; i++)
            {
                rig.Client.Update();
            }
            long before = GC.GetAllocatedBytesForCurrentThread();
            for (int i = 0; i < 100000; i++)
            {
                rig.Client.Update();
            }
            long allocated = GC.GetAllocatedBytesForCurrentThread() - before;
            Check(allocated == 0, "100000 idle Updates allocated " + allocated + " bytes");
            Check(rig.Client.BusyForMs >= 0 && rig.Client.ServerNowMs > 0, "reads work");
        }

        static void PersistentStoreKeepsIdentity()
        {
            SystemTextJson json = new SystemTextJson();
            string dir = Path.Combine(Path.GetTempPath(), "paxos-arena-store-" + Guid.NewGuid().ToString("N"));
            PersistentDataIntentStore store = new PersistentDataIntentStore(json, dir);
            try
            {
                ClientState old = new ClientState
                {
                    device_id = "9f86d081884c7d659a2feaa0c55ad015",
                    device_secret = "2c26b46b68ffc68ff99b453c1d30413413422d706483bfa0f98a5e886266e7ae",
                    player_id = "p-old",
                };
                Directory.CreateDirectory(dir);
                File.WriteAllText(store.FilePath + ".tmp", json.ToJson(old), Encoding.UTF8);
                File.WriteAllText(store.FilePath, "");
                Eq("p-old", store.Load().player_id, "an unreadable file falls back to a complete .tmp");

                bool refused = false;
                try
                {
                    new PersistentDataIntentStore(json, dir).Dispose();
                }
                catch (InvalidOperationException)
                {
                    refused = true;
                }
                Check(refused, "a second store on the same directory is refused");

                ArenaClient client = new ArenaClient(new ArenaClientOptions { BaseUrls = new[] { "http://a.test" }, Jurisdiction = "TR", Age = 30 },
                    new FakeTransport(), store, json, () => 0);
                Eq("p-old", client.PlayerId, "the old identity survives");
                client.Flush();
                Eq(old.device_id, json.FromJson<ClientState>(File.ReadAllText(store.FilePath)).device_id, "and is saved again");
                client.Dispose();

                File.WriteAllText(store.FilePath, "{\"version\":1,\"device_");
                File.WriteAllText(store.FilePath + ".bak", "not json");
                if (File.Exists(store.FilePath + ".tmp"))
                {
                    File.Delete(store.FilePath + ".tmp");
                }
                Eq("", store.Load().device_id, "nothing readable gives a fresh state");
                Check(!File.Exists(store.FilePath) && File.Exists(store.FilePath + ".corrupt-1") &&
                      File.Exists(store.FilePath + ".bak.corrupt-1"), "unreadable files are moved aside, not overwritten");

                store.Dispose();
                PersistentDataIntentStore reopened = new PersistentDataIntentStore(json, dir);
                reopened.Save(new ClientState { player_id = "p-new" });
                Eq("p-new", reopened.Load().player_id, "a disposed store's directory can be opened again");
                reopened.Dispose();
            }
            finally
            {
                store.Dispose();
                if (Directory.Exists(dir))
                {
                    Directory.Delete(dir, true);
                }
            }
        }
    }
}
