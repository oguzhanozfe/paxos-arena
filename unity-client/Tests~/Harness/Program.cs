using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Threading;

namespace PaxosArena.Client.Harness
{
    /// <summary>
    /// Entry point of the harness. "--unit" runs the unit tests with a fake
    /// transport; "--play" and "--operator" run the flow of section 11.6 of the
    /// contract against a live cluster. The exit code is 0 when everything
    /// passes, 1 on a failure and 2 on bad arguments.
    /// </summary>
    public static class Program
    {
        const string Usage =
            "usage:\n" +
            "  Harness --unit [--filter TEXT]\n" +
            "  Harness --play URL[,URL...] --operator URL[,URL...] [--players N] [--kill-leader-command CMD]\n" +
            "          [--store DIR] [--step-timeout SECONDS]";

        public static int Main(string[] args)
        {
            HarnessOptions options;
            string error;
            if (!HarnessOptions.TryParse(args, out options, out error))
            {
                Console.Error.WriteLine(error);
                Console.Error.WriteLine(Usage);
                return 2;
            }
            if (options.Unit)
            {
                return UnitTests.Run(options.Filter) == 0 ? 0 : 1;
            }
            try
            {
                new LiveFlow(options).Run();
                Console.WriteLine("PASS  live flow");
                return 0;
            }
            catch (HarnessFailure e)
            {
                Console.WriteLine("FAIL  " + e.Message);
                return 1;
            }
        }
    }

    public sealed class HarnessOptions
    {
        public bool Unit;
        public string Filter = "";
        public string[] Play = new string[0];
        public string[] Operator = new string[0];
        public int Players = 3;
        public string KillLeaderCommand = "";
        public string StoreDir = "";
        public int StepTimeoutSeconds = 60;

        static readonly string[] ValueFlags =
        {
            "--filter", "--play", "--operator", "--players", "--kill-leader-command", "--store", "--step-timeout",
        };

        public static bool TryParse(string[] args, out HarnessOptions options, out string error)
        {
            options = new HarnessOptions();
            error = "";
            for (int i = 0; i < args.Length; i++)
            {
                string arg = args[i];
                if (arg == "--unit")
                {
                    options.Unit = true;
                    continue;
                }
                if (Array.IndexOf(ValueFlags, arg) < 0)
                {
                    error = "unknown argument " + arg;
                    return false;
                }
                if (i + 1 >= args.Length)
                {
                    error = "missing value for " + arg;
                    return false;
                }
                string value = args[++i];
                switch (arg)
                {
                    case "--filter":
                        options.Filter = value;
                        break;
                    case "--play":
                        options.Play = value.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
                        break;
                    case "--operator":
                        options.Operator = value.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
                        break;
                    case "--players":
                        if (!int.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out options.Players) || options.Players < 1 || options.Players > 50)
                        {
                            error = "--players is 1 to 50";
                            return false;
                        }
                        break;
                    case "--kill-leader-command":
                        options.KillLeaderCommand = value;
                        break;
                    case "--store":
                        options.StoreDir = value;
                        break;
                    case "--step-timeout":
                        if (!int.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out options.StepTimeoutSeconds) || options.StepTimeoutSeconds < 1)
                        {
                            error = "--step-timeout is a positive number of seconds";
                            return false;
                        }
                        break;
                    default:
                        error = "unknown argument " + arg;
                        return false;
                }
            }
            if (!options.Unit && (options.Play.Length == 0 || options.Operator.Length == 0))
            {
                error = "--unit, or both --play and --operator, are required";
                return false;
            }
            return true;
        }
    }

    public sealed class HarnessFailure : Exception
    {
        public HarnessFailure(string message) : base(message)
        {
        }
    }

    /// <summary>The flow of section 11.6, each step asserted.</summary>
    public sealed class LiveFlow
    {
        sealed class Player
        {
            public int Index;
            public string Dir = "";
            public FileIntentStore Store;
            public HttpClientTransport Transport;
            public ArenaClient Client;
            public string Id = "";
            public readonly List<IntentOutcome> Outcomes = new List<IntentOutcome>();
            public readonly List<EventItem> Events = new List<EventItem>();
            public int Resyncs;
            public readonly long[] RoundScores = new long[3];
            public string LastMoveBody = "";
            public string LastMovePath = "";
            public IntentOutcome LastMove;
            public bool IllegalMoveSent;
            public readonly List<HttpClientTransport> Transports = new List<HttpClientTransport>();

            public long Total
            {
                get { return RoundScores[0] + RoundScores[1] + RoundScores[2]; }
            }
        }

        readonly HarnessOptions options;
        readonly SystemTextJson json = new SystemTextJson();
        readonly Stopwatch clock = Stopwatch.StartNew();
        readonly HttpClient raw = new HttpClient(new SocketsHttpHandler { AllowAutoRedirect = false }) { Timeout = TimeSpan.FromSeconds(40) };
        readonly List<Player> players = new List<Player>();
        string tid = "";
        bool killed;

        public LiveFlow(HarnessOptions options)
        {
            this.options = options;
        }

        public void Run()
        {
            string root = options.StoreDir.Length > 0
                ? options.StoreDir
                : Path.Combine(Path.GetTempPath(), "paxos-arena-harness-" + Guid.NewGuid().ToString("N"));
            Step("stores under " + root);
            try
            {
                CreateTournament();
                for (int i = 0; i < options.Players; i++)
                {
                    Player p = new Player { Index = i, Dir = Path.Combine(root, "player-" + i) };
                    players.Add(p);
                    Build(p);
                    SessionsAndJoin(p);
                }
                for (int round = 1; round <= 3; round++)
                {
                    foreach (Player p in players)
                    {
                        PlayRound(p, round);
                    }
                }
                if (options.KillLeaderCommand.Length > 0 && !killed)
                {
                    Fail("the kill-leader command never ran");
                }
                if (!players[0].IllegalMoveSent)
                {
                    Fail("no round offered a column to play illegally");
                }
                ReplaysAndSequenceErrors(players[0]);
                Restart(players[players.Count > 1 ? 1 : 0]);
                Dictionary<string, long[]> payouts = LeaderboardCloseSettle();
                Events(payouts);
                Claims(payouts);
                LedgerThroughOperatorApi(payouts);
                int sessions = 0;
                foreach (Player p in players)
                {
                    foreach (HttpClientTransport transport in p.Transports)
                    {
                        sessions += Array.FindAll(transport.Exchanges, e => e.Status == 200 && e.Method == "POST" && e.Url.EndsWith("/v1/session", StringComparison.Ordinal)).Length;
                    }
                }
                Step("sessions answered 200: " + sessions + " for " + players.Count + " players (two each in step 2; the rest refreshed a token before expiry or after a restart)");
            }
            finally
            {
                foreach (Player p in players)
                {
                    if (p.Client != null)
                    {
                        p.Client.Dispose();
                    }
                    if (p.Transport != null)
                    {
                        p.Transport.Dispose();
                    }
                }
            }
        }

        // ---- step 1 ----

        void CreateTournament()
        {
            tid = "harness-" + DateTime.UtcNow.ToString("yyyyMMdd-HHmmss", CultureInfo.InvariantCulture) + "-" +
                  Guid.NewGuid().ToString("N").Substring(0, 6);
            int n = options.Players;
            string prizes = n >= 3 ? "[5000,3000,2000]" : n == 2 ? "[7000,3000]" : "[10000]";
            string body = "{\"id\":\"" + tid + "\",\"rules\":{\"entry_fee\":500,\"rake_bps\":1000,\"prize_bps\":" + prizes +
                          ",\"min_entrants\":" + n + ",\"max_entrants\":100,\"max_score\":14400,\"min_age\":18," +
                          "\"tie_break\":\"earliest_submission\",\"exclusions\":{\"version\":1,\"jurisdictions\":[]},\"game\":\"ladder-v1\"}}";
            HttpResponse r = Operator("POST", "/v1/tournaments", body);
            Check(r.Status == 201, "create tournament: HTTP " + r.Status + " " + r.Body);
            Step("1 created " + tid);
        }

        // ---- step 2 ----

        void SessionsAndJoin(Player p)
        {
            ArenaResult<SessionResponse> first = Ok<SessionResponse>(p, "first session", done => p.Client.OpenSession(done));
            Check(first.Value.new_player, "player " + p.Index + ": first session is not a new player");
            p.Id = first.Value.player_id;
            Check(p.Id.StartsWith("p-", StringComparison.Ordinal) && p.Id.Length == 28, "player id shape: " + p.Id);
            ArenaResult<SessionResponse> second = Ok<SessionResponse>(p, "second session", done => p.Client.OpenSession(done));
            Check(second.Value.player_id == p.Id && !second.Value.new_player, "second session: " + second.Value.player_id + " new " + second.Value.new_player);

            TournamentSummary summary = FindTournament(p);
            Check(summary.eligible && !summary.joined, "tournament not eligible for player " + p.Index);
            Check(summary.game == "ladder-v1" && summary.rounds == 3 && summary.max_score == 14400, "tournament summary rules");
            ArenaResult<JoinResponse> join = Ok<JoinResponse>(p, "join", done => p.Client.Join(tid, done));
            Check(join.Value.tournament_id == tid && join.Value.entry_fee == 500 && join.Value.rounds == 3 && join.Value.join_seq == p.Index + 1,
                "join body: seq " + join.Value.join_seq);
            Step("2 player " + p.Index + " " + p.Id + " has sessions and joined");
        }

        TournamentSummary FindTournament(Player p)
        {
            for (int offset = 0; ; offset += 100)
            {
                int page = offset;
                ArenaResult<TournamentListResponse> list = Ok<TournamentListResponse>(p, "list tournaments",
                    done => p.Client.ListTournaments(TournamentStatus.Open, page, 100, done));
                foreach (TournamentSummary s in list.Value.tournaments)
                {
                    if (s.tournament_id == tid)
                    {
                        return s;
                    }
                }
                if (offset + 100 >= list.Value.total)
                {
                    Fail("tournament " + tid + " not listed");
                }
            }
        }

        // ---- step 3 ----

        void PlayRound(Player p, int round)
        {
            ArenaResult<RoundResponse> dealt = Ok<RoundResponse>(p, "deal round " + round, done => p.Client.Deal(tid, round, done));
            RoundView deal = dealt.Value.round;
            int cards = 0;
            foreach (ColumnView c in deal.columns)
            {
                cards += c.cards.Length;
            }
            Check(deal.columns.Length == 7 && cards == 35 && deal.stock_count == 16 && deal.move_index == 0, "deal shape of round " + round);
            Check(deal.status == RoundStatus.Playing && deal.seed == "" && deal.commitment.Length == 64, "deal hides the seed and commits");
            RoundAudit audit = p.Client.FindAudit(tid, round);
            Check(audit != null && audit.commitment == deal.commitment, "deal audit stored");

            RoundView view = deal;
            List<KeyValuePair<string, int>> moves = new List<KeyValuePair<string, int>>();
            while (view.status == RoundStatus.Playing)
            {
                Check(Same(LadderAudit.PlayableFromView(view), view.playable_columns), "playable_columns at move " + view.move_index);
                Check(view.seed == "", "seed revealed while playing");
                if (p.Index == 0 && !p.IllegalMoveSent)
                {
                    int illegal = IllegalColumn(view);
                    if (illegal >= 0)
                    {
                        IllegalMove(p, round, view, illegal);
                    }
                }
                RoundView before = view;
                string kind = before.playable_columns.Length > 0 ? MoveKind.Play : MoveKind.Draw;
                int column = kind == MoveKind.Play ? before.playable_columns[0] : MoveKind.NoColumn;
                Check(kind == MoveKind.Play || before.can_draw, "no legal move offered at move " + before.move_index);
                if (options.KillLeaderCommand.Length > 0 && !killed && p.Index == 0 && round == 2 && view.move_index == 5)
                {
                    view = KillLeaderWithMoveUnanswered(p, round, before, kind, column);
                }
                else
                {
                    ArenaResult<RoundResponse> moved = Ok<RoundResponse>(p, "move " + before.move_index + " of round " + round, done =>
                    {
                        if (kind == MoveKind.Play)
                        {
                            p.Client.Play(tid, round, before.move_index, column, done);
                        }
                        else
                        {
                            p.Client.Draw(tid, round, before.move_index, done);
                        }
                    });
                    view = moved.Value.round;
                }
                Check(view.move_index == before.move_index + 1, "move_index " + view.move_index + " after " + before.move_index);
                moves.Add(new KeyValuePair<string, int>(kind, column));
                p.LastMove = LastOutcome(p, IntentRoutes.Move);
                p.LastMovePath = "/v1/tournaments/" + tid + "/rounds/" + round + "/moves";
                p.LastMoveBody = json.ToJson(new MoveRequest { seq = p.LastMove.Seq, move_index = before.move_index, kind = kind, column = column });
                Check(moves.Count <= 51, "more than 51 moves");
            }
            ArenaResult<RoundResponse> finished = Ok<RoundResponse>(p, "finish round " + round, done => p.Client.Finish(tid, round, done));
            RoundView final = finished.Value.round;
            Check(final.status == RoundStatus.Finished && final.score == view.score && final.move_index == view.move_index,
                "finishing a finished round returns its final view");

            Check(final.seed.Length == 64, "no seed on the finished round");
            Check(LadderAudit.Commit(final.seed) == deal.commitment, "the revealed seed does not match the commitment");
            LadderAudit.Board board = new LadderAudit.Board(LadderAudit.Shuffle(final.seed));
            string diff = board.Compare(deal);
            Check(diff == "", "deal differs from the revealed seed: " + diff);
            foreach (KeyValuePair<string, int> move in moves)
            {
                Check(board.Apply(move.Key, move.Value), "an accepted move is illegal on the replayed board");
            }
            diff = board.Compare(final);
            Check(diff == "", "final board differs from the replay: " + diff);
            Check(board.Over() == final.finish_reason, "finish reason " + final.finish_reason + ", replay says " + board.Over());
            p.Client.RemoveAudit(tid, round);
            p.RoundScores[round - 1] = final.score;
            Step("3 player " + p.Index + " round " + round + ": " + moves.Count + " moves, " + final.finish_reason + ", score " + final.score + ", seed verified");
        }

        /// <summary>A non-empty column whose top card does not fit the waste card, or -1.</summary>
        static int IllegalColumn(RoundView view)
        {
            for (int c = 0; c < view.columns.Length; c++)
            {
                if (view.columns[c].cards.Length > 0 && Array.IndexOf(view.playable_columns, c) < 0)
                {
                    return c;
                }
            }
            return -1;
        }

        /// <summary>
        /// Plays a column the rules refuse: the answer is a definitive 409
        /// illegal_move that consumed its sequence number, the board is
        /// unchanged, and the client carries on without resynchronising.
        /// </summary>
        void IllegalMove(Player p, int round, RoundView view, int column)
        {
            int resyncs = p.Resyncs;
            ArenaResult<RoundResponse> bad = Await<RoundResponse>(p, "illegal play of column " + column,
                done => p.Client.Play(tid, round, view.move_index, column, done));
            Check(!bad.Ok && bad.Error.Status == 409 && bad.Error.Code == ErrorCodes.IllegalMove && !bad.Error.Retryable,
                "an illegal move must be answered 409 illegal_move, got " + (bad.Ok ? "success" : bad.Error.ToString()));
            IntentOutcome outcome = LastOutcome(p, IntentRoutes.Move);
            Exchange answer = LastAnswer(p.Transport, outcome.IdempotencyKey);
            Check(answer != null && answer.Header(Headers.NextSeq) == (outcome.Seq + 1).ToString(CultureInfo.InvariantCulture),
                "the illegal move's answer must carry X-Arena-Next-Seq " + (outcome.Seq + 1) + ", got " + (answer == null ? "no answer" : answer.Header(Headers.NextSeq)));
            Check(p.Store.Load().last_assigned_seq == outcome.Seq && p.Client.PendingCount == 0, "the rejection is definitive and keeps the numbering");
            ArenaResult<RoundResponse> read = Ok<RoundResponse>(p, "round after the illegal move", done => p.Client.GetRound(tid, round, done));
            Check(read.Value.round.move_index == view.move_index && json.ToJson(read.Value.round) == json.ToJson(view),
                "the illegal move changed the board");
            Check(p.Resyncs == resyncs, "an illegal move must not resynchronise the client");
            p.IllegalMoveSent = true;
            Step("3 player " + p.Index + " round " + round + ": play of column " + column + " at move " + view.move_index +
                 " answered 409 illegal_move; seq " + outcome.Seq + " consumed, board unchanged");
        }

        /// <summary>
        /// The leader answers a move, the answer never reaches the game, the
        /// leader's process is killed and the app dies with it. A client rebuilt
        /// from the store resends the move with its key: the cached leader does
        /// not answer, a follower redirects, and the new leader replays the
        /// recorded result.
        /// </summary>
        RoundView KillLeaderWithMoveUnanswered(Player p, int round, RoundView before, string kind, int column)
        {
            Check(options.Operator.Length == options.Play.Length,
                "--kill-leader-command needs one --operator URL per --play URL, in the same node order");
            string oldLeader = p.Store.Load().leader_url;
            int oldIndex = Array.IndexOf(options.Play, oldLeader);
            Check(oldIndex >= 0, "the client caches the leader before the kill, got '" + oldLeader + "'");
            // Let the player's intents bucket (burst 20, one per 100 ms) refill,
            // so the leader applies the move instead of answering 429.
            Thread.Sleep(2500);
            bool oldCallback = false;
            if (kind == MoveKind.Play)
            {
                p.Client.Play(tid, round, before.move_index, column, r => oldCallback = true);
            }
            else
            {
                p.Client.Draw(tid, round, before.move_index, r => oldCallback = true);
            }
            p.Client.Update();
            ClientState stored = p.Store.Load();
            Check(stored.pending.Length == 1 && stored.pending[0].route == IntentRoutes.Move, "the move is stored before it is sent");
            string key = stored.pending[0].idempotency_key;
            long seq = stored.pending[0].seq;
            // Wait for the answer without dispatching it to the client.
            long deadline = clock.ElapsedMilliseconds + options.StepTimeoutSeconds * 1000L;
            while (!Array.Exists(p.Transport.Exchanges, e => e.Key == key))
            {
                Check(clock.ElapsedMilliseconds < deadline, "the leader did not answer the move within " + options.StepTimeoutSeconds + " s");
                Thread.Sleep(2);
            }
            Exchange first = LastAnswer(p.Transport, key);
            Check(first != null && first.Status == 200 && first.Url.StartsWith(oldLeader + "/", StringComparison.Ordinal),
                "the leader must have applied the move before the kill: " + Describe(first));
            KillLeader();
            // The app dies too, with the answer still undelivered.
            p.Client.Dispose();
            p.Transport.Dispose();
            int newIndex = WaitForNewLeader(oldIndex);
            int followerIndex = 0;
            while (followerIndex == oldIndex || followerIndex == newIndex)
            {
                followerIndex++;
            }
            Check(followerIndex < options.Play.Length, "a follower survives the kill");
            // The store still caches the killed leader. With the base URLs in
            // this order, the client that drops it moves on to the follower.
            Build(p, new[] { options.Play[oldIndex], options.Play[followerIndex], options.Play[newIndex] });
            PumpUntil(() => FindOutcome(p, key) != null, "the stored move completed after the kill");
            IntentOutcome outcome = FindOutcome(p, key);
            Check(outcome.Error == null && outcome.Status == 200 && outcome.Seq == seq,
                "resumed move: " + (outcome.Error != null ? outcome.Error.ToString() : outcome.Status.ToString()));
            RoundResponse resumed = json.FromJson<RoundResponse>(outcome.Body);
            Check(resumed.replayed && resumed.round.move_index == before.move_index + 1,
                "the new leader must replay the move applied before the kill: replayed " + resumed.replayed + ", move_index " + resumed.round.move_index);
            Check(resumed.slot.ToString(CultureInfo.InvariantCulture) == first.Header(Headers.Slot),
                "the replay names slot " + resumed.slot + ", the first answer " + first.Header(Headers.Slot));
            List<Exchange> sends = new List<Exchange>();
            foreach (Exchange e in p.Transport.Exchanges)
            {
                if (e.Key == key)
                {
                    sends.Add(e);
                }
            }
            bool lost = sends.Exists(e => e.Status == 0 && e.Url.StartsWith(options.Play[oldIndex] + "/", StringComparison.Ordinal));
            bool redirected = sends.Exists(e => e.Status == 307 && e.Url.StartsWith(options.Play[followerIndex] + "/", StringComparison.Ordinal) &&
                                                e.Header(Headers.Location).StartsWith(options.Play[newIndex] + "/", StringComparison.Ordinal));
            Exchange last = sends.Count > 0 ? sends[sends.Count - 1] : null;
            Check(lost && redirected && last != null && last.Status == 200 && last.Url.StartsWith(options.Play[newIndex] + "/", StringComparison.Ordinal),
                "the resend must fail on the killed leader, follow the follower's 307 and end on the new leader: " + string.Join("; ", sends.ConvertAll(Describe)));
            Check(p.Store.Load().leader_url == options.Play[newIndex], "the new leader is cached");
            Check(!oldCallback && p.Client.PendingCount == 0, "resume bookkeeping");
            p.LastMove = outcome;
            Step("3 player " + p.Index + " round " + round + ": move " + before.move_index + " (seq " + seq + ", key " + key +
                 ") applied, leader killed before the answer was delivered, client rebuilt from its store; resend: " +
                 string.Join(" -> ", sends.ConvertAll(e => Origin(e.Url) + " " + (e.Status == 0 ? "no response" : e.Status.ToString(CultureInfo.InvariantCulture)))) +
                 " replayed");
            return resumed.round;
        }

        /// <summary>Polls the operator API of every node but the killed one until one is a ready leader.</summary>
        int WaitForNewLeader(int killed)
        {
            long deadline = clock.ElapsedMilliseconds + options.StepTimeoutSeconds * 1000L;
            while (true)
            {
                for (int i = 0; i < options.Operator.Length; i++)
                {
                    if (i == killed)
                    {
                        continue;
                    }
                    HttpResponse r = SendOnce("GET", options.Operator[i] + "/v1/node", "", "", "");
                    if (r.Status != 200)
                    {
                        continue;
                    }
                    using (JsonDocument doc = JsonDocument.Parse(r.Body))
                    {
                        JsonElement node = doc.RootElement;
                        if (node.GetProperty("role").GetString() == "leader" && node.GetProperty("ready").GetBoolean())
                        {
                            Step("3 node at " + options.Operator[i] + " leads after the kill");
                            return i;
                        }
                    }
                }
                Check(clock.ElapsedMilliseconds < deadline, "no new leader within " + options.StepTimeoutSeconds + " s of the kill");
                Thread.Sleep(50);
            }
        }

        /// <summary>The last exchange for key that received an answer other than a redirect.</summary>
        static Exchange LastAnswer(HttpClientTransport transport, string key)
        {
            Exchange found = null;
            foreach (Exchange e in transport.Exchanges)
            {
                if (e.Key == key && e.Status != 0 && e.Status != 307)
                {
                    found = e;
                }
            }
            return found;
        }

        static string Describe(Exchange e)
        {
            if (e == null)
            {
                return "no exchange";
            }
            return e.Method + " " + e.Url + " -> " + (e.Status == 0 ? "no response" : e.Status + " " + e.Body);
        }

        static string Origin(string url)
        {
            Uri uri;
            return Uri.TryCreate(url, UriKind.Absolute, out uri) ? uri.GetLeftPart(UriPartial.Authority) : url;
        }

        void KillLeader()
        {
            Step("3 running the kill-leader command");
            ProcessStartInfo start = new ProcessStartInfo("/bin/sh")
            {
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
            };
            start.ArgumentList.Add("-c");
            start.ArgumentList.Add(options.KillLeaderCommand);
            using (Process process = Process.Start(start))
            {
                string output = process.StandardOutput.ReadToEnd() + process.StandardError.ReadToEnd();
                process.WaitForExit();
                Step("3 kill-leader command exited " + process.ExitCode + (output.Length > 0 ? ": " + output.Trim() : ""));
            }
            killed = true;
        }

        // ---- step 4 ----

        void ReplaysAndSequenceErrors(Player p)
        {
            Exchange firstAnswer = LastAnswer(p.Transport, p.LastMove.IdempotencyKey);
            Check(firstAnswer != null && firstAnswer.Status == p.LastMove.Status && firstAnswer.Body == p.LastMove.Body, "the last move's answer was recorded");
            HttpResponse replay = RawPost(p, p.LastMovePath, p.LastMove.IdempotencyKey, p.LastMoveBody);
            Check(replay.Status == p.LastMove.Status, "replay status " + replay.Status + ", first " + p.LastMove.Status);
            const string FirstPrefix = "{\"replayed\":false,";
            const string ReplayPrefix = "{\"replayed\":true,";
            Check(p.LastMove.Body.StartsWith(FirstPrefix, StringComparison.Ordinal) &&
                  replay.Body == ReplayPrefix + p.LastMove.Body.Substring(FirstPrefix.Length),
                "the replayed body must equal the recorded response but for replayed:\n  first  " + p.LastMove.Body + "\n  replay " + replay.Body);
            foreach (string header in new[] { Headers.Slot, Headers.NextSeq })
            {
                Check(replay.Header(header).Length > 0 && replay.Header(header) == firstAnswer.Header(header),
                    header + " of the replay is '" + replay.Header(header) + "', the first answer's '" + firstAnswer.Header(header) + "'");
            }
            Step("4 resent the last move with its key (seq " + p.LastMove.Seq + "): status " + replay.Status + ", " + Headers.Slot + " " +
                 replay.Header(Headers.Slot) + ", body byte-identical but for replayed:true");

            // A captured move under a new key: its number was used, and nothing
            // is consumed. The rejection is recorded under the new key.
            string staleKey = Guid.NewGuid().ToString("N");
            long next = p.Store.Load().last_assigned_seq + 1;
            HttpResponse stale = RawPost(p, p.LastMovePath, staleKey, p.LastMoveBody);
            ErrorBody staleError = json.FromJson<ErrorBody>(stale.Body);
            Check(stale.Status == 409 && staleError.code == ErrorCodes.StaleSeq && !staleError.retryable,
                "a used seq under a new key must be 409 stale_seq, got " + stale.Status + " " + stale.Body);
            Check(stale.Header(Headers.NextSeq) == next.ToString(CultureInfo.InvariantCulture),
                "stale_seq carries X-Arena-Next-Seq " + next + ", got '" + stale.Header(Headers.NextSeq) + "'");
            HttpResponse staleAgain = RawPost(p, p.LastMovePath, staleKey, p.LastMoveBody);
            Check(staleAgain.Status == 409 && staleAgain.Body == stale.Body && staleAgain.Header(Headers.Slot) == stale.Header(Headers.Slot),
                "the recorded stale_seq must be answered again unchanged, got " + staleAgain.Status + " " + staleAgain.Body);
            Step("4 the last move's body (seq " + p.LastMove.Seq + ") under a new key: 409 stale_seq, " + Headers.NextSeq + " " + next +
                 "; resent with that key: the same 409");

            SequenceError(p, -1, ErrorCodes.StaleSeq);
            SequenceError(p, 3, ErrorCodes.SeqGap);
        }

        void SequenceError(Player p, long shift, string code)
        {
            Rebuild(p, state => state.last_assigned_seq += shift);
            int resyncs = p.Resyncs;
            ArenaResult<RoundResponse> bad = Await<RoundResponse>(p, "finish with a " + code, done => p.Client.Finish(tid, 3, done));
            Check(!bad.Ok && bad.Error.Code == code, "expected " + code + ", got " + (bad.Ok ? "success" : bad.Error.ToString()));
            PumpUntil(() => p.Resyncs > resyncs, "Resynced after " + code);
            ArenaResult<RoundResponse> good = Ok<RoundResponse>(p, "finish after " + code, done => p.Client.Finish(tid, 3, done));
            Check(good.Value.round.status == RoundStatus.Finished, "finish after resync");
            Step("4 " + code + " answered 409 and the client resynchronised");
        }

        // ---- step 5 ----

        void Restart(Player p)
        {
            bool oldCallback = false;
            p.Client.Finish(tid, 3, r => oldCallback = true);
            p.Client.Update();
            ClientState stored = p.Store.Load();
            Check(stored.pending.Length == 1, "the intent is stored before the restart");
            string key = stored.pending[0].idempotency_key;
            p.Client.Dispose();
            p.Transport.Dispose();
            Build(p);
            PumpUntil(() => FindOutcome(p, key) != null, "IntentCompleted for the stored intent after the restart");
            IntentOutcome outcome = FindOutcome(p, key);
            Check(outcome.Error == null && outcome.Status == 200, "restarted intent: " + (outcome.Error != null ? outcome.Error.ToString() : outcome.Status.ToString()));
            Check(!oldCallback && p.Client.PendingCount == 0, "restart bookkeeping");
            Step("5 player " + p.Index + " restarted with an intent in flight; resent with key " + key);
        }

        // ---- step 6 ----

        Dictionary<string, long[]> LeaderboardCloseSettle()
        {
            Player p = players[0];
            ArenaResult<LeaderboardResponse> board = Ok<LeaderboardResponse>(p, "leaderboard", done => p.Client.GetLeaderboard(tid, 0, 100, done));
            Check(board.Value.entrants == players.Count && board.Value.rows.Length == players.Count, "leaderboard rows");
            for (int i = 0; i < board.Value.rows.Length; i++)
            {
                LeaderboardRow row = board.Value.rows[i];
                Player owner = players.Find(x => x.Id == row.player_id);
                Check(owner != null && row.total_score == owner.Total && row.rounds_finished == 3, "leaderboard row " + row.player_id);
                Check(i == 0 || board.Value.rows[i - 1].total_score >= row.total_score, "leaderboard order");
                Check(row.place == i + 1, "places are distinct before close");
            }
            Check(board.Value.me.player_id == p.Id, "me row");

            HttpResponse closed = Operator("POST", "/v1/tournaments/" + tid + "/close", "{}");
            Check(closed.Status == 200, "close: HTTP " + closed.Status + " " + closed.Body);
            HttpResponse settled = Operator("POST", "/v1/tournaments/" + tid + "/settle", "{\"exclusions\":{\"version\":1,\"jurisdictions\":[]}}");
            Check(settled.Status == 200, "settle: HTTP " + settled.Status + " " + settled.Body);

            Dictionary<string, long[]> payouts = new Dictionary<string, long[]>();
            using (JsonDocument doc = JsonDocument.Parse(settled.Body))
            {
                JsonElement t = doc.RootElement.GetProperty("tournament");
                Check(t.GetProperty("status").GetString() == TournamentStatus.Settled, "status after settle");
                long pool = t.GetProperty("pool").GetInt64();
                payouts["pool"] = new[] { pool, 0 };
                foreach (JsonElement payout in t.GetProperty("payouts").EnumerateArray())
                {
                    string player = payout.GetProperty("player").GetString();
                    long amount = payout.GetProperty("amount").GetInt64();
                    bool withheld = payout.GetProperty("withheld").GetBoolean();
                    long[] sums;
                    if (!payouts.TryGetValue(player, out sums))
                    {
                        sums = new long[2];
                        payouts[player] = sums;
                    }
                    sums[withheld ? 1 : 0] += amount;
                }
            }
            ArenaResult<LeaderboardResponse> final = Ok<LeaderboardResponse>(p, "final leaderboard", done => p.Client.GetLeaderboard(tid, 0, 100, done));
            Check(final.Value.final && final.Value.status == TournamentStatus.Settled, "final leaderboard");
            Step("6 leaderboard matches the totals; closed and settled, pool " + payouts["pool"][0]);
            return payouts;
        }

        // ---- step 7 ----

        void Events(Dictionary<string, long[]> payouts)
        {
            foreach (Player p in players)
            {
                long[] sums;
                bool paid = payouts.TryGetValue(p.Id, out sums) && sums[0] > 0;
                KeyValuePair<string, int>[] expected =
                {
                    new KeyValuePair<string, int>(EventType.RoundStarted, 3),
                    new KeyValuePair<string, int>(EventType.RoundFinished, 3),
                    new KeyValuePair<string, int>(EventType.EntryScored, 1),
                    new KeyValuePair<string, int>(EventType.TournamentStatus + ":" + TournamentStatus.Closed, 1),
                    new KeyValuePair<string, int>(EventType.TournamentStatus + ":" + TournamentStatus.Settled, 1),
                    new KeyValuePair<string, int>(EventType.PayoutAvailable, paid ? 1 : 0),
                };
                // A replica that has not applied the settle yet answers with
                // fewer events; scan again from cursor 0 until the step timeout.
                long deadline = clock.ElapsedMilliseconds + options.StepTimeoutSeconds * 1000L;
                string mismatch;
                while (true)
                {
                    Dictionary<string, int> counts = ScanEvents(p);
                    mismatch = "";
                    foreach (KeyValuePair<string, int> want in expected)
                    {
                        int n;
                        counts.TryGetValue(want.Key, out n);
                        if (n != want.Value && mismatch.Length == 0)
                        {
                            mismatch = n + " " + want.Key + " events, expected " + want.Value;
                        }
                    }
                    if (mismatch.Length == 0 || clock.ElapsedMilliseconds > deadline)
                    {
                        break;
                    }
                    Thread.Sleep(300);
                }
                Check(mismatch.Length == 0, "player " + p.Index + ": " + mismatch);
                PumpUntil(() => p.Events.Exists(e => e.type == EventType.EntryScored && e.player_id == p.Id), "the SDK follower delivered entry_scored");
                Step("7 player " + p.Index + " events from cursor 0 are complete; the follower is at " + p.Client.Events.Cursor);
            }
        }

        Dictionary<string, int> ScanEvents(Player p)
        {
            Dictionary<string, int> counts = new Dictionary<string, int>();
            long cursor = 0;
            while (true)
            {
                HttpResponse r = RawGet(p, "/v1/events?cursor=" + cursor + "&wait_ms=0&tournament_id=" + tid);
                Check(r.Status == 200, "events: HTTP " + r.Status + " " + r.Body);
                EventsResponse page = json.FromJson<EventsResponse>(r.Body);
                foreach (EventItem e in page.events)
                {
                    Check(e.tournament_id == tid, "event of another tournament");
                    Check(e.player_id == "" || e.player_id == p.Id, "event of another player");
                    string name = e.type == EventType.TournamentStatus ? e.type + ":" + e.status : e.type;
                    int n;
                    counts.TryGetValue(name, out n);
                    counts[name] = n + 1;
                }
                Check(page.cursor >= cursor, "events cursor went backwards");
                cursor = page.cursor;
                if (!page.has_more)
                {
                    return counts;
                }
            }
        }

        // ---- step 8 ----

        void Claims(Dictionary<string, long[]> payouts)
        {
            long claimed = 0;
            long withheld = 0;
            foreach (Player p in players)
            {
                long[] sums;
                payouts.TryGetValue(p.Id, out sums);
                long expected = sums != null ? sums[0] : 0;
                withheld += sums != null ? sums[1] : 0;
                if (expected == 0)
                {
                    ArenaResult<ClaimResponse> none = Await<ClaimResponse>(p, "claim without payout", done => p.Client.ClaimPayout(tid, done));
                    Check(!none.Ok && none.Error.Code == ErrorCodes.NoPayout, "expected no_payout");
                    Step("8 player " + p.Index + " has nothing to claim");
                    continue;
                }
                ArenaResult<ClaimResponse> claim = Ok<ClaimResponse>(p, "claim", done => p.Client.ClaimPayout(tid, done));
                Check(claim.Value.amount == expected, "claimed " + claim.Value.amount + ", expected " + expected);
                Check(claim.Value.posting_key == "claim:" + tid + ":" + p.Id, "posting key " + claim.Value.posting_key);
                claimed += claim.Value.amount;
                IntentOutcome first = LastOutcome(p, IntentRoutes.Claim);

                ArenaResult<ClaimResponse> twice = Await<ClaimResponse>(p, "second claim", done => p.Client.ClaimPayout(tid, done));
                Check(!twice.Ok && twice.Error.Code == ErrorCodes.AlreadyClaimed, "a new key must be already_claimed");

                HttpResponse replay = RawPost(p, "/v1/tournaments/" + tid + "/payout/claim", first.IdempotencyKey,
                    json.ToJson(new SeqRequest { seq = first.Seq }));
                ClaimResponse replayed = json.FromJson<ClaimResponse>(replay.Body);
                Check(replay.Status == 200 && replayed.replayed && replayed.amount == expected, "the same key replays the claim");
                Step("8 player " + p.Index + " claimed " + expected + " once");
            }
            long pool = payouts["pool"][0];
            Check(claimed == pool - withheld, "claims " + claimed + " != pool " + pool + " - withheld " + withheld);
            Step("8 claims sum to the pool minus withheld amounts");
        }

        /// <summary>
        /// Reads the tournament's ledger through the operator API: one fee per
        /// entrant, one prize posting per paid place, and exactly one claim
        /// posting per paid player, with the claimed amount.
        /// </summary>
        void LedgerThroughOperatorApi(Dictionary<string, long[]> payouts)
        {
            HttpResponse r = Operator("GET", "/v1/tournaments/" + tid + "/ledger", "");
            Check(r.Status == 200, "ledger: HTTP " + r.Status + " " + r.Body);
            Dictionary<string, int> kinds = new Dictionary<string, int>();
            Dictionary<string, int> keys = new Dictionary<string, int>();
            Dictionary<string, long> claims = new Dictionary<string, long>();
            int claimPostings = 0;
            using (JsonDocument doc = JsonDocument.Parse(r.Body))
            {
                foreach (JsonElement posting in doc.RootElement.GetProperty("postings").EnumerateArray())
                {
                    string kind = posting.GetProperty("kind").GetString();
                    string key = posting.GetProperty("key").GetString();
                    int n;
                    kinds.TryGetValue(kind, out n);
                    kinds[kind] = n + 1;
                    keys.TryGetValue(key, out n);
                    keys[key] = n + 1;
                    if (kind == "claim")
                    {
                        claimPostings++;
                        string player = posting.GetProperty("player").GetString();
                        Check(key == "claim:" + tid + ":" + player && posting.GetProperty("debit").GetString() == "player:" + player &&
                              posting.GetProperty("credit").GetString() == "claims:" + tid, "claim posting " + posting.GetRawText());
                        long sum;
                        claims.TryGetValue(player, out sum);
                        claims[player] = sum + posting.GetProperty("amount").GetInt64();
                    }
                }
            }
            foreach (KeyValuePair<string, int> key in keys)
            {
                Check(key.Value == 1, "posting key " + key.Key + " appears " + key.Value + " times");
            }
            int paid = 0;
            foreach (Player p in players)
            {
                long[] sums;
                long expected = payouts.TryGetValue(p.Id, out sums) ? sums[0] : 0;
                long got;
                claims.TryGetValue(p.Id, out got);
                Check(got == expected, "player " + p.Index + " claimed " + got + " in the ledger, expected " + expected);
                if (expected > 0)
                {
                    paid++;
                }
            }
            int fees, prizes;
            kinds.TryGetValue("entry_fee", out fees);
            kinds.TryGetValue("prize", out prizes);
            Check(claimPostings == paid && fees == players.Count && prizes == paid,
                "ledger postings by kind: " + string.Join(", ", new List<string>(kinds.Keys).ConvertAll(k => k + " " + kinds[k])));
            Step("8 ledger through the operator API: " + fees + " entry fees, " + prizes + " prizes, exactly one claim posting for each of the " +
                 paid + " paid players, every posting key once");
        }

        // ---- plumbing ----

        void Build(Player p)
        {
            Build(p, options.Play);
        }

        void Build(Player p, string[] baseUrls)
        {
            p.Transport = new HttpClientTransport();
            p.Transports.Add(p.Transport);
            p.Store = new FileIntentStore(json, p.Dir);
            p.Client = new ArenaClient(new ArenaClientOptions
            {
                BaseUrls = baseUrls,
                Jurisdiction = "TR",
                Age = 30,
                FollowEvents = true,
            }, p.Transport, p.Store, json, () => clock.ElapsedMilliseconds);
            p.Client.IntentCompleted += o => p.Outcomes.Add(o);
            p.Client.Resynced += () => p.Resyncs++;
            p.Client.Events.Received += e => p.Events.Add(e);
        }

        void Rebuild(Player p, Action<ClientState> edit)
        {
            p.Client.Dispose();
            p.Transport.Dispose();
            ClientState state = p.Store.Load();
            edit(state);
            p.Store.Save(state);
            Build(p);
        }

        void PumpAll()
        {
            foreach (Player p in players)
            {
                if (p.Client != null)
                {
                    p.Transport.Dispatch();
                    p.Client.Update();
                }
            }
        }

        void PumpUntil(Func<bool> done, string what)
        {
            long deadline = clock.ElapsedMilliseconds + options.StepTimeoutSeconds * 1000L;
            while (!done())
            {
                if (clock.ElapsedMilliseconds > deadline)
                {
                    Fail(what + ": not within " + options.StepTimeoutSeconds + " s");
                }
                PumpAll();
                Thread.Sleep(2);
            }
        }

        ArenaResult<T> Await<T>(Player p, string what, Action<Action<ArenaResult<T>>> start)
        {
            ArenaResult<T> result = null;
            start(r => result = r);
            PumpUntil(() => result != null, "player " + p.Index + " " + what);
            return result;
        }

        ArenaResult<T> Ok<T>(Player p, string what, Action<Action<ArenaResult<T>>> start)
        {
            ArenaResult<T> result = Await(p, what, start);
            Check(result.Ok, "player " + p.Index + " " + what + ": " + (result.Error != null ? result.Error.ToString() : "failed"));
            return result;
        }

        static IntentOutcome LastOutcome(Player p, string route)
        {
            for (int i = p.Outcomes.Count - 1; i >= 0; i--)
            {
                if (p.Outcomes[i].Route == route)
                {
                    return p.Outcomes[i];
                }
            }
            throw new HarnessFailure("no " + route + " outcome recorded");
        }

        static IntentOutcome FindOutcome(Player p, string key)
        {
            return p.Outcomes.Find(o => o.IdempotencyKey == key);
        }

        HttpResponse Operator(string method, string path, string body)
        {
            return Send(options.Operator, method, path, body, "op" + Guid.NewGuid().ToString("N"), "");
        }

        HttpResponse RawPost(Player p, string path, string key, string body)
        {
            return Send(options.Play, "POST", path, body, key, p.Store.Load().session_token);
        }

        HttpResponse RawGet(Player p, string path)
        {
            return Send(options.Play, "GET", path, "", "", p.Store.Load().session_token);
        }

        /// <summary>
        /// Sends one request outside the SDK: follows a 307 once; waits out a
        /// 429 or an in_flight answer; and on a lost connection or a 503 waits
        /// and tries the next base URL.
        /// </summary>
        HttpResponse Send(string[] bases, string method, string path, string body, string key, string token)
        {
            long deadline = clock.ElapsedMilliseconds + options.StepTimeoutSeconds * 1000L;
            int index = 0;
            string target = bases[0] + path;
            while (true)
            {
                HttpResponse r = SendOnce(method, target, body, key, token);
                if (r.Status == 307 && r.Header("Location").Length > 0)
                {
                    r = SendOnce(method, r.Header("Location"), body, key, token);
                }
                bool retry = r.Status == 0 || r.Status == 429 || r.Status == 503 || r.Status == 307 ||
                             (r.Status == 409 && r.Body.Contains("\"in_flight\""));
                if (!retry || clock.ElapsedMilliseconds > deadline)
                {
                    return r;
                }
                if (r.Status == 307 && r.Header("Location").Length > 0)
                {
                    target = r.Header("Location");
                }
                else if (r.Status == 0 || r.Status == 503)
                {
                    index = (index + 1) % bases.Length;
                    target = bases[index] + path;
                }
                Thread.Sleep((int)Math.Max(300, 1000 * Backoff.ParseRetryAfter(r.Header("Retry-After"))));
            }
        }

        HttpResponse SendOnce(string method, string url, string body, string key, string token)
        {
            try
            {
                using (HttpRequestMessage message = new HttpRequestMessage(new HttpMethod(method), url))
                {
                    if (method != "GET")
                    {
                        ByteArrayContent content = new ByteArrayContent(Encoding.UTF8.GetBytes(body));
                        content.Headers.ContentType = new System.Net.Http.Headers.MediaTypeHeaderValue("application/json");
                        message.Content = content;
                    }
                    if (key.Length > 0)
                    {
                        message.Headers.TryAddWithoutValidation("Idempotency-Key", key);
                    }
                    if (token.Length > 0)
                    {
                        message.Headers.TryAddWithoutValidation("Authorization", "Bearer " + token);
                    }
                    using (HttpResponseMessage reply = raw.Send(message))
                    {
                        List<HttpHeader> headers = new List<HttpHeader>();
                        foreach (KeyValuePair<string, IEnumerable<string>> pair in reply.Headers)
                        {
                            headers.Add(new HttpHeader(pair.Key, string.Join(",", pair.Value)));
                        }
                        using (StreamReader reader = new StreamReader(reply.Content.ReadAsStream(), Encoding.UTF8))
                        {
                            return new HttpResponse { Status = (int)reply.StatusCode, Body = reader.ReadToEnd(), Headers = headers.ToArray() };
                        }
                    }
                }
            }
            catch (Exception e)
            {
                return new HttpResponse { TransportError = e.Message };
            }
        }

        static bool Same(int[] a, int[] b)
        {
            if (a.Length != b.Length)
            {
                return false;
            }
            for (int i = 0; i < a.Length; i++)
            {
                if (a[i] != b[i])
                {
                    return false;
                }
            }
            return true;
        }

        void Step(string message)
        {
            Console.WriteLine("ok    " + message);
        }

        static void Check(bool condition, string message)
        {
            if (!condition)
            {
                throw new HarnessFailure(message);
            }
        }

        static void Fail(string message)
        {
            throw new HarnessFailure(message);
        }
    }
}
