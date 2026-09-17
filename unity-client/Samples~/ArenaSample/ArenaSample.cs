using System;
using System.Collections.Generic;
using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using PaxosArena.Client.UnityAdapters;
using UnityEngine;

namespace PaxosArena.Client.Sample
{
    /// <summary>
    /// A whole play flow in one MonoBehaviour with IMGUI (section 11.4): open a
    /// session, list tournaments, join, deal, play or draw from the server's
    /// view, finish, read the leaderboard and claim the payout. It shows the
    /// current view, whether the client is busy, the connection state, the
    /// last events and whether a finished round's revealed seed matches the
    /// commitment received with the deal.
    ///
    /// The sample never changes the board itself: every button sends an intent
    /// and the view is replaced only by the server's answer. Callbacks run
    /// inside ArenaClient.Update, never inside OnGUI.
    ///
    /// After a cold start (the OS kills a backgrounded app) nothing but the
    /// store survives: the tournament list names the round in play
    /// (round_in_play), and the sample reads that round from the server
    /// before it offers input again.
    ///
    /// OnGUI runs several times per frame (layout, input, repaint) and must lay
    /// out the same controls each time, so everything that can change between
    /// those passes, such as how long the client has been busy, is computed
    /// once per frame in Update, together with the text it shows.
    ///
    /// Put it on a GameObject with an ArenaClientBehaviour whose base URLs point
    /// at the play listeners. For a local http:// server, allow downloads over
    /// HTTP in the player settings of development builds.
    /// </summary>
    [RequireComponent(typeof(ArenaClientBehaviour))]
    public sealed class ArenaSample : MonoBehaviour
    {
        const int MaxLines = 10;

        readonly List<string> eventLines = new List<string>();
        readonly List<string> logLines = new List<string>();

        ArenaClientBehaviour host;
        ArenaClient subscribed;
        TournamentSummary tournament;
        RoundView view;
        LeaderboardResponse leaderboard;
        string auditLine = "";
        Vector2 scroll;

        // Computed in Update, read by OnGUI.
        int busyTier;
        string statusText = "";
        string stateText = "";
        bool stateDirty = true;
        long shownSecondsLeft = -1;

        void Start()
        {
            host = GetComponent<ArenaClientBehaviour>();
            host.ClientCreated += Subscribe;
            if (host.Client != null)
            {
                Subscribe(host.Client);
            }
        }

        void OnDestroy()
        {
            if (host != null)
            {
                host.ClientCreated -= Subscribe;
            }
        }

        void Subscribe(ArenaClient client)
        {
            if (subscribed == client)
            {
                return;
            }
            subscribed = client;
            client.IntentCompleted += outcome => Log("intent " + outcome.Route + " seq " + outcome.Seq + ": " +
                (outcome.Error == null ? "HTTP " + outcome.Status : outcome.Error.ToString()));
            client.Resynced += () =>
            {
                Log("resynchronised: reading the round and the tournaments again");
                Refresh(client);
            };
            client.Resumed += () =>
            {
                Log("resumed");
                Refresh(client);
            };
            client.ConnectionChanged += state => Log("connection " + state);
            client.Halted += error => Log("halted: " + error);
            client.Events.Received += item => OnEvent(client, item);
        }

        void Update()
        {
            ArenaClient client = host != null ? host.Client : null;
            if (client == null)
            {
                return;
            }
            long busyFor = client.BusyForMs;
            busyTier = busyFor > 10000 ? 2 : busyFor > 1000 ? 1 : 0;
            statusText = "player " + (client.PlayerId.Length > 0 ? client.PlayerId : "(none yet)") +
                         "   session " + client.HasSession +
                         "   connection " + client.Connection +
                         "   busy " + client.Busy;
            long secondsLeft = SecondsLeft(client);
            if (stateDirty || secondsLeft != shownSecondsLeft)
            {
                stateText = DescribeState(secondsLeft);
                shownSecondsLeft = secondsLeft;
                stateDirty = false;
            }
        }

        void OnGUI()
        {
            ArenaClient client = host != null ? host.Client : null;
            GUILayout.BeginArea(new Rect(10, 10, Screen.width - 20, Screen.height - 20));
            scroll = GUILayout.BeginScrollView(scroll);
            if (client == null)
            {
                GUILayout.Label("Set the base URLs on ArenaClientBehaviour, or call Initialize.");
            }
            else
            {
                DrawControls(client);
                GUILayout.Label(stateText);
            }
            GUILayout.EndScrollView();
            GUILayout.EndArea();
        }

        void DrawControls(ArenaClient client)
        {
            GUILayout.Label(statusText);
            if (busyTier == 2)
            {
                GUILayout.Label("The connection is being restored. Your last move is kept and will be sent.");
            }
            else if (busyTier == 1)
            {
                GUILayout.Label("reconnecting...");
            }

            if (GUILayout.Button("Open session"))
            {
                client.OpenSession(r => Log(r.Ok
                    ? "session for " + r.Value.player_id + (r.Value.new_player ? " (new player)" : "")
                    : "session failed: " + r.Error));
            }
            if (GUILayout.Button("List tournaments"))
            {
                ListTournaments(client);
            }

            string tid = tournament != null ? tournament.tournament_id : "";
            bool input = tournament != null && !client.Busy;
            GUI.enabled = input && tournament.eligible;
            if (GUILayout.Button("Join " + tid))
            {
                client.Join(tid, r =>
                {
                    Log(r.Ok ? "joined as entrant " + r.Value.join_seq : "join failed: " + r.Error);
                    ListTournaments(client);
                });
            }

            int next = NextRound();
            GUI.enabled = input && next > 0;
            if (GUILayout.Button(next > 0 ? "Deal round " + next : "Deal"))
            {
                client.Deal(tid, next, r => OnRound(client, tid, next, r));
            }

            bool playing = view != null && view.status == RoundStatus.Playing;
            GUI.enabled = input && playing;
            GUILayout.BeginHorizontal();
            if (playing)
            {
                // Buttons come from the server's view, with its move_index.
                int round = view.round;
                for (int i = 0; i < view.playable_columns.Length; i++)
                {
                    int column = view.playable_columns[i];
                    if (GUILayout.Button("Play column " + column))
                    {
                        client.Play(tid, round, view.move_index, column, r => OnRound(client, tid, round, r));
                    }
                }
                if (view.can_draw && GUILayout.Button("Draw"))
                {
                    client.Draw(tid, round, view.move_index, r => OnRound(client, tid, round, r));
                }
            }
            GUILayout.EndHorizontal();
            if (GUILayout.Button(playing ? "Finish round " + view.round : "Finish") && playing)
            {
                int round = view.round;
                client.Finish(tid, round, r => OnRound(client, tid, round, r));
            }

            GUI.enabled = tournament != null;
            if (GUILayout.Button("Leaderboard"))
            {
                RefreshLeaderboard(client);
            }
            GUI.enabled = input;
            if (GUILayout.Button("Claim payout"))
            {
                client.ClaimPayout(tid, r => Log(r.Ok
                    ? "claimed " + r.Value.amount + (r.Value.replayed ? " (replayed)" : "")
                    : "claim failed: " + r.Error));
            }
            GUI.enabled = true;
        }

        /// <summary>Everything below the buttons, as one text; rebuilt only when something changed.</summary>
        string DescribeState(long secondsLeft)
        {
            StringBuilder sb = new StringBuilder();
            if (tournament != null)
            {
                sb.Append("tournament ").Append(tournament.tournament_id).Append(": ").Append(tournament.status)
                  .Append(", entrants ").Append(tournament.entrants).Append(", fee ").Append(tournament.entry_fee)
                  .Append(", joined ").Append(tournament.joined).Append(", rounds finished ").Append(tournament.rounds_finished)
                  .Append("\n\n");
            }
            sb.Append(DescribeView(secondsLeft)).Append("\n\n");
            if (auditLine.Length > 0)
            {
                sb.Append(auditLine).Append("\n\n");
            }
            if (leaderboard != null)
            {
                sb.Append("leaderboard (").Append(leaderboard.final ? "final" : "provisional").Append(")\n");
                for (int i = 0; i < leaderboard.rows.Length; i++)
                {
                    LeaderboardRow row = leaderboard.rows[i];
                    sb.Append(row.place).Append(". ").Append(row.player_id).Append("  ").Append(row.total_score)
                      .Append("  rounds ").Append(row.rounds_finished);
                    if (leaderboard.final)
                    {
                        sb.Append("  amount ").Append(row.amount).Append(row.withheld ? " (withheld)" : "")
                          .Append(row.claimed ? " (claimed)" : "");
                    }
                    sb.Append('\n');
                }
                sb.Append('\n');
            }
            sb.Append("events\n").Append(string.Join("\n", eventLines.ToArray())).Append("\n\n");
            sb.Append("log\n").Append(string.Join("\n", logLines.ToArray()));
            return sb.ToString();
        }

        /// <summary>Whole seconds to the round's deadline by the server clock estimate; -1 when not shown.</summary>
        long SecondsLeft(ArenaClient client)
        {
            long serverNow = client.ServerNowMs;
            if (view == null || view.status != RoundStatus.Playing || serverNow <= 0)
            {
                return -1;
            }
            long left = view.deadline_ms - serverNow;
            return left > 0 ? left / 1000 : 0;
        }

        string DescribeView(long secondsLeft)
        {
            if (view == null)
            {
                return "no round";
            }
            StringBuilder sb = new StringBuilder();
            sb.Append("round ").Append(view.round).Append(": ").Append(view.status);
            if (view.finish_reason.Length > 0)
            {
                sb.Append(" (").Append(view.finish_reason).Append(')');
            }
            sb.Append(", move ").Append(view.move_index).Append(", score ").Append(view.score)
              .Append(", cleared ").Append(view.cleared).Append('\n');
            for (int c = 0; c < view.columns.Length; c++)
            {
                sb.Append("column ").Append(c).Append(": ").Append(string.Join(" ", view.columns[c].cards)).Append('\n');
            }
            sb.Append("waste ").Append(view.waste_top).Append(" (").Append(view.waste_count).Append(")  stock ")
              .Append(view.stock_count);
            if (secondsLeft >= 0)
            {
                // Display only: the server decides the deadline.
                sb.Append("  time left ").Append(secondsLeft).Append(" s");
            }
            return sb.ToString();
        }

        void ListTournaments(ArenaClient client)
        {
            client.ListTournaments(TournamentStatus.All, 0, 50, r =>
            {
                if (!r.Ok)
                {
                    Log("tournaments failed: " + r.Error);
                    return;
                }
                TournamentSummary chosen = null;
                TournamentSummary[] list = r.Value.tournaments;
                for (int i = 0; i < list.Length && chosen == null; i++)
                {
                    if (tournament != null && list[i].tournament_id == tournament.tournament_id)
                    {
                        chosen = list[i];
                    }
                }
                for (int i = 0; i < list.Length && chosen == null; i++)
                {
                    if (list[i].joined && list[i].status == TournamentStatus.Open)
                    {
                        chosen = list[i];
                    }
                }
                for (int i = 0; i < list.Length && chosen == null; i++)
                {
                    if (list[i].eligible)
                    {
                        chosen = list[i];
                    }
                }
                tournament = chosen;
                stateDirty = true;
                Log(chosen == null ? "no tournament to play" : "selected " + chosen.tournament_id);
                // A cold start has no view: read the round in play from the
                // server rather than wait for it to expire.
                if (chosen != null && chosen.joined && chosen.round_in_play > 0 &&
                    (view == null || view.tournament_id != chosen.tournament_id))
                {
                    ReadRound(client, chosen.tournament_id, chosen.round_in_play);
                }
            });
        }

        int NextRound()
        {
            if (tournament == null || !tournament.joined)
            {
                return 0;
            }
            if (view != null && view.tournament_id == tournament.tournament_id)
            {
                if (view.status == RoundStatus.Playing)
                {
                    return 0;
                }
                return view.round < 3 ? view.round + 1 : 0;
            }
            // No view of this tournament yet: a round in play is read, not dealt.
            return tournament.round_in_play > 0 ? 0 : tournament.next_round;
        }

        void OnRound(ArenaClient client, string tournamentId, int round, ArenaResult<RoundResponse> result)
        {
            if (!result.Ok)
            {
                Log("round intent failed: " + result.Error);
                // The view may be stale (move_index_mismatch, round_expired,
                // a resynchronisation): read the round the intent was for.
                // A failed deal has no round to read (round_not_started).
                ReadRound(client, tournamentId, round);
                return;
            }
            ShowRound(client, result.Value.round);
        }

        void ReadRound(ArenaClient client, string tournamentId, int round)
        {
            if (string.IsNullOrEmpty(tournamentId) || round < 1 || round > 3)
            {
                return;
            }
            client.GetRound(tournamentId, round, r =>
            {
                if (r.Ok)
                {
                    ShowRound(client, r.Value.round);
                }
                else
                {
                    Log("reading the round failed: " + r.Error);
                }
            });
        }

        void ShowRound(ArenaClient client, RoundView next)
        {
            view = next;
            stateDirty = true;
            if (view.status == RoundStatus.Finished)
            {
                CheckCommitment(client);
            }
        }

        void CheckCommitment(ArenaClient client)
        {
            RoundAudit audit = client.FindAudit(view.tournament_id, view.round);
            string expected = audit != null ? audit.commitment : view.commitment;
            bool matches = view.seed.Length == 64 && Commitment(view.seed) == expected;
            auditLine = "round " + view.round + ": the revealed seed " + (matches ? "matches" : "does not match") +
                        " the commitment " + (audit != null ? "received with the deal" : "in the view");
            if (audit != null && matches)
            {
                client.RemoveAudit(view.tournament_id, view.round);
            }
        }

        void OnEvent(ArenaClient client, EventItem item)
        {
            eventLines.Add(item.slot + " " + item.type + " " + item.tournament_id +
                           (item.round > 0 ? " round " + item.round : "") +
                           (item.status.Length > 0 ? " " + item.status : "") +
                           (item.total_score > 0 ? " total " + item.total_score : "") +
                           (item.amount > 0 ? " amount " + item.amount : ""));
            Trim(eventLines);
            stateDirty = true;
            if (tournament == null || item.tournament_id != tournament.tournament_id)
            {
                return;
            }
            if (item.type == ArenaEventType.LeaderboardChanged || item.type == ArenaEventType.TournamentStatus)
            {
                RefreshLeaderboard(client);
            }
            if (item.type == ArenaEventType.TournamentStatus)
            {
                ListTournaments(client);
            }
            if (item.type == ArenaEventType.RoundFinished && view != null && view.round == item.round &&
                view.status == RoundStatus.Playing)
            {
                ReadRound(client, item.tournament_id, item.round);
            }
            if (item.type == ArenaEventType.RoundStarted && (view == null || view.round < item.round))
            {
                // Dealt before a restart, or from another session of the player.
                ReadRound(client, item.tournament_id, item.round);
            }
        }

        /// <summary>On Resumed and Resynced: the list (which reads a round in play when there is no view), the round shown, the leaderboard.</summary>
        void Refresh(ArenaClient client)
        {
            ListTournaments(client);
            if (view != null)
            {
                ReadRound(client, view.tournament_id, view.round);
            }
            RefreshLeaderboard(client);
        }

        void RefreshLeaderboard(ArenaClient client)
        {
            if (tournament == null)
            {
                return;
            }
            client.GetLeaderboard(tournament.tournament_id, 0, 10, r =>
            {
                if (r.Ok)
                {
                    leaderboard = r.Value;
                    stateDirty = true;
                }
                else
                {
                    Log("leaderboard failed: " + r.Error);
                }
            });
        }

        void Log(string line)
        {
            logLines.Add(line);
            Trim(logLines);
            stateDirty = true;
        }

        static void Trim(List<string> lines)
        {
            while (lines.Count > MaxLines)
            {
                lines.RemoveAt(0);
            }
        }

        /// <summary>SHA-256("paxos-arena/commit/v1" || 0x00 || seed) as lower-case hex (section 5.3).</summary>
        static string Commitment(string seedHex)
        {
            byte[] prefix = Encoding.ASCII.GetBytes("paxos-arena/commit/v1");
            byte[] input = new byte[prefix.Length + 1 + seedHex.Length / 2];
            Array.Copy(prefix, input, prefix.Length);
            for (int i = 0; i < seedHex.Length / 2; i++)
            {
                input[prefix.Length + 1 + i] = byte.Parse(seedHex.Substring(2 * i, 2), NumberStyles.HexNumber, CultureInfo.InvariantCulture);
            }
            using (SHA256 sha = SHA256.Create())
            {
                byte[] digest = sha.ComputeHash(input);
                StringBuilder sb = new StringBuilder(64);
                for (int i = 0; i < digest.Length; i++)
                {
                    sb.Append(digest[i].ToString("x2", CultureInfo.InvariantCulture));
                }
                return sb.ToString();
            }
        }
    }
}
