using System;
using System.Globalization;

namespace PaxosArena.Client
{
    /// <summary>
    /// Follows GET /v1/events with a long-poll (section 7.3.10). The cursor is a
    /// slot, the same on every replica, and is stored with the client state. It
    /// advances only after every event of a response was handed to
    /// <see cref="Received"/>, so a handler that throws sees those events again,
    /// after a backoff.
    /// The poll runs while the client has a session, is not paused and
    /// <see cref="ArenaClientOptions.FollowEvents"/> is set; it is independent of
    /// the intent queue. Until a response has given the client a server clock
    /// estimate (at start and after a resume) a poll asks with wait_ms=0, so it
    /// returns at once.
    /// </summary>
    public sealed class EventFollower
    {
        readonly ArenaClient client;
        Call call;
        string sentFilter = "";
        string tournamentId = "";
        long notBefore;
        int failures;

        internal EventFollower(ArenaClient client)
        {
            this.client = client;
        }

        /// <summary>The slot after which the next poll asks for events.</summary>
        public long Cursor
        {
            get { return client.State.events_cursor; }
        }

        /// <summary>
        /// "" for every entered tournament, or one tournament id. A change takes
        /// effect at the next poll; a poll in flight under the old filter is
        /// discarded without advancing the cursor.
        /// </summary>
        public string TournamentId
        {
            get { return tournamentId; }
            set { tournamentId = value ?? ""; }
        }

        /// <summary>The last failure of the poll, cleared by the next successful response.</summary>
        public ArenaError LastError { get; private set; }

        /// <summary>Raised once per event, in slot order, from <see cref="ArenaClient.Update"/>.</summary>
        public event Action<EventItem> Received;

        internal void Tick(long now)
        {
            if (call != null || now < notBefore || !client.CanFollowEvents)
            {
                return;
            }
            call = NewPoll();
        }

        // A method of its own: a lambda capturing the call in Tick would
        // allocate its closure on every Update, before the early return.
        Call NewPoll()
        {
            int wait = client.HasClockEstimate ? client.Options.EventsWaitMs : 0;
            if (wait < 0)
            {
                wait = 0;
            }
            if (wait > 25000)
            {
                wait = 25000;
            }
            sentFilter = tournamentId;
            string path = "/v1/events?cursor=" + Cursor.ToString(CultureInfo.InvariantCulture) +
                          "&wait_ms=" + wait.ToString(CultureInfo.InvariantCulture);
            if (sentFilter.Length > 0)
            {
                path += "&tournament_id=" + Uri.EscapeDataString(sentFilter);
            }
            Call c = client.NewCall(CallKind.Events, "GET", path, "", "", true, wait + 10000);
            c.LongPoll = wait > 0;
            c.Answered = (response, error) => Answered(c, response, error);
            c.Abandoned = error => Abandoned(c, error);
            return c;
        }

        /// <summary>Forgets the poll after the client dropped every call (a halt or a new player).</summary>
        internal void Detach()
        {
            call = null;
            notBefore = 0;
            failures = 0;
        }

        /// <summary>Lets the next poll start at once, as on resume.</summary>
        internal void ResetBackoff()
        {
            notBefore = 0;
            failures = 0;
        }

        void Answered(Call c, HttpResponse response, ErrorBody error)
        {
            client.RemoveCall(c);
            if (call != c)
            {
                return;
            }
            call = null;
            long now = client.MonotonicNow();
            if (response.Status >= 200 && response.Status < 300)
            {
                if (sentFilter != tournamentId)
                {
                    notBefore = 0;
                    return;
                }
                EventsResponse body = client.Parse<EventsResponse>(response.Body);
                if (body == null)
                {
                    Failed(new ArenaError(response.Status, LocalErrorCodes.BadResponse,
                        "the events response could not be read", true), 0, now);
                    return;
                }
                failures = 0;
                notBefore = 0;
                LastError = null;
                EventItem[] items = body.events ?? new EventItem[0];
                Action<EventItem> handler = Received;
                for (int i = 0; i < items.Length && handler != null; i++)
                {
                    // A handler that throws stops the delivery; the cursor stays,
                    // so the next poll hands these events over again. The
                    // exception leaves ArenaClient.Update at its end.
                    if (items[i] != null && !client.Invoke(handler, items[i]))
                    {
                        failures++;
                        notBefore = now + client.BackoffPolicy.DelayMs(failures, 0);
                        return;
                    }
                }
                client.AdvanceEventsCursor(body.cursor);
                return;
            }
            Failed(client.ErrorFrom(response, error), Backoff.ParseRetryAfter(response.Header(Headers.RetryAfter)), now);
        }

        void Abandoned(Call c, ArenaError error)
        {
            if (call == c)
            {
                call = null;
                LastError = error;
            }
        }

        void Failed(ArenaError error, long retryAfterSeconds, long now)
        {
            failures++;
            LastError = error;
            notBefore = now + client.BackoffPolicy.DelayMs(failures, retryAfterSeconds);
        }
    }
}
