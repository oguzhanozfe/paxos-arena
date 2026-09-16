using System;
using System.Collections.Generic;

namespace PaxosArena.Client
{
    /// <summary>
    /// The write-ahead queue of intents (section 9.2.1). An intent gets its key
    /// and sequence number when it is created and is saved to the store before
    /// anything is sent; the client sends only the head, resends it byte for
    /// byte, and removes it only after a definitive answer. Callbacks live in
    /// memory and die with the process; the stored intents do not.
    /// </summary>
    internal sealed class IntentQueue
    {
        readonly ClientState state;
        readonly Action save;
        readonly int capacity;
        readonly Dictionary<string, Action<IntentOutcome>> callbacks =
            new Dictionary<string, Action<IntentOutcome>>(StringComparer.Ordinal);

        internal IntentQueue(ClientState state, Action save, int capacity)
        {
            this.state = state;
            this.save = save;
            this.capacity = capacity < 1 ? 1 : capacity;
        }

        public int Count
        {
            get { return state.pending.Length; }
        }

        /// <summary>The intent to send next, or null.</summary>
        public PendingIntent Head
        {
            get { return state.pending.Length == 0 ? null : state.pending[0]; }
        }

        /// <summary>
        /// Assigns seq = last_assigned_seq + 1, builds the body, appends the
        /// intent and saves the state. With the queue full it assigns nothing
        /// and returns null with a queue_full error. If saving throws, the
        /// state is restored and the exception propagates.
        /// </summary>
        public PendingIntent Create(string route, string path, Func<long, string> body, string key,
                                    long createdAtMs, Action<IntentOutcome> callback, out ArenaError error)
        {
            error = null;
            PendingIntent[] before = state.pending;
            if (before.Length >= capacity)
            {
                error = new ArenaError(0, LocalErrorCodes.QueueFull,
                    before.Length + " intents are waiting for an answer", false);
                return null;
            }
            long previousSeq = state.last_assigned_seq;
            long seq = previousSeq + 1;
            PendingIntent intent = new PendingIntent
            {
                idempotency_key = key,
                seq = seq,
                method = "POST",
                path = path,
                body = body(seq),
                route = route,
                created_at_ms = createdAtMs,
            };
            PendingIntent[] after = new PendingIntent[before.Length + 1];
            Array.Copy(before, after, before.Length);
            after[before.Length] = intent;
            state.pending = after;
            state.last_assigned_seq = seq;
            try
            {
                save();
            }
            catch
            {
                state.pending = before;
                state.last_assigned_seq = previousSeq;
                throw;
            }
            if (callback != null)
            {
                callbacks[key] = callback;
            }
            return intent;
        }

        /// <summary>Removes the head from memory; the caller saves.</summary>
        public void RemoveHead()
        {
            PendingIntent[] before = state.pending;
            if (before.Length == 0)
            {
                return;
            }
            PendingIntent[] after = new PendingIntent[before.Length - 1];
            Array.Copy(before, 1, after, 0, after.Length);
            state.pending = after;
        }

        /// <summary>Removes every intent from memory and returns them in order; the caller saves.</summary>
        public PendingIntent[] RemoveAll()
        {
            PendingIntent[] all = state.pending;
            state.pending = new PendingIntent[0];
            return all;
        }

        /// <summary>Returns and forgets the callback of an intent, or null when it has none in this process.</summary>
        public Action<IntentOutcome> TakeCallback(string key)
        {
            Action<IntentOutcome> callback;
            if (key != null && callbacks.TryGetValue(key, out callback))
            {
                callbacks.Remove(key);
                return callback;
            }
            return null;
        }
    }
}
