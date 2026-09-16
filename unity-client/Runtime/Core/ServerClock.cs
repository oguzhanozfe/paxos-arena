namespace PaxosArena.Client
{
    /// <summary>
    /// Estimates the server's clock from X-Arena-Server-Time-Ms (section 9.4).
    /// Every response gives the sample header + rtt / 2 - local receive time,
    /// with both local values from a monotonic clock that does not jump when
    /// the user changes the device time. The estimate uses the sample with the
    /// smallest round trip among the last eight. The device's wall clock is
    /// never read.
    /// </summary>
    public sealed class ServerClock
    {
        public const int Window = 8;

        readonly long[] offsets = new long[Window];
        readonly long[] rtts = new long[Window];
        int count;
        int next;

        /// <summary>True once a sample exists.</summary>
        public bool HasEstimate
        {
            get { return count > 0; }
        }

        /// <summary>Adds one sample; a header of 0 or below is ignored.</summary>
        public void AddSample(long serverTimeMs, long rttMs, long localAtReceiveMs)
        {
            if (serverTimeMs <= 0)
            {
                return;
            }
            if (rttMs < 0)
            {
                rttMs = 0;
            }
            offsets[next] = serverTimeMs + rttMs / 2 - localAtReceiveMs;
            rtts[next] = rttMs;
            next = (next + 1) % Window;
            if (count < Window)
            {
                count++;
            }
        }

        /// <summary>Server time minus monotonic time, from the best sample; 0 without samples.</summary>
        public long OffsetMs
        {
            get
            {
                if (count == 0)
                {
                    return 0;
                }
                int oldest = count < Window ? 0 : next;
                int best = oldest;
                for (int i = 1; i < count; i++)
                {
                    int k = (oldest + i) % Window;
                    // Equal round trips prefer the newer sample.
                    if (rtts[k] <= rtts[best])
                    {
                        best = k;
                    }
                }
                return offsets[best];
            }
        }

        /// <summary>The estimated server time at monotonic time localNowMs; 0 without samples.</summary>
        public long NowMs(long localNowMs)
        {
            return count == 0 ? 0 : localNowMs + OffsetMs;
        }

        /// <summary>Forgets every sample, as on resume, when the monotonic clock may not have advanced.</summary>
        public void Clear()
        {
            count = 0;
            next = 0;
        }
    }
}
