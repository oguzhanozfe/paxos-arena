using System;
using System.Globalization;

namespace PaxosArena.Client
{
    /// <summary>
    /// Full-jitter exponential backoff with Retry-After as a floor (section 9.3):
    /// <code>
    /// delay_ms = uniform random in [0, min(cap, base * 2^(failures - 1))]
    /// delay_ms = max(delay_ms, 1000 * Retry-After)   when the header is present
    /// </code>
    /// </summary>
    public sealed class Backoff
    {
        readonly int baseMs;
        readonly int capMs;
        readonly Func<double> random;

        /// <param name="random">Returns a value in [0, 1); null uses System.Random.</param>
        public Backoff(int baseMs, int capMs, Func<double> random)
        {
            this.baseMs = baseMs < 1 ? 1 : baseMs;
            this.capMs = capMs < this.baseMs ? this.baseMs : capMs;
            if (random == null)
            {
                Random r = new Random();
                random = r.NextDouble;
            }
            this.random = random;
        }

        /// <summary>The upper bound of the delay after the given number of consecutive failures.</summary>
        public long CeilingMs(int failures)
        {
            if (failures <= 0)
            {
                return 0;
            }
            long ceiling = baseMs;
            for (int i = 1; i < failures && ceiling < capMs; i++)
            {
                ceiling *= 2;
            }
            return ceiling < capMs ? ceiling : capMs;
        }

        /// <summary>A delay for the given failure count; retryAfterSeconds of 0 or below means no header.</summary>
        public long DelayMs(int failures, long retryAfterSeconds)
        {
            long ceiling = CeilingMs(failures);
            double u = random();
            if (u < 0 || double.IsNaN(u))
            {
                u = 0;
            }
            long delay = (long)Math.Floor(u * (ceiling + 1));
            if (delay > ceiling)
            {
                delay = ceiling;
            }
            if (retryAfterSeconds > 0)
            {
                long floor = retryAfterSeconds > long.MaxValue / 1000 ? long.MaxValue : retryAfterSeconds * 1000;
                if (delay < floor)
                {
                    delay = floor;
                }
            }
            return delay;
        }

        /// <summary>Reads a Retry-After header in whole seconds; 0 when absent or not a number of seconds.</summary>
        public static long ParseRetryAfter(string value)
        {
            long seconds;
            if (!string.IsNullOrEmpty(value) &&
                long.TryParse(value.Trim(), NumberStyles.None, CultureInfo.InvariantCulture, out seconds))
            {
                return seconds;
            }
            return 0;
        }
    }
}
