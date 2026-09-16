using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace PaxosArena.Client.Harness
{
    /// <summary>One request the transport sent and what came back, for the harness's assertions.</summary>
    public sealed class Exchange
    {
        public string Method = "";
        public string Url = "";

        /// <summary>The request's Idempotency-Key, or "".</summary>
        public string Key = "";

        /// <summary>The HTTP status, or 0 when no response arrived.</summary>
        public int Status;

        public string Body = "";
        public HttpHeader[] Headers = new HttpHeader[0];

        public string Header(string name)
        {
            foreach (HttpHeader h in Headers)
            {
                if (string.Equals(h.Name, name, StringComparison.OrdinalIgnoreCase))
                {
                    return h.Value;
                }
            }
            return "";
        }
    }

    /// <summary>
    /// IHttpTransport over HttpClient, used only by the harness. Redirects are
    /// disabled. Requests run on the thread pool; their callbacks are queued and
    /// run by <see cref="Dispatch"/>, which the harness calls on the thread that
    /// calls ArenaClient.Update, as the interface requires. Every exchange is
    /// also recorded as soon as it ends, dispatched or not.
    /// </summary>
    public sealed class HttpClientTransport : IHttpTransport, IDisposable
    {
        readonly HttpClient http;
        readonly ConcurrentQueue<Action> ready = new ConcurrentQueue<Action>();
        readonly ConcurrentQueue<Exchange> exchanges = new ConcurrentQueue<Exchange>();
        CancellationTokenSource cancel = new CancellationTokenSource();
        int inFlight;

        public HttpClientTransport()
        {
            SocketsHttpHandler handler = new SocketsHttpHandler
            {
                AllowAutoRedirect = false,
                UseCookies = false,
                PooledConnectionLifetime = TimeSpan.FromMinutes(1),
            };
            http = new HttpClient(handler) { Timeout = Timeout.InfiniteTimeSpan };
        }

        public int InFlight
        {
            get { return Volatile.Read(ref inFlight); }
        }

        /// <summary>The exchanges that ended so far, in the order they ended.</summary>
        public Exchange[] Exchanges
        {
            get { return exchanges.ToArray(); }
        }

        public void Send(HttpRequest request, Action<HttpResponse> done)
        {
            Interlocked.Increment(ref inFlight);
            CancellationToken token = cancel.Token;
            _ = Task.Run(async () =>
            {
                HttpResponse response = await Run(request, token).ConfigureAwait(false);
                exchanges.Enqueue(new Exchange
                {
                    Method = request.Method,
                    Url = request.Url,
                    Key = request.Header(Headers.IdempotencyKey),
                    Status = response.Status,
                    Body = response.Body,
                    Headers = response.Headers,
                });
                Interlocked.Decrement(ref inFlight);
                ready.Enqueue(() => done(response));
            });
        }

        public void CancelAll()
        {
            CancellationTokenSource old = cancel;
            cancel = new CancellationTokenSource();
            old.Cancel();
        }

        /// <summary>Runs the queued callbacks on the calling thread; returns how many ran.</summary>
        public int Dispatch()
        {
            int n = 0;
            Action action;
            while (ready.TryDequeue(out action))
            {
                action();
                n++;
            }
            return n;
        }

        public void Dispose()
        {
            cancel.Cancel();
            http.Dispose();
        }

        async Task<HttpResponse> Run(HttpRequest request, CancellationToken cancelled)
        {
            using (CancellationTokenSource timeout = CancellationTokenSource.CreateLinkedTokenSource(cancelled))
            {
                if (request.TimeoutMs > 0)
                {
                    timeout.CancelAfter(request.TimeoutMs);
                }
                try
                {
                    using (HttpRequestMessage message = new HttpRequestMessage(new HttpMethod(request.Method), request.Url))
                    {
                        if (request.Method != "GET")
                        {
                            ByteArrayContent content = new ByteArrayContent(Encoding.UTF8.GetBytes(request.Body ?? ""));
                            content.Headers.ContentType = new MediaTypeHeaderValue("application/json");
                            message.Content = content;
                        }
                        foreach (HttpHeader h in request.Headers)
                        {
                            if (!string.Equals(h.Name, Headers.ContentType, StringComparison.OrdinalIgnoreCase))
                            {
                                message.Headers.TryAddWithoutValidation(h.Name, h.Value);
                            }
                        }
                        using (HttpResponseMessage reply = await http.SendAsync(message, HttpCompletionOption.ResponseContentRead, timeout.Token).ConfigureAwait(false))
                        {
                            List<HttpHeader> headers = new List<HttpHeader>();
                            foreach (KeyValuePair<string, IEnumerable<string>> pair in reply.Headers)
                            {
                                headers.Add(new HttpHeader(pair.Key, string.Join(",", pair.Value)));
                            }
                            foreach (KeyValuePair<string, IEnumerable<string>> pair in reply.Content.Headers)
                            {
                                headers.Add(new HttpHeader(pair.Key, string.Join(",", pair.Value)));
                            }
                            string body = await reply.Content.ReadAsStringAsync(timeout.Token).ConfigureAwait(false);
                            return new HttpResponse
                            {
                                Status = (int)reply.StatusCode,
                                Body = body ?? "",
                                Headers = headers.ToArray(),
                            };
                        }
                    }
                }
                catch (OperationCanceledException) when (!cancelled.IsCancellationRequested)
                {
                    return new HttpResponse { TimedOut = true, TransportError = "timed out after " + request.TimeoutMs + " ms" };
                }
                catch (OperationCanceledException)
                {
                    return new HttpResponse { TransportError = "cancelled" };
                }
                catch (Exception e)
                {
                    return new HttpResponse { TransportError = e.GetType().Name + ": " + e.Message };
                }
            }
        }
    }
}
