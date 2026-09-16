using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;

namespace PaxosArena.Client.Harness
{
    /// <summary>
    /// The client over real HTTP on the loopback interface: HttpClientTransport
    /// against two local listeners, one answering 307 to the other. It checks
    /// what the fake transport cannot: that redirects are not followed by the
    /// HTTP stack, that headers and status survive the transport, that the
    /// followed request carries the same key, body and token, and that a
    /// request timeout arrives as TimedOut.
    /// </summary>
    public static class HttpTransportTests
    {
        public static void RoundTrip()
        {
            int portA = FreePort();
            int portB = FreePort();
            string originA = "http://127.0.0.1:" + portA;
            string originB = "http://127.0.0.1:" + portB;
            SystemTextJson json = new SystemTextJson();
            ConcurrentQueue<string> seenA = new ConcurrentQueue<string>();
            ConcurrentQueue<string> seenB = new ConcurrentQueue<string>();

            using (HttpListener follower = Listen(portA))
            using (HttpListener leader = Listen(portB))
            using (HttpClientTransport transport = new HttpClientTransport())
            {
                Serve(follower, (request, body, response) =>
                {
                    seenA.Enqueue(request.HttpMethod + " " + request.Url.PathAndQuery + " key=" + request.Headers["Idempotency-Key"]);
                    response.StatusCode = 307;
                    response.Headers["Location"] = originB + request.Url.PathAndQuery;
                    response.Headers["X-Arena-Leader"] = originB;
                    return "{\"code\":\"not_leader\",\"message\":\"node 2 leads\",\"retryable\":true}";
                });
                Serve(leader, (request, body, response) =>
                {
                    string path = request.Url.AbsolutePath;
                    seenB.Enqueue(request.HttpMethod + " " + path + " key=" + request.Headers["Idempotency-Key"] +
                                  " auth=" + request.Headers["Authorization"] + " type=" + request.ContentType + " body=" + body);
                    response.Headers["X-Arena-Server-Time-Ms"] = "1789200000000";
                    response.Headers["X-Arena-Leader"] = originB;
                    if (path == "/slow")
                    {
                        Thread.Sleep(1500);
                        return "{}";
                    }
                    if (path == "/v1/session")
                    {
                        response.StatusCode = 200;
                        return json.ToJson(new SessionResponse
                        {
                            slot = 3,
                            next_seq = 1,
                            player_id = "p-mzxw6ytboi4dqnbrgm2wqzlmn4",
                            new_player = true,
                            session_token = "v1.k1.payload.mac",
                            issued_at_ms = 1789200000000,
                            expires_at_ms = 1789203600000,
                            jurisdiction = "TR",
                            age = 30,
                        });
                    }
                    if (path == "/v1/tournaments/t1/join")
                    {
                        response.StatusCode = 201;
                        response.Headers["X-Arena-Slot"] = "40";
                        response.Headers["X-Arena-Next-Seq"] = "2";
                        return json.ToJson(new JoinResponse { slot = 40, next_seq = 2, tournament_id = "t1", join_seq = 1, entry_fee = 500, rounds = 3 });
                    }
                    response.StatusCode = 404;
                    return "{\"code\":\"not_found\",\"message\":\"no route\",\"retryable\":false}";
                });

                MemoryStore store = new MemoryStore(json);
                Stopwatch clock = Stopwatch.StartNew();
                ArenaClient client = new ArenaClient(new ArenaClientOptions
                {
                    BaseUrls = new[] { originA },
                    Jurisdiction = "TR",
                    Age = 30,
                    FollowEvents = false,
                }, transport, store, json, () => clock.ElapsedMilliseconds);

                ArenaResult<JoinResponse> joined = null;
                client.Join("t1", r => joined = r);
                long deadline = clock.ElapsedMilliseconds + 10000;
                while (joined == null && clock.ElapsedMilliseconds < deadline)
                {
                    transport.Dispatch();
                    client.Update();
                    Thread.Sleep(1);
                }
                Expect(joined != null, "join answered over HTTP");
                Expect(joined.Ok && joined.Value.join_seq == 1 && joined.Slot == 40, "join result: " + (joined.Error != null ? joined.Error.ToString() : "ok"));
                ClientState state = json.FromJson<ClientState>(store.Saved);
                Expect(state.leader_url == originB, "leader cached as " + state.leader_url);
                Expect(state.last_intent_slot == 40, "slot header read");
                Expect(client.ServerNowMs > 1789200000000 - 1000, "server time header read");

                string[] a = seenA.ToArray();
                string[] b = seenB.ToArray();
                Expect(a.Length == 1 && a[0].StartsWith("POST /v1/session key=", StringComparison.Ordinal),
                    "the follower saw only the first session request: " + string.Join(" | ", a));
                Expect(b.Length == 2, "the leader saw the followed session and the join: " + string.Join(" | ", b));
                string sessionKey = a[0].Substring("POST /v1/session key=".Length);
                Expect(b[0].StartsWith("POST /v1/session key=" + sessionKey + " auth= type=application/json body={\"device_id\":", StringComparison.Ordinal),
                    "the followed session request is identical: " + b[0]);
                Expect(b[1].StartsWith("POST /v1/tournaments/t1/join key=", StringComparison.Ordinal) &&
                       b[1].EndsWith(" auth=Bearer v1.k1.payload.mac type=application/json body={\"seq\":1}", StringComparison.Ordinal),
                    "the join went straight to the cached leader: " + b[1]);

                HttpResponse slow = null;
                transport.Send(new HttpRequest { Method = "GET", Url = originB + "/slow", TimeoutMs = 200 }, r => slow = r);
                deadline = clock.ElapsedMilliseconds + 5000;
                while (slow == null && clock.ElapsedMilliseconds < deadline)
                {
                    transport.Dispatch();
                    Thread.Sleep(1);
                }
                Expect(slow != null && slow.Status == 0 && slow.TimedOut, "a slow request times out");

                HttpResponse cancelled = null;
                transport.Send(new HttpRequest { Method = "GET", Url = originB + "/slow", TimeoutMs = 5000 }, r => cancelled = r);
                Thread.Sleep(50);
                transport.CancelAll();
                deadline = clock.ElapsedMilliseconds + 5000;
                while (cancelled == null && clock.ElapsedMilliseconds < deadline)
                {
                    transport.Dispatch();
                    Thread.Sleep(1);
                }
                Expect(cancelled != null && cancelled.Status == 0 && !cancelled.TimedOut, "a cancelled request calls back once without a status");
                client.Dispose();
                follower.Stop();
                leader.Stop();
            }
        }

        static void Expect(bool condition, string message)
        {
            if (!condition)
            {
                throw new Exception(message);
            }
        }

        static int FreePort()
        {
            TcpListener probe = new TcpListener(IPAddress.Loopback, 0);
            probe.Start();
            int port = ((IPEndPoint)probe.LocalEndpoint).Port;
            probe.Stop();
            return port;
        }

        static HttpListener Listen(int port)
        {
            HttpListener listener = new HttpListener();
            listener.Prefixes.Add("http://127.0.0.1:" + port + "/");
            listener.Start();
            return listener;
        }

        static void Serve(HttpListener listener, Func<HttpListenerRequest, string, HttpListenerResponse, string> handle)
        {
            Thread thread = new Thread(() =>
            {
                while (listener.IsListening)
                {
                    HttpListenerContext context;
                    try
                    {
                        context = listener.GetContext();
                    }
                    catch (Exception)
                    {
                        return;
                    }
                    ThreadPool.QueueUserWorkItem(_ =>
                    {
                        try
                        {
                            string body;
                            using (StreamReader reader = new StreamReader(context.Request.InputStream, Encoding.UTF8))
                            {
                                body = reader.ReadToEnd();
                            }
                            string reply = handle(context.Request, body, context.Response);
                            byte[] bytes = Encoding.UTF8.GetBytes(reply);
                            context.Response.ContentType = "application/json";
                            context.Response.ContentLength64 = bytes.Length;
                            context.Response.OutputStream.Write(bytes, 0, bytes.Length);
                            context.Response.OutputStream.Close();
                        }
                        catch (Exception)
                        {
                            // The client went away (a timeout or a cancellation).
                        }
                    });
                }
            });
            thread.IsBackground = true;
            thread.Start();
        }
    }
}
