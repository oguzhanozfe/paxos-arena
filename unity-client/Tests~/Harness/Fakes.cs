using System;
using System.Collections.Generic;

namespace PaxosArena.Client.Harness
{
    /// <summary>A transport that records requests and answers only when a test tells it to.</summary>
    public sealed class FakeTransport : IHttpTransport
    {
        public sealed class Exchange
        {
            public HttpRequest Request;
            public Action<HttpResponse> Done;
            public bool Answered;

            public string Path
            {
                get { return new Uri(Request.Url).PathAndQuery; }
            }

            public string Origin
            {
                get { return new Uri(Request.Url).GetLeftPart(UriPartial.Authority); }
            }
        }

        public readonly List<Exchange> Sent = new List<Exchange>();
        public int CancelCalls;

        /// <summary>Runs inside Send, before the request is recorded.</summary>
        public Action<HttpRequest> OnSend;

        public void Send(HttpRequest request, Action<HttpResponse> done)
        {
            if (OnSend != null)
            {
                OnSend(request);
            }
            Sent.Add(new Exchange { Request = request, Done = done });
        }

        public void CancelAll()
        {
            CancelCalls++;
            foreach (Exchange e in Sent.ToArray())
            {
                if (!e.Answered)
                {
                    e.Answered = true;
                    e.Done(new HttpResponse { TransportError = "cancelled" });
                }
            }
        }

        public List<Exchange> Open()
        {
            List<Exchange> open = new List<Exchange>();
            foreach (Exchange e in Sent)
            {
                if (!e.Answered)
                {
                    open.Add(e);
                }
            }
            return open;
        }

        public void Answer(Exchange e, HttpResponse response)
        {
            if (e.Answered)
            {
                throw new InvalidOperationException("exchange answered twice: " + e.Request.Url);
            }
            e.Answered = true;
            e.Done(response);
        }
    }

    /// <summary>A store that keeps the serialized state in memory, as a file would.</summary>
    public sealed class MemoryStore : IIntentStore
    {
        readonly IJson json;
        public string Saved = "";
        public int Saves;

        /// <summary>The next saves that throw, as a full disk would.</summary>
        public int FailSaves;

        public MemoryStore(IJson json)
        {
            this.json = json;
        }

        public ClientState Load()
        {
            return Saved.Length == 0 ? new ClientState() : json.FromJson<ClientState>(Saved);
        }

        public void Save(ClientState state)
        {
            if (FailSaves > 0)
            {
                FailSaves--;
                throw new System.IO.IOException("No space left on device");
            }
            Saved = json.ToJson(state);
            Saves++;
        }
    }
}
