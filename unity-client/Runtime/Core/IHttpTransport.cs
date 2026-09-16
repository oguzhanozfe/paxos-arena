using System;

namespace PaxosArena.Client
{
    /// <summary>One HTTP header.</summary>
    public sealed class HttpHeader
    {
        public string Name = "";
        public string Value = "";

        public HttpHeader()
        {
        }

        public HttpHeader(string name, string value)
        {
            Name = name ?? "";
            Value = value ?? "";
        }
    }

    /// <summary>A request the client asks a transport to send.</summary>
    public sealed class HttpRequest
    {
        public string Method = "GET";

        /// <summary>Absolute URL, query included.</summary>
        public string Url = "";

        /// <summary>UTF-8 JSON body; empty for GET.</summary>
        public string Body = "";

        public HttpHeader[] Headers = new HttpHeader[0];

        /// <summary>Whole-request timeout in milliseconds; 0 means none.</summary>
        public int TimeoutMs;

        /// <summary>Returns the value of a request header, or "" when absent.</summary>
        public string Header(string name)
        {
            return HeaderLookup.Find(Headers, name);
        }
    }

    /// <summary>What a transport observed for one request.</summary>
    public sealed class HttpResponse
    {
        /// <summary>The HTTP status, or 0 when no HTTP response arrived.</summary>
        public int Status;

        public string Body = "";
        public HttpHeader[] Headers = new HttpHeader[0];

        /// <summary>True when the request ended because its timeout elapsed.</summary>
        public bool TimedOut;

        /// <summary>Why no response arrived; set when Status is 0.</summary>
        public string TransportError = "";

        /// <summary>Case-insensitive header lookup; "" when absent.</summary>
        public string Header(string name)
        {
            return HeaderLookup.Find(Headers, name);
        }
    }

    /// <summary>
    /// Sends one request at a time on behalf of the client. An implementation
    /// must not follow redirects, and must call <c>done</c> exactly once per
    /// request, on the thread that calls <see cref="ArenaClient.Update"/>,
    /// including for requests ended by <see cref="CancelAll"/>.
    /// </summary>
    public interface IHttpTransport
    {
        void Send(HttpRequest request, Action<HttpResponse> done);

        /// <summary>Aborts every request in flight. Their callbacks still run once.</summary>
        void CancelAll();
    }

    internal static class HeaderLookup
    {
        internal static string Find(HttpHeader[] headers, string name)
        {
            if (headers == null || name == null)
            {
                return "";
            }
            for (int i = 0; i < headers.Length; i++)
            {
                HttpHeader h = headers[i];
                if (h != null && string.Equals(h.Name, name, StringComparison.OrdinalIgnoreCase))
                {
                    return h.Value ?? "";
                }
            }
            return "";
        }
    }
}
