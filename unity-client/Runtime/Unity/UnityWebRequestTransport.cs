using System;
using System.Collections.Generic;
using System.Text;
using UnityEngine.Networking;

namespace PaxosArena.Client.Unity
{
    /// <summary>
    /// <see cref="IHttpTransport"/> over UnityWebRequest (section 11.3). Redirects
    /// are never followed (redirectLimit = 0), so the client sees a 307 and
    /// resends to the leader itself. Completion is observed through the
    /// request's AsyncOperation.completed event, which Unity raises on the
    /// main thread, the thread that pumps <see cref="ArenaClient.Update"/>.
    /// </summary>
    public sealed class UnityWebRequestTransport : IHttpTransport
    {
        readonly List<UnityWebRequest> inFlight = new List<UnityWebRequest>();

        public void Send(HttpRequest request, Action<HttpResponse> done)
        {
            if (request == null)
            {
                throw new ArgumentNullException(nameof(request));
            }
            if (done == null)
            {
                throw new ArgumentNullException(nameof(done));
            }
            UnityWebRequest web = null;
            UnityWebRequestAsyncOperation operation;
            try
            {
                web = new UnityWebRequest(request.Url, request.Method);
                web.downloadHandler = new DownloadHandlerBuffer();
                if (request.Method != "GET")
                {
                    UploadHandlerRaw upload = new UploadHandlerRaw(Encoding.UTF8.GetBytes(request.Body ?? ""));
                    upload.contentType = "application/json";
                    web.uploadHandler = upload;
                }
                HttpHeader[] headers = request.Headers ?? new HttpHeader[0];
                for (int i = 0; i < headers.Length; i++)
                {
                    HttpHeader h = headers[i];
                    // The upload handler sets Content-Type.
                    if (h == null || string.Equals(h.Name, Headers.ContentType, StringComparison.OrdinalIgnoreCase))
                    {
                        continue;
                    }
                    web.SetRequestHeader(h.Name, h.Value);
                }
                web.timeout = request.TimeoutMs <= 0 ? 0 : (request.TimeoutMs + 999) / 1000;
                web.redirectLimit = 0;
                operation = web.SendWebRequest();
            }
            catch (Exception e)
            {
                // Refused before sending, for example an insecure URL the
                // player settings do not allow.
                if (web != null)
                {
                    web.Dispose();
                }
                done(new HttpResponse { TransportError = e.Message });
                return;
            }
            inFlight.Add(web);
            UnityWebRequest sent = web;
            operation.completed += _ => Complete(sent, done);
        }

        public void CancelAll()
        {
            if (inFlight.Count == 0)
            {
                return;
            }
            // Abort completes each request; its completed handler still runs
            // and calls done once with Status 0.
            UnityWebRequest[] snapshot = inFlight.ToArray();
            for (int i = 0; i < snapshot.Length; i++)
            {
                snapshot[i].Abort();
            }
        }

        void Complete(UnityWebRequest web, Action<HttpResponse> done)
        {
            if (!inFlight.Remove(web))
            {
                return;
            }
            HttpResponse response = new HttpResponse();
            try
            {
                long code = web.responseCode;
                if (code != 0)
                {
                    // Protocol errors (4xx, 5xx, a 307 with redirectLimit 0)
                    // still carry the status, headers and body the client needs.
                    response.Status = (int)code;
                    response.Headers = CopyHeaders(web.GetResponseHeaders());
                    response.Body = web.downloadHandler != null ? web.downloadHandler.text ?? "" : "";
                }
                else
                {
                    string error = web.error ?? "";
                    response.TransportError = error.Length > 0 ? error : "no response";
                    response.TimedOut = error.IndexOf("timeout", StringComparison.OrdinalIgnoreCase) >= 0 ||
                                        error.IndexOf("timed out", StringComparison.OrdinalIgnoreCase) >= 0;
                }
            }
            catch (Exception e)
            {
                response = new HttpResponse { TransportError = "reading the response failed: " + e.Message };
            }
            finally
            {
                web.Dispose();
            }
            done(response);
        }

        static HttpHeader[] CopyHeaders(Dictionary<string, string> headers)
        {
            if (headers == null || headers.Count == 0)
            {
                return new HttpHeader[0];
            }
            HttpHeader[] result = new HttpHeader[headers.Count];
            int i = 0;
            foreach (KeyValuePair<string, string> pair in headers)
            {
                result[i++] = new HttpHeader(pair.Key, pair.Value);
            }
            return result;
        }
    }
}
