using System;
using System.Collections;
using System.Diagnostics;
using UnityEngine;
using Debug = UnityEngine.Debug;

namespace PaxosArena.Client.UnityAdapters
{
    /// <summary>
    /// Hosts an <see cref="ArenaClient"/> in a scene (section 11.3). Awake builds
    /// the client from <see cref="UnityWebRequestTransport"/>,
    /// <see cref="PersistentDataIntentStore"/>, <see cref="JsonUtilityJson"/> and a
    /// Stopwatch; a coroutine calls <see cref="ArenaClient.Update"/> every frame
    /// on the main thread, which drives sends, retries and the events long-poll.
    ///
    /// Pause and resume: OnApplicationPause(true) pauses the client (aborts
    /// requests, persists the store) and OnApplicationPause(false) resumes it
    /// (clears clock samples, resends the head intent at once, restarts the
    /// events poll, raises Resumed). Losing focus flushes the store; regaining
    /// focus resumes a client that is still paused, for platforms that do not
    /// deliver OnApplicationPause(false).
    ///
    /// One behaviour per store: the store refuses a second client on the same
    /// file, and a behaviour that finds the store taken (a duplicated
    /// DontDestroyOnLoad object, a second scene with its own behaviour) logs an
    /// error and builds no client. Keep one instance, for example by destroying
    /// the new object in Awake when one already exists.
    /// </summary>
    [DisallowMultipleComponent]
    public sealed class ArenaClientBehaviour : MonoBehaviour
    {
        [SerializeField]
        [Tooltip("Public play URLs of the replicas, or one balancer URL.")]
        string[] baseUrls = new string[0];

        [SerializeField]
        [Tooltip("2 to 8 upper-case letters; recorded by the first session of the device.")]
        string jurisdiction = "";

        [SerializeField]
        [Tooltip("0 to 150; recorded by the first session of the device.")]
        int age = 0;

        [SerializeField]
        bool followEvents = true;

        Stopwatch stopwatch;
        Coroutine pump;
        PersistentDataIntentStore store;

        /// <summary>The client; null until Awake has run with base URLs, or until <see cref="Initialize"/>.</summary>
        public ArenaClient Client { get; private set; }

        /// <summary>Raised once the client exists, from Awake or <see cref="Initialize"/>.</summary>
        public event Action<ArenaClient> ClientCreated;

        void Awake()
        {
            stopwatch = Stopwatch.StartNew();
            if (baseUrls != null && baseUrls.Length > 0)
            {
                Initialize(new ArenaClientOptions
                {
                    BaseUrls = baseUrls,
                    Jurisdiction = jurisdiction,
                    Age = age,
                    FollowEvents = followEvents,
                });
            }
        }

        /// <summary>Builds the client from code instead of the serialized fields; does nothing if it exists.</summary>
        public void Initialize(ArenaClientOptions options)
        {
            if (Client != null)
            {
                return;
            }
            if (stopwatch == null)
            {
                stopwatch = Stopwatch.StartNew();
            }
            JsonUtilityJson json = new JsonUtilityJson();
            try
            {
                store = new PersistentDataIntentStore(json);
            }
            catch (InvalidOperationException e)
            {
                Debug.LogError("ArenaClientBehaviour: no client built: " + e.Message);
                return;
            }
            try
            {
                Client = new ArenaClient(options, new UnityWebRequestTransport(), store, json, ElapsedMs);
            }
            catch
            {
                store.Dispose();
                store = null;
                throw;
            }
            Action<ArenaClient> handler = ClientCreated;
            if (handler != null)
            {
                handler(Client);
            }
        }

        void OnEnable()
        {
            if (pump == null)
            {
                pump = StartCoroutine(Pump());
            }
        }

        void OnDisable()
        {
            if (pump != null)
            {
                StopCoroutine(pump);
                pump = null;
            }
        }

        void OnApplicationPause(bool pauseStatus)
        {
            if (Client == null)
            {
                return;
            }
            if (pauseStatus)
            {
                Client.Pause();
            }
            else
            {
                Client.Resume();
            }
        }

        void OnApplicationFocus(bool hasFocus)
        {
            if (Client == null)
            {
                return;
            }
            if (hasFocus)
            {
                Client.Resume();
            }
            else
            {
                Client.Flush();
            }
        }

        void OnApplicationQuit()
        {
            if (Client != null)
            {
                Client.Flush();
            }
        }

        void OnDestroy()
        {
            if (Client != null)
            {
                Client.Dispose();
                Client = null;
            }
            if (store != null)
            {
                store.Dispose();
                store = null;
            }
        }

        IEnumerator Pump()
        {
            while (true)
            {
                Step();
                yield return null;
            }
        }

        void Step()
        {
            if (Client == null)
            {
                return;
            }
            try
            {
                Client.Update();
            }
            catch (Exception e)
            {
                // An exception from a game callback must not end the pump.
                Debug.LogException(e);
            }
        }

        long ElapsedMs()
        {
            return stopwatch.ElapsedMilliseconds;
        }
    }
}
