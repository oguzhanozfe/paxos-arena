# unity-client

The C# client SDK for the play API of paxos-arena, packaged for the Unity
Package Manager as `com.paxosarena.client` (Unity 2022.3 or later). A mobile
card game uses it to send intents to the cluster and show what the cluster
decides. It handles device sessions, idempotent intents with per-player
sequence numbers that survive app pause, restart and network loss, leader
redirects, and an events long-poll.

The contract the SDK implements, with every route, JSON body, error code and
client rule, is [`docs/UNITY-INTEGRATION.md`](../docs/UNITY-INTEGRATION.md).
Section 11 of that document specifies the SDK and wins over this summary.

The client decides nothing. It never computes a score, a legal move, a
standing or a payout, and it never changes a board locally. A tap sends an
intent, and the board changes when the server's view arrives.

## Install

The package has no dependencies beyond two built-in engine modules
(`jsonserialize` and `unitywebrequest`).

In the editor, open **Window > Package Manager > + > Add package from git
URL** and enter:

```
https://github.com/oguzhanozfe/paxos-arena.git?path=unity-client
```

Or add it to `Packages/manifest.json`:

```json
{
  "dependencies": {
    "com.paxosarena.client": "https://github.com/oguzhanozfe/paxos-arena.git?path=unity-client"
  }
}
```

Append `#<tag or commit>` to the URL to pin a version. To work on the SDK
itself, use a local checkout instead:
`"com.paxosarena.client": "file:../../paxos-arena/unity-client"`.

The sample is listed under **Samples** on the package's page in the Package
Manager. Importing it also copies `link.xml` into `Assets` (see IL2CPP below).

## Quick start

1. Add an `ArenaClientBehaviour` to a GameObject that lives for the whole
   session (for example with `DontDestroyOnLoad`). Set **Base Urls** to the
   public play URLs of the replicas (or one balancer URL), and set
   **Jurisdiction** and **Age**. The first session of a device records these
   two values. `Awake` builds the client, and a coroutine pumps it every frame.
2. Use `ArenaClientBehaviour.Client` from `Start` or later. You can also call
   `Initialize(options)` from code instead of setting the fields.

```csharp
using PaxosArena.Client;
using PaxosArena.Client.Unity;
using UnityEngine;

public sealed class Table : MonoBehaviour
{
    [SerializeField] ArenaClientBehaviour arena;
    string tournamentId = "";
    RoundView view;

    void Start()
    {
        ArenaClient client = arena.Client;
        client.IntentCompleted += outcome => Debug.Log(outcome.Route + " seq " + outcome.Seq + " -> " + outcome.Status);
        client.Resumed += () => { if (view != null) client.GetRound(tournamentId, view.round, OnRound); };
        client.Resynced += () => { if (view != null) client.GetRound(tournamentId, view.round, OnRound); };
        client.Events.Received += item => Debug.Log("event " + item.type + " at slot " + item.slot);

        client.ListTournaments(TournamentStatus.Open, 0, 20, list =>
        {
            if (!list.Ok) { Debug.LogWarning(list.Error); return; }
            foreach (TournamentSummary t in list.Value.tournaments)
            {
                if (!t.eligible) continue;
                tournamentId = t.tournament_id;
                client.Join(tournamentId, joined =>
                {
                    if (joined.Ok) client.Deal(tournamentId, 1, OnRound);
                });
                return;
            }
        });
    }

    void OnRound(ArenaResult<RoundResponse> result)
    {
        if (!result.Ok) { Debug.LogWarning(result.Error); return; }
        view = result.Value.round;   // render columns, waste_top, stock_count, score
    }

    // Input comes from the server's view and carries its move_index.
    public void TapColumn(int column)
    {
        if (arena.Client.Busy || view == null || System.Array.IndexOf(view.playable_columns, column) < 0) return;
        arena.Client.Play(tournamentId, view.round, view.move_index, column, OnRound);
    }

    public void TapStock()
    {
        if (arena.Client.Busy || view == null || !view.can_draw) return;
        arena.Client.Draw(tournamentId, view.round, view.move_index, OnRound);
    }
}
```

`Samples~/ArenaSample/ArenaSample.cs` is the whole flow in one MonoBehaviour
with IMGUI: session, list, join, deal, play and draw, finish, leaderboard and
claim. It also checks the revealed seed against the commitment. For a local
`http://` server, allow downloads over HTTP in the player settings of
development builds.

## What the client does for you

| Concern | Behaviour |
|---|---|
| Identity | On first launch it draws `device_id` (32 hex characters) and `device_secret` (64 hex characters) and keeps them in the store. It never logs or displays the secret. |
| Sessions | It opens a session at start when it has no token, 5 minutes before the token expires by server time, and after any `401`. A session replay that carries an expired token is followed by a session with a new key. `403 device_mismatch` halts the client (`HaltError`, `Halted`). |
| Intents | `Join`, `Deal`, `Play`, `Draw`, `Finish` and `ClaimPayout` draw a 32-hex key and assign `seq = last + 1`. They write the intent to the store before anything is sent. Only the head of the queue is sent, and it is resent byte for byte with the same key until a definitive answer. At most `MaxPendingIntents` (64) intents wait; one more fails with `queue_full`. |
| Answers | `2xx`, a `409` rule rejection or `403` removes the head and delivers the answer. `307` is followed once. `401` refreshes the session and resends at once. Retryable answers, timeouts and lost connections back off and retry. `stale_seq`, `seq_gap` and client-bug statuses (`400`, `404`, `405`, `413`, `422`) resynchronise: every pending intent fails with `resync_required`, numbering restarts from `X-Arena-Next-Seq` or from a new session, and `Resynced` is raised. |
| Backoff | Full jitter: a random delay in `[0, min(8000, 250 * 2^(failures-1))]` ms, never below `Retry-After`. After three failures in a row `Connection` becomes `Offline`, and the client keeps retrying. Reads give up after `MaxReadFailures` (3). |
| Leader | A `307` is followed once to its `Location`, and that origin is cached in the store as the leader. A second `307` in a row is not followed; it counts as a retryable failure. A leader that fails without a response, or answers `503 no_leader`, is dropped for the last `X-Arena-Leader` or the next base URL. |
| Reads | `GetRound` sends `min_slot` = the slot of the last definitive intent, so a player never sees a board older than their own last move. |
| Clock | The server time is estimated from `X-Arena-Server-Time-Ms` on a monotonic clock, using the lowest-RTT sample of the last eight, and is cleared on resume. `ServerNowMs` is for display, such as a countdown. The device's wall clock is never read. |
| Events | `Events` long-polls `GET /v1/events` (25 s wait, 35 s timeout) from a cursor kept in the store. The cursor advances only after `Received` has run for every event of a response. Set `Events.TournamentId` to follow one tournament. |
| Audit | The deal view and commitment of each round are kept (`FindAudit`) until the game checks the revealed seed and calls `RemoveAudit`. |

Every callback receives an `ArenaResult<T>`. On failure, `Error.Code` holds a
server code (`ErrorCodes`) or a local one (`LocalErrorCodes`: `queue_full`,
`resync_required`, `no_response`, `bad_response`). `IntentCompleted` fires for
every definitive answer, including answers to intents created before a
restart, whose callbacks died with the old process.

## Threading

- The Core is single-threaded and callback-driven. There is no `async`, no
  threads, no blocking and no locks. Call every `ArenaClient` member from one
  thread, the one that calls `Update()`. In Unity that is the main thread.
- Every callback and event (`IntentCompleted`, `Resynced`, `Resumed`,
  `ConnectionChanged`, `Halted`, `Events.Received` and each method's `done`)
  runs inside `Update()`. None runs inside the method that started the
  request, not even a local failure such as `queue_full`, so it is safe to
  call the client from any callback.
- `IHttpTransport` implementations must call `done` exactly once, on the
  `Update()` thread. `UnityWebRequestTransport` uses the request's
  `AsyncOperation.completed`, which Unity raises on the main thread. The
  harness's `HttpClientTransport` queues completions and runs them from
  `Dispatch()`, just before it calls `Update()`.
- If a callback throws, the exception leaves `Update()`. The client's state
  is already saved, and the answers not yet delivered stay queued for the next
  `Update()`. `ArenaClientBehaviour` logs the exception and keeps pumping.
- The client reads time only through the `Func<long>` it is given. A
  `Stopwatch` does not jump when the user changes the device time.

## Pause, resume and restarts

- `OnApplicationPause(true)` calls `Pause()`. The client stops starting
  requests, aborts the requests in flight (the events poll, and an intent on
  the wire, which stays in the store) and persists the store.
- `OnApplicationPause(false)` calls `Resume()`. The client clears the clock
  samples and resets every backoff. On the next `Update()` it raises
  `Resumed`, opens a session if one is needed, resends the head intent and
  restarts the events poll from the stored cursor, all at once. On `Resumed`,
  read the round in play (`GetRound`) and refresh the leaderboard.
- Losing focus flushes the store. Regaining focus resumes a client that is
  still paused. `Resume` does nothing unless the client is paused, so the
  focus and pause callbacks may both arrive.
- A killed app needs nothing. The store is the write-ahead record, and the
  next client on the same store resends the stored intents with their keys
  and reports each answer through `IntentCompleted`.
- Nothing is dropped because of time. An intent resent hours later receives
  its recorded result, or a rule rejection such as `round_expired`.
- `Busy` is true while an intent is pending, and `BusyForMs` tells how long
  the head has been unresolved. Section 9.6 of the contract says what to show
  when it passes 1 second, and again when it passes 10 seconds.

`PersistentDataIntentStore` keeps everything in
`Application.persistentDataPath/paxos-arena/client-state.json`. Each save
writes a `.tmp` file, flushes it to disk and replaces the state file
(`File.Replace` with a `.bak` backup, or copy, delete and move where the
platform refuses). Loading falls back from the file to the `.tmp` (when the
file is missing) and then to the `.bak`. The file holds the device secret. A
game that keeps the secret in the platform keystore implements `IIntentStore`
itself.

## IL2CPP stripping

The models are created only by `JsonUtility`, so managed code stripping can
remove them. `Runtime/link.xml` preserves `PaxosArena.Client.Core` and
`PaxosArena.Client.Unity`, and every model carries a `PreserveAttribute`
declared in the Core, which the linker recognises by name. Unity documents
`link.xml` files under `Assets`: importing the sample places a copy there. A
project that does not import the sample copies `Runtime/link.xml` to
`Assets/PaxosArena/link.xml`.

## Layout

```
unity-client/
  package.json, README.md, *.meta       UPM manifest; .meta files, because git packages are read-only
  Runtime/
    link.xml
    Core/                               PaxosArena.Client.Core: netstandard2.1, C# 9, noEngineReferences
      ArenaClient.cs                    sessions, queue engine, redirects, reads, pause and resume
      ArenaClientOptions.cs  Backoff.cs  ServerClock.cs  Errors.cs  PreserveAttribute.cs
      IHttpTransport.cs  IIntentStore.cs  IJson.cs
      IntentQueue.cs                    write-ahead queue (internal)
      EventFollower.cs                  GET /v1/events long-poll
      Models/                           Requests.cs  Responses.cs  Reads.cs  Names.cs
    Unity/                              PaxosArena.Client.Unity
      UnityWebRequestTransport.cs  PersistentDataIntentStore.cs  JsonUtilityJson.cs  ArenaClientBehaviour.cs
  Samples~/ArenaSample/                 ArenaSample.cs, its asmdef, a copy of link.xml
  Tests~/                               never imported by the editor
    Harness/                            dotnet console app: unit tests and the live flow
      CoreCheck/CoreCheck.csproj        Runtime/Core alone against netstandard2.1
    UnityStubs/                         compile-only stubs of the engine APIs the SDK uses
      UnityCheck/UnityCheck.csproj      Runtime/Unity as its own assembly
      SampleCheck/SampleCheck.csproj    the sample as its own assembly
```

## Checking the SDK without Unity

The checks need a .NET SDK, version 8 or later. The harness targets the
installed SDK's framework (`net10.0` by default); pass
`--property:HarnessTargetFramework=net8.0` to build with an older one. Run from the
repository root:

```
# Core, Unity adapters and sample compiled as separate netstandard2.1 / C# 9 assemblies
dotnet build "unity-client/Tests~/UnityStubs/SampleCheck/SampleCheck.csproj"

# Unit tests of the Core with a fake transport and clock
dotnet run --project "unity-client/Tests~/Harness/Harness.csproj" -- --unit

# The flow of section 11.6 against a live cluster started with -play-listen, for example
# ARENA_SESSION_KEYS=k1=$(openssl rand -hex 32) ARENA_DEAL_SECRET=$(openssl rand -hex 32) \
#     arena -nodes 3 -listen 127.0.0.1:8081 -play-listen 127.0.0.1:9081
dotnet run --project "unity-client/Tests~/Harness/Harness.csproj" -- \
    --play http://127.0.0.1:9081,http://127.0.0.1:9082,http://127.0.0.1:9083 \
    --operator http://127.0.0.1:8081,http://127.0.0.1:8082,http://127.0.0.1:8083 \
    --players 3 [--kill-leader-command CMD]
```

`--kill-leader-command` runs CMD in the middle of round 2 and requires the
flow to finish anyway. That only makes sense with one replica per process
(`arena -id N -peers ... -wal ... -play-listen ... -play-urls ...`), where CMD
kills the leader's process and the other two keep a majority. The harness
runs it while a move's answer is undelivered, rebuilds player 0's client
from its store, and requires the resend to fail on the killed leader, follow
a follower's `307` and come back from the new leader as a replay. It needs
one `--operator` URL per `--play` URL, in node order.

Besides the steps of contract section 11.6, the live flow plays one illegal
move (`409 illegal_move`, the number consumed, the board unchanged), sends
the last move's body under a new key (`409 stale_seq`), and reads the
tournament's ledger through the operator API at the end: exactly one claim
posting per paid player.

`scripts/e2e.sh` (or `make e2e`) does all of this unattended: it builds
`arena` and the harness, starts three replicas as processes on `127.0.0.1`,
runs the flow, runs it again with the leader killed by `SIGKILL`, restarts
the killed replica from its wal file, and checks that every replica reports
the same state hash. It prints a `SKIP` line and exits 0 without a .NET SDK.

The unit tests cover backoff and `Retry-After`, the server clock, the session
at start and its credentials, and intents that are stored before sending and
sent one at a time. They also cover byte-identical resends, rule rejections,
resynchronisation from `stale_seq` and from `seq_gap`, the session refresh
after `401`, the single redirect follow and the leader cache, and restarts
with a stored intent. The rest check `queue_full`, pause and resume, the
events cursor, halting on `device_mismatch`, a change of player, `min_slot`
on reads, the request watchdog, the JSON shapes of the contract, the Appendix
A deals and games, and the atomic file store.

The stubs in `Tests~/UnityStubs/UnityStubs.cs` exist only for compilation;
every member throws. What still needs a real Unity build is listed in the
contract (11.3, 11.5). Check that a `307` with `redirectLimit = 0` reaches
the client on each target platform, that a stripped IL2CPP build keeps the
models, that `File.Replace` works under `persistentDataPath`, and that
`JsonUtility` reads the generic `JsonArray<T>` wrapper.
