# unity-client

The C# client SDK for the play API of paxos-arena, packaged for the Unity
Package Manager as `com.paxosarena.client` (Unity 2022.3 or later). It lets a
mobile card game send intents to the cluster and render what the cluster
decides: device sessions, idempotent intents with per-player sequence
numbers that survive app pause, restart and network loss, leader redirects,
and an events long-poll.

Status: layout agreed, code not written yet. This directory holds only this
file. The contract the SDK implements, including every route, JSON body,
error code and client rule, is
[`docs/UNITY-INTEGRATION.md`](../docs/UNITY-INTEGRATION.md); section 11
specifies the SDK and wins over this summary.

## Layout

```
unity-client/
  package.json                          UPM package com.paxosarena.client, "unity": "2022.3",
                                        built-in modules jsonserialize and unitywebrequest only
  README.md                             this file
  Runtime/
    link.xml                            preserves both runtime assemblies under IL2CPP stripping
    Core/                               pure C#: no UnityEngine, netstandard2.1, C# 9, no async
      PaxosArena.Client.Core.asmdef     noEngineReferences: true
      ArenaClient.cs                    sessions, intent queue, redirects, reads; pumped by Update()
      ArenaClientOptions.cs
      IHttpTransport.cs                 HttpRequest, HttpResponse, HttpHeader; never follows redirects
      IIntentStore.cs                   ClientState, PendingIntent, RoundAudit; atomic load and save
      IJson.cs
      IntentQueue.cs                    write-ahead queue, one intent in flight, same key on resend
      EventFollower.cs                  GET /v1/events long-poll with a stored cursor
      ServerClock.cs                    server time from X-Arena-Server-Time-Ms on a monotonic clock
      Backoff.cs                        full-jitter exponential backoff, Retry-After as a floor
      Errors.cs                         ArenaError, ArenaResult<T>, error codes, ArenaException
      Models/                           [Serializable] classes with snake_case public fields
        Requests.cs
        Responses.cs
        Reads.cs
        Names.cs                        string constants for statuses, reasons, kinds, event types
    Unity/                              UnityEngine adapters
      PaxosArena.Client.Unity.asmdef    references PaxosArena.Client.Core
      UnityWebRequestTransport.cs       redirectLimit = 0
      PersistentDataIntentStore.cs      Application.persistentDataPath/paxos-arena/client-state.json
      JsonUtilityJson.cs
      ArenaClientBehaviour.cs           coroutine that pumps the client; OnApplicationPause handling
  Samples~/
    ArenaSample/
      PaxosArena.Client.Sample.asmdef
      ArenaSample.cs                    MonoBehaviour: session, join, deal, moves, finish,
                                        leaderboard, payout claim
      link.xml                          copy of Runtime/link.xml, imported into Assets with the sample
  Tests~/
    Harness/
      Harness.csproj                    dotnet console project compiling Runtime/Core sources
      Program.cs                        the whole flow against a live server
      HttpClientTransport.cs
      FileIntentStore.cs
      SystemTextJson.cs
      LadderAudit.cs                    shuffle and rules in C#, to verify revealed seeds
      CoreCheck/
        CoreCheck.csproj                Runtime/Core alone against netstandard2.1
```

`Samples~` and `Tests~` end in `~`, so the editor imports neither.

## IL2CPP stripping

The models are created only by `JsonUtility`, so managed code stripping can
remove them. `Runtime/link.xml` preserves `PaxosArena.Client.Core` and
`PaxosArena.Client.Unity`. Unity documents `link.xml` files under `Assets`:
importing the sample places a copy there; a project that does not import it
copies `Runtime/link.xml` to `Assets/PaxosArena/link.xml`. Check a stripped
IL2CPP build during integration.

## Harness

```
dotnet run --project unity-client/Tests~/Harness/Harness.csproj -- \
    --play http://127.0.0.1:9081 --operator http://127.0.0.1:8081 --players 3
```

It runs the flow of section 11.6 of the contract against a live cluster:
sessions, join, three rounds per player with every revealed seed verified,
replays and sequence errors, a client restart with an intent in flight,
leaderboard, close and settle through the operator API, events, and payout
claims.
