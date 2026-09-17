// COMPILE-ONLY CHECK. NEVER SHIPPED.
//
// Game code imports UnityEngine and the SDK together. This file fails to
// compile if a public name of the SDK clashes with an engine name (the
// IMGUI EventType, UnityEngine.Scripting.Preserve), or if an SDK namespace
// hides the engine's global Unity.* namespaces from code under
// PaxosArena.Client, as a namespace named PaxosArena.Client.Unity did.

using PaxosArena.Client;
using PaxosArena.Client.UnityAdapters;
using UnityEngine;
using UnityEngine.Scripting;

namespace ArenaGame.Checks
{
    [Preserve]
    internal static class NameClashCheck
    {
        internal static string Names(EventType imgui, ArenaClientBehaviour behaviour)
        {
            return imgui.ToString() + ArenaEventType.RoundStarted + (behaviour != null);
        }
    }
}

namespace PaxosArena.Client.Sample.Checks
{
    internal static class NamespaceShadowCheck
    {
        internal static string Engine(Unity.Collections.Allocator allocator)
        {
            return allocator.ToString();
        }
    }
}
