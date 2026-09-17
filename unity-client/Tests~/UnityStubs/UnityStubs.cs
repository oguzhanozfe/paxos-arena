// COMPILE-ONLY STUBS. NOT UNITY. NEVER SHIPPED.
//
// These declarations mirror the signatures of the few UnityEngine and
// UnityEngine.Networking members that Runtime/Unity and Samples~/ArenaSample
// use, so that the dotnet checks in Tests~ catch type errors without a Unity
// installation. Every member throws NotSupportedException; nothing here runs.
// The folder ends in "~", so the Unity editor never imports it. When the SDK
// starts using another engine API, add its exact signature here.

using System;
using System.Collections;
using System.Collections.Generic;

namespace UnityStubs
{
    internal static class CompileOnly
    {
        internal static NotSupportedException Fail()
        {
            return new NotSupportedException("compile-only stub of a Unity API; this code runs only inside Unity");
        }
    }
}

namespace UnityEngine
{
    using UnityStubs;

    public class Object
    {
    }

    public class Component : Object
    {
        public T GetComponent<T>()
        {
            throw CompileOnly.Fail();
        }
    }

    public class Behaviour : Component
    {
        public bool enabled
        {
            get { throw CompileOnly.Fail(); }
            set { throw CompileOnly.Fail(); }
        }
    }

    public class MonoBehaviour : Behaviour
    {
        public Coroutine StartCoroutine(IEnumerator routine)
        {
            throw CompileOnly.Fail();
        }

        public void StopCoroutine(Coroutine routine)
        {
            throw CompileOnly.Fail();
        }
    }

    public class YieldInstruction
    {
    }

    public sealed class Coroutine : YieldInstruction
    {
        Coroutine()
        {
        }
    }

    public class AsyncOperation : YieldInstruction
    {
        public bool isDone
        {
            get { throw CompileOnly.Fail(); }
        }

        public event Action<AsyncOperation> completed
        {
            add { throw CompileOnly.Fail(); }
            remove { throw CompileOnly.Fail(); }
        }
    }

    public static class Application
    {
        public static string persistentDataPath
        {
            get { throw CompileOnly.Fail(); }
        }
    }

    public static class Debug
    {
        public static void Log(object message)
        {
            throw CompileOnly.Fail();
        }

        public static void LogWarning(object message)
        {
            throw CompileOnly.Fail();
        }

        public static void LogError(object message)
        {
            throw CompileOnly.Fail();
        }

        public static void LogException(Exception exception)
        {
            throw CompileOnly.Fail();
        }
    }

    public static class JsonUtility
    {
        public static string ToJson(object obj)
        {
            throw CompileOnly.Fail();
        }

        public static T FromJson<T>(string json)
        {
            throw CompileOnly.Fail();
        }
    }

    [AttributeUsage(AttributeTargets.Field)]
    public sealed class SerializeField : Attribute
    {
    }

    public abstract class PropertyAttribute : Attribute
    {
    }

    [AttributeUsage(AttributeTargets.Field, Inherited = true, AllowMultiple = false)]
    public class TooltipAttribute : PropertyAttribute
    {
        public readonly string tooltip;

        public TooltipAttribute(string tooltip)
        {
            this.tooltip = tooltip;
        }
    }

    [AttributeUsage(AttributeTargets.Class, AllowMultiple = true)]
    public sealed class RequireComponent : Attribute
    {
        public Type m_Type0;

        public RequireComponent(Type requiredComponent)
        {
            m_Type0 = requiredComponent;
        }
    }

    [AttributeUsage(AttributeTargets.Class, Inherited = false)]
    public sealed class DisallowMultipleComponent : Attribute
    {
    }

    // Declared so that the compile checks see the engine names the SDK's
    // public names must not clash with (SampleCheck/NameClashCheck.cs).
    public enum EventType
    {
        MouseDown = 0,
        Layout = 8,
        Repaint = 7,
    }

    public sealed class Event
    {
        public static Event current
        {
            get { throw CompileOnly.Fail(); }
        }

        public EventType type
        {
            get { throw CompileOnly.Fail(); }
        }
    }

    public struct Vector2
    {
        public float x;
        public float y;

        public Vector2(float x, float y)
        {
            this.x = x;
            this.y = y;
        }
    }

    public struct Rect
    {
        public Rect(float x, float y, float width, float height)
        {
            throw CompileOnly.Fail();
        }
    }

    public sealed class Screen
    {
        public static int width
        {
            get { throw CompileOnly.Fail(); }
        }

        public static int height
        {
            get { throw CompileOnly.Fail(); }
        }
    }

    public sealed class GUILayoutOption
    {
        GUILayoutOption()
        {
        }
    }

    public class GUI
    {
        public static bool enabled
        {
            get { throw CompileOnly.Fail(); }
            set { throw CompileOnly.Fail(); }
        }
    }

    public class GUILayout
    {
        public static void Label(string text, params GUILayoutOption[] options)
        {
            throw CompileOnly.Fail();
        }

        public static bool Button(string text, params GUILayoutOption[] options)
        {
            throw CompileOnly.Fail();
        }

        public static void BeginHorizontal(params GUILayoutOption[] options)
        {
            throw CompileOnly.Fail();
        }

        public static void EndHorizontal()
        {
            throw CompileOnly.Fail();
        }

        public static void BeginArea(Rect screenRect)
        {
            throw CompileOnly.Fail();
        }

        public static void EndArea()
        {
            throw CompileOnly.Fail();
        }

        public static Vector2 BeginScrollView(Vector2 scrollPosition, params GUILayoutOption[] options)
        {
            throw CompileOnly.Fail();
        }

        public static void EndScrollView()
        {
            throw CompileOnly.Fail();
        }
    }
}

namespace UnityEngine.Scripting
{
    [AttributeUsage(AttributeTargets.Assembly | AttributeTargets.Class | AttributeTargets.Struct | AttributeTargets.Enum |
                    AttributeTargets.Constructor | AttributeTargets.Method | AttributeTargets.Property | AttributeTargets.Field |
                    AttributeTargets.Event | AttributeTargets.Interface | AttributeTargets.Delegate, Inherited = false)]
    public class PreserveAttribute : Attribute
    {
    }
}

namespace UnityEngine.iOS
{
    using UnityStubs;

    public static class Device
    {
        public static void SetNoBackupFlag(string path)
        {
            throw CompileOnly.Fail();
        }
    }
}

namespace Unity.Collections
{
    // Stands for the engine's global Unity.* namespaces, which a namespace
    // named PaxosArena.Client.Unity would hide from code under PaxosArena.Client.
    public enum Allocator
    {
        Invalid = 0,
        Temp = 2,
    }
}

namespace UnityEngine.Networking
{
    using UnityStubs;

    public class UnityWebRequest : IDisposable
    {
        public enum Result
        {
            InProgress,
            Success,
            ConnectionError,
            ProtocolError,
            DataProcessingError,
        }

        public UnityWebRequest(string url, string method)
        {
            throw CompileOnly.Fail();
        }

        public DownloadHandler downloadHandler
        {
            get { throw CompileOnly.Fail(); }
            set { throw CompileOnly.Fail(); }
        }

        public UploadHandler uploadHandler
        {
            get { throw CompileOnly.Fail(); }
            set { throw CompileOnly.Fail(); }
        }

        /// <summary>Seconds; 0 means none.</summary>
        public int timeout
        {
            get { throw CompileOnly.Fail(); }
            set { throw CompileOnly.Fail(); }
        }

        public int redirectLimit
        {
            get { throw CompileOnly.Fail(); }
            set { throw CompileOnly.Fail(); }
        }

        public long responseCode
        {
            get { throw CompileOnly.Fail(); }
        }

        public string error
        {
            get { throw CompileOnly.Fail(); }
        }

        public Result result
        {
            get { throw CompileOnly.Fail(); }
        }

        public void SetRequestHeader(string name, string value)
        {
            throw CompileOnly.Fail();
        }

        public UnityWebRequestAsyncOperation SendWebRequest()
        {
            throw CompileOnly.Fail();
        }

        public Dictionary<string, string> GetResponseHeaders()
        {
            throw CompileOnly.Fail();
        }

        public void Abort()
        {
            throw CompileOnly.Fail();
        }

        public void Dispose()
        {
            throw CompileOnly.Fail();
        }
    }

    public class UnityWebRequestAsyncOperation : AsyncOperation
    {
        public UnityWebRequest webRequest
        {
            get { throw CompileOnly.Fail(); }
        }
    }

    public class DownloadHandler : IDisposable
    {
        public string text
        {
            get { throw CompileOnly.Fail(); }
        }

        public void Dispose()
        {
            throw CompileOnly.Fail();
        }
    }

    public sealed class DownloadHandlerBuffer : DownloadHandler
    {
    }

    public class UploadHandler : IDisposable
    {
        public string contentType
        {
            get { throw CompileOnly.Fail(); }
            set { throw CompileOnly.Fail(); }
        }

        public void Dispose()
        {
            throw CompileOnly.Fail();
        }
    }

    public sealed class UploadHandlerRaw : UploadHandler
    {
        public UploadHandlerRaw(byte[] data)
        {
            throw CompileOnly.Fail();
        }
    }
}
