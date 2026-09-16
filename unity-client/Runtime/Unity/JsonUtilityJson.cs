using System;
using UnityEngine;

namespace PaxosArena.Client.Unity
{
    /// <summary>
    /// <see cref="IJson"/> over UnityEngine.JsonUtility. Every body of the play
    /// API is one object (section 8), so the client never needs a top-level
    /// array; JsonUtility cannot write or read one, so <see cref="ToJson"/> and
    /// <see cref="FromJson{T}"/> refuse arrays and <see cref="ToJsonArray{T}"/>
    /// and <see cref="FromJsonArray{T}"/> wrap them in an object instead.
    /// </summary>
    public sealed class JsonUtilityJson : IJson
    {
        public string ToJson(object value)
        {
            if (value == null)
            {
                return "";
            }
            if (value is Array)
            {
                throw new ArgumentException("JsonUtility cannot write a top-level array; use ToJsonArray", nameof(value));
            }
            return JsonUtility.ToJson(value);
        }

        public T FromJson<T>(string json)
        {
            if (string.IsNullOrEmpty(json))
            {
                return default(T);
            }
            if (typeof(T).IsArray)
            {
                throw new ArgumentException("JsonUtility cannot read a top-level array; use FromJsonArray", nameof(json));
            }
            return JsonUtility.FromJson<T>(json);
        }

        /// <summary>Writes an array as a JSON array, through a wrapper object.</summary>
        public static string ToJsonArray<T>(T[] items)
        {
            string wrapped = JsonUtility.ToJson(new JsonArray<T> { items = items ?? new T[0] });
            // {"items":[...]} -> [...]
            int start = wrapped.IndexOf('[');
            int end = wrapped.LastIndexOf(']');
            return start >= 0 && end > start ? wrapped.Substring(start, end - start + 1) : "[]";
        }

        /// <summary>Reads a JSON array, through a wrapper object.</summary>
        public static T[] FromJsonArray<T>(string json)
        {
            if (string.IsNullOrEmpty(json))
            {
                return new T[0];
            }
            JsonArray<T> wrapper = JsonUtility.FromJson<JsonArray<T>>("{\"items\":" + json + "}");
            return wrapper != null && wrapper.items != null ? wrapper.items : new T[0];
        }
    }

    /// <summary>The wrapper object JsonUtility needs around an array.</summary>
    [Serializable]
    [Preserve]
    public sealed class JsonArray<T>
    {
        public T[] items = new T[0];
    }
}
