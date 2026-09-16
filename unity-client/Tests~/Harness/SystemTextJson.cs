using System.Text.Json;

namespace PaxosArena.Client.Harness
{
    /// <summary>
    /// IJson over System.Text.Json with public fields, used only by the harness.
    /// The models declare every field with a non-null initial value, so this
    /// writes the same JSON as JsonUtility: fields in declaration order, no
    /// nulls, no whitespace.
    /// </summary>
    public sealed class SystemTextJson : IJson
    {
        static readonly JsonSerializerOptions Options = new JsonSerializerOptions
        {
            IncludeFields = true,
        };

        public string ToJson(object value)
        {
            return value == null ? "" : JsonSerializer.Serialize(value, value.GetType(), Options);
        }

        public T FromJson<T>(string json)
        {
            return JsonSerializer.Deserialize<T>(json, Options);
        }
    }
}
