namespace PaxosArena.Client
{
    /// <summary>
    /// The JSON codec the client uses for every body and for the stored state.
    /// In Unity it is JsonUtility; the models follow section 8 of the contract
    /// (public snake_case fields, no maps, no nulls, no top-level arrays), so
    /// any field-based serializer reads and writes the same JSON.
    /// </summary>
    public interface IJson
    {
        /// <summary>Serializes one object; throws for a value it cannot write.</summary>
        string ToJson(object value);

        /// <summary>Parses one object; may throw on malformed input, which the client treats as a bad response.</summary>
        T FromJson<T>(string json);
    }
}
