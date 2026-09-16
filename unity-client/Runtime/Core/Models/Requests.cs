using System;

namespace PaxosArena.Client
{
    // Request bodies. The server rejects unknown fields, and a field-based
    // serializer writes every public field, so these classes hold exactly the
    // documented fields and nothing else (section 8, rule 8).

    /// <summary>Body of POST /v1/session.</summary>
    [Serializable]
    [Preserve]
    public sealed class SessionRequest
    {
        public string device_id = "";
        public string device_secret = "";
        public string jurisdiction = "";
        public int age;
    }

    /// <summary>Body of the join, deal, finish and claim routes.</summary>
    [Serializable]
    [Preserve]
    public sealed class SeqRequest
    {
        public long seq;
    }

    /// <summary>Body of the moves route.</summary>
    [Serializable]
    [Preserve]
    public sealed class MoveRequest
    {
        public long seq;
        public int move_index;
        public string kind = "";
        public int column;
    }
}
