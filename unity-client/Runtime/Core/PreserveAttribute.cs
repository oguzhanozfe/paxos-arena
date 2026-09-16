using System;

namespace PaxosArena.Client
{
    /// <summary>
    /// Marks a type the managed code stripper must keep. The models are created
    /// only by the JSON codec, so nothing else references their constructors
    /// and fields. The Unity linker recognises an attribute named
    /// PreserveAttribute from any assembly, so the Core needs no reference to
    /// UnityEngine for it. Runtime/link.xml preserves the whole assembly as
    /// well; section 11.5 of the contract asks for both.
    /// </summary>
    [AttributeUsage(
        AttributeTargets.Assembly | AttributeTargets.Class | AttributeTargets.Struct | AttributeTargets.Method |
        AttributeTargets.Constructor | AttributeTargets.Field | AttributeTargets.Property | AttributeTargets.Enum |
        AttributeTargets.Interface | AttributeTargets.Delegate | AttributeTargets.Event,
        Inherited = false)]
    public sealed class PreserveAttribute : Attribute
    {
    }
}
