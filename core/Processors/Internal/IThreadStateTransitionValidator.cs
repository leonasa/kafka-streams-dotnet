namespace Streamiz.Kafka.Net.Processors.Internal
{
    internal interface IThreadStateTransitionValidator
    {
        bool IsValidTransition(IThreadStateTransitionValidator newState);
    }
}
