using Streamiz.Kafka.Net.Processors.Internal;

namespace Streamiz.Kafka.Net.Processors
{
    internal delegate void ThreadStateListener(IThread thread, IThreadStateTransitionValidator old, IThreadStateTransitionValidator @new);

    internal delegate void GlobalThreadStateListener(GlobalStreamThread thread, IThreadStateTransitionValidator old, IThreadStateTransitionValidator @new);

}
