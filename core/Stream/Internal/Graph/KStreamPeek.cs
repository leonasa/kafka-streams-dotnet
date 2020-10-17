using Streamiz.Kafka.Net.Processors;
using System;

namespace Streamiz.Kafka.Net.Stream.Internal.Graph
{
    internal class KStreamPeek<K, V> : IProcessorSupplier<K, V>
    {
        public bool ForwardDownStream { get; }
        public Action<K, V> Action { get; }

        public KStreamPeek(Action<K, V> action, bool forwardDownStream)
        {
            Action = action;
            ForwardDownStream = forwardDownStream;
        }

        public IProcessor<K, V> Get() => new KStreamPeekProcessor<K, V>(Action, ForwardDownStream);
    }
}
