using System;

namespace Streamiz.Kafka.Net.Processors
{
    internal class KStreamPeekProcessor<K, V> : AbstractProcessor<K, V>
    {
        private readonly Action<K, V> action;
        private readonly bool forwardDownStream;

        public KStreamPeekProcessor(Action<K, V> action, bool forwardDownStream)
        {
            this.action = action;
            this.forwardDownStream = forwardDownStream;
        }

        public override void Process(K key, V value)
        {
            LogProcessingKeyValue(key, value);
            action.Invoke(key, value);
            if (forwardDownStream)
                Forward(key, value);
        }
    }
}
