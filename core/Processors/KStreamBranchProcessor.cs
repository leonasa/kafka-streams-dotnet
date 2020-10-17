using System;

namespace Streamiz.Kafka.Net.Processors
{
    internal class KStreamBranchProcessor<K, V> : AbstractProcessor<K, V>
    {
        private readonly Func<K, V, bool>[] predicates;
        private readonly string[] childNodes;

        public KStreamBranchProcessor(Func<K, V, bool>[] predicates, string[] childNodes)
        {
            this.predicates = predicates;
            this.childNodes = childNodes;
        }

        public override void Process(K key, V value)
        {
            LogProcessingKeyValue(key, value);
            for (int i = 0; i < predicates.Length; i++)
            {
                if (predicates[i].Invoke(key, value))
                {
                    Forward(key, value, childNodes[i]);
                    break;
                }
            }
        }
    }
}
