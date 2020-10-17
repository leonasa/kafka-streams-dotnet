﻿namespace Streamiz.Kafka.Net.Processors.Internal
{
    internal class StaticTopicNameExtractor<K, V> : ITopicNameExtractor<K, V>
    {
        public string TopicName { get; }

        public StaticTopicNameExtractor(string topicName)
        {
            TopicName = topicName;
        }

        public string Extract(K key, V value, IRecordContext recordContext) => TopicName;
    }
}
