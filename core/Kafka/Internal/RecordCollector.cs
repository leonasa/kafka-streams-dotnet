﻿using Confluent.Kafka;
using log4net;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.SerDes;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Kafka.Internal
{
    internal class RecordCollector : IRecordCollector
    {
        // IF EOS DISABLED, ONE PRODUCER BY TASK BUT ONE INSTANCE RECORD COLLECTOR BY TASK
        // WHEN CLOSING TASK, WE MUST DISPOSE PRODUCER WHEN NO MORE INSTANCE OF RECORD COLLECTOR IS PRESENT
        // IT'S A GARBAGE COLLECTOR LIKE
        private static readonly IDictionary<string, int> instanceProducer = new Dictionary<string, int>();
        private static readonly object _lock = new object();

        private IProducer<byte[], byte[]> producer;
        //private readonly IDictionary<TopicPartition, long> offsets;
        private readonly string logPrefix;
        private readonly ILog log = Logger.GetLogger(typeof(RecordCollector));

        public RecordCollector(string logPrefix)
        {
            this.logPrefix = $"{logPrefix}";
            //offsets = new Dictionary<TopicPartition, long>();
        }

        public void Init(ref IProducer<byte[], byte[]> producer)
        {
            this.producer = producer;

            string producerName = producer.Name.Split("#")[0];
            lock (_lock)
            {
                if (instanceProducer.ContainsKey(producerName))
                    ++instanceProducer[producerName];
                else
                    instanceProducer.Add(producerName, 1);
            }
        }

        public void Close()
        {
            log.Debug($"{logPrefix}Closing producer");
            if(producer != null)
            {
                lock (_lock)
                {
                    string producerName = producer.Name.Split("#")[0];
                    if (--instanceProducer[producerName] <= 0)
                    {
                        producer.Dispose();
                        producer = null;
                    }
                }
            }
        }

        public void Flush()
        {
            log.Debug($"{logPrefix}Flusing producer");
            if (producer != null)
            {
                producer.Flush();
            }
        }

        public void Send<K, V>(string topic, K key, V value, Headers headers, long timestamp, ISerDes<K> keySerializer, ISerDes<V> valueSerializer)
        {
            var k = key != null ? keySerializer.Serialize(key, new SerializationContext(MessageComponentType.Key, topic,headers)) : null;
            var v = value != null ? valueSerializer.Serialize(value, new SerializationContext(MessageComponentType.Value, topic, headers)) : null;

            producer?.Produce(
                topic,
                new Message<byte[], byte[]> { Key = k, Value = v });

            // NOT USED FOR MOMENT
            //producer?.Produce(
            //    topic, 
            //    new Message<byte[], byte[]> { Key = k, Value = v },
            //    (report) => {
            //        if (report.Error.Code == ErrorCode.NoError && report.Status == PersistenceStatus.Persisted)
            //        {
            //            if (offsets.ContainsKey(report.TopicPartition) && offsets[report.TopicPartition] <= report.Offset)
            //                offsets[report.TopicPartition] = report.Offset;
            //            else
            //                offsets.Add(report.TopicPartition, report.Offset);
            //        }
            //    });
        }
    }
}
