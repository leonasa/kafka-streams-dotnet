﻿using Streamiz.Kafka.Net.Mock.Pipes;
using Streamiz.Kafka.Net.SerDes;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Streamiz.Kafka.Net.Mock
{
    /// <summary>
    /// <see cref="TestInputTopic{K, V}"/> is used to pipe records to topic in <see cref="TopologyTestDriver"/> and it's NOT THREADSAFE.
    /// To use <see cref="TestInputTopic{K, V}"/> create a new instance via
    /// <see cref="TopologyTestDriver.CreateInputTopic{K, V}(string)"/>.
    /// In actual test code, you can pipe new record values, keys and values or list of keyvalue pairs.
    /// If you have multiple source topics, you need to create a <see cref="TestInputTopic{K, V}"/> for each.
    /// <example>
    /// Processing messages
    /// <code>
    /// var inputTopic = driver.CreateInputTopic&lt;string, string&gt;("test");
    /// inputTopic.PipeInput("key1", "hello");
    /// </code>
    /// </example>
    /// </summary>
    /// <typeparam name="K">Key type</typeparam>
    /// <typeparam name="V">Value type</typeparam>
    public class TestInputTopic<K, V>
    {
        private readonly IPipeInput pipe;
        private readonly IStreamConfig configuration;
        private readonly ISerDes<K> keySerdes;
        private readonly ISerDes<V> valueSerdes;

        private TestInputTopic()
        {

        }

        internal TestInputTopic(IPipeInput pipe, IStreamConfig configuration, ISerDes<K> keySerdes, ISerDes<V> valueSerdes)
        {
            this.pipe = pipe;
            this.configuration = configuration;
            this.keySerdes = keySerdes;
            this.valueSerdes = valueSerdes;
        }

        internal IPipeInput Pipe => pipe;

        private (byte[], byte[]) GetBytes(K key, V value)
        {
            byte[] k = GetKeyBytes(key);
            byte[] v = GetValueBytes(value);
            return (k, v);
        }

        private byte[] GetKeyBytes(K key)
        {
            if (key != null)
            {
                if (keySerdes != null)
                    return keySerdes.Serialize(key, new Confluent.Kafka.SerializationContext(Confluent.Kafka.MessageComponentType.Key, pipe.TopicName));
                return configuration.DefaultKeySerDes.SerializeObject(key, new Confluent.Kafka.SerializationContext(Confluent.Kafka.MessageComponentType.Key, pipe.TopicName));
            }

            return null;
        }

        private byte[] GetValueBytes(V value)
        {
            if (value != null)
            {
                if (valueSerdes != null)
                    return valueSerdes.Serialize(value, new Confluent.Kafka.SerializationContext(Confluent.Kafka.MessageComponentType.Value, pipe.TopicName));
                return configuration.DefaultValueSerDes.SerializeObject(value, new Confluent.Kafka.SerializationContext(Confluent.Kafka.MessageComponentType.Value, pipe.TopicName));
            }

            return null;
        }

        #region Pipe One Input

        private void PipeInput(TestRecord<K, V> record)
        {
            DateTime ts = record.Timestamp.HasValue ? record.Timestamp.Value : DateTime.Now;
            var tuple = GetBytes(record.Key, record.Value);
            pipe.Pipe(tuple.Item1, tuple.Item2, ts);
            pipe.Flush();
        }

        /// <summary>
        /// Send an input record with the given record on the topic and then commit the records.
        /// </summary>
        /// <param name="value">value record</param>
        public void PipeInput(V value)
            => PipeInput(new TestRecord<K, V> { Value = value });

        /// <summary>
        /// Send an input record with the given record on the topic and then commit the records.
        /// </summary>
        /// <param name="value">value record</param>
        /// <param name="timestamp">Timestamp to record</param>
        public void PipeInput(V value, DateTime timestamp)
            => PipeInput(new TestRecord<K, V> { Value = value, Timestamp = timestamp });

        /// <summary>
        /// Send an input record with the given record on the topic and then commit the records.
        /// </summary>
        /// <param name="key">key record</param>
        /// <param name="value">value record</param>
        public void PipeInput(K key, V value)
            => PipeInput(new TestRecord<K, V> { Value = value, Key = key });

        /// <summary>
        /// Send an input record with the given record on the topic and then commit the records.
        /// </summary>
        /// <param name="key">key record</param>
        /// <param name="value">value record</param>
        /// <param name="timestamp">Timestamp to record</param>
        public void PipeInput(K key, V value, DateTime timestamp)
            => PipeInput(new TestRecord<K, V> { Key = key, Value = value, Timestamp = timestamp });

        #endregion

        #region Pipe List Inputs

        /// <summary>
        /// Send input records with the given record list on the topic, flush at the end.
        /// </summary>
        /// <param name="records">List of records (key, value and timestamp)</param>
        public void PipeInputs(IEnumerable<TestRecord<K,V>> records)
        {
            foreach (var record in records)
            {
                DateTime ts = record.Timestamp.HasValue ? record.Timestamp.Value : DateTime.Now;
                var tuple = GetBytes(record.Key, record.Value);
                pipe.Pipe(tuple.Item1, tuple.Item2, ts);
            }

            pipe.Flush();
        }

        /// <summary>
        /// Send input records with the given record list on the topic, flush at the end.
        /// </summary>
        /// <param name="valueInputs">List of values</param>
        public void PipeInputs(IEnumerable<V> valueInputs)
            => PipeInputs(valueInputs.Select(v => new TestRecord<K, V> { Value = v }));

        /// <summary>
        /// Send input records with the given record list on the topic, flush at the end.
        /// </summary>
        /// <param name="inputs">List of keyvalues</param>
        public void PipeInputs(IEnumerable<KeyValuePair<K, V>> inputs)
            => PipeInputs(inputs.Select(kv => new TestRecord<K, V> { Value = kv.Value, Key = kv.Key }));

        /// <summary>
        /// Send input records with the given record list on the topic, flush at the end.
        /// </summary>
        /// <param name="inputs">List of keyvalues</param>
        /// <param name="timestamp">Date of the first record</param>
        /// <param name="advance">Timespan added at the previous record to get the current timestamp</param>
        public void PipeInputs(IEnumerable<KeyValuePair<K,V>> inputs, DateTime timestamp, TimeSpan advance)
        {
            DateTime ts = timestamp;
            var records = new List<TestRecord<K, V>>();
            foreach(var i in inputs)
            {
                var r = new TestRecord<K, V>
                {
                    Key = i.Key,
                    Value = i.Value,
                    Timestamp = ts
                };
                records.Add(r);
                ts += advance;
            }

            this.PipeInputs(records);
        }

        #endregion
    }
}