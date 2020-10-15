﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Threading;
using Confluent.Kafka;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Kafka;
using static Streamiz.Kafka.Net.Mock.Kafka.MockConsumerInformation;

namespace Streamiz.Kafka.Net.Mock.Kafka
{
    internal class MockConsumerInformation
    {
        internal class MockTopicPartitionOffset
        {
            public string Topic { get; set; }
            public int Partition { get; set; }
            public long OffsetComitted { get; set; }
            public long OffsetConsumed { get; set; }

            public override bool Equals(object obj)
            {
                return obj is MockTopicPartitionOffset &&
                    ((MockTopicPartitionOffset)obj).Topic.Equals(Topic) &&
                    ((MockTopicPartitionOffset)obj).Partition.Equals(Partition);
            }

            public override int GetHashCode()
            {
                return Topic.GetHashCode() & Partition.GetHashCode() ^ 33333;
            }
        }

        public string GroupId { get; set; }
        public string Name { get; set; }
        public List<TopicPartition> Partitions { get; set; }
        public List<string> Topics { get; set; }
        public MockConsumer Consumer { get; set; }
        public IConsumerRebalanceListener RebalanceListener { get; set; }
        public bool Assigned { get; set; }
        public List<MockTopicPartitionOffset> TopicPartitionsOffset { get; set; }

        public override int GetHashCode()
        {
            return Name.GetHashCode();
        }

        public override bool Equals(object obj)
        {
            return obj is MockConsumerInformation && ((MockConsumerInformation)obj).Name.Equals(Name);
        }
    }

    internal class MockCluster
    {
        private readonly object rebalanceLock = new object();
        private readonly int DEFAULT_NUMBER_PARTITIONS;

        #region Ctor

        public MockCluster(int defaultNumberPartitions = 1)
        {
            DEFAULT_NUMBER_PARTITIONS = defaultNumberPartitions;
        }

        public void Destroy()
        {
            topics.Clear();
            consumers.Clear();
            consumerGroups.Clear();
        }

        #endregion

        private readonly IDictionary<string, MockTopic> topics = new Dictionary<string, MockTopic>();
        private readonly IDictionary<string, MockConsumerInformation> consumers = new Dictionary<string, MockConsumerInformation>();
        private readonly IDictionary<string, List<string>> consumerGroups = new Dictionary<string, List<string>>();

        #region Topic Gesture

        private void CreateTopic(string topic) => CreateTopic(topic, DEFAULT_NUMBER_PARTITIONS);

        private bool CreateTopic(string topic, int partitions)
        {
            if (!topics.Values.Any(t => t.Name.Equals(topic, StringComparison.InvariantCultureIgnoreCase)))
            {
                var t = new MockTopic(topic, partitions);
                topics.Add(topic, t);
                return true;
            }
            return false;
        }

        internal void CloseConsumer(string name)
        {
            if (consumers.ContainsKey(name))
            {
                Unsubscribe(consumers[name].Consumer);
                consumers.Remove(name);
            }
        }

        internal void SubscribeTopic(MockConsumer consumer, IEnumerable<string> topics)
        {
            foreach (var t in topics)
            {
                CreateTopic(t);
            }

            if (!consumers.ContainsKey(consumer.Name))
            {
                var cons = new MockConsumerInformation
                {
                    GroupId = consumer.MemberId,
                    Name = consumer.Name,
                    Consumer = consumer,
                    Topics = new List<string>(topics),
                    RebalanceListener = consumer.Listener,
                    Partitions = new List<TopicPartition>(),
                    TopicPartitionsOffset = new List<MockTopicPartitionOffset>()
                };
                consumers.Add(consumer.Name, cons);

                if (consumerGroups.ContainsKey(consumer.MemberId))
                {
                    consumerGroups[consumer.MemberId].Add(consumer.Name);
                }
                else
                {
                    consumerGroups.Add(consumer.MemberId, new List<string> { consumer.Name });
                }
            }
            else
            {
                throw new StreamsException($"Client {consumer.Name} already subscribe topic. Please call unsucribe before");
            }
        }

        internal void Unsubscribe(MockConsumer mockConsumer)
        {
            if (consumers.ContainsKey(mockConsumer.Name))
            {
                var c = consumers[mockConsumer.Name];
                Unassign(mockConsumer);
                c.Topics.Clear();
            }
        }

        #endregion

        #region Partitions Gesture

        internal List<TopicPartitionOffset> Comitted(MockConsumer mockConsumer)
        {
            var c = consumers[mockConsumer.Name];
            List<TopicPartitionOffset> list = new List<TopicPartitionOffset>();
            foreach (var p in c.Partitions)
            {
                var offset = c.TopicPartitionsOffset.FirstOrDefault(t => t.Topic.Equals(p.Topic) && t.Partition.Equals(p.Partition));
                if (offset != null)
                {
                    list.Add(new TopicPartitionOffset(new TopicPartition(p.Topic, p.Partition), new Offset(offset.OffsetComitted)));
                }
            }
            return list;
        }

        internal WatermarkOffsets GetWatermarkOffsets(TopicPartition topicPartition)
        {
            var topic = topics[topicPartition.Topic];
            var p = topic.GetPartition(topicPartition.Partition);
            return new WatermarkOffsets(new Offset(p.LowOffset), new Offset(p.HighOffset));
        }

        private IEnumerable<TopicPartition> Generate(string topic, int start, int number)
        {
            for (int i = start; i < start + number; ++i)
            {
                yield return new TopicPartition(topic, i);
            }
        }

        private void NeedRebalance()
        {
            foreach (var group in consumerGroups.Keys)
            {
                var map = GenerateMap(group);

                if (map.All(kv => kv.Key.Assigned)) 
                    continue;
                
                var newPartitions = BuildNewPartitions(map);

                Rebalance(map, newPartitions);
            }
        }

        private void Rebalance(Dictionary<MockConsumerInformation, List<TopicPartition>> map, Dictionary<MockConsumerInformation, List<TopicPartition>> newPartitions)
        {
            foreach (var consumer in map.Select(kv => kv.Key))
            {
                if (consumer.Partitions.Count > 0)
                {
                    consumer.Partitions.Clear();
                    var pList = consumer.TopicPartitionsOffset.Select(f => new TopicPartitionOffset(f.Topic, f.Partition, f.OffsetComitted)).ToList();
                    consumer.RebalanceListener?.PartitionsRevoked(consumer.Consumer, pList);
                }

                if (!newPartitions.ContainsKey(consumer)) 
                    continue;
                
                var partitions = newPartitions[consumer];
                consumer.Partitions.AddRange(partitions);
                consumer.TopicPartitionsOffset.AddRange(GenerateTopicPartitionsOffset(partitions, consumer));
                consumer.RebalanceListener?.PartitionsAssigned(consumer.Consumer, consumer.Partitions);
                consumer.Assigned = true;
            }
        }

        private IEnumerable<MockTopicPartitionOffset> GenerateTopicPartitionsOffset(List<TopicPartition> partitions, MockConsumerInformation consumer)
        {
            return partitions.Where(partition => !ConsumerHasOffSetInTopicOrPartition(consumer, partition))
                            .Select(k => new MockTopicPartitionOffset
            {
                OffsetComitted = 0,
                OffsetConsumed = 0,
                Partition = k.Partition,
                Topic = k.Topic
            });
        }

        private bool ConsumerHasOffSetInTopicOrPartition(MockConsumerInformation consumer, TopicPartition k)
        {
            return consumer.TopicPartitionsOffset.Any(partitionOffset => partitionOffset.Topic.Equals(k.Topic) && partitionOffset.Partition.Equals(k.Partition));
        }

        private Dictionary<MockConsumerInformation, List<TopicPartition>>  BuildNewPartitions(Dictionary<MockConsumerInformation, List<TopicPartition>> map)
        {
            Dictionary<MockConsumerInformation, List<TopicPartition>> newPartitions = new Dictionary<MockConsumerInformation, List<TopicPartition>>();
            var allTopics = map.SelectMany(kv => kv.Value).Select(t => t.Topic).Distinct().Union(map.SelectMany(kv => kv.Key.Topics)).Distinct().ToList();

            foreach (var topic in allTopics)
            {
                BuildPartitionsToTopics(map, topic, newPartitions);
            }

            return newPartitions;
        }

        private void BuildPartitionsToTopics(Dictionary<MockConsumerInformation, List<TopicPartition>> map, string topic, Dictionary<MockConsumerInformation, List<TopicPartition>> newPartitions)
        {
            var topicPartitionNumber = topics[topic].PartitionNumber;
            var numberConsumerSub = map.Count(kv => kv.Key.Topics.Contains(topic));
            int numbPartEach = topicPartitionNumber / numberConsumerSub;
            int modulo = topicPartitionNumber % numberConsumerSub;

            int j = 0, i = 0;

            foreach (var consumer in map.Where(kv => kv.Key.Topics.Contains(topic)).Select(kv => kv.Key))
            {
                var parts = BuildTopicPartitions(i, numberConsumerSub, topic, j, numbPartEach, modulo).ToList();

                if (newPartitions.ContainsKey(consumer))
                    newPartitions[consumer].AddRange(parts);
                else
                    newPartitions.Add(consumer, parts);

                ++i;
                j += numbPartEach;
            }
        }

        private IEnumerable<TopicPartition> BuildTopicPartitions(int i, int numberConsumerSub, string topic, int j, int numbPartEach, int modulo)
        {
            if (i == numberConsumerSub - 1)
                return Generate(topic, j, numbPartEach + modulo);
            
            return Generate(topic, j, numbPartEach);
        }

        private Dictionary<MockConsumerInformation, List<TopicPartition>> GenerateMap(string group)
        {
            var map = new Dictionary<MockConsumerInformation, List<TopicPartition>>();
            var cons = consumerGroups[group];
            foreach (var c in cons)
            {
                var consumer = consumers[c];
                map.Add(consumer, consumer.Partitions);
            }

            return map;
        }

        internal void Assign(MockConsumer mockConsumer, IEnumerable<TopicPartition> topicPartitions)
        {
            foreach (var t in topicPartitions)
            {
                CreateTopic(t.Topic);
            }

            if (consumers.ContainsKey(mockConsumer.Name))
            {
                var c = consumers[mockConsumer.Name];
                if (c.Partitions.Count == 0)
                {
                    lock (rebalanceLock)
                    {
                        List<MockConsumerInformation> customerToRebalance = new List<MockConsumerInformation>();

                        foreach (var p in topicPartitions)
                        {
                            var info = consumers.Select(kp => kp.Value)
                                .Where(i => i.GroupId.Equals(mockConsumer.MemberId))
                                .FirstOrDefault(i => i.Partitions.Contains(p));
                            if (info != null && !customerToRebalance.Contains(info))
                            {
                                customerToRebalance.Add(info);
                            }
                        }

                        foreach (var cus in customerToRebalance)
                        {
                            var parts = cus.Partitions.Join(topicPartitions, t => t, t => t, (t1, t2) => t1);
                            var pList = cus.TopicPartitionsOffset.Select(f => new TopicPartitionOffset(f.Topic, f.Partition, f.OffsetComitted)).ToList();
                            cus.RebalanceListener?.PartitionsRevoked(cus.Consumer, pList);
                            foreach (var j in parts)
                            {
                                cus.Partitions.Remove(j);
                            }

                            cus.RebalanceListener?.PartitionsAssigned(cus.Consumer, cus.Partitions);
                            cus.Assigned = true;
                        }

                        c.Partitions = new List<TopicPartition>(topicPartitions);
                        foreach (var k in topicPartitions)
                        {
                            if (!c.TopicPartitionsOffset.Any(m => m.Topic.Equals(k.Topic) && m.Partition.Equals(k.Partition)))
                            {
                                c.TopicPartitionsOffset.Add(new MockTopicPartitionOffset
                                {
                                    OffsetComitted = 0,
                                    OffsetConsumed = 0,
                                    Partition = k.Partition,
                                    Topic = k.Topic
                                });
                            }
                        }
                        c.RebalanceListener?.PartitionsAssigned(c.Consumer, c.Partitions);
                        c.Assigned = true;
                    }
                }
                else
                {
                    throw new StreamsException($"Consumer {mockConsumer.Name} was already assigned partitions. Please call unassigne before !");
                }
            }
            else
            {
                throw new StreamsException($"Consumer {mockConsumer.Name} unknown !");
            }
        }

        internal void Unassign(MockConsumer mockConsumer)
        {
            if (consumers.ContainsKey(mockConsumer.Name))
            {
                lock (rebalanceLock)
                {
                    var c = consumers[mockConsumer.Name];
                    var pList = c.TopicPartitionsOffset.Select(f => new TopicPartitionOffset(f.Topic, f.Partition, f.OffsetComitted)).ToList();
                    c.RebalanceListener?.PartitionsRevoked(c.Consumer, pList);

                    // Rebalance on other consumer in the same group
                    var otherConsumers = consumerGroups[mockConsumer.MemberId].Where(i => consumers.ContainsKey(i)).Select(i => consumers[i]).Where(i => !i.Name.Equals(mockConsumer.Name)).ToList();
                    if (otherConsumers.Count > 0)
                    {
                        int partEach = c.Partitions.Count / otherConsumers.Count;
                        int modulo = c.Partitions.Count % otherConsumers.Count;

                        int j = 0;
                        for (int i = 0; i < otherConsumers.Count; ++i)
                        {
                            List<TopicPartition> parts = null;
                            if (i == otherConsumers.Count - 1)
                            {
                                parts = c.Partitions.GetRange(j, j + partEach + modulo);
                            }
                            else
                            {
                                parts = c.Partitions.GetRange(j, j + partEach);
                            }

                            otherConsumers[i].Partitions.AddRange(parts);
                            foreach (var k in parts)
                            {
                                if (!otherConsumers[i].TopicPartitionsOffset.Any(m => m.Topic.Equals(k.Topic) && m.Partition.Equals(k.Partition)))
                                {
                                    otherConsumers[i].TopicPartitionsOffset.Add(new MockTopicPartitionOffset
                                    {
                                        OffsetComitted = 0,
                                        OffsetConsumed = 0,
                                        Partition = k.Partition,
                                        Topic = k.Topic
                                    });
                                }
                            }

                            otherConsumers[i].RebalanceListener?.PartitionsAssigned(otherConsumers[i].Consumer, otherConsumers[i].Partitions);
                            otherConsumers[i].Assigned = true;
                            j += partEach;
                        }
                    }

                    c.Partitions.Clear();
                    c.Assigned = false;
                }
            }
            else
            {
                throw new StreamsException($"Consumer {mockConsumer.Name} unknown !");
            }
        }

        #endregion

        #region Consumer (Read + Commit) Gesture

        internal List<TopicPartitionOffset> Commit(MockConsumer mockConsumer)
        {
            if (consumers.ContainsKey(mockConsumer.Name))
            {
                var c = consumers[mockConsumer.Name];
                foreach (var p in c.TopicPartitionsOffset)
                {
                    p.OffsetComitted = p.OffsetConsumed;
                }

                return c.TopicPartitionsOffset.Select(t => new TopicPartitionOffset(new TopicPartition(t.Topic, t.Partition), t.OffsetComitted)).ToList();
            }

            throw new StreamsException($"Consumer {mockConsumer.Name} unknown !");
        }

        internal void Commit(MockConsumer mockConsumer, IEnumerable<TopicPartitionOffset> offsets)
        {
            if (consumers.ContainsKey(mockConsumer.Name))
            {
                var c = consumers[mockConsumer.Name];
                foreach (var o in offsets)
                {
                    var p = c.TopicPartitionsOffset.FirstOrDefault(t => t.Topic.Equals(o.Topic) && t.Partition.Equals(o.Partition));
                    if (p != null)
                    {
                        p.OffsetConsumed = o.Offset.Value;
                        p.OffsetComitted = o.Offset.Value;
                    }
                }
            }
            else
            {
                throw new StreamsException($"Consumer {mockConsumer.Name} unknown !");
            }
        }

        internal ConsumeResult<byte[], byte[]> Consume(MockConsumer mockConsumer, TimeSpan timeout)
        {
            foreach (var t in mockConsumer.Subscription)
            {
                CreateTopic(t);
            }

            DateTime dt = DateTime.Now;
            ConsumeResult<byte[], byte[]> result = null;
            if (consumers.ContainsKey(mockConsumer.Name))
            {
                var c = consumers[mockConsumer.Name];
                if (!c.Assigned)
                {
                    lock (rebalanceLock)
                    {
                        NeedRebalance();
                    }
                }

                lock (rebalanceLock)
                {
                    foreach (var p in c.Partitions)
                    {
                        if (timeout != TimeSpan.Zero && (dt + timeout) < DateTime.Now)
                        {
                            break;
                        }

                        var topic = topics[p.Topic];
                        var offset = c.TopicPartitionsOffset.FirstOrDefault(t => t.Topic.Equals(p.Topic) && t.Partition.Equals(p.Partition));
                        var record = topic.GetMessage(p.Partition, offset.OffsetConsumed);
                        if (record != null)
                        {
                            result = new ConsumeResult<byte[], byte[]>
                            {
                                Offset = offset.OffsetConsumed,
                                Topic = p.Topic,
                                Partition = p.Partition,
                                Message = new Message<byte[], byte[]> { Key = record.Key, Value = record.Value }
                            };
                            ++offset.OffsetConsumed;
                            break;
                        }
                    }
                }

                return result;
            }

            throw new StreamsException($"Consumer {mockConsumer.Name} unknown !");
        }

        internal ConsumeResult<byte[], byte[]> Consume(MockConsumer mockConsumer, CancellationToken cancellationToken)
            => Consume(mockConsumer, TimeSpan.FromSeconds(10));

        #endregion

        #region Producer Gesture

        internal DeliveryReport<byte[], byte[]> Produce(string topic, Message<byte[], byte[]> message)
        {
            DeliveryReport<byte[], byte[]> r = new DeliveryReport<byte[], byte[]>();

            CreateTopic(topic);

            // TODO : implement hashpartitionumber
            var i = RandomNumberGenerator.GetInt32(0, topics[topic].PartitionNumber);
            topics[topic].AddMessage(message.Key, message.Value, i);

            r.Message = message;
            r.Partition = i;
            r.Topic = topic;
            r.Timestamp = new Timestamp(DateTime.Now);
            r.Error = new Error(ErrorCode.NoError);
            r.Status = PersistenceStatus.Persisted;
            return r;
        }

        internal DeliveryReport<byte[], byte[]> Produce(TopicPartition topicPartition, Message<byte[], byte[]> message)
        {
            DeliveryReport<byte[], byte[]> r = new DeliveryReport<byte[], byte[]>();
            r.Status = PersistenceStatus.NotPersisted;
            CreateTopic(topicPartition.Topic);
            if (topics[topicPartition.Topic].PartitionNumber > topicPartition.Partition)
            {
                topics[topicPartition.Topic].AddMessage(message.Key, message.Value, topicPartition.Partition);
                r.Status = PersistenceStatus.Persisted;
            }
            else
            {
                topics[topicPartition.Topic].CreateNewPartitions(topicPartition.Partition);
                topics[topicPartition.Topic].AddMessage(message.Key, message.Value, topicPartition.Partition);
                r.Status = PersistenceStatus.Persisted;
            }
            r.Message = message;
            r.Partition = topicPartition.Partition;
            r.Topic = topicPartition.Topic;
            r.Timestamp = new Timestamp(DateTime.Now);
            r.Error = new Error(ErrorCode.NoError);
            r.Status = PersistenceStatus.Persisted;
            // TODO r.Offset
            return r;
        }

        #endregion
    }
}
