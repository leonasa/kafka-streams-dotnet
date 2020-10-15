using System;
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
            public long OffsetCommitted { get; set; }
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
            mockedTopics.Clear();
            consumers.Clear();
            consumerGroups.Clear();
        }

        #endregion

        private readonly IDictionary<string, MockTopic> mockedTopics = new Dictionary<string, MockTopic>();
        private readonly IDictionary<string, MockConsumerInformation> consumers = new Dictionary<string, MockConsumerInformation>();
        private readonly IDictionary<string, List<string>> consumerGroups = new Dictionary<string, List<string>>();

        #region Topic Gesture

        private void CreateTopic(string topic) => CreateTopic(topic, DEFAULT_NUMBER_PARTITIONS);

        private bool CreateTopic(string topic, int partitions)
        {
            if (!mockedTopics.Values.Any(t => t.Name.Equals(topic, StringComparison.InvariantCultureIgnoreCase)))
            {
                var t = new MockTopic(topic, partitions);
                mockedTopics.Add(topic, t);
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
            var topicList = topics.ToList();
            CreateTopics(topicList);

            if (!consumers.ContainsKey(consumer.Name))
            {
                var cons = new MockConsumerInformation
                {
                    GroupId = consumer.MemberId,
                    Name = consumer.Name,
                    Consumer = consumer,
                    Topics = topicList,
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

        private void CreateTopics(IEnumerable<string> topics)
        {
            foreach (var t in topics)
            {
                CreateTopic(t);
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

        internal List<TopicPartitionOffset> Committed(MockConsumer mockConsumer)
        {
            var c = consumers[mockConsumer.Name];
            List<TopicPartitionOffset> list = new List<TopicPartitionOffset>();
            foreach (var p in c.Partitions)
            {
                var offset = c.TopicPartitionsOffset.FirstOrDefault(t => t.Topic.Equals(p.Topic) && t.Partition.Equals(p.Partition));
                if (offset != null)
                {
                    list.Add(new TopicPartitionOffset(new TopicPartition(p.Topic, p.Partition), new Offset(offset.OffsetCommitted)));
                }
            }
            return list;
        }

        internal WatermarkOffsets GetWatermarkOffsets(TopicPartition topicPartition)
        {
            var topic = mockedTopics[topicPartition.Topic];
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
                EvokeRebalance(consumer);

                if (!newPartitions.ContainsKey(consumer)) 
                    continue;
                
                var partitions = newPartitions[consumer];
                consumer.Partitions.AddRange(partitions);
                consumer.TopicPartitionsOffset.AddRange(GenerateTopicPartitionsOffset(partitions, consumer));
                consumer.RebalanceListener?.PartitionsAssigned(consumer.Consumer, consumer.Partitions);
                consumer.Assigned = true;
            }
        }

        private void EvokeRebalance(MockConsumerInformation consumer)
        {
            if (consumer.Partitions.Count <= 0) return;

            consumer.Partitions.Clear();
            var topicPartitionOffsets = consumer.TopicPartitionsOffset.Select(f => new TopicPartitionOffset(f.Topic, f.Partition, f.OffsetCommitted)).ToList();

            consumer.RebalanceListener?.PartitionsRevoked(consumer.Consumer, topicPartitionOffsets);
        }

        private IEnumerable<MockTopicPartitionOffset> GenerateTopicPartitionsOffset(List<TopicPartition> partitions, MockConsumerInformation consumer)
        {
            return partitions.Where(partition => !ConsumerHasOffSetInTopicOrPartition(consumer, partition)).Select(k => new MockTopicPartitionOffset
            {
                OffsetCommitted = 0,
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
            var topicPartitionNumber = mockedTopics[topic].PartitionNumber;
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
            var partitions = topicPartitions.ToList();
            CreateTopics(partitions.Select(p=>p.Topic));

            if (consumers.ContainsKey(mockConsumer.Name))
            {
                var consumerInformation = consumers[mockConsumer.Name];
                if (consumerInformation.Partitions.Count != 0)
                {
                    throw new StreamsException($"Consumer {mockConsumer.Name} was already assigned partitions. Please call unassigned before !");
                }

                Assign(mockConsumer, partitions, consumerInformation);
            }
            else
            {
                throw new StreamsException($"Consumer {mockConsumer.Name} unknown !");
            }
        }

        private void Assign(MockConsumer mockConsumer, List<TopicPartition> partitions, MockConsumerInformation consumerInformation)
        {
            lock (rebalanceLock)
            {
                var customerToRebalance = BuildCustomerToRebalance(mockConsumer, partitions);

                Rebalance(partitions, customerToRebalance);

                consumerInformation.Partitions = new List<TopicPartition>(partitions);

                consumerInformation.TopicPartitionsOffset = BuildMockTopicPartitionOffsets(partitions, consumerInformation).ToList();

                consumerInformation.RebalanceListener?.PartitionsAssigned(consumerInformation.Consumer, consumerInformation.Partitions);
                consumerInformation.Assigned = true;
            }
        }

        private IEnumerable<MockTopicPartitionOffset> BuildMockTopicPartitionOffsets(IEnumerable<TopicPartition> partitions, MockConsumerInformation consumerInformation)
        {
            foreach (var partition in partitions)
            {
                if (!consumerInformation.TopicPartitionsOffset.Any(m => m.Topic.Equals(partition.Topic) && m.Partition.Equals(partition.Partition)))
                {
                    yield return new MockTopicPartitionOffset
                    {
                        OffsetCommitted = 0,
                        OffsetConsumed = 0,
                        Partition = partition.Partition,
                        Topic = partition.Topic
                    };
                }
            }
        }

        private void Rebalance(List<TopicPartition> partitions, List<MockConsumerInformation> customerToRebalance)
        {
            foreach (var mockConsumerInformation in customerToRebalance)
            {
                var parts = mockConsumerInformation.Partitions.Join(partitions, t => t, t => t, (t1, t2) => t1);
                var topicPartitionOffsets = mockConsumerInformation.TopicPartitionsOffset.Select(f => new TopicPartitionOffset(f.Topic, f.Partition, f.OffsetCommitted)).ToList();
                mockConsumerInformation.RebalanceListener?.PartitionsRevoked(mockConsumerInformation.Consumer, topicPartitionOffsets);
                foreach (var j in parts)
                {
                    mockConsumerInformation.Partitions.Remove(j);
                }

                mockConsumerInformation.RebalanceListener?.PartitionsAssigned(mockConsumerInformation.Consumer, mockConsumerInformation.Partitions);
                mockConsumerInformation.Assigned = true;
            }
        }

        private List<MockConsumerInformation> BuildCustomerToRebalance(MockConsumer mockConsumer, List<TopicPartition> partitions)
        {
            List<MockConsumerInformation> customerToRebalance = new List<MockConsumerInformation>();

            foreach (var p in partitions)
            {
                var info = consumers.Select(kp => kp.Value).FirstOrDefault(i => i.GroupId.Equals(mockConsumer.MemberId) && i.Partitions.Contains(p));
                if (info != null && !customerToRebalance.Contains(info))
                {
                    customerToRebalance.Add(info);
                }
            }

            return customerToRebalance;
        }

        internal void Unassign(MockConsumer mockConsumer)
        {
            if (consumers.ContainsKey(mockConsumer.Name))
            {
                lock (rebalanceLock)
                {
                    var consumer = consumers[mockConsumer.Name];
                    var otherConsumers = OtherConsumerInTheSameGroup(mockConsumer);
                    if (otherConsumers.Count > 0)
                    {
                        RebalanceOtherConsumerInTheSameGroup(consumer, otherConsumers);
                    }

                    consumer.Partitions.Clear();
                    consumer.Assigned = false;
                }
            }
            else
            {
                throw new StreamsException($"Consumer {mockConsumer.Name} unknown !");
            }
        }

        private void RebalanceOtherConsumerInTheSameGroup(MockConsumerInformation consumer, List<MockConsumerInformation> otherConsumers)
        {
            var topicPartitionOffsets = consumer.TopicPartitionsOffset.Select(f => new TopicPartitionOffset(f.Topic, f.Partition, f.OffsetCommitted)).ToList();
            consumer.RebalanceListener?.PartitionsRevoked(consumer.Consumer, topicPartitionOffsets);

            int partEach = consumer.Partitions.Count / otherConsumers.Count;
            int modulo = consumer.Partitions.Count % otherConsumers.Count;

            int j = 0;
            for (int i = 0; i < otherConsumers.Count; ++i)
            {
                var partitions = ExtractTopicPartitions(i, otherConsumers, consumer, j, partEach, modulo);

                otherConsumers[i].Partitions.AddRange(partitions);
                otherConsumers[i].TopicPartitionsOffset = BuildTopicPartitionsOffset(partitions, otherConsumers[i]).ToList();
                otherConsumers[i].RebalanceListener?.PartitionsAssigned(otherConsumers[i].Consumer, otherConsumers[i].Partitions);
                otherConsumers[i].Assigned = true;

                j += partEach;
            }
        }

        private IEnumerable<MockTopicPartitionOffset> BuildTopicPartitionsOffset(List<TopicPartition> partitions, MockConsumerInformation mockConsumerInformation)
        {
            foreach (var partition in partitions)
            {
                if (!mockConsumerInformation.TopicPartitionsOffset.Any(m => m.Topic.Equals(partition.Topic) && m.Partition.Equals(partition.Partition)))
                {
                    yield return new MockTopicPartitionOffset
                    {
                        OffsetCommitted = 0,
                        OffsetConsumed = 0,
                        Partition = partition.Partition,
                        Topic = partition.Topic
                    };
                }
            }
        }

        private static List<TopicPartition> ExtractTopicPartitions(int i, List<MockConsumerInformation> otherConsumers, MockConsumerInformation consumer, int j, int partEach, int modulo)
        {
            return i == otherConsumers.Count - 1 ? consumer.Partitions.GetRange(j, j + partEach + modulo) : consumer.Partitions.GetRange(j, j + partEach);
        }

        private List<MockConsumerInformation> OtherConsumerInTheSameGroup(MockConsumer mockConsumer)
        {
            return consumerGroups[mockConsumer.MemberId].Where(i => consumers.ContainsKey(i)).Select(i => consumers[i]).Where(i => !i.Name.Equals(mockConsumer.Name)).ToList();
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
                    p.OffsetCommitted = p.OffsetConsumed;
                }

                return c.TopicPartitionsOffset.Select(t => new TopicPartitionOffset(new TopicPartition(t.Topic, t.Partition), t.OffsetCommitted)).ToList();
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
                        p.OffsetCommitted = o.Offset.Value;
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
            CreateTopics(mockConsumer.Subscription);

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

                        var topic = mockedTopics[p.Topic];
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
            var i = RandomNumberGenerator.GetInt32(0, mockedTopics[topic].PartitionNumber);
            mockedTopics[topic].AddMessage(message.Key, message.Value, i);

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
            DeliveryReport<byte[], byte[]> r = new DeliveryReport<byte[], byte[]> {Status = PersistenceStatus.NotPersisted};
            CreateTopic(topicPartition.Topic);
            if (mockedTopics[topicPartition.Topic].PartitionNumber > topicPartition.Partition)
            {
                mockedTopics[topicPartition.Topic].AddMessage(message.Key, message.Value, topicPartition.Partition);
                r.Status = PersistenceStatus.Persisted;
            }
            else
            {
                mockedTopics[topicPartition.Topic].CreateNewPartitions(topicPartition.Partition);
                mockedTopics[topicPartition.Topic].AddMessage(message.Key, message.Value, topicPartition.Partition);
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
