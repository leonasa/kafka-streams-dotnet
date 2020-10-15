﻿using Confluent.Kafka;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Kafka;
using System;
using System.Collections.Generic;
using System.Threading;

namespace Streamiz.Kafka.Net.Mock.Kafka
{
    internal class MockConsumer : IConsumer<byte[], byte[]>
    {
        private readonly string groupId;
        private readonly string clientId;
        private readonly MockCluster cluster;

        public MockConsumer(MockCluster cluster, string groupId, string clientId)
        {
            this.cluster = cluster;
            this.groupId = groupId;
            this.clientId = clientId;
        }

        public IConsumerRebalanceListener Listener { get; private set; }

        #region IConsumer Impl

        public string MemberId => groupId;

        public List<TopicPartition> Assignment { get; } = new List<TopicPartition>();

        public List<string> Subscription { get; } = new List<string>();

        public IConsumerGroupMetadata ConsumerGroupMetadata => null;

        public Handle Handle => null;

        public string Name => clientId;

        public int AddBrokers(string brokers) => 0;

        public void Assign(TopicPartition partition)
        {
            cluster.Assign(this, new List<TopicPartition> { partition });
        }

        public void Assign(TopicPartitionOffset partition)
        {
            // TODO
        }

        public void Assign(IEnumerable<TopicPartitionOffset> partitions)
        {
            // TODO
        }

        public void Assign(IEnumerable<TopicPartition> partitions)
        {
            cluster.Assign(this, partitions);
        }

        public void Close()
        {
            cluster.CloseConsumer(Name);
            Assignment.Clear();
            Subscription.Clear();
        }

        public List<TopicPartitionOffset> Commit()
        {
            return cluster.Commit(this);
        }

        public void Commit(IEnumerable<TopicPartitionOffset> offsets)
        {
            cluster.Commit(this, offsets);
        }

        public void Commit(ConsumeResult<byte[], byte[]> result)
            => Commit(new List<TopicPartitionOffset> { new TopicPartitionOffset(result.TopicPartition, result.Offset + 1) });

        public List<TopicPartitionOffset> Committed(TimeSpan timeout)
        {
            return cluster.Comitted(this);
        }

        public List<TopicPartitionOffset> Committed(IEnumerable<TopicPartition> partitions, TimeSpan timeout)
        {
            // TODO : 
            throw new NotImplementedException();
        }

        public void Dispose()
        {
            Close();
        }

        public WatermarkOffsets GetWatermarkOffsets(TopicPartition topicPartition)
        {
            return cluster.GetWatermarkOffsets(topicPartition);
        }

        public List<TopicPartitionOffset> OffsetsForTimes(IEnumerable<TopicPartitionTimestamp> timestampsToSearch, TimeSpan timeout)
        {
            // TODO : 
            throw new NotImplementedException();
        }

        public void Pause(IEnumerable<TopicPartition> partitions)
        {
            // TODO : 
        }

        public Offset Position(TopicPartition partition)
        {
            // TODO
            throw new NotImplementedException();
        }

        public WatermarkOffsets QueryWatermarkOffsets(TopicPartition topicPartition, TimeSpan timeout)
        {
            // TODO
            throw new NotImplementedException();
        }

        public void Resume(IEnumerable<TopicPartition> partitions)
        {
            // TODO
            throw new NotImplementedException();
        }

        public void Seek(TopicPartitionOffset tpo)
        {
            // TODO
            throw new NotImplementedException();
        }

        public void StoreOffset(TopicPartitionOffset offset)
        {
            // TODO
            throw new NotImplementedException();
        }

        public void StoreOffset(ConsumeResult<byte[], byte[]> result)
        {
            // TODO
            throw new NotImplementedException();
        }

        public void Subscribe(IEnumerable<string> topics)
        {
            cluster.SubscribeTopic(this, topics);
            Subscription.AddRange(topics);
        }

        public void Subscribe(string topic)
        {
            cluster.SubscribeTopic(this, new List<string> { topic });
            Subscription.Add(topic);
        }

        public void Unassign()
        {
            cluster.Unassign(this);
            Assignment.Clear();
        }

        public void Unsubscribe()
        {
            cluster.Unsubscribe(this);
            Subscription.Clear();
        }

        public ConsumeResult<byte[], byte[]> Consume(int millisecondsTimeout)
            => Consume(TimeSpan.FromMilliseconds(millisecondsTimeout));

        public ConsumeResult<byte[], byte[]> Consume(CancellationToken cancellationToken = default)
        {
            if (Subscription.Count == 0)
                throw new StreamsException("No subscription have been done !");

            return cluster.Consume(this, cancellationToken);
        }

        public ConsumeResult<byte[], byte[]> Consume(TimeSpan timeout)
        {
            if (Subscription.Count == 0)
                throw new StreamsException("No subscription have been done !");

            return cluster.Consume(this, timeout);
        }

        #endregion

        internal void SetRebalanceListener(IConsumerRebalanceListener rebalanceListener)
        {
            Listener = new MockWrappedConsumerRebalanceListener(rebalanceListener, this);
        }

        public void PartitionsAssigned(List<TopicPartition> partitions)
        {
            Assignment.Clear();
            Assignment.AddRange(partitions);
        }

        public void PartitionsRevoked(List<TopicPartitionOffset> partitions)
        {
            foreach (var p in partitions)
                if (Assignment.Contains(p.TopicPartition))
                    Assignment.Remove(p.TopicPartition);
        }
    }
}