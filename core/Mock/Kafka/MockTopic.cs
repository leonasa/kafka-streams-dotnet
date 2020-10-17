﻿using System.Collections.Generic;
using Confluent.Kafka;

namespace Streamiz.Kafka.Net.Mock.Kafka
{
    internal class MockTopic
    {
        private readonly List<MockPartition> partitions;

        public MockTopic(string topic, int part)
        {
            Name = topic;
            PartitionNumber = part;

            partitions = new List<MockPartition>();
            for (int i = 0; i < PartitionNumber; ++i)
                partitions.Add(new MockPartition(i));
        }

        public string Name { get; }
        public int PartitionNumber { get; private set; }

        public void AddMessage(byte[] key, byte[] value, int partition)
        {
            partitions[partition].AddMessageInLog(key, value);
        }

        public TestRecord<byte[], byte[]> GetMessage(int partition, long consumerOffset)
        {
            if (partitions[partition].Size > consumerOffset)
            {
                return partitions[partition].GetMessage(consumerOffset);
            }
            else
                return null;
        }

        internal void CreateNewPartitions(Partition partition)
        {
            var diff = partition - (PartitionNumber - 1);
            for (int i = 0; i < diff; ++i)
                partitions.Add(new MockPartition(PartitionNumber - 1 + i));

            PartitionNumber = PartitionNumber + diff;
        }
    
        internal MockPartition GetPartition(int partition)
        {
            if (PartitionNumber - 1 >= partition)
                return partitions[partition];
            else
            {
                CreateNewPartitions(new Partition(partition));
                return partitions[partition];
            }
        }
    }
}
