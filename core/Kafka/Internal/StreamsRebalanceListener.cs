﻿using Confluent.Kafka;
using log4net;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.Processors.Internal;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Streamiz.Kafka.Net.Kafka.Internal
{
    internal class StreamsRebalanceListener : IConsumerRebalanceListener
    {
        private readonly ILog log = Logger.GetLogger(typeof(StreamsRebalanceListener));
        private readonly TaskManager manager;

        internal StreamThread Thread { get; set; }

        public StreamsRebalanceListener(TaskManager manager)
        {
            this.manager = manager;
        }

        public void PartitionsAssigned(IConsumer<byte[], byte[]> consumer, List<TopicPartition> partitions)
        {
            DateTime start = DateTime.Now;
            manager.RebalanceInProgress = true;
            manager.CreateTasks(partitions);
            Thread.SetState(ThreadState.PARTITIONS_ASSIGNED);
            manager.RebalanceInProgress = false;

            StringBuilder sb = new StringBuilder();
            sb.AppendLine($"Partition assignment took {DateTime.Now - start} ms.");
            sb.AppendLine($"\tCurrently assigned active tasks: {string.Join(",", manager.ActiveTaskIds)}");
            sb.AppendLine($"\tRevoked assigned active tasks: {string.Join(",", manager.RevokeTaskIds)}");
            log.Info(sb.ToString());
        }

        public void PartitionsRevoked(IConsumer<byte[], byte[]> consumer, List<TopicPartitionOffset> partitions)
        {
            DateTime start = DateTime.Now;
            manager.RebalanceInProgress = true;
            manager.RevokeTasks(new List<TopicPartition>(partitions.Select(p => p.TopicPartition)));
            Thread.SetState(ThreadState.PARTITIONS_REVOKED);
            manager.RebalanceInProgress = false;

            StringBuilder sb = new StringBuilder();
            sb.AppendLine($"Partition revocation took {DateTime.Now - start} ms");
            sb.AppendLine($"\tCurrent suspended active tasks: {string.Join(",", partitions.Select(p => $"{p.Topic}-{p.Partition}"))}");
            log.Info(sb.ToString());
        }
    }
}
