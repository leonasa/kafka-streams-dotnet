using Confluent.Kafka;
using log4net;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.Stream.Internal;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Streamiz.Kafka.Net.Processors
{
    internal abstract class AbstractTask : ITask
    {
        protected readonly IStreamConfig configuration;
        protected readonly IConsumer<byte[], byte[]> consumer;
        protected bool commitNeeded = false;
        protected readonly IStateManager stateMgr;
        protected ILog log;
        protected readonly string logPrefix;

        protected AbstractTask(TaskId id, IEnumerable<TopicPartition> partition, ProcessorTopology topology, IConsumer<byte[], byte[]> consumer, IStreamConfig config)
        {
            log = Logger.GetLogger(GetType());
            logPrefix = $"stream-task[{id.Id}|{id.Partition}] ";

            var topicPartitions = partition.ToList();

            Partition = topicPartitions;
            Id = id;
            Topology = topology;

            this.consumer = consumer;
            configuration = config;

            stateMgr = new ProcessorStateManager(id, topicPartitions);
        }

        public ProcessorTopology Topology { get; }

        public ProcessorContext Context { get; protected set; }

        public TaskId Id { get; }

        public IEnumerable<TopicPartition> Partition { get; }

        public ICollection<TopicPartition> ChangelogPartitions { get; internal set; }

        public bool HasStateStores => Topology.StateStores.Count != 0;

        public string ApplicationId => configuration.ApplicationId;

        public bool CommitNeeded => commitNeeded;

        public bool IsClosed { get; protected set; }

        public void RequestCommit()
        {
        }

        #region Abstract

        public abstract PartitionGrouper Grouper { get; }
        public abstract bool CanProcess(long now);
        public abstract void Close();
        public abstract void Commit();
        public abstract IStateStore GetStore(string name);
        public abstract void InitializeTopology();
        public abstract bool InitializeStateStores();
        public abstract void Resume();
        public abstract void Suspend();

        #endregion

        protected void RegisterStateStores()
        {
            if (!Topology.StateStores.Any() && !Topology.GlobalStateStores.Any())
            {
                return;
            }

            log.Debug($"{logPrefix}Initializing state stores");

            foreach (var (key, store) in Topology.StateStores)
            {
                log.Debug($"{logPrefix}Initializing store {key}");
                store.Init(Context, store);
            }

            foreach (var (key, store) in Topology.GlobalStateStores.Where(k => !Topology.StateStores.ContainsKey(k.Key)))
            {
                log.Debug($"{logPrefix}Initializing store {key}");
                store.Init(Context, store);
            }
        }

        protected virtual void FlushState()
        {
            try
            {
                stateMgr.Flush();
            }
            catch (Exception e)
            {
                log.Error($"{logPrefix}Error during flush state store with exception :", e);
                throw;
            }
        }

        protected void CloseStateManager()
        {
            log.Debug($"{logPrefix}Closing state manager");
            try
            {
                stateMgr.Close();
            }
            catch (Exception e)
            {
                log.Error($"{logPrefix}Error during closing state store with exception :", e);
                throw;
            }
        }
    }
}
