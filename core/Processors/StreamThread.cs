﻿using Confluent.Kafka;
using log4net;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Kafka;
using Streamiz.Kafka.Net.Kafka.Internal;
using Streamiz.Kafka.Net.Processors.Internal;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace Streamiz.Kafka.Net.Processors
{
    internal class StreamThread : IThread, IDisposable
    {
        #region Static 

        public static string GetTaskProducerClientId(string threadClientId, TaskId taskId)
        {
            return threadClientId + "-" + taskId + "-producer";
        }

        public static string GetThreadProducerClientId(string threadClientId)
        {
            return threadClientId + "-producer";
        }

        public static string GetConsumerClientId(string threadClientId)
        {
            return threadClientId + "-consumer";
        }

        public static string GetRestoreConsumerClientId(string threadClientId)
        {
            return threadClientId + "-restore-consumer";
        }

        // currently admin client is shared among all threads
        public static string GetSharedAdminClientId(string clientId)
        {
            return clientId + "-admin";
        }

        internal static IThread Create(string threadId, string clientId, InternalTopologyBuilder builder, IStreamConfig configuration, IKafkaSupplier kafkaSupplier, IAdminClient adminClient, int threadInd)
        {
            string logPrefix = $"stream-thread[{threadId}] ";
            var log = Logger.GetLogger(typeof(StreamThread));
            var customerID = $"{clientId}-StreamThread-{threadInd}";
            IProducer<byte[], byte[]> producer = null;

            // Due to limitations outlined in KIP-447 (which KIP-447 overcomes), it is
            // currently necessary to use a separate producer per input partition. The
            // producerState dictionary is used to keep track of these, and the current
            // consumed offset.
            // https://cwiki.apache.org/confluence/display/KAFKA/KIP-447%3A+Producer+scalability+for+exactly+once+semantics
            // IF Guarantee is AT_LEAST_ONCE, producer is the same of all StreamTasks in this thread, 
            // ELSE one producer by StreamTask.
            if (configuration.Guarantee == ProcessingGuarantee.AT_LEAST_ONCE)
            {
                log.Info($"{logPrefix}Creating shared producer client");
                producer = kafkaSupplier.GetProducer(configuration.ToProducerConfig(GetThreadProducerClientId(threadId)));
            }

            var taskCreator = new TaskCreator(builder, configuration, threadId, kafkaSupplier, producer);
            var manager = new TaskManager(builder, taskCreator, adminClient);

            var listener = new StreamsRebalanceListener(manager);

            log.Info($"{logPrefix}Creating consumer client");
            var consumer = kafkaSupplier.GetConsumer(configuration.ToConsumerConfig(customerID), listener);
            manager.Consumer = consumer;

            var thread = new StreamThread(threadId, manager, consumer, builder, configuration);
            listener.Thread = thread;

            return thread;
        }

        #endregion

        public ThreadState State { get; private set; }

        private readonly IStreamConfig streamConfig;
        private readonly ILog log = Logger.GetLogger(typeof(StreamThread));
        private readonly Thread thread;
        private readonly IConsumer<byte[], byte[]> consumer;
        private readonly TaskManager manager;
        private readonly InternalTopologyBuilder builder;
        private readonly TimeSpan consumeTimeout;
        private readonly string logPrefix = "";
        private readonly long commitTimeMs = 0;
        private CancellationToken token;
        private DateTime lastCommit = DateTime.Now;
        
        private int numIterations = 1;
        private long lastPollMs;

        private readonly object stateLock = new object();

        public event ThreadStateListener StateChanged;

        private StreamThread(string threadId, TaskManager manager, IConsumer<byte[], byte[]> consumer, InternalTopologyBuilder builder, IStreamConfig configuration)
            : this(threadId, manager, consumer, builder, TimeSpan.FromMilliseconds(configuration.PollMs), configuration.CommitIntervalMs)

        {
            streamConfig = configuration;
        }

        private StreamThread(string threadId, TaskManager manager, IConsumer<byte[], byte[]> consumer, InternalTopologyBuilder builder, TimeSpan timeSpan, long commitInterval)
        {
            this.manager = manager;
            this.consumer = consumer;
            this.builder = builder;
            consumeTimeout = timeSpan;
            logPrefix = $"stream-thread[{threadId}] ";
            commitTimeMs = commitInterval;

            thread = new Thread(Run);
            thread.Name = threadId;
            Name = threadId;

            State = ThreadState.CREATED;
        }

        #region IThread Impl

        public string Name { get; }

        public bool IsRunning { get; private set; } = false;

        public bool IsDisposable { get; private set; } = false;

        public bool ThrowException { get; set; } = true;

        public int Id => thread.ManagedThreadId;

        public void Dispose() => Close(true);

        public void Run()
        {
            Exception exception = null;
            if (IsRunning)
            {
                while (!token.IsCancellationRequested)
                {
                    if (exception != null)
                    {
                        Close(false);
                        if (ThrowException)
                            throw new StreamsException(exception);
                        else
                        {
                            IsRunning = false;
                            break;
                        }
                    }

                    try
                    {
                        IEnumerable<ConsumeResult<byte[], byte[]>> records = null;
                        long now = DateTime.Now.GetMilliseconds();

                        if (State == ThreadState.PARTITIONS_ASSIGNED)
                        {
                            records = PollRequest(TimeSpan.Zero);
                        }
                        else if (State == ThreadState.PARTITIONS_REVOKED)
                        {
                            records = PollRequest(TimeSpan.Zero);
                        }
                        else if (State == ThreadState.RUNNING || State == ThreadState.STARTING)
                        {
                            records = PollRequest(consumeTimeout);
                        }
                        else
                        {
                            log.Error($"{logPrefix}Unexpected state {State} during normal iteration");
                            throw new StreamsException($"Unexpected state {State} during normal iteration");
                        }

                        DateTime n = DateTime.Now;

                        if (records != null && records.Count() > 0)
                        {
                            foreach (var record in records)
                            {
                                var task = manager.ActiveTaskFor(record.TopicPartition);
                                if (task != null)
                                {
                                    if (task.IsClosed)
                                    {
                                        log.Info($"Stream task {task.Id} is already closed, probably because it got unexpectedly migrated to another thread already. Notifying the thread to trigger a new rebalance immediately.");
                                        // TODO gesture this behaviour
                                        //throw new TaskMigratedException(task);
                                    }
                                    else
                                        task.AddRecord(record);
                                }
                                else
                                {
                                    log.Error($"Unable to locate active task for received-record partition {record.TopicPartition}. Current tasks: {string.Join(",", manager.ActiveTaskIds)}");
                                    throw new NullReferenceException($"Task was unexpectedly missing for partition {record.TopicPartition}");
                                }
                            }
                            
                            log.Info($"Add {records.Count()} records in tasks in {DateTime.Now - n}");
                        }

                        int processed = 0;
                        long timeSinceLastPoll = 0;
                        do
                        {
                            processed = 0;
                            for (int i = 0; i < numIterations; ++i)
                            {
                                processed = manager.Process(now);

                                if (processed == 0)
                                    break;
                                // NOT AVAILABLE NOW, NEED PROCESSOR API
                                //if (processed > 0)
                                //    manager.MaybeCommitPerUserRequested();
                                //else
                                //    break;
                            }

                            timeSinceLastPoll = Math.Max(DateTime.Now.GetMilliseconds() - lastPollMs, 0);


                            if (MaybeCommit())
                            {
                                numIterations = numIterations > 1 ? numIterations / 2 : numIterations;
                            }
                            
                            else if (timeSinceLastPoll > streamConfig.MaxPollIntervalMs.Value / 2)
                            {
                                numIterations = numIterations > 1 ? numIterations / 2 : numIterations;
                                break;
                            }
                            else if (processed > 0)
                            {
                                numIterations++;

                            }
                        } while (processed > 0);

                        if (State == ThreadState.RUNNING)
                            MaybeCommit();

                        if (State == ThreadState.PARTITIONS_ASSIGNED)
                        {
                            SetState(ThreadState.RUNNING);
                        }

                        if (records.Any())
                            log.Info($"Processing {records.Count()} records in {DateTime.Now - n}");
                    }
                    catch (KafkaException e)
                    {
                        log.Error($"{logPrefix}Encountered the following unexpected Kafka exception during processing, tis usually indicate Streams internal errors:", e);
                        exception = e;
                    }
                    catch (Exception e)
                    {
                        log.Error($"{logPrefix}Encountered the following error during processing:", e);
                        exception = e;
                    }
                }

                while (IsRunning)
                {
                    // Use for waiting end of disposing
                    Thread.Sleep(100);
                }

                // Dispose consumer
                try
                {
                    consumer.Dispose();
                }
                catch (Exception e)
                {
                    log.Error($"{logPrefix}Failed to close consumer due to the following error:", e);
                }
            }
        }

        public void Start(CancellationToken token)
        {
            log.Info($"{logPrefix}Starting");
            if (SetState(ThreadState.STARTING) == null)
            {
                log.Info($"{logPrefix}StreamThread already shutdown. Not running");
                IsRunning = false;
                return;
            }

            this.token = token;
            IsRunning = true;
            consumer.Subscribe(builder.GetSourceTopics());
            thread.Start();
        }

        public IEnumerable<ITask> ActiveTasks => manager.ActiveTasks;

        #endregion

        private bool MaybeCommit()
        {
            int committed = 0;
            if (DateTime.Now - lastCommit > TimeSpan.FromMilliseconds(commitTimeMs))
            {
                DateTime beginCommit = DateTime.Now;
                log.Debug($"Committing all active tasks {string.Join(",", manager.ActiveTaskIds)} since {(DateTime.Now - lastCommit).TotalMilliseconds}ms has elapsed (commit interval is {commitTimeMs}ms)");
                committed = manager.CommitAll();
                if (committed > 0)
                    log.Debug($"Committed all active tasks {string.Join(",", manager.ActiveTaskIds)} in {(DateTime.Now - beginCommit).TotalMilliseconds}ms");

                if (committed == -1)
                {
                    log.Debug("Unable to commit as we are in the middle of a rebalance, will try again when it completes.");
                }
                else
                {
                    lastCommit = DateTime.Now;
                }
            }
            return committed > 0;
        }

        private void Close(bool cleanUp = true)
        {
            try
            {
                if (!IsDisposable)
                {
                    log.Info($"{logPrefix}Shutting down");

                    SetState(ThreadState.PENDING_SHUTDOWN);

                    manager.Close();
                    consumer.Unsubscribe();
                    IsRunning = false;
                    if (cleanUp)
                        thread.Join();

                    SetState(ThreadState.DEAD);
                    log.Info($"{logPrefix}Shutdown complete");
                    IsDisposable = true;
                }
            }
            catch (Exception e)
            {
                log.Error($"{logPrefix}Failed to close stream thread due to the following error:", e);
            }
        }

        private IEnumerable<ConsumeResult<byte[], byte[]>> PollRequest(TimeSpan ts)
        {
            lastPollMs = DateTime.Now.GetMilliseconds();
            return consumer.ConsumeRecords(ts);
        }

        internal ThreadState SetState(ThreadState newState)
        {
            ThreadState oldState;

            lock (stateLock)
            {
                oldState = State;

                if (State == ThreadState.PENDING_SHUTDOWN && newState != ThreadState.DEAD)
                {
                    log.Debug($"{logPrefix}Ignoring request to transit from PENDING_SHUTDOWN to {newState}: only DEAD state is a valid next state");
                    // when the state is already in PENDING_SHUTDOWN, all other transitions will be
                    // refused but we do not throw exception here
                    return null;
                }
                else if (State == ThreadState.DEAD)
                {
                    log.Debug($"{logPrefix}Ignoring request to transit from DEAD to {newState}: no valid next state after DEAD");
                    // when the state is already in NOT_RUNNING, all its transitions
                    // will be refused but we do not throw exception here
                    return null;
                }
                else if (!State.IsValidTransition(newState))
                {
                    string logPrefix = "";
                    log.Error($"{logPrefix}Unexpected state transition from {oldState} to {newState}");
                    throw new StreamsException($"{logPrefix}Unexpected state transition from {oldState} to {newState}");
                }
                else
                {
                    log.Info($"{logPrefix}State transition from {oldState} to {newState}");
                }

                State = newState;
            }

            StateChanged?.Invoke(this, oldState, State);

            return oldState;
        }
    
        // FOR TEST
        internal IEnumerable<TopicPartitionOffset> GetCommittedOffsets(IEnumerable<TopicPartition> partitions, TimeSpan timeout)
            => consumer.Committed(partitions, timeout);
    }
}