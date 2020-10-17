using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Confluent.Kafka;
using log4net;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Kafka;
using Streamiz.Kafka.Net.Kafka.Internal;
using Streamiz.Kafka.Net.Processors.Internal;

namespace Streamiz.Kafka.Net.Processors
{
    internal class StreamThread : IThread
    {
        private readonly InternalTopologyBuilder builder;
        private readonly long commitTimeMs;
        private readonly IConsumer<byte[], byte[]> consumer;
        private readonly TimeSpan consumeTimeout;
        private readonly ILog log = Logger.GetLogger(typeof(StreamThread));
        private readonly string logPrefix;
        private readonly TaskManager manager;

        private readonly object stateLock = new object();

        private readonly IStreamConfig streamConfig;
        private readonly Thread thread;
        private DateTime lastCommit = DateTime.Now;
        private long lastPollMs;

        private int numIterations = 1;
        private CancellationToken token;

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

        public ThreadState State { get; private set; }

        public event ThreadStateListener StateChanged;

        private bool MaybeCommit()
        {
            var committed = 0;
            if (DateTime.Now - lastCommit > TimeSpan.FromMilliseconds(commitTimeMs))
            {
                var beginCommit = DateTime.Now;
                log.Debug($"Committing all active tasks {string.Join(",", manager.ActiveTaskIds)} since {(DateTime.Now - lastCommit).TotalMilliseconds}ms has elapsed (commit interval is {commitTimeMs}ms)");
                committed = manager.CommitAll();
                if (committed > 0)
                    log.Debug($"Committed all active tasks {string.Join(",", manager.ActiveTaskIds)} in {(DateTime.Now - beginCommit).TotalMilliseconds}ms");

                if (committed == -1)
                    log.Debug("Unable to commit as we are in the middle of a rebalance, will try again when it completes.");
                else
                    lastCommit = DateTime.Now;
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

        private List<ConsumeResult<byte[], byte[]>> PollRequest(TimeSpan ts)
        {
            lastPollMs = DateTime.Now.GetMilliseconds();
            return consumer.ConsumeRecords(ts).ToList();
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

                if (State == ThreadState.DEAD)
                {
                    log.Debug($"{logPrefix}Ignoring request to transit from DEAD to {newState}: no valid next state after DEAD");
                    // when the state is already in NOT_RUNNING, all its transitions
                    // will be refused but we do not throw exception here
                    return null;
                }

                if (!State.IsValidTransition(newState))
                {
                    var logPrefix = "";
                    log.Error($"{logPrefix}Unexpected state transition from {oldState} to {newState}");
                    throw new StreamsException($"{logPrefix}Unexpected state transition from {oldState} to {newState}");
                }

                log.Info($"{logPrefix}State transition from {oldState} to {newState}");

                State = newState;
            }

            StateChanged?.Invoke(this, oldState, State);

            return oldState;
        }

        // FOR TEST
        internal IEnumerable<TopicPartitionOffset> GetCommittedOffsets(IEnumerable<TopicPartition> partitions, TimeSpan timeout)
        {
            return consumer.Committed(partitions, timeout);
        }

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
            var logPrefix = $"stream-thread[{threadId}] ";
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

        #region IThread Impl

        public string Name { get; }

        public bool IsRunning { get; private set; }

        public bool IsDisposable { get; private set; }

        public bool ThrowException { get; set; } = true;

        public int Id => thread.ManagedThreadId;

        public void Dispose()
        {
            Close();
        }

        public void Run()
        {
            if (IsRunning)
            {
                while (!token.IsCancellationRequested)
                    try
                    {
                        RunConsumers();
                    }
                    catch (KafkaException exception)
                    {
                        var message = $"{logPrefix}Encountered the following unexpected Kafka exception during processing, tis usually indicate Streams internal errors:";
                        HandleException(message, exception);
                        break;
                    }
                    catch (Exception exception)
                    {
                        var message = $"{logPrefix}Encountered the following error during processing:";
                        HandleException(message, exception);
                        break;
                    }

                WaitingEndOfDisposing();
            }
        }

        private void RunConsumers()
        {
            var now = DateTime.Now.GetMilliseconds();

            var records = BuildConsumeResults();

            var dateTimeNow = DateTime.Now;

            ActiveTask(records, dateTimeNow);

            Process(now);

            FinishUpRun(records, dateTimeNow);
        }

        private void FinishUpRun(List<ConsumeResult<byte[], byte[]>> records, DateTime dateTimeNow)
        {
            if (State == ThreadState.RUNNING)
                MaybeCommit();

            if (State == ThreadState.PARTITIONS_ASSIGNED)
                SetState(ThreadState.RUNNING);

            if (records!.Any())
                log.Info($"Processing {records.Count} records in {DateTime.Now - dateTimeNow}");
        }

        private void ActiveTask(List<ConsumeResult<byte[], byte[]>> records, DateTime dateTimeNow)
        {
            foreach (var record in records)
            {
                var task = manager.ActiveTaskFor(record.TopicPartition);
                if (task != null)
                {
                    if (task.IsClosed)
                        log.Info($"Stream task {task.Id} is already closed, probably because it got unexpectedly migrated to another thread already. Notifying the thread to trigger a new rebalance immediately.");
                    // TODO gesture this behaviour
                    //throw new TaskMigratedException(task);
                    else
                        task.AddRecord(record);
                }
                else
                {
                    log.Error($"Unable to locate active task for received-record partition {record.TopicPartition}. Current tasks: {string.Join(",", manager.ActiveTaskIds)}");
                    throw new NullReferenceException($"Task was unexpectedly missing for partition {record.TopicPartition}");
                }
            }

            log.Info($"Add {records.Count} records in tasks in {DateTime.Now - dateTimeNow}");
        }

        private void Process(long now)
        {
            int processed;
            do
            {
                processed = GetProcessedThreads(now);

                var timeSinceLastPoll = Math.Max(DateTime.Now.GetMilliseconds() - lastPollMs, 0);

                if (ShouldBreakProcessing(timeSinceLastPoll, processed))
                    break;
            } while (processed > 0);
        }

        private bool ShouldBreakProcessing(long timeSinceLastPoll, int processed)
        {
            if (MaybeCommit())
            {
                numIterations = CalculateNumIterations();
            }
            else if (streamConfig.MaxPollIntervalMs != null && timeSinceLastPoll > streamConfig.MaxPollIntervalMs.Value / 2)
            {
                numIterations = CalculateNumIterations();
                return true;
            }
            else if (processed > 0)
            {
                numIterations++;
            }

            return false;
        }

        private int CalculateNumIterations()
        {
            return numIterations > 1 ? numIterations / 2 : numIterations;
        }

        private int GetProcessedThreads(long now)
        {
            var processed = 0;
            for (var i = 0; i < numIterations; ++i)
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

            return processed;
        }

        private List<ConsumeResult<byte[], byte[]>> BuildConsumeResults()
        {
            List<ConsumeResult<byte[], byte[]>> records;

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

            return records ?? new List<ConsumeResult<byte[], byte[]>>();
        }

        private void WaitingEndOfDisposing()
        {
            while (IsRunning)
                // Use for waiting end of disposing
                Thread.Sleep(100);

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

        private void HandleException(string message, Exception exception)
        {
            log.Error(message, exception);
            Close(false);
            if (ThrowException)
                throw new StreamsException(exception);
            IsRunning = false;
        }

        public void Start(CancellationToken cancellationToken)
        {
            log.Info($"{logPrefix}Starting");
            if (SetState(ThreadState.STARTING) == null)
            {
                log.Info($"{logPrefix}StreamThread already shutdown. Not running");
                IsRunning = false;
                return;
            }

            token = cancellationToken;
            IsRunning = true;
            consumer.Subscribe(builder.GetSourceTopics());
            thread.Start();
        }

        public IEnumerable<ITask> ActiveTasks => manager.ActiveTasks;

        #endregion
    }
}