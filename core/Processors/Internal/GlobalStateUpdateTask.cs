using Confluent.Kafka;
using log4net;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Stream.Internal;
using System.Collections.Generic;
using System.Linq;

namespace Streamiz.Kafka.Net.Processors.Internal
{
    internal class GlobalStateUpdateTask : IGlobalStateMaintainer
    {
        private readonly IGlobalStateManager globalStateManager;
        private readonly ProcessorTopology topology;
        private readonly ILog log = Logger.GetLogger(typeof(GlobalStateUpdateTask));
        private readonly ProcessorContext context;
        private IDictionary<string, IProcessor> topicToProcessor = new Dictionary<string, IProcessor>();

        public GlobalStateUpdateTask(IGlobalStateManager globalStateManager, ProcessorTopology topology, ProcessorContext context)
        {
            this.globalStateManager = globalStateManager;
            this.topology = topology;
            this.context = context;
        }

        public void Close()
        {
            globalStateManager.Close();
        }

        public void FlushState()
        {
            globalStateManager.Flush();
        }

        public IDictionary<TopicPartition, long> Initialize()
        {
            topicToProcessor =
                globalStateManager
                .Initialize()
                .Select(storeName => topology.StoresToTopics[storeName])
                .ToDictionary(
                    topic => topic,
                    topic => topology.SourceOperators.Single(x => (x.Value as ISourceProcessor).TopicName == topic).Value);

            InitTopology();
            return globalStateManager.ChangelogOffsets;
        }

        public void Update(ConsumeResult<byte[], byte[]> record)
        {
            var processor = topicToProcessor[record.Topic];

            context.SetRecordMetaData(record);

            var recordInfo = $"Topic:{record.Topic}|Partition:{record.Partition.Value}|Offset:{record.Offset}|Timestamp:{record.Message.Timestamp.UnixTimestampMs}";
            log.Debug($"Start processing one record [{recordInfo}]");
            processor.Process(record.Message.Key, record.Message.Value);
            log.Debug($"Completed processing one record [{recordInfo}]");
        }

        private void InitTopology()
        {
            foreach (var processor in topology.ProcessorOperators.Values)
            {
                log.Debug($"Initializing topology with processor source : {processor}.");
                processor.Init(context);
            }
        }
    }
}
