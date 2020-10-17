using Streamiz.Kafka.Net.Processors;
using System.Collections.Generic;
using System.Linq;

namespace Streamiz.Kafka.Net.Stream.Internal
{
    internal class ProcessorTopology
    {
        internal IDictionary<string, IProcessor> SourceOperators { get; }
        internal IDictionary<string, IProcessor> SinkOperators { get; }
        internal IDictionary<string, IProcessor> ProcessorOperators { get; }
        internal IDictionary<string, IStateStore> StateStores { get; }

        internal IDictionary<string, IStateStore> GlobalStateStores { get; }
        internal IDictionary<string, string> StoresToTopics { get; }

        internal ProcessorTopology(
            IProcessor rootProcessor,
            IDictionary<string, IProcessor> sources,
            IDictionary<string, IProcessor> sinks,
            IDictionary<string, IProcessor> processors,
            IDictionary<string, IStateStore> stateStores,
            IDictionary<string, IStateStore> globalStateStores,
            IDictionary<string, string> storesToTopics)
        {
            SourceOperators = sources;
            SinkOperators = sinks;
            ProcessorOperators = processors;
            StateStores = stateStores;
            GlobalStateStores = globalStateStores;
            StoresToTopics = storesToTopics;
        }

        internal ISourceProcessor GetSourceProcessor(string name)
        {
            var processor = SourceOperators.FirstOrDefault(kp => kp.Value is ISourceProcessor sourceProcessor && sourceProcessor.TopicName.Equals(name));
            return processor.Value as ISourceProcessor;
        }
    }
}
