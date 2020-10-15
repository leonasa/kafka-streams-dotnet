using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Stream.Internal.Graph.Nodes;
using Streamiz.Kafka.Net.Table;
using Streamiz.Kafka.Net.Table.Internal;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Stream.Internal
{
    internal class GroupedStreamAggregateBuilder<K, V>
    {
        private readonly InternalStreamBuilder builder;
        private readonly List<string> sourceNodes;
        private readonly StreamGraphNode node;

        public GroupedStreamAggregateBuilder(InternalStreamBuilder builder, List<string> sourceNodes, StreamGraphNode node)
        {
            this.builder = builder;
            this.sourceNodes = sourceNodes;
            this.node = node;
        }

        internal IKTable<K, VR> Build<VR>(
            string functionName,
            StoreBuilder<TimestampedKeyValueStore<K, VR>> storeBuilder,
            IKStreamAggProcessorSupplier<K, K, V, VR> aggregateSupplier,
            string queryableStoreName,
            ISerDes<K> keySerdes,
            ISerDes<VR> valueSerdes)
        {
            // if repartition required TODO
            // ELSE
            StatefulProcessorNode<K, V, TimestampedKeyValueStore<K, VR>> statefulProcessorNode =
               new StatefulProcessorNode<K, V, TimestampedKeyValueStore<K, VR>>(
                   functionName,
                   new ProcessorParameters<K, V>(aggregateSupplier, functionName),
                   storeBuilder);

            builder.AddGraphNode(node, statefulProcessorNode);

            return new KTable<K, V, VR>(functionName,
                                    keySerdes,
                                    valueSerdes,
                                    sourceNodes,
                                    aggregateSupplier,
                                    statefulProcessorNode,
                                    builder);
        }

        internal IKTable<KR, VR> BuildWindow<KR, VR>(
            string functionName,
            StoreBuilder<TimestampedWindowStore<K, VR>> storeBuilder,
            IKStreamAggProcessorSupplier<K, KR, V, VR> aggregateSupplier,
            string queryableStoreName,
            ISerDes<KR> keySerdes,
            ISerDes<VR> valueSerdes)
        {
            // if repartition required TODO
            // ELSE
            StatefulProcessorNode<K, V, TimestampedWindowStore<K, VR>> statefulProcessorNode =
               new StatefulProcessorNode<K, V, TimestampedWindowStore<K, VR>>(
                   functionName,
                   new ProcessorParameters<K, V>(aggregateSupplier, functionName),
                   storeBuilder);

            builder.AddGraphNode(node, statefulProcessorNode);

            return new KTableGrouped<K, KR, V, VR>(functionName,
                                    keySerdes,
                                    valueSerdes,
                                    sourceNodes,
                                    queryableStoreName,
                                    aggregateSupplier,
                                    statefulProcessorNode,
                                    builder);
        }
    }
}
