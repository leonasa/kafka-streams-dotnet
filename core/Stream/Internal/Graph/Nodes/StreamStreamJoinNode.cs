﻿using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.State;

namespace Streamiz.Kafka.Net.Stream.Internal.Graph.Nodes
{
    internal class StreamStreamJoinNode<K, V1, V2, VR> : BaseJoinProcessorNode<K, V1, V2, VR>
    {
        private readonly ProcessorParameters<K, V1> windowedLeftParams;
        private readonly ProcessorParameters<K, V2> windowedRightParams;
        private readonly StoreBuilder<WindowStore<K, V1>> windowedLeftStoreBuilder;
        private readonly StoreBuilder<WindowStore<K, V2>> windowedRightStoreBuilder;

        public StreamStreamJoinNode(
            string name,
            IValueJoiner<V1, V2, VR> valueJoiner,
            ProcessorParameters<K, V1> joinLeftParams,
            ProcessorParameters<K, V2> joinRightParams,
            ProcessorParameters<K, VR> joinMergeParams,
            ProcessorParameters<K, V1> windowedLeftParams,
            ProcessorParameters<K, V2> windowedRightParams,
            StoreBuilder<WindowStore<K, V1>> windowedLeftStoreBuilder,
            StoreBuilder<WindowStore<K, V2>> windowedRightStoreBuilder)
            : base(name, valueJoiner, joinLeftParams, joinRightParams, joinMergeParams, null, null)
        {
            this.windowedLeftParams = windowedLeftParams;
            this.windowedRightParams = windowedRightParams;
            this.windowedLeftStoreBuilder = windowedLeftStoreBuilder;
            this.windowedRightStoreBuilder = windowedRightStoreBuilder;
        }

        public override void WriteToTopology(InternalTopologyBuilder builder)
        {
            string leftProcessorName = JoinLeftParams.ProcessorName;
            string rightProcessorName = JoinRightParams.ProcessorName;
            string windowedLeftProcessorName = windowedLeftParams.ProcessorName;
            string windowedRightProcessorName = windowedRightParams.ProcessorName;

            builder.AddProcessor(leftProcessorName, JoinLeftParams.Processor, windowedLeftProcessorName);
            builder.AddProcessor(rightProcessorName, JoinRightParams.Processor, windowedRightProcessorName);
            builder.AddProcessor(JoinMergeParams.ProcessorName, JoinMergeParams.Processor, leftProcessorName, rightProcessorName);
            builder.AddStateStore(windowedLeftStoreBuilder, windowedLeftProcessorName, rightProcessorName);
            builder.AddStateStore(windowedRightStoreBuilder, windowedRightProcessorName, leftProcessorName);
        }
    }
}
