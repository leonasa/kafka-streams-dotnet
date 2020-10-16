﻿using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.State;

namespace Streamiz.Kafka.Net.Stream.Internal.Graph.Nodes
{
    internal class StatefulProcessorNode<K, V, S> : ProcessorGraphNode<K, V>
        where S : IStateStore
    {
        private readonly string[] storeNames;
        private readonly IStoreBuilder<S> storeBuilder;

        /// <summary>
        /// Create a node representing a stateful processor,
        /// where the store needs to be built and registered as part of building this node.
        /// </summary>
        /// <param name="nameNode"></param>
        /// <param name="parameters"></param>
        /// <param name="storeBuilder"></param>
        public StatefulProcessorNode(string nameNode, ProcessorParameters<K, V> parameters, IStoreBuilder<S> storeBuilder)
            : base(nameNode, parameters)
        {
            storeNames = null;
            this.storeBuilder = storeBuilder;
        }

        public override void WriteToTopology(InternalTopologyBuilder builder)
        {
            builder.AddProcessor(ProcessorParameters.ProcessorName, ProcessorParameters.Processor, ParentNodeNames());

            if (storeNames != null && storeNames.Length > 0)
            {
                builder.ConnectProcessorAndStateStore(ProcessorParameters.ProcessorName, storeNames);
            }

            if (storeBuilder != null)
            {
                builder.AddStateStore(storeBuilder, ProcessorParameters.ProcessorName);
            }
        }
    }
}
