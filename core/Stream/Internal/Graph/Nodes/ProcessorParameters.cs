﻿using Streamiz.Kafka.Net.Processors;
using System;

namespace Streamiz.Kafka.Net.Stream.Internal.Graph.Nodes
{
    internal class ProcessorParameters<K, V>
    {
        public IProcessorSupplier<K, V> Processor { get; }
        public string ProcessorName { get; }

        public ProcessorParameters(IProcessorSupplier<K, V> processorSupplier, String processorName)
        {
            this.Processor = processorSupplier;
            this.ProcessorName = processorName;
        }


        public override string ToString()
        {
            return "ProcessorParameters{" +
                "processor class=" + Processor.Get().GetType() +
                ", processor name='" + ProcessorName + '\'' +
                '}';
        }
    }
}
