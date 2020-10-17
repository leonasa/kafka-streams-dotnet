﻿using Streamiz.Kafka.Net.Stream;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Processors
{
    internal class KStreamFlatMapValuesProcessor<K, V, VR> : AbstractProcessor<K, V>
    {
        private readonly IValueMapperWithKey<K, V, IEnumerable<VR>> mapper;

        public KStreamFlatMapValuesProcessor(IValueMapperWithKey<K, V, IEnumerable<VR>> mapper)
        {
            this.mapper = mapper;
        }


        public override void Process(K key, V value)
        {
            LogProcessingKeyValue(key, value);
            foreach (var newValue in mapper.Apply(key, value))
                Forward(key, newValue);
        }
    }
}
