﻿using Streamiz.Kafka.Net.Processors;

namespace Streamiz.Kafka.Net.Table.Internal.Graph
{
    internal class KTableSource<K, V> : IProcessorSupplier<K, V>
    {
        private bool sendOldValues;

        public string StoreName { get; }
        public string QueryableName { get; private set; }

        public KTableSource(string storeName, string queryableName)
        {
            this.StoreName = storeName;
            this.QueryableName = queryableName;
            this.sendOldValues = false;
        }

        public void EnableSendingOldValues()
        {
            this.sendOldValues = true;
            this.QueryableName = StoreName;
        }

        public void Materialize()
        {
            this.QueryableName = StoreName;
        }

        public IProcessor<K, V> Get() => new KTableSourceProcessor<K, V>(this.QueryableName, this.sendOldValues);
    }
}
