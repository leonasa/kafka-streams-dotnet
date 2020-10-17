using Streamiz.Kafka.Net.Processors;

namespace Streamiz.Kafka.Net.Table.Internal.Graph
{
    internal class KTableSource<K, V> : IProcessorSupplier<K, V>
    {
        private bool sendOldValues;

        public string StoreName { get; }
        public string QueryableName { get; private set; }

        public KTableSource(string storeName, string queryableName)
        {
            StoreName = storeName;
            QueryableName = queryableName;
            sendOldValues = false;
        }

        public void EnableSendingOldValues()
        {
            sendOldValues = true;
            QueryableName = StoreName;
        }

        public void Materialize()
        {
            QueryableName = StoreName;
        }

        public IProcessor<K, V> Get() => new KTableSourceProcessor<K, V>(QueryableName, sendOldValues);
    }
}
