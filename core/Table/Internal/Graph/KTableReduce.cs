using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.Stream;

namespace Streamiz.Kafka.Net.Table.Internal.Graph
{
    internal class KTableReduce<K, V> : IKTableProcessorSupplier<K, V, V>
    {
        private readonly string storeName;
        private readonly IReducer<V> adder;
        private readonly IReducer<V> substractor;

        private bool sendOldValues;


        public KTableReduce(string storeName, IReducer<V> adder, IReducer<V> substractor)
        {
            this.storeName = storeName;
            this.adder = adder;
            this.substractor = substractor;
        }

        public IKTableValueGetterSupplier<K, V> View
            => new KTableMaterializedValueGetterSupplier<K, V>(storeName);

        public void EnableSendingOldValues()
        {
            sendOldValues = true;
        }

        public IProcessor<K, Change<V>> Get()
            => new KTableReduceProcessor<K, V>(storeName, sendOldValues, adder, substractor);
    }
}
