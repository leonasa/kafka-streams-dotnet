using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State.Supplier;

namespace Streamiz.Kafka.Net.State.Internal.Builder
{
    internal class TimestampedWindowStoreBuilder<K, V>
        : AbstractStoreBuilder<K, ValueAndTimestamp<V>, TimestampedWindowStore<K, V>>
    {
        private readonly IWindowBytesStoreSupplier supplier;

        public TimestampedWindowStoreBuilder(IWindowBytesStoreSupplier supplier, ISerDes<K> keySerde, ISerDes<V> valueSerde)
            : base(supplier.Name, keySerde, valueSerde == null ? null : new ValueAndTimestampSerDes<V>(valueSerde))
        {
            this.supplier = supplier;
        }

        public override TimestampedWindowStore<K, V> Build()
        {
            var store = supplier.Get();
            return new TimestampedWindowStoreImpl<K, V>(store, supplier.WindowSize.Value, keySerdes, valueSerdes);
        }
    }
}
