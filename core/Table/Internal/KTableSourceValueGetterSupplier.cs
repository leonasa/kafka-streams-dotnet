﻿using Streamiz.Kafka.Net.State;

namespace Streamiz.Kafka.Net.Table.Internal
{
    internal class KTableSourceValueGetterSupplier<K, V> : IKTableValueGetterSupplier<K, V>
    {
        internal class KTableSourceValueGetter : IKTableValueGetter<K, V>
        {
            private readonly string storeName;
            private TimestampedKeyValueStore<K, V> store;

            public KTableSourceValueGetter(string storeName)
            {
                this.storeName = storeName;
            }

            public void Close() { }

            public ValueAndTimestamp<V> Get(K key) => store.Get(key);

            public void Init(ProcessorContext context)
            {
                store = (TimestampedKeyValueStore<K, V>)context.GetStateStore(storeName);
            }

        }

        private readonly string queryableStoreName;

        public KTableSourceValueGetterSupplier(string queryableStoreName)
        {
            this.queryableStoreName = queryableStoreName;
        }

        public string[] StoreNames => new string[1] { queryableStoreName };

        public IKTableValueGetter<K, V> Get() => new KTableSourceValueGetter(this.queryableStoreName);
    }
}
