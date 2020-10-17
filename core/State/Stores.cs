using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State.InMemory;
using Streamiz.Kafka.Net.State.Internal.Builder;
using Streamiz.Kafka.Net.State.Supplier;
using System;

namespace Streamiz.Kafka.Net.State
{
    internal static class Stores
    {
        public static IKeyValueBytesStoreSupplier PersistentKeyValueStore(string name)
        {
            // TODO : RocksDB IMPLEMENTATION
            //return new RocksDbKeyValueBytesStoreSupplier(name, true);
            return null;
        }

        public static IKeyValueBytesStoreSupplier InMemoryKeyValueStore(string name)
        {
            return new InMemoryKeyValueBytesStoreSupplier(name);
        }

        public static IWindowBytesStoreSupplier InMemoryWindowStore(string name, TimeSpan retention, TimeSpan windowSize)
        {
            return new InMemoryWindowStoreSupplier(name, retention, (long)windowSize.TotalMilliseconds);
        }

        public static IStoreBuilder<ITimestampedKeyValueStore<K, V>> TimestampedKeyValueStoreBuilder<K, V>(IKeyValueBytesStoreSupplier supplier, ISerDes<K> keySerde, ISerDes<V> valueSerde)
        {
            return new TimestampedKeyValueStoreBuilder<K, V>(supplier, keySerde, valueSerde);
        } 
        
        public static IStoreBuilder<ITimestampedWindowStore<K, V>> TimestampedWindowStoreBuilder<K, V>(IWindowBytesStoreSupplier supplier, ISerDes<K> keySerde, ISerDes<V> valueSerde)
        {
            return new TimestampedWindowStoreBuilder<K, V>(supplier, keySerde, valueSerde);
        }

        public static IStoreBuilder<IWindowStore<K, V>> WindowStoreBuilder<K, V>(IWindowBytesStoreSupplier supplier, ISerDes<K> keySerdes, ISerDes<V> valueSerdes)
        {
            return new WindowStoreBuilder<K, V>(supplier, keySerdes, valueSerdes);
        }
    }
}
