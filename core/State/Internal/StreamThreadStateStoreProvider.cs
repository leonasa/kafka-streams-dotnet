﻿using Streamiz.Kafka.Net.Processors;
using System.Linq;
using System.Collections.Generic;
using Streamiz.Kafka.Net.Errors;

namespace Streamiz.Kafka.Net.State.Internal
{
    internal class StreamThreadStateStoreProvider
    {
        private readonly IThread streamThread;

        public StreamThreadStateStoreProvider(IThread streamThread)
        {
            this.streamThread = streamThread;
        }

        public IEnumerable<T> Stores<T, K, V>(StoreQueryParameters<T, K, V> storeQueryParameters) 
            where T : class
        {
            // TODO: handle 'staleStoresEnabled' and 'partition' when they are added to StoreQueryParameters

            if (this.streamThread.State == ThreadState.DEAD)
            {
                return Enumerable.Empty<T>();
            }
            if (!(this.streamThread.State == ThreadState.RUNNING))
            {
                throw new InvalidStateStoreException($"Cannot get state store {storeQueryParameters.StoreName} because " +
                    $"the stream thread is {streamThread.State}, not RUNNING");
            }

            List<T> stores = new List<T>();
            foreach (var streamTask in streamThread.ActiveTasks)
            {
                IStateStore store = streamTask.GetStore(storeQueryParameters.StoreName);
                if (store != null && storeQueryParameters.QueryableStoreType.Accepts(store))
                {
                    if (!store.IsOpen)
                    {
                        throw new InvalidStateStoreException($"Cannot get state store {storeQueryParameters.StoreName} for task {streamTask} because the " +
                            $"store is not open. The state store may have migrated to another instances.");
                    }
                    else if (store is TimestampedWindowStore<K, V> && storeQueryParameters.QueryableStoreType is WindowStoreType<K, V>) {
                        T t = new ReadOnlyWindowStoreFacade<K, V>(store as TimestampedWindowStore<K, V>) as T;
                        stores.Add(t);
                    }
                    if (store is TimestampedKeyValueStore<K, V> && storeQueryParameters.QueryableStoreType is KeyValueStoreType<K, V>)
                    {
                        T t = new ReadOnlyKeyValueStoreFacade<K, V>(store as TimestampedKeyValueStore<K, V>) as T;
                        stores.Add(t);
                    }
                    else if (store is T)
                        stores.Add(store as T);
                }
            }
            return stores;
        }
    }
}
