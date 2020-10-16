using Streamiz.Kafka.Net.Processors;
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

            if (streamThread.State == ThreadState.DEAD)
            {
                yield break;
            }
            if (streamThread.State != ThreadState.RUNNING)
            {
                throw new InvalidStateStoreException($"Cannot get state store {storeQueryParameters.StoreName} because the stream thread is {streamThread.State}, not RUNNING");
            }

            foreach (var streamTask in streamThread.ActiveTasks)
            {
                var store = Store(storeQueryParameters, streamTask);
                if (store != null) 
                    yield return store;
            }
        }

        private T Store<T, K, V>(StoreQueryParameters<T, K, V> storeQueryParameters, ITask streamTask) where T : class
        {
            var stateStore = streamTask.GetStore(storeQueryParameters.StoreName);
            if (stateStore != null && storeQueryParameters.QueryableStoreType.Accepts(stateStore))
            {
                if (!stateStore.IsOpen)
                {
                    throw new InvalidStateStoreException($"Cannot get state store {storeQueryParameters.StoreName} for task {streamTask} because the store is not open. The state store may have migrated to another instances.");
                }

                switch (stateStore)
                {
                    case TimestampedWindowStore<K, V> windowStore when storeQueryParameters.QueryableStoreType is WindowStoreType<K, V>:
                        return new ReadOnlyWindowStoreFacade<K, V>(windowStore) as T;
                    case ITimestampedKeyValueStore<K, V> valueStore when storeQueryParameters.QueryableStoreType is KeyValueStoreType<K, V>:
                        return new ReadOnlyKeyValueStoreFacade<K, V>(valueStore) as T;
                    case T store:
                        return store;
                }
            }

            return null;
        }
    }
}
