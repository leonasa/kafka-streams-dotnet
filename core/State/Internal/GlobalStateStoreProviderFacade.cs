using System.Collections.Generic;

namespace Streamiz.Kafka.Net.State.Internal
{
    internal class GlobalStateStoreProviderFacade<T, K, V> : IStateStoreProvider<T, K, V> where T : class
    {
        readonly GlobalStateStoreProvider globalStateStoreProvider;

        public GlobalStateStoreProviderFacade(GlobalStateStoreProvider globalStateStoreProvider)
        {
            this.globalStateStoreProvider = globalStateStoreProvider;
        }

        public IEnumerable<T> Stores(string storeName, IQueryableStoreType<T, K, V> queryableStoreType)
        {
            return globalStateStoreProvider.Stores(StoreQueryParameters.FromNameAndType(storeName, queryableStoreType));
        }
    }
}
