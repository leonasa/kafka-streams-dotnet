using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.SerDes;
using System;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.State.Internal.Builder
{
    internal abstract class AbstractStoreBuilder<K, V, T> : IStoreBuilder<T>
        where T : IStateStore
    {
        private IDictionary<string, string> logConfig = new Dictionary<string, string>();
        protected readonly string name;
        protected readonly ISerDes<K> keySerdes;
        protected readonly ISerDes<V> valueSerdes;
        // private bool enableCaching;
        private bool enableLogging = true;

        public string Name => name;
        public IDictionary<string, string> LogConfig => logConfig;
        public bool LoggingEnabled => enableLogging;

        protected AbstractStoreBuilder(String name, ISerDes<K> keySerde, ISerDes<V> valueSerde)
        {
            this.name = name;
            keySerdes = keySerde;
            valueSerdes = valueSerde;
        }

        public IStoreBuilder<T> WithCachingEnabled()
        {
            //enableCaching = true;
            return this;
        }

        public IStoreBuilder<T> WithCachingDisabled()
        {
            //enableCaching = false;
            return this;
        }

        public IStoreBuilder<T> WithLoggingEnabled(IDictionary<String, String> config)
        {
            enableLogging = true;
            logConfig = config;
            return this;
        }

        public IStoreBuilder<T> WithLoggingDisabled()
        {
            enableLogging = false;
            logConfig.Clear();
            return this;
        }

        public abstract T Build();

        IStateStore IStoreBuilder.Build() => this.Build();
    }
}
