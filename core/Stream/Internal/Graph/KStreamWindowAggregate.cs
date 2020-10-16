﻿using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Table.Internal;
using System;

namespace Streamiz.Kafka.Net.Stream.Internal.Graph
{
    internal class KStreamWindowAggregate<K, V, Agg, W> : IKStreamAggProcessorSupplier<K, Windowed<K>, V, Agg>
        where W : Window
    {
        private readonly string storeName;
        private readonly Initializer<Agg> initializer;
        private readonly IAggregator<K, V, Agg> aggregator;

        private bool sendOldValues;

        public KStreamWindowAggregate(WindowOptions<W> windowOptions, string storeName, Func<Agg> initializerFunction, Func<K, V, Agg, Agg> aggregatorFunction)
            : this(windowOptions, storeName, new WrappedInitializer<Agg>(initializerFunction), new WrappedAggregator<K, V, Agg>(aggregatorFunction))
        {
        }

        public KStreamWindowAggregate(WindowOptions<W> windowOptions, string storeName, Initializer<Agg> initializer, IAggregator<K, V, Agg> aggregator)
        {
            WindowOptions = windowOptions;
            this.storeName = storeName;
            this.initializer = initializer;
            this.aggregator = aggregator;
        }

        public WindowOptions<W> WindowOptions { get; }

        public void EnableSendingOldValues()
        {
            sendOldValues = true;
        }

        public IProcessor<K, V> Get()
            => new KStreamWindowAggregateProcessor<K, V, Agg, W>(WindowOptions, initializer, aggregator, storeName, sendOldValues);

        public IKTableValueGetterSupplier<Windowed<K>, Agg> View()
            => new GenericKTableValueGetterSupplier<Windowed<K>, Agg>(
                new string[] { storeName },
                new WindowKeyValueStoreGetter<K, Agg>(storeName));
    }
}
