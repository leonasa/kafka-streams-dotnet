﻿using System;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.State;

namespace Streamiz.Kafka.Net.Table.Internal.Graph
{
    internal class KTableFilter<K, V> : IKTableProcessorSupplier<K, V, V>
    {
        internal class KTableFilterValueGetter : IKTableValueGetter<K, V>
        {
            private readonly IKTableValueGetter<K, V> ktablegetter;
            private readonly bool filterNot;
            private readonly Func<K, V, bool> predicate;

            public KTableFilterValueGetter(bool filterNot, Func<K, V, bool> predicate, IKTableValueGetter<K, V> getter)
            {
                ktablegetter = getter;
                this.filterNot = filterNot;
                this.predicate = predicate;
            }

            public void Close() => ktablegetter.Close();

            public ValueAndTimestamp<V> Get(K key) => ComputeValue(key, ktablegetter.Get(key));

            public void Init(ProcessorContext context) => ktablegetter.Init(context);

            private ValueAndTimestamp<V> ComputeValue(K key, ValueAndTimestamp<V> valueAndTimestamp)
            {
                ValueAndTimestamp<V> newValueAndTimestamp = null;

                if (valueAndTimestamp != null)
                {
                    V value = valueAndTimestamp.Value;
                    if (filterNot ^ predicate.Invoke(key, value))
                    {
                        newValueAndTimestamp = valueAndTimestamp;
                    }
                }

                return newValueAndTimestamp;
            }
        }

        private readonly IKTableGetter<K, V> parent;
        private readonly Func<K, V, bool> predicate;
        private readonly bool filterNot;
        private readonly string queryableStoreName;
        private bool sendOldValues;

        public KTableFilter(IKTableGetter<K, V> parent, Func<K, V, bool> predicate, bool filterNot, string queryableStoreName)
        {
            this.parent = parent;
            this.predicate = predicate;
            this.filterNot = filterNot;
            this.queryableStoreName = queryableStoreName;
        }

        public IKTableValueGetterSupplier<K, V> View
        {
            get
            {
                // if the KTable is materialized, use the materialized store to return getter value;
                // otherwise rely on the parent getter and apply filter on-the-fly
                if (queryableStoreName != null)
                {
                    return new KTableMaterializedValueGetterSupplier<K, V>(queryableStoreName);
                }
                else
                {
                    var supplier = parent.ValueGetterSupplier;
                    return new GenericKTableValueGetterSupplier<K, V>(
                        supplier.StoreNames,
                        new KTableFilterValueGetter(filterNot, predicate, supplier.Get()));
                }
            }
        }

        public void EnableSendingOldValues()
        {
            parent.EnableSendingOldValues();
            sendOldValues = true;
        }

        public IProcessor<K, Change<V>> Get() => new KTableFilterProcessor<K, V>(predicate, filterNot, queryableStoreName, sendOldValues);
    }
}
