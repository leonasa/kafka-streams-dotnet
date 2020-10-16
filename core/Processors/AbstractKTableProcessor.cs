﻿using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Table.Internal;

namespace Streamiz.Kafka.Net.Processors
{
    internal abstract class AbstractKTableProcessor<K, V, KS, VS> : AbstractProcessor<K, Change<V>>
    {
        protected readonly string queryableStoreName;
        protected readonly bool sendOldValues;
        private readonly bool throwException;
        protected ITimestampedKeyValueStore<KS, VS> store;
        protected TimestampedTupleForwarder<K, V> tupleForwarder;

        protected AbstractKTableProcessor(string queryableStoreName, bool sendOldValues, bool throwExceptionStateNull = false)
        {
            this.queryableStoreName = queryableStoreName;
            this.sendOldValues = sendOldValues;
            throwException = throwExceptionStateNull;
        }

        public override void Init(ProcessorContext context)
        {
            base.Init(context);

            if (queryableStoreName != null)
            {
                store = (ITimestampedKeyValueStore<KS, VS>)context.GetStateStore(queryableStoreName);
                tupleForwarder = new TimestampedTupleForwarder<K, V>(this, sendOldValues);
            }

            if (throwException && (queryableStoreName == null || store == null))
                throw new StreamsException($"{logPrefix}Processor {Name} doesn't have queryable store name. Please set a correct name to materialized view !");
        }
    }
}
