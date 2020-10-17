﻿using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.SerDes;

namespace Streamiz.Kafka.Net.State.Internal
{
    internal class TimestampedWindowStoreImpl<K, V> :
        WrappedWindowStore<K, ValueAndTimestamp<V>>, ITimestampedWindowStore<K, V>
    {
        private bool initStoreSerdes;

        public TimestampedWindowStoreImpl(IWindowStore<Bytes, byte[]> wrapped, long windowSizeMs, ISerDes<K> keySerdes, ISerDes<ValueAndTimestamp<V>> valueSerdes)
            : base(wrapped, keySerdes, valueSerdes)
        {
        }

        public override void InitStoreSerde(ProcessorContext context)
        {
            if (!initStoreSerdes)
            {
                keySerdes = keySerdes == null ? context.Configuration.DefaultKeySerDes as ISerDes<K> : keySerdes;
                valueSerdes = valueSerdes == null ? new ValueAndTimestampSerDes<V>(context.Configuration.DefaultValueSerDes as ISerDes<V>) : valueSerdes;
                initStoreSerdes = true;
            }
        }
    }
}
