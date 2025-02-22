﻿using Streamiz.Kafka.Net.Crosscutting;

namespace Streamiz.Kafka.Net.State.Supplier
{
    /// <summary>
    /// A store supplier that can be used to create one or more <see cref="IWindowStore{K,V}"/> instances of type &lt;Bytes, byte[]&gt;.
    /// 
    /// For any stores implementing the <see cref="IWindowStore{K,V}"/> interface, null value bytes are considered as "not exist". 
    /// This means:
    /// 1. Null value bytes in put operations should be treated as delete.
    /// 2. Null value bytes should never be returned in range query results.
    /// </summary>
    public interface IWindowBytesStoreSupplier : IStoreSupplier<IWindowStore<Bytes, byte[]>>
    {
        /// <summary>
        /// The size of the windows (in milliseconds) any store created from this supplier is creating.
        /// Can be null
        /// </summary>
        public long? WindowSize { get; set; }

        /// <summary>
        /// The time period for which the <see cref="IWindowStore{K,V}"/> will retain historic data.
        /// </summary>
        public long Retention { get; }
    }
}
