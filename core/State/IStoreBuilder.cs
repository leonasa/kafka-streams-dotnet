using Streamiz.Kafka.Net.Processors;
using System;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.State
{
    /// <summary>
    /// Build a <see cref="IStateStore"/> wrapped with optional caching and logging.
    /// </summary>
    internal interface IStoreBuilder
    {
        IDictionary<string, string> LogConfig { get; }
        bool LoggingEnabled { get; }
        string Name { get; }
        IStateStore Build();
    }

    /// <summary>
    /// Build a <see cref="IStateStore"/> wrapped with optional caching and logging.
    /// </summary>
    /// <typeparam name="T">the type of store to build</typeparam>
    internal interface IStoreBuilder<T>  : IStoreBuilder
        where T : IStateStore
    {
        IStoreBuilder<T> WithCachingEnabled();
        IStoreBuilder<T> WithCachingDisabled();
        IStoreBuilder<T> WithLoggingEnabled(IDictionary<String, String> config);
        IStoreBuilder<T> WithLoggingDisabled();
        new T Build();
    }
}
