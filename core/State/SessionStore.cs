﻿using Streamiz.Kafka.Net.Processors;

namespace Streamiz.Kafka.Net.State
{
    /// <summary>
    /// NOT IMPLEMENTED FOR MOMENT
    /// </summary>
    /// <typeparam name="K"></typeparam>
    /// <typeparam name="AGG"></typeparam>
    public interface SessionStore<K,AGG> : IStateStore, ReadOnlySessionStore<K,AGG>
    {
    }
}
