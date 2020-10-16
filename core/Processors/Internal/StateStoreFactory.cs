﻿using Streamiz.Kafka.Net.State;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Processors.Internal
{
    internal class StateStoreFactory
    {
        private readonly IStoreBuilder storeBuilder;
        private readonly Dictionary<(string, int), IStateStore> stores =
            new Dictionary<(string, int), IStateStore>();
        internal readonly List<string> users = new List<string>();

        public StateStoreFactory(IStoreBuilder builder)
        {
            storeBuilder = builder;
        }

        public string Name => storeBuilder.Name;
        public bool LoggingEnabled => storeBuilder.LoggingEnabled;
        public IDictionary<string, string> LogConfig => storeBuilder.LogConfig;

        public IStateStore Build(TaskId taskId)
        {
            if (taskId != null)
            {
                if (stores.ContainsKey((Name, taskId.Partition)))
                {
                    return stores[(Name, taskId.Partition)];
                }
                else
                {
                    var store = storeBuilder.Build();
                    stores.Add((Name, taskId.Partition), store);
                    return store;
                }
            }
            else
            {
                return storeBuilder.Build();
            }
        }
    }
}
