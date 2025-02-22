﻿using NUnit.Framework;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Streamiz.Kafka.Net.Tests.Processors
{
    public class KTableFilterNotTests
    {
        [Test]
        public void ShouldNotAllowNullFilterNotAction()
        {
            var builder = new StreamBuilder();
            var table = builder.Table<string, string>("ktable-topic");
            Assert.Throws<ArgumentNullException>(() => table.FilterNot(null));
        }

        [Test]
        public void FilterNotOneElement()
        {
            var builder = new StreamBuilder();
            var data = new List<KeyValuePair<string, string>>();
            data.Add(KeyValuePair.Create("key1", "test1234"));
            data.Add(KeyValuePair.Create("key2", "test"));
            data.Add(KeyValuePair.Create("key3", "paper"));

            builder.Table<string, string>("table-topic")
                .FilterNot((k, v) => v.Contains("test", StringComparison.InvariantCultureIgnoreCase), InMemory<string, string>.As("test-store"));

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "table-test-filter";

            Topology t = builder.Build();

            using var driver = new TopologyTestDriver(t, config);
            var inputTopic = driver.CreateInputTopic<string, string>("table-topic");
            inputTopic.PipeInputs(data);

            var store = driver.GetKeyValueStore<string, string>("test-store");
            Assert.IsNotNull(store);
            Assert.AreEqual(store.All().Count(), 1);

            var r1 = store.Get("key1");
            var r3 = store.Get("key3");
            Assert.AreEqual(null, r1);
            Assert.AreEqual("paper", r3);
        }

        [Test]
        public void FilterNotWithElements()
        {
            var builder = new StreamBuilder();
            var data = new List<KeyValuePair<string, string>>();
            data.Add(KeyValuePair.Create("key2", "car"));
            data.Add(KeyValuePair.Create("key3", "paper"));

            builder.Table<string, string>("table-topic")
                .FilterNot((k, v) => v.Contains("test", StringComparison.InvariantCultureIgnoreCase), InMemory<string, string>.As("test-store"));

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "table-test-filter";

            Topology t = builder.Build();

            using var driver = new TopologyTestDriver(t, config);
            var inputTopic = driver.CreateInputTopic<string, string>("table-topic");
            inputTopic.PipeInputs(data);

            var store = driver.GetKeyValueStore<string, string>("test-store");
            Assert.IsNotNull(store);
            Assert.AreEqual(store.All().Count(), 2);

            var r2 = store.Get("key2");
            var r3 = store.Get("key3");
            Assert.AreEqual("car", r2);
            Assert.AreEqual("paper", r3);
        }

        [Test]
        public void FilterNotNoElement()
        {
            var builder = new StreamBuilder();
            var data = new List<KeyValuePair<string, string>>();
            data.Add(KeyValuePair.Create("key1", "test1234"));
            data.Add(KeyValuePair.Create("key2", "test"));
            data.Add(KeyValuePair.Create("key3", "testkfkjdf"));

            builder.Table<string, string>("table-topic")
                .FilterNot((k, v) => v.Contains("test", StringComparison.InvariantCultureIgnoreCase), InMemory<string, string>.As("test-store"));

            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = "table-test-filter";

            Topology t = builder.Build();

            using var driver = new TopologyTestDriver(t, config);
            var inputTopic = driver.CreateInputTopic<string, string>("table-topic");
            inputTopic.PipeInputs(data);

            var store = driver.GetKeyValueStore<string, string>("test-store");
            Assert.IsNotNull(store);
            Assert.AreEqual(store.All().Count(), 0);

            var r1 = store.Get("key1");
            var r2 = store.Get("key2");
            Assert.AreEqual(null, r1);
            Assert.AreEqual(null, r2);
        }

    }
}
