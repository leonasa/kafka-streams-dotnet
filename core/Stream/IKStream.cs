using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Table;
using System;
using System.Collections;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Stream
{
    /// <summary>
    /// <see cref="IKStream{K, V}"/> is an abstraction of a record stream of <see cref="KeyValuePair{K, V}"/> pairs, i.e., each record is an independent entity/event in the real world.
    /// For example a user X might buy two items I1 and I2, and thus there might be two records &lt;K:I1&gt;, &lt;K:I2&gt; in the stream.
    /// A <see cref="IKStream{K, V}"/> is either <see cref="StreamBuilder.Stream{K, V}(string)"/> defined from one topic that
    /// are consumed message by message or the result of a <see cref="IKStream{K, V}"/> transformation.
    /// A <see cref="Table.IKTable{K, V}"/> can also be <see cref="Table.IKTable{K, V}.ToStream(string)"/> converted into a <see cref="IKStream{K, V}"/>.
    /// </summary>
    /// <typeparam name="K">Type of key</typeparam>
    /// <typeparam name="V">Type of value</typeparam>
    public interface IKStream<K, V>
    {
        /// <summary>
        /// Creates an array of <see cref="IKStream{K, V}"/> from this stream by branching the records in the original stream based on
        /// the supplied predicates.
        /// Each record is evaluated against the supplied predicates, and predicates are evaluated in order.
        /// 
        /// Each stream in the result array corresponds position-wise (index) to the predicate in the supplied predicates.
        /// 
        /// The branching happens on first-match: A record in the original stream is assigned to the corresponding result
        /// stream for the first predicate that evaluates to true, and is assigned to this stream only.
        /// A record will be dropped if none of the predicates evaluate to true.
        /// This is a stateless record-by-record operation.
        /// </summary>
        /// <param name="predicates">the ordered list of predicate instances</param>
        /// <returns>Multiple distinct substreams of this <see cref="IKStream{K, V}"/></returns>
        IKStream<K, V>[] Branch(params Func<K, V, bool>[] predicates);

        /// <summary>
        /// Creates an array of <see cref="IKStream{K, V}"/> from this stream by branching the records in the original stream based on
        /// the supplied predicates.
        /// Each record is evaluated against the supplied predicates, and predicates are evaluated in order.
        /// 
        /// Each stream in the result array corresponds position-wise (index) to the predicate in the supplied predicates.
        /// 
        /// The branching happens on first-match: A record in the original stream is assigned to the corresponding result
        /// stream for the first predicate that evaluates to true, and is assigned to this stream only.
        /// A record will be dropped if none of the predicates evaluate to true.
        /// This is a stateless record-by-record operation.
        /// </summary>
        /// <param name="named">A <see cref="string"/> config used to name the processor in the topology. Default : null</param>
        /// <param name="predicates">the ordered list of predicate instances</param>
        /// <exception cref="ArgumentNullException">Throw <see cref="ArgumentNullException"/> if one (or multiple) predicate function is null</exception>
        /// <returns>multiple distinct substreams of this <see cref="IKStream{K, V}"/></returns>
        IKStream<K, V>[] Branch(string named, params Func<K, V, bool>[] predicates);

        /// <summary>
        /// Create a new <see cref="IKStream{K, V}"/>
        /// that consists of all records of this stream which satisfy the given predicate.
        /// All records that DO NOT satisfy the predicate are dropped.
        /// This is a stateless record-by-record operation.
        /// </summary>
        /// <param name="predicate">A filter predicate that is applied to each record</param>
        /// <param name="named">A <see cref="string"/> config used to name the processor in the topology. Default : null</param>
        /// <returns>A <see cref="IKStream{K, V}"/> that contains only those records that satisfy the given predicate</returns>
        /// <exception cref="ArgumentNullException">Throw <see cref="ArgumentNullException"/> when predicate function is null</exception>
        IKStream<K, V> Filter(Func<K, V, bool> predicate, string named = null);

        /// <summary>
        /// Create a new <see cref="IKStream{K, V}"/>
        /// that consists of all records of this stream which do NOT satisfy the given predicate.
        /// All records that DO satisfy the predicate are dropped.
        /// This is a stateless record-by-record operation.
        /// </summary>
        /// <param name="predicate">A filter predicate that is applied to each record</param>
        /// <param name="named">A <see cref="string"/> config used to name the processor in the topology. Default : null</param>
        /// <returns>A <see cref="IKStream{K, V}"/> that contains only those records that DO NOT satisfy the given predicate</returns>
        /// <exception cref="ArgumentNullException">Throw <see cref="ArgumentNullException"/> when predicate function is null</exception>
        IKStream<K, V> FilterNot(Func<K, V, bool> predicate, string named = null);

        /// <summary>
        /// Materialize this stream to a topic using default serializers specified in the config and producer's.
        /// The specified topic should be manually created before it is used(i.e., before the Kafka Streams application is
        /// started).
        /// </summary>
        /// <param name="topicName">the topic name</param>
        /// <param name="named">A <see cref="string"/> config used to name the processor in the topology. Default : null</param>
        /// <exception cref="ArgumentNullException">Throw <see cref="ArgumentNullException"/> if <paramref name="topicName"/> is null</exception>
        /// /// <exception cref="ArgumentException">Throw <see cref="ArgumentException"/> if <paramref name="topicName"/> is incorrect</exception>
        void To(string topicName, string named = null);

        /// <summary>
        /// Dynamically materialize this stream to topics using default serializers specified in the config and producer's.
        /// The topic names for each record to send to is dynamically determined based on the <code>Func&lt;K, V, IRecordContext, string&gt;</code>.
        /// </summary>
        /// <param name="topicExtractor">Extractor function to determine the name of the Kafka topic to write to for each record</param>
        /// <param name="named">A <see cref="string"/> config used to name the processor in the topology. Default : null</param>
        void To(Func<K, V, IRecordContext, string> topicExtractor, string named = null);

        /// <summary>
        /// Dynamically materialize this stream to topics using default serializers specified in the config and producer's.
        /// The topic names for each record to send to is dynamically determined based on the <see cref="ITopicNameExtractor&lt;K, V&gt;"/>}.
        /// </summary>
        /// <param name="topicExtractor">The extractor to determine the name of the Kafka topic to write to for each record</param>
        /// <param name="named">A <see cref="string"/> config used to name the processor in the topology. Default : null</param>
        void To(ITopicNameExtractor<K, V> topicExtractor, string named = null);

        /// <summary>
        /// Materialize this stream to a topic using <typeparamref name="KS"/> and <typeparamref name="VS"/> serializers specified in the method parameters.
        /// The specified topic should be manually created before it is used(i.e., before the Kafka Streams application is
        /// started).
        /// </summary>
        /// <typeparam name="KS">New type key serializer</typeparam>
        /// <typeparam name="VS">New type value serializer</typeparam>
        /// <param name="topicName">the topic name</param>
        /// <param name="named">A <see cref="string"/> config used to name the processor in the topology. Default : null</param>
        void To<KS, VS>(string topicName, string named = null) where KS : ISerDes<K>, new() where VS : ISerDes<V>, new();

        /// <summary>
        /// Dynamically materialize this stream to a topic using <typeparamref name="KS"/> and <typeparamref name="VS"/> serializers specified in the method parameters.
        /// The topic names for each record to send to is dynamically determined based on the <code>Func&lt;K, V, IRecordContext, string&gt;</code>.
        /// </summary>
        /// <typeparam name="KS">New type key serializer</typeparam>
        /// <typeparam name="VS">New type value serializer</typeparam>
        /// <param name="topicExtractor">Extractor function to determine the name of the Kafka topic to write to for each record</param>
        /// <param name="named">A <see cref="string"/> config used to name the processor in the topology. Default : null</param>
        void To<KS, VS>(Func<K, V, IRecordContext, string> topicExtractor, string named = null) where KS : ISerDes<K>, new() where VS : ISerDes<V>, new();

        /// <summary>
        /// Dynamically materialize this stream to a topic using <typeparamref name="KS"/> and <typeparamref name="VS"/> serializers specified in the method parameters.
        /// The topic names for each record to send to is dynamically determined based on the <see cref="ITopicNameExtractor&lt;K, V&gt;"/>}.
        /// </summary>
        /// <typeparam name="KS">New type key serializer</typeparam>
        /// <typeparam name="VS">New type value serializer</typeparam>
        /// <param name="topicExtractor">The extractor to determine the name of the Kafka topic to write to for each record</param>
        /// <param name="named">A <see cref="string"/> config used to name the processor in the topology. Default : null</param>
        void To<KS, VS>(ITopicNameExtractor<K, V> topicExtractor, string named = null) where KS : ISerDes<K>, new() where VS : ISerDes<V>, new();

        /// <summary>
        /// Transform each record of the input stream into zero or more records in the output stream (bot
        /// can be altered arbitrarily).
        /// The provided <see cref="IKeyValueMapper{K, V, VR}"/> is applied to each input record and computes zero or more
        /// Thus, an input record &lt;<typeparamref name="K"/>, <typeparamref name="V"/>&gt; can be transformed into an output record &lt;<typeparamref name="KR"/>, <typeparamref name="VR"/>&gt;
        /// This is a stateless record-by-record operation.
        /// The provided <see cref="IKeyValueMapper{K, V, VR}"/> must return an <see cref="IEnumerable"/> and the return value must not be null.
        /// Flat-mapping records might result in an internal data redistribution if a key based operator 
        /// or join) is applied to the result <see cref="IKStream{KR, VR}"/>.
        /// </summary>
        /// <typeparam name="KR">the key type of the result stream</typeparam>
        /// <typeparam name="VR">the value type of the result stream</typeparam>
        /// <param name="mapper">A <see cref="IKeyValueMapper{K, V, VR}"/> mapper that computes the new output records</param>
        /// <param name="named">A <see cref="string"/> config used to name the processor in the topology. Default : null</param>
        /// <returns>A <see cref="IKStream{KR, VR}"/> that contains more or less records with new key and value (possibly of different type)</returns>
        /// <exception cref="ArgumentNullException">Throw <see cref="ArgumentNullException"/> when mapper function is null</exception>
        IKStream<KR, VR> FlatMap<KR, VR>(IKeyValueMapper<K, V, IEnumerable<KeyValuePair<KR, VR>>> mapper, string named = null);

        /// <summary>
        /// Transform each record of the input stream into zero or more records in the output stream (bot
        /// can be altered arbitrarily).
        /// The provided <code>Func&lt;K, V, IEnumerable&lt;KeyValuePair&lt;KR,VR&gt;&gt;&gt;</code> is applied to each input record and computes zero or more
        /// Thus, an input record &lt;<typeparamref name="K"/>, <typeparamref name="V"/>&gt; can be transformed into an output record &lt;<typeparamref name="KR"/>, <typeparamref name="VR"/>&gt;
        /// This is a stateless record-by-record operation.
        /// The provided <code>Func&lt;K, V, IEnumerable&lt;KeyValuePair&lt;KR,VR&gt;&gt;&gt;</code> must return an <see cref="IEnumerable"/> and the return value must not be null.
        /// Flat-mapping records might result in an internal data redistribution if a key based operator 
        /// or join) is applied to the result <see cref="IKStream{KR, VR}"/>.
        /// </summary>
        /// <typeparam name="KR">the key type of the result stream</typeparam>
        /// <typeparam name="VR">the value type of the result stream</typeparam>
        /// <param name="mapper">A function mapper that computes the new output records</param>
        /// <param name="named">A <see cref="string"/> config used to name the processor in the topology. Default : null</param>
        /// <returns>A <see cref="IKStream{KR, VR}"/> that contains more or less records with new key and value (possibly of different type)</returns>
        /// <exception cref="ArgumentNullException">Throw <see cref="ArgumentNullException"/> when mapper function is null</exception>
        IKStream<KR, VR> FlatMap<KR, VR>(Func<K, V, IEnumerable<KeyValuePair<KR, VR>>> mapper, string named = null);

        /// <summary>
        /// Transform each record of the input stream into zero or more records with the same (unmodified) key in the output stream.
        /// The provided <see cref="IValueMapper{V, VR}"/> is applied to each input record and computes zero or more
        /// Thus, an input record &lt;<typeparamref name="K"/>, <typeparamref name="V"/>&gt; can be transformed into an output record &lt;<typeparamref name="K"/>, <typeparamref name="VR"/>&gt;
        /// This is a stateless record-by-record operation.
        /// The provided <see cref="IValueMapper{V, VR}"/> must return an <see cref="IEnumerable{VR}"/> and the return value must not be null.
        /// Flat-mapping records might result in an internal data redistribution if a key based operator 
        /// or join) is applied to the result <see cref="IKStream{K, VR}"/>.
        /// </summary>
        /// <typeparam name="VR">the value type of the result stream</typeparam>
        /// <param name="mapper">A <see cref="IValueMapper{V, VR}"/> mapper that computes the new output records</param>
        /// <param name="named">A <see cref="string"/> config used to name the processor in the topology. Default : null</param>
        /// <returns>A <see cref="IKStream{K, VR}"/> that contains more or less records with new key and value (possibly of different type)</returns>
        /// <exception cref="ArgumentNullException">Throw <see cref="ArgumentNullException"/> when mapper function is null</exception>
        IKStream<K, VR> FlatMapValues<VR>(IValueMapper<V, IEnumerable<VR>> mapper, string named = null);

        /// <summary>
        /// Transform each record of the input stream into zero or more records with the same (unmodified) key in the output stream.
        /// The provided <code>Func&lt;V, IEnumerable&lt;VR&gt;</code> is applied to each input record and computes zero or more
        /// Thus, an input record &lt;<typeparamref name="K"/>, <typeparamref name="V"/>&gt; can be transformed into an output record &lt;<typeparamref name="K"/>, <typeparamref name="VR"/>&gt;
        /// This is a stateless record-by-record operation.
        /// The provided <code>Func&lt;V, IEnumerable&lt;VR&gt;</code> must return an <see cref="IEnumerable"/> and the return value must not be null.
        /// Flat-mapping records might result in an internal data redistribution if a key based operator 
        /// or join) is applied to the result <see cref="IKStream{KR, VR}"/>.
        /// </summary>
        /// <typeparam name="VR">the value type of the result stream</typeparam>
        /// <param name="mapper">A function mapper that computes the new output records</param>
        /// <param name="named">A <see cref="string"/> config used to name the processor in the topology. Default : null</param>
        /// <returns>A <see cref="IKStream{KR, VR}"/> that contains more or less records with new key and value (possibly of different type)</returns>
        /// /// <exception cref="ArgumentNullException">Throw <see cref="ArgumentNullException"/> when mapper function is null</exception>
        IKStream<K, VR> FlatMapValues<VR>(Func<V, IEnumerable<VR>> mapper, string named = null);

        /// <summary>
        /// Transform each record of the input stream into zero or more records with the same (unmodified) key in the output stream.
        /// The provided <see cref="IValueMapperWithKey{K, V, VR}"/> is applied to each input record and computes zero or more
        /// Thus, an input record &lt;<typeparamref name="K"/>, <typeparamref name="V"/>&gt; can be transformed into an output record &lt;<typeparamref name="K"/>, <typeparamref name="VR"/>&gt;
        /// This is a stateless record-by-record operation.
        /// The provided <see cref="IValueMapperWithKey{K, V, VR}"/> must return an <see cref="IEnumerable{VR}"/> and the return value must not be null.
        /// Flat-mapping records might result in an internal data redistribution if a key based operator 
        /// or join) is applied to the result <see cref="IKStream{K, VR}"/>.
        /// </summary>
        /// <typeparam name="VR">the value type of the result stream</typeparam>
        /// <param name="mapper">A <see cref="IValueMapperWithKey{K, V, VR}"/> mapper that computes the new output records</param>
        /// <param name="named">A <see cref="string"/> config used to name the processor in the topology. Default : null</param>
        /// <returns>A <see cref="IKStream{K, VR}"/> that contains more or less records with new key and value (possibly of different type)</returns>
        /// /// <exception cref="ArgumentNullException">Throw <see cref="ArgumentNullException"/> when mapper function is null</exception>
        IKStream<K, VR> FlatMapValues<VR>(IValueMapperWithKey<K, V, IEnumerable<VR>> mapper, string named = null);

        /// <summary>
        /// Transform each record of the input stream into zero or more records with the same (unmodified) key in the output stream.
        /// The provided <code>Func&lt;K, V, IEnumerable&lt;VR&gt;</code> is applied to each input record and computes zero or more
        /// Thus, an input record &lt;<typeparamref name="K"/>, <typeparamref name="V"/>&gt; can be transformed into an output record &lt;<typeparamref name="K"/>, <typeparamref name="VR"/>&gt;
        /// This is a stateless record-by-record operation.
        /// The provided <code>Func&lt;K, V, IEnumerable&lt;VR&gt;</code> must return an <see cref="IEnumerable"/> and the return value must not be null.
        /// Flat-mapping records might result in an internal data redistribution if a key based operator 
        /// or join) is applied to the result <see cref="IKStream{KR, VR}"/>.
        /// </summary>
        /// <typeparam name="VR">the value type of the result stream</typeparam>
        /// <param name="mapper">A function mapper that computes the new output records</param>
        /// <param name="named">A <see cref="string"/> config used to name the processor in the topology. Default : null</param>
        /// <returns>A <see cref="IKStream{KR, VR}"/> that contains more or less records with new key and value (possibly of different type)</returns>
        /// <exception cref="ArgumentNullException">Throw <see cref="ArgumentNullException"/> when mapper function is null</exception>
        IKStream<K, VR> FlatMapValues<VR>(Func<K, V, IEnumerable<VR>> mapper, string named = null);

        /// <summary>
        /// Perform an action on each record of {@code KStream}.
        /// This is a stateless record-by-record operation
        /// Note that this is a terminal operation that returns void.
        /// </summary>
        /// <param name="action">An action to perform on each record</param>
        /// <param name="named">A <see cref="string"/> config used to name the processor in the topology. Default : null</param>
        /// <exception cref="ArgumentNullException">Throw <see cref="ArgumentNullException"/> when action is null</exception>
        void Foreach(Action<K, V> action, string named = null);

        /// <summary>
        /// Print the records of this KStream using the options provided by <see cref="Printed{K, V}"/>
        /// Note that this is mainly for debugging/testing purposes, and it will try to flush on each record print.
        /// It SHOULD NOT be used for production usage if performance requirements are concerned.
        /// </summary>
        /// <param name="printed">Printed options for printing</param>
        /// <exception cref="ArgumentNullException">Throw <see cref="ArgumentNullException"/> when printed is null</exception>
        void Print(Printed<K, V> printed);

        /// <summary>
        /// Transform each record of the input stream into a new record in the output stream (both key and value type can be
        /// altered arbitrarily).
        /// The provided <see cref="IKeyValueMapper{K, V, VR}"/> is applied to each input record and computes a new output record.
        /// Thus, an input record &lt;<typeparamref name="K"/>, <typeparamref name="V"/>&gt; can be transformed into an output record &lt;<typeparamref name="KR"/>, <typeparamref name="VR"/>&gt;
        /// This is a stateless record-by-record operation.
        /// The example below normalizes the String key to upper-case letters and counts the number of token of the value string.
        /// Mapping records might result in an internal data redistribution if a key based operator (like an aggregation or
        /// join) is applied to the result <see cref="IKStream{KR, VR}"/>. (<seealso cref="IKStream{K, V}.MapValues{VR}(IValueMapper{V, VR}, string)"/>
        /// </summary>
        /// <typeparam name="KR">the key type of the result stream</typeparam>
        /// <typeparam name="VR">the value type of the result stream</typeparam>
        /// <param name="mapper">A <see cref="IKeyValueMapper{K, V, VR}"/> mapper that computes a new output record</param>
        /// <param name="named">A <see cref="string"/> config used to name the processor in the topology. Default : null</param>
        /// <returns>A <see cref="IKStream{KR, VR}"/> that contains records with new key and value (possibly both of different type)</returns>
        /// <exception cref="ArgumentNullException">Throw <see cref="ArgumentNullException"/> when mapper function is null</exception>
        IKStream<KR, VR> Map<KR, VR>(IKeyValueMapper<K, V, KeyValuePair<KR, VR>> mapper, string named = null);

        /// <summary>
        /// Transform each record of the input stream into a new record in the output stream (both key and value type can be
        /// altered arbitrarily).
        /// The provided <code>Func&lt;K, V, KeyValuePair&lt;KR, VR&gt;&gt;</code> is applied to each input record and computes a new output record.
        /// Thus, an input record &lt;<typeparamref name="K"/>, <typeparamref name="V"/>&gt; can be transformed into an output record &lt;<typeparamref name="KR"/>, <typeparamref name="VR"/>&gt;
        /// This is a stateless record-by-record operation.
        /// The example below normalizes the String key to upper-case letters and counts the number of token of the value string.
        /// <example>
        /// <code>
        /// var stream = builder.Stream&lt;string, string&gt;("topic");
        /// var outerStream = stream.Map((k,v) => new KeyValuePair<typeparamref name="KR"/>, <typeparamref name="VR"/>>(k.ToUpperCase(), v.Split(" ").Length)
        /// </code>
        /// </example>
        /// Mapping records might result in an internal data redistribution if a key based operator (like an aggregation or
        /// join) is applied to the result <see cref="IKStream{KR, VR}"/>. (<seealso cref="IKStream{K, V}.MapValues{VR}(Func{K, V, VR}, string)"/>
        /// </summary>
        /// <typeparam name="KR">the key type of the result stream</typeparam>
        /// <typeparam name="VR">the value type of the result stream</typeparam>
        /// <param name="mapper">A mapper function that computes a new output record</param>
        /// <param name="named">A <see cref="string"/> config used to name the processor in the topology. Default : null</param>
        /// <returns>A <see cref="IKStream{KR, VR}"/> that contains records with new key and value (possibly both of different type)</returns>
        /// <exception cref="ArgumentNullException">Throw <see cref="ArgumentNullException"/> when mapper function is null</exception>
        IKStream<KR, VR> Map<KR, VR>(Func<K, V, KeyValuePair<KR, VR>> mapper, string named = null);

        /// <summary>
        /// Transform the value of each input record into a new value (with possible new type) of the output record.
        /// The provided <see cref="IValueMapper{V, VR}"/> is applied to each input record value and computes a new value for it.
        /// Thus, an input record &lt;<typeparamref name="K"/>, <typeparamref name="V"/>&gt; can be transformed into an output record &lt;<typeparamref name="K"/>, <typeparamref name="VR"/>&gt;.
        /// This is a stateless record-by-record operation.
        /// Setting a new value preserves data co-location with respect to the key.
        /// Thus, NO internal data redistribution is required if a key based operator (like an aggregation or join) is applied to the result <see cref="IKStream{K, VR}"/>.
        /// </summary>
        /// <typeparam name="VR">the value type of the result stream</typeparam>
        /// <param name="mapper">A <see cref="IValueMapper{V, VR}"/> mapper that computes a new output value</param>
        /// <param name="named">A <see cref="string"/> config used to name the processor in the topology. Default : null</param>
        /// <returns>A <see cref="IKStream{K, VR}"/> that contains records with unmodified key and new values (possibly of different type)</returns>
        /// <exception cref="ArgumentNullException">Throw <see cref="ArgumentNullException"/> when mapper function is null</exception>
        IKStream<K, VR> MapValues<VR>(IValueMapper<V, VR> mapper, string named = null);

        /// <summary>
        /// Transform the value of each input record into a new value (with possible new type) of the output record.
        /// The provided <code>Func&lt;V, VR&gt;</code> is applied to each input record value and computes a new value for it.
        /// Thus, an input record &lt;<typeparamref name="K"/>, <typeparamref name="V"/>&gt; can be transformed into an output record &lt;<typeparamref name="K"/>, <typeparamref name="VR"/>&gt;.
        /// This is a stateless record-by-record operation.
        /// <example>
        /// <code>
        /// var stream = builder.Stream&lt;string, string&gt;("topic");
        /// var outerStream = stream.MapValues((v) => v.Split(" ").Length)
        /// </code>
        /// </example>
        /// Setting a new value preserves data co-location with respect to the key.
        /// Thus, NO internal data redistribution is required if a key based operator (like an aggregation or join) is applied to the result <see cref="IKStream{K, VR}"/>.
        /// </summary>
        /// <typeparam name="VR">the value type of the result stream</typeparam>
        /// <param name="mapper">A function mapper that computes a new output value</param>
        /// <param name="named">A <see cref="string"/> config used to name the processor in the topology. Default : null</param>
        /// <returns>A <see cref="IKStream{K, VR}"/> that contains records with unmodified key and new values (possibly of different type)</returns>
        /// <exception cref="ArgumentNullException">Throw <see cref="ArgumentNullException"/> when mapper function is null</exception>
        IKStream<K, VR> MapValues<VR>(Func<V, VR> mapper, string named = null);

        /// <summary>
        /// Transform the value of each input record into a new value (with possible new type) of the output record.
        /// The provided <see cref="IValueMapperWithKey{K, V, VR}"/> is applied to each input record value and computes a new value for it.
        /// Thus, an input record &lt;<typeparamref name="K"/>, <typeparamref name="V"/>&gt; can be transformed into an output record &lt;<typeparamref name="K"/>, <typeparamref name="VR"/>&gt;.
        /// This is a stateless record-by-record operation.
        /// Note that the key is read-only and should not be modified, as this can lead to corrupt partitioning.
        /// Setting a new value preserves data co-location with respect to the key.
        /// Thus, NO internal data redistribution is required if a key based operator (like an aggregation or join) is applied to the result <see cref="IKStream{K, VR}"/>.
        /// </summary>
        /// <typeparam name="VR">the value type of the result stream</typeparam>
        /// <param name="mapper">A <see cref="IValueMapperWithKey{K, V, VR}"/> mapper that computes a new output value</param>
        /// <param name="named">A <see cref="string"/> config used to name the processor in the topology. Default : null</param>
        /// <returns>A <see cref="IKStream{K, VR}"/> that contains records with unmodified key and new values (possibly of different type)</returns>
        /// <exception cref="ArgumentNullException">Throw <see cref="ArgumentNullException"/> when mapper function is null</exception>
        IKStream<K, VR> MapValues<VR>(IValueMapperWithKey<K, V, VR> mapper, string named = null);

        /// <summary>
        /// Transform the value of each input record into a new value (with possible new type) of the output record.
        /// The provided <code>Func&lt;K, V, VR&gt;</code> is applied to each input record value and computes a new value for it.
        /// Thus, an input record &lt;<typeparamref name="K"/>, <typeparamref name="V"/>&gt; can be transformed into an output record &lt;<typeparamref name="K"/>, <typeparamref name="VR"/>&gt;.
        /// This is a stateless record-by-record operation.
        /// <example>
        /// <code>
        /// var stream = builder.Stream&lt;string, string&gt;("topic");
        /// var outerStream = stream.MapValues((k,v) => v.Split(" ").Length)
        /// </code>
        /// </example>
        /// Note that the key is read-only and should not be modified, as this can lead to corrupt partitioning.
        /// Setting a new value preserves data co-location with respect to the key.
        /// Thus, NO internal data redistribution is required if a key based operator (like an aggregation or join) is applied to the result <see cref="IKStream{K, VR}"/>.
        /// </summary>
        /// <typeparam name="VR">the value type of the result stream</typeparam>
        /// <param name="mapper">A function mapper that computes a new output value</param>
        /// <param name="named">A <see cref="string"/> config used to name the processor in the topology. Default : null</param>
        /// <returns>A <see cref="IKStream{K, VR}"/> that contains records with unmodified key and new values (possibly of different type)</returns>
        /// <exception cref="ArgumentNullException">Throw <see cref="ArgumentNullException"/> when mapper function is null</exception>
        IKStream<K, VR> MapValues<VR>(Func<K, V, VR> mapper, string named = null);

        /// <summary>
        /// Perform an action on each record of <see cref="IKStream{K, V}"/>.
        /// This is a stateless record-by-record operation.
        /// Peek is a non-terminal operation that triggers a side effect(such as logging or statistics collection)
        /// and returns an unchanged stream.
        /// Note that since this operation is stateless, it may execute multiple times for a single record in failure cases.
        /// </summary>
        /// <param name="action">An action to perform on each record</param>
        /// <param name="named">A <see cref="string"/> config used to name the processor in the topology. Default : null</param>
        /// <exception cref="ArgumentNullException">Throw <see cref="ArgumentNullException"/> exception if <paramref name="action"/> is null</exception>
        /// <returns>Itself</returns>
        IKStream<K, V> Peek(Action<K, V> action, string named = null);

        /// <summary>
        /// Set a new key (with possibly new type) for each input record.
        /// The provided <see cref="IKeyValueMapper{K, V, VR}"/> is applied to each input record and computes a new key for it.
        /// Thus, an input record &lt;<typeparamref name="K"/>, <typeparamref name="V"/>&gt; can be transformed into an output record &lt;<typeparamref name="KR"/>, <typeparamref name="V"/>&gt;.
        /// This is a stateless record-by-record operation.
        /// For example, you can use this transformation to set a key for a key-less input record &lt;null,<typeparamref name="V"/>&gt; by
        /// extracting a key from the value within your <see cref="IKeyValueMapper{K, V, VR}"/>
        /// The example below computes the new key as the length of the value string.
        /// <example>
        /// <code>
        /// var stream = builder.Stream&lt;string, string&gt;("key-less-topic");
        /// var keyStream = stream.SelectKey((k,v) => v.Length);
        /// </code>
        /// </example>
        /// Setting a new key might result in an internal data redistribution if a key based operator (like an aggregation or
        /// join) is applied to the result <see cref="IKStream{KR, V}"/>
        /// </summary>
        /// <typeparam name="KR">the new key type of the result stream</typeparam>
        /// <param name="mapper">A <see cref="IKeyValueMapper{K, V, VR}"/> mapper that computes a new key for each record</param>
        /// <param name="named">A <see cref="string"/> config used to name the processor in the topology. Default : null</param>
        /// <returns>A <see cref="IKStream{KR, V}"/> that contains records with new key (possibly of different type) and unmodified value</returns>
        /// <exception cref="ArgumentNullException">Throw <see cref="ArgumentNullException"/> when mapper function is null</exception>
        IKStream<KR, V> SelectKey<KR>(IKeyValueMapper<K, V, KR> mapper, string named = null);

        /// <summary>
        /// Set a new key (with possibly new type) for each input record.
        /// The provided <code>Func&lt;K, V, VR&gt;</code> is applied to each input record and computes a new key for it.
        /// Thus, an input record &lt;<typeparamref name="K"/>, <typeparamref name="V"/>&gt; can be transformed into an output record &lt;<typeparamref name="KR"/>, <typeparamref name="V"/>&gt;.
        /// This is a stateless record-by-record operation.
        /// For example, you can use this transformation to set a key for a key-less input record &lt;null,<typeparamref name="V"/>&gt; by
        /// extracting a key from the value within your <code>Func&lt;K, V, VR&gt;</code>
        /// The example below computes the new key as the length of the value string.
        /// <example>
        /// <code>
        /// var stream = builder.Stream&lt;string, string&gt;("key-less-topic");
        /// var keyStream = stream.SelectKey((k,v) => v.Length);
        /// </code>
        /// </example>
        /// Setting a new key might result in an internal data redistribution if a key based operator (like an aggregation or
        /// join) is applied to the result <see cref="IKStream{KR, V}"/>
        /// </summary>
        /// <typeparam name="KR">the new key type of the result stream</typeparam>
        /// <param name="mapper">A function mapper that computes a new key for each record</param>
        /// <param name="named">A <see cref="string"/> config used to name the processor in the topology. Default : null</param>
        /// <returns>A <see cref="IKStream{KR, V}"/> that contains records with new key (possibly of different type) and unmodified value</returns>
        /// <exception cref="ArgumentNullException">Throw <see cref="ArgumentNullException"/> when mapper function is null</exception>
        IKStream<KR, V> SelectKey<KR>(Func<K, V, KR> mapper, string named = null);

        /// <summary>
        /// Group the records of this <see cref="IKStream{K, V}"/> on a new key that is selected using the provided <see cref="IKeyValueMapper{K, V, VR}"/> and default serializers and deserializers.
        /// Grouping a stream on the record key is required before an aggregation operator can be applied to the data <see cref="IKGroupedStream{KR, V}"/>
        /// The provider <see cref="IKeyValueMapper{K, V, VR}"/> selects a new key (which may or may not be of the same type) while preserving the
        /// original values.
        /// If the new record key is null the record will not be included in the resulting.
        /// Because a new key is selected, an internal repartitioning topic may need to be created in Kafka if a
        /// later operator depends on the newly selected key.
        /// This topic will be named "${applicationId}-&lt;name&gt;-repartition", where "applicationId" is user-specified in
        /// <see cref="IStreamConfig"/> via parameter <see cref="IStreamConfig.ApplicationId"/>.
        /// "&lt;name&gt;" is an internally generated name, and "-repartition" is a fixed suffix.
        /// All data of this stream will be redistributed through the repartitioning topic by writing all records to it,
        /// and rereading all records from it, such that the resulting <see cref="IKGroupedStream{KR, V}"/> is partitioned on the new key.
        /// This operation is equivalent to calling <see cref="IKStream{K, V}.SelectKey{KR}(Func{K, V, KR}, string)"/> followed by <see cref="IKStream{K, V}.GroupByKey(string)"/>.
        /// If the key type is changed, it is recommended to use <see cref="IKStream{K, V}.GroupBy{KR}(Func{K, V, KR}, string)"/> instead.
        /// </summary>
        /// <typeparam name="KR">the key type of the result</typeparam>
        /// <param name="keySelector">A <see cref="IKeyValueMapper{K, V, VR}"/> selector that computes a new key for grouping</param>
        /// <param name="named">A <see cref="string"/> config used to name the processor in the topology. Default : null</param>
        /// <returns>A <see cref="IKGroupedStream{KR, V}"/> that contains the grouped records of the original <see cref="IKStream{K, V}"/></returns>
        /// <exception cref="ArgumentNullException">Throw <see cref="ArgumentNullException"/> when selector function is null</exception>
        IKGroupedStream<KR, V> GroupBy<KR>(IKeyValueMapper<K, V, KR> keySelector, string named = null);

        /// <summary>
        /// Group the records of this <see cref="IKStream{K, V}"/> on a new key that is selected using the provided <code>Func&lt;K, V, VR&gt;</code> and default serializers and deserializers.
        /// Grouping a stream on the record key is required before an aggregation operator can be applied to the data <see cref="IKGroupedStream{KR, V}"/>
        /// The provider <code>Func&lt;K, V, VR&gt;</code> selects a new key (which may or may not be of the same type) while preserving the
        /// original values.
        /// If the new record key is null the record will not be included in the resulting.
        /// Because a new key is selected, an internal repartitioning topic may need to be created in Kafka if a
        /// later operator depends on the newly selected key.
        /// This topic will be named "${applicationId}-&lt;name&gt;-repartition", where "applicationId" is user-specified in
        /// <see cref="IStreamConfig"/> via parameter <see cref="IStreamConfig.ApplicationId"/>.
        /// "&lt;name&gt;" is an internally generated name, and "-repartition" is a fixed suffix.
        /// All data of this stream will be redistributed through the repartitioning topic by writing all records to it,
        /// and rereading all records from it, such that the resulting <see cref="IKGroupedStream{KR, V}"/> is partitioned on the new key.
        /// This operation is equivalent to calling <see cref="IKStream{K, V}.SelectKey{KR}(Func{K, V, KR}, string)"/> followed by <see cref="IKStream{K, V}.GroupByKey(string)"/>.
        /// If the key type is changed, it is recommended to use <see cref="IKStream{K, V}.GroupBy{KR}(Func{K, V, KR}, string)"/> instead.
        /// </summary>
        /// <typeparam name="KR">the key type of the result</typeparam>
        /// <param name="keySelector">A function selector that computes a new key for grouping</param>
        /// <param name="named">A <see cref="string"/> config used to name the processor in the topology. Default : null</param>
        /// <returns>A <see cref="IKGroupedStream{KR, V}"/> that contains the grouped records of the original <see cref="IKStream{K, V}"/></returns>
        /// <exception cref="ArgumentNullException">Throw <see cref="ArgumentNullException"/> when selector function is null</exception>
        IKGroupedStream<KR, V> GroupBy<KR>(Func<K, V, KR> keySelector, string named = null);

        /// <summary>
        /// Group the records of this <see cref="IKStream{K, V}"/> on a new key that is selected using the provided <see cref="IKeyValueMapper{K, V, VR}"/> and default serializers and deserializers.
        /// Grouping a stream on the record key is required before an aggregation operator can be applied to the data <see cref="IKGroupedStream{KR, V}"/>
        /// The provider <see cref="IKeyValueMapper{K, V, VR}"/> selects a new key (which may or may not be of the same type) while preserving the
        /// original values.
        /// If the new record key is null the record will not be included in the resulting.
        /// Because a new key is selected, an internal repartitioning topic may need to be created in Kafka if a
        /// later operator depends on the newly selected key.
        /// This topic will be named "${applicationId}-&lt;name&gt;-repartition", where "applicationId" is user-specified in
        /// <see cref="IStreamConfig"/> via parameter <see cref="IStreamConfig.ApplicationId"/>.
        /// "&lt;name&gt;" is an internally generated name, and "-repartition" is a fixed suffix.
        /// All data of this stream will be redistributed through the repartitioning topic by writing all records to it,
        /// and rereading all records from it, such that the resulting <see cref="IKGroupedStream{KR, V}"/> is partitioned on the new key.
        /// This operation is equivalent to calling <see cref="IKStream{K, V}.SelectKey{KR}(Func{K, V, KR}, string)"/> followed by <see cref="IKStream{K, V}.GroupByKey(string)"/>.
        /// If the key type is changed, it is recommended to use <see cref="IKStream{K, V}.GroupBy{KR}(Func{K, V, KR}, string)"/> instead.
        /// </summary>
        /// <typeparam name="KR">the key type of the result</typeparam>
        /// <typeparam name="KRS">New serializer for <typeparamref name="KR"/> type</typeparam>
        /// <param name="keySelector">A <see cref="IKeyValueMapper{K, V, VR}"/> selector that computes a new key for grouping</param>
        /// <param name="named">A <see cref="string"/> config used to name the processor in the topology. Default : null</param>
        /// <returns>A <see cref="IKGroupedStream{KR, V}"/> that contains the grouped records of the original <see cref="IKStream{K, V}"/></returns>
        /// <exception cref="ArgumentNullException">Throw <see cref="ArgumentNullException"/> when selector function is null</exception>
        IKGroupedStream<KR, V> GroupBy<KR, KRS>(IKeyValueMapper<K, V, KR> keySelector, string named = null) where KRS : ISerDes<KR>, new();

        /// <summary>
        /// Group the records of this <see cref="IKStream{K, V}"/> on a new key that is selected using the provided <code>Func&lt;K, V, VR&gt;</code> and default serializers and deserializers.
        /// Grouping a stream on the record key is required before an aggregation operator can be applied to the data <see cref="IKGroupedStream{KR, V}"/>
        /// The provider <code>Func&lt;K, V, VR&gt;</code> selects a new key (which may or may not be of the same type) while preserving the
        /// original values.
        /// If the new record key is null the record will not be included in the resulting.
        /// Because a new key is selected, an internal repartitioning topic may need to be created in Kafka if a
        /// later operator depends on the newly selected key.
        /// This topic will be named "${applicationId}-&lt;name&gt;-repartition", where "applicationId" is user-specified in
        /// <see cref="IStreamConfig"/> via parameter <see cref="IStreamConfig.ApplicationId"/>.
        /// "&lt;name&gt;" is an internally generated name, and "-repartition" is a fixed suffix.
        /// All data of this stream will be redistributed through the repartitioning topic by writing all records to it,
        /// and rereading all records from it, such that the resulting <see cref="IKGroupedStream{KR, V}"/> is partitioned on the new key.
        /// This operation is equivalent to calling <see cref="IKStream{K, V}.SelectKey{KR}(Func{K, V, KR}, string)"/> followed by <see cref="IKStream{K, V}.GroupByKey(string)"/>.
        /// If the key type is changed, it is recommended to use <see cref="IKStream{K, V}.GroupBy{KR}(Func{K, V, KR}, string)"/> instead.
        /// </summary>
        /// <typeparam name="KR">the key type of the result</typeparam>
        /// <typeparam name="KRS">New serializer for <typeparamref name="KR"/> type</typeparam>
        /// <param name="keySelector">A function selector that computes a new key for grouping</param>
        /// <param name="named">A <see cref="string"/> config used to name the processor in the topology. Default : null</param>
        /// <returns>A <see cref="IKGroupedStream{KR, V}"/> that contains the grouped records of the original <see cref="IKStream{K, V}"/></returns>
        /// <exception cref="ArgumentNullException">Throw <see cref="ArgumentNullException"/> when selector function is null</exception>
        IKGroupedStream<KR, V> GroupBy<KR, KRS>(Func<K, V, KR> keySelector, string named = null) where KRS : ISerDes<KR>, new();

        /// <summary>
        /// Group the records by their current key into a <see cref="IKGroupedStream{K, V}"/> while preserving the original values
        /// and default serializers and deserializers.
        /// Grouping a stream on the record key is required before an aggregation operator can be applied to the data.
        /// If a record key is null , the record will not be included in the resulting.
        /// If a key changing operator was used before this operation and no data redistribution happened afterwards an internal repartitioning topic may need to be created in Kafka if a later
        /// operator depends on the newly selected key.
        /// This topic will be named "${applicationId}-&lt;name&gt;-repartition", where "applicationId" is user-specified in
        /// <see cref="IStreamConfig"/> via parameter <see cref="IStreamConfig.ApplicationId"/>,
        /// "&lt;name&gt;" is an internally generated name, and "-repartition" is a fixed suffix.
        /// For this case, all data of this stream will be redistributed through the repartitioning topic by writing all
        /// records to it, and rereading all records from it, such that the resulting <see cref="IKGroupedStream{K, V}"/> is partitioned
        /// correctly on its key.
        /// If the last key changing operator changed the key type, it is recommended to use <see cref="IKStream{K, V}.GroupByKey{KS, VS}(string)"/>.
        /// </summary>
        /// <param name="named">A <see cref="string"/> config used to name the processor in the topology. Default : null</param>
        /// <returns>A <see cref="IKGroupedStream{K, V}"/> that contains the grouped records of the original <see cref="IKStream{K, V}"/></returns>
        IKGroupedStream<K, V> GroupByKey(string named = null);

        /// <summary>
        /// Group the records by their current key into a <see cref="IKGroupedStream{K, V}"/> while preserving the original values
        /// and default serializers and deserializers.
        /// Grouping a stream on the record key is required before an aggregation operator can be applied to the data.
        /// If a record key is null , the record will not be included in the resulting.
        /// If a key changing operator was used before this operation and no data redistribution happened afterwards an internal repartitioning topic may need to be created in Kafka if a later
        /// operator depends on the newly selected key.
        /// This topic will be named "${applicationId}-&lt;name&gt;-repartition", where "applicationId" is user-specified in
        /// <see cref="IStreamConfig"/> via parameter <see cref="IStreamConfig.ApplicationId"/>,
        /// "&lt;name&gt;" is an internally generated name, and "-repartition" is a fixed suffix.
        /// For this case, all data of this stream will be redistributed through the repartitioning topic by writing all
        /// records to it, and rereading all records from it, such that the resulting <see cref="IKGroupedStream{K, V}"/> is partitioned
        /// correctly on its key.
        /// If the last key changing operator changed the key type, it is recommended to use <see cref="IKStream{K, V}.GroupByKey(string)"/>.
        /// </summary>
        /// <typeparam name="KS">Serializer for <typeparamref name="K"/></typeparam>
        /// <typeparam name="VS">Serializer for <typeparamref name="V"/></typeparam>
        /// <param name="named">A <see cref="string"/> config used to name the processor in the topology. Default : null</param>
        /// <returns>A <see cref="IKGroupedStream{K, V}"/> that contains the grouped records of the original <see cref="IKStream{K, V}"/></returns>
        IKGroupedStream<K, V> GroupByKey<KS, VS>(string named = null) where KS : ISerDes<K>, new() where VS : ISerDes<V>, new();

        /// <summary>
        /// Join records of this stream with <see cref="IKTable{K, V0}"/>'s records using non-windowed inner equi join with default
        /// serializers and deserializers.
        /// The join is a primary key table lookup join with join attribute <code> stream.key == table.key</code>.
        /// "Table lookup join" means, that results are only computed if KStream records are processed.
        /// This is done by performing a lookup for matching records in the <em>current</em> (i.e., processing time) internal
        /// <see cref="IKTable{K, V0}"/> state.
        /// In contrast, processing <see cref="IKTable{K, V0}"/> input records will only update the internal <see cref="IKTable{K, V0}"/> state and
        /// will not produce any result records.
        /// <para>
        /// For each <see cref="IKStream{K, V}"/> record that finds a corresponding record in <see cref="IKTable{K, V0}"/> the provided
        /// <see cref="Func{V, V0, VR}"/> joiner will be called to compute a value (with arbitrary type) for the result record.
        /// The key of the result record is the same as for both joining input records.
        /// If an <see cref="IKStream{K, V}"/> input record key or value is null the record will not be included in the join
        /// operation and thus no output record will be added to the resulting <see cref="IKStream{K, V}"/>.
        /// </para>
        /// </summary>
        /// <typeparam name="V0">the value type of the table</typeparam>
        /// <typeparam name="VR">the value type of the result stream</typeparam>
        /// <typeparam name="V0S">serdes's value of table</typeparam>
        /// <param name="table">the <see cref="IKTable{K, V0}"/> to be joined with this stream</param>
        /// <param name="valueJoiner">a function joiner that computes the join result for a pair of matching records</param>
        /// <param name="named">a name config used to name the processor in the topology</param>
        /// <returns>a <see cref="IKStream{K, V}"/> that contains join-records for each key and values computed by the given
        /// joiner, one for each matched record-pair with the same key</returns>
        IKStream<K, VR> Join<V0, VR, V0S>(IKTable<K, V0> table, Func<V, V0, VR> valueJoiner, string named = null)
            where V0S : ISerDes<V0>, new();

        /// <summary>
        /// Join records of this stream with <see cref="IKTable{K, V0}"/>'s records using non-windowed inner equi join with default
        /// serializers and deserializers.
        /// The join is a primary key table lookup join with join attribute <code> stream.key == table.key</code>.
        /// "Table lookup join" means, that results are only computed if KStream records are processed.
        /// This is done by performing a lookup for matching records in the <em>current</em> (i.e., processing time) internal
        /// <see cref="IKTable{K, V0}"/> state.
        /// In contrast, processing <see cref="IKTable{K, V0}"/> input records will only update the internal <see cref="IKTable{K, V0}"/> state and
        /// will not produce any result records.
        /// <para>
        /// For each <see cref="IKStream{K, V}"/> record that finds a corresponding record in <see cref="IKTable{K, V0}"/> the provided
        /// <see cref="IValueJoiner{V, V0, VR}"/> joiner will be called to compute a value (with arbitrary type) for the result record.
        /// The key of the result record is the same as for both joining input records.
        /// If an <see cref="IKStream{K, V}"/> input record key or value is null the record will not be included in the join
        /// operation and thus no output record will be added to the resulting <see cref="IKStream{K, V}"/>.
        /// </para>
        /// </summary>
        /// <typeparam name="V0">the value type of the table</typeparam>
        /// <typeparam name="VR">the value type of the result stream</typeparam>
        /// <typeparam name="V0S">serdes's value of table</typeparam>
        /// <param name="table">the <see cref="IKTable{K, V0}"/> to be joined with this stream</param>
        /// <param name="valueJoiner">a <see cref="IValueJoiner{V, V0, VR}"/> joiner that computes the join result for a pair of matching records</param>
        /// <param name="named">a name config used to name the processor in the topology</param>
        /// <returns>a <see cref="IKStream{K, V}"/> that contains join-records for each key and values computed by the given
        /// joiner, one for each matched record-pair with the same key</returns>
        IKStream<K, VR> Join<V0, VR, V0S>(IKTable<K, V0> table, IValueJoiner<V, V0, VR> valueJoiner, string named = null)
            where V0S : ISerDes<V0>, new();

        /// <summary>
        /// Join records of this stream with <see cref="IKTable{K, V0}"/>'s records using non-windowed inner equi join with default
        /// serializers and deserializers.
        /// The join is a primary key table lookup join with join attribute <code> stream.key == table.key</code>.
        /// "Table lookup join" means, that results are only computed if KStream records are processed.
        /// This is done by performing a lookup for matching records in the <em>current</em> (i.e., processing time) internal
        /// <see cref="IKTable{K, V0}"/> state.
        /// In contrast, processing <see cref="IKTable{K, V0}"/> input records will only update the internal <see cref="IKTable{K, V0}"/> state and
        /// will not produce any result records.
        /// <para>
        /// For each <see cref="IKStream{K, V}"/> record that finds a corresponding record in <see cref="IKTable{K, V0}"/> the provided
        /// <see cref="Func{V, V0, VR}"/> joiner will be called to compute a value (with arbitrary type) for the result record.
        /// The key of the result record is the same as for both joining input records.
        /// If an <see cref="IKStream{K, V}"/> input record key or value is null the record will not be included in the join
        /// operation and thus no output record will be added to the resulting <see cref="IKStream{K, V}"/>.
        /// </para>
        /// </summary>
        /// <typeparam name="V0">the value type of the table</typeparam>
        /// <typeparam name="VR">the value type of the result stream</typeparam>
        /// <param name="table">the <see cref="IKTable{K, V0}"/> to be joined with this stream</param>
        /// <param name="valueJoiner">a function joiner that computes the join result for a pair of matching records</param>
        /// <param name="named">a name config used to name the processor in the topology</param>
        /// <returns>a <see cref="IKStream{K, V}"/> that contains join-records for each key and values computed by the given
        /// joiner, one for each matched record-pair with the same key</returns>
        IKStream<K, VR> Join<V0, VR>(IKTable<K, V0> table, Func<V, V0, VR> valueJoiner, string named = null);

        /// <summary>
        /// Join records of this stream with <see cref="IKTable{K, V0}"/>'s records using non-windowed inner equi join with default
        /// serializers and deserializers.
        /// The join is a primary key table lookup join with join attribute <code> stream.key == table.key</code>.
        /// "Table lookup join" means, that results are only computed if KStream records are processed.
        /// This is done by performing a lookup for matching records in the <em>current</em> (i.e., processing time) internal
        /// <see cref="IKTable{K, V0}"/> state.
        /// In contrast, processing <see cref="IKTable{K, V0}"/> input records will only update the internal <see cref="IKTable{K, V0}"/> state and
        /// will not produce any result records.
        /// <para>
        /// For each <see cref="IKStream{K, V}"/> record that finds a corresponding record in <see cref="IKTable{K, V0}"/> the provided
        /// <see cref="IValueJoiner{V, V0, VR}"/> joiner will be called to compute a value (with arbitrary type) for the result record.
        /// The key of the result record is the same as for both joining input records.
        /// If an <see cref="IKStream{K, V}"/> input record key or value is null the record will not be included in the join
        /// operation and thus no output record will be added to the resulting <see cref="IKStream{K, V}"/>.
        /// </para>
        /// </summary>
        /// <typeparam name="V0">the value type of the table</typeparam>
        /// <typeparam name="VR">the value type of the result stream</typeparam>
        /// <param name="table">the <see cref="IKTable{K, V0}"/> to be joined with this stream</param>
        /// <param name="valueJoiner">a <see cref="IValueJoiner{V, V0, VR}"/> joiner that computes the join result for a pair of matching records</param>
        /// <param name="named">a name config used to name the processor in the topology</param>
        /// <returns>a <see cref="IKStream{K, V}"/> that contains join-records for each key and values computed by the given
        /// joiner, one for each matched record-pair with the same key</returns>
        IKStream<K, VR> Join<V0, VR>(IKTable<K, V0> table, IValueJoiner<V, V0, VR> valueJoiner, string named = null);


        /// <summary>
        /// Join records of this stream with <see cref="IGlobalKTable{K0, V0}"/>'s records using non-windowed inner equi join.
        /// The join is a primary key table lookup join with join attribute
        /// <code>mapper(stream.keyValue) == table.key</code>.
        /// "Table lookup join" means, that results are only computed if <see cref="IKStream{K, V}"/> records are processed.
        /// This is done by performing a lookup for matching records in the <em>current</em> internal <see cref="IGlobalKTable{K0, V0}"/>
        /// state.
        /// In contrast, processing <see cref="IGlobalKTable{K0, V0}"/> input records will only update the internal <see cref="IGlobalKTable{K0, V0}"/>
        /// state and will not produce any result records.
        /// <para>
        /// For each <see cref="IKStream{K, V}"/> record that finds a corresponding record in <see cref="IGlobalKTable{K0, V0}"/> the provided
        /// function joiner will be called to compute a value (with arbitrary type) for the result record.
        /// The key of the result record is the same as the key of this <see cref="IKStream{K, V}"/>.
        /// If a <see cref="IKStream{K, V}"/> input record key or value is null the record will not be included in the join
        /// operation and thus no output record will be added to the resulting <see cref="IKStream{K, V}"/>.
        /// If key mapper returns null implying no match exists, no output record will be added to the
        /// resulting <see cref="IKStream{K, V}"/>.
        /// </para>
        /// </summary>
        /// <typeparam name="K0">the key type of <see cref="IGlobalKTable{K0, V0}"/></typeparam>
        /// <typeparam name="V0">the value type of <see cref="IGlobalKTable{K0, V0}"/></typeparam>
        /// <typeparam name="VR">the value type of the resulting</typeparam>
        /// <param name="globalTable">the global table to be joined with this stream</param>
        /// <param name="keyMapper">instance of mapper used to map from the (key, value) of this stream to the key of the global table</param>
        /// <param name="valueJoiner">a function joiner that computes the join result for a pair of matching records</param>
        /// <param name="named">a name config used to name the processor in the topology</param>
        /// <returns>a <see cref="IKStream{K, V}"/> that contains join-records for each key and values computed by the given joiner, one output for each input stream's record</returns>
        IKStream<K, VR> Join<K0, V0, VR>(IGlobalKTable<K0, V0> globalTable, Func<K, V, K0> keyMapper, Func<V, V0, VR> valueJoiner, string named = null);

        /// <summary>
        /// Join records of this stream with <see cref="IGlobalKTable{K0, V0}"/>'s records using non-windowed inner equi join.
        /// The join is a primary key table lookup join with join attribute
        /// <code>mapper(stream.keyValue) == table.key</code>.
        /// "Table lookup join" means, that results are only computed if <see cref="IKStream{K, V}"/> records are processed.
        /// This is done by performing a lookup for matching records in the <em>current</em> internal <see cref="IGlobalKTable{K0, V0}"/>
        /// state.
        /// In contrast, processing <see cref="IGlobalKTable{K0, V0}"/> input records will only update the internal <see cref="IGlobalKTable{K0, V0}"/>
        /// state and will not produce any result records.
        /// <para>
        /// For each <see cref="IKStream{K, V}"/> record that finds a corresponding record in <see cref="IGlobalKTable{K0, V0}"/> the provided
        /// <see cref="IValueJoiner{V, V0, VR}"/> joiner will be called to compute a value (with arbitrary type) for the result record.
        /// The key of the result record is the same as the key of this <see cref="IKStream{K, V}"/>.
        /// If a <see cref="IKStream{K, V}"/> input record key or value is null the record will not be included in the join
        /// operation and thus no output record will be added to the resulting <see cref="IKStream{K, V}"/>.
        /// If key mapper returns null implying no match exists, no output record will be added to the
        /// resulting <see cref="IKStream{K, V}"/>.
        /// </para>
        /// </summary>
        /// <typeparam name="K0">the key type of <see cref="IGlobalKTable{K0, V0}"/></typeparam>
        /// <typeparam name="V0">the value type of <see cref="IGlobalKTable{K0, V0}"/></typeparam>
        /// <typeparam name="VR">the value type of the resulting</typeparam>
        /// <param name="globalTable">the global table to be joined with this stream</param>
        /// <param name="keyMapper">instance of <see cref="IKeyValueMapper{K, V, K0}"/> mapper used to map from the (key, value) of this stream to the key of the global table</param>
        /// <param name="valueJoiner">a <see cref="IValueJoiner{V, V0, VR}"/> joiner that computes the join result for a pair of matching records</param>
        /// <param name="named">a name config used to name the processor in the topology</param>
        /// <returns>a <see cref="IKStream{K, V}"/> that contains join-records for each key and values computed by the given joiner, one output for each input stream's record</returns>
        IKStream<K, VR> Join<K0, V0, VR>(IGlobalKTable<K0, V0> globalTable, IKeyValueMapper<K, V, K0> keyMapper, IValueJoiner<V, V0, VR> valueJoiner, string named = null);

        /// <summary>
        /// Join records of this stream with another <code>IKStream</code>'s records using windowed inner equi join using the
        /// <see cref="StreamJoinProps"/> instance for configuration of the <see cref="ISerDes{K}"/> key serde, <see cref="ISerDes{V}"/> stream's value
        /// serde, <see cref="ISerDes{V0}"/> the other stream's value serde, and used state stores.
        /// The join is computed on the records' key with join attribute {@code thisKStream.key == otherKStream.key}.
        /// Furthermore, two records are only joined if their timestamps are close to each other as defined by the given
        /// <see cref="JoinWindowOptions"/>, i.e., the window defines an additional join predicate on the record timestamps.
        /// <p>
        /// For each pair of records meeting both join predicates the provided <see cref="Func{V1, V2, VR}"/> will be called to compute
        /// a value (with arbitrary type) for the result record.
        /// The key of the result record is the same as for both joining input records.
        /// If an input record key or value is <code>null</code> the record will not be included in the join operation and thus no
        /// output record will be added to the resulting <see cref="IKStream{K, V}"/>.
        /// </p>
        /// <para>
        /// Both input streams (or to be more precise, their underlying source topics) need to have the same number of
        /// partitions.
        /// If this is not the case, you would need to call {@link #through(String)} (for one input stream) before doing the
        /// join, using a pre-created topic with the "correct" number of partitions.
        /// Furthermore, both input streams need to be co-partitioned on the join key (i.e., use the same partitioner).
        /// If this requirement is not met, Kafka Streams will automatically repartition the data, i.e., it will create an
        /// internal repartitioning topic in Kafka and write and re-read the data via this topic before the actual join.
        /// The repartitioning topic will be named "${applicationId}-&lt;name&gt;-repartition", where "applicationId" is
        /// user-specified in <see cref="StreamConfig"/> via parameter <see cref="StreamConfig.ApplicationId"/>
        /// , "&lt;name&gt;" is an internally generated name, and "-repartition" is a fixed suffix.
        /// </para>
        /// <para>
        /// Repartitioning can happen for one or both of the joining <see cref="IKStream{K, V}"/>s.
        /// For this case, all data of the stream will be redistributed through the repartitioning topic by writing all
        /// records to it, and rereading all records from it, such that the join input  <see cref="IKStream{K, V}"/> is partitiond
        /// correctly on its key.
        /// </para>
        /// <para>
        /// Both of the joining <see cref="IKStream{K, V}"/>s will be materialized in local state stores with auto-generated store names,
        /// unless a name is provided via a <see cref="Materialized{K, V, S}"/> instance.
        /// For failure and recovery each store will be backed by an internal changelog topic that will be created in Kafka.
        /// The changelog topic will be named "${applicationId}-&lt;storename&gt;-changelog", where "applicationId" is user-specified
        /// in <see cref="StreamConfig"/> via parameter <see cref="StreamConfig.ApplicationId"/>, "storeName" is an
        /// internally generated name, and "-changelog" is a fixed suffix.
        /// </para>
        /// You can retrieve all generated internal topic names via <see cref="Topology.Describe"/>.
        /// </summary>
        /// <typeparam name="V0">the value type of the other stream</typeparam>
        /// <typeparam name="VR">the value type of the result stream</typeparam>
        /// <typeparam name="V0S">the serdes value type of the other stream</typeparam>
        /// <param name="stream">the <see cref="IKStream{K, V0}"/> to be joined with this stream</param>
        /// <param name="valueJoiner">a <see cref="Func{V, V0, VR}"/> joiner function that computes the join result for a pair of matching records</param>
        /// <param name="windows">the specification of the <see cref="JoinWindowOptions"/></param>
        /// <param name="props">a <see cref="StreamJoinProps"/> used to configure join stores</param>
        /// <returns>a <see cref="IKStream{K, VR}"/> that contains join-records for each key and values computed by the given <see cref="IValueJoiner{V, V0, VR}"/> , one for each matched record-pair with the same key and within the joining window intervals</returns>
        IKStream<K, VR> Join<V0, VR, V0S>(IKStream<K, V0> stream, Func<V, V0, VR> valueJoiner, JoinWindowOptions windows, StreamJoinProps props = null)
            where V0S : ISerDes<V0>, new();




        /// <summary>
        /// Join records of this stream with another <code>IKStream</code>'s records using windowed inner equi join using the
        /// <see cref="StreamJoinProps"/> instance for configuration of the <see cref="ISerDes{K}"/> key serde, <see cref="ISerDes{V}"/> stream's value
        /// serde, <see cref="ISerDes{V0}"/> the other stream's value serde, and used state stores.
        /// The join is computed on the records' key with join attribute {@code thisKStream.key == otherKStream.key}.
        /// Furthermore, two records are only joined if their timestamps are close to each other as defined by the given
        /// <see cref="JoinWindowOptions"/>, i.e., the window defines an additional join predicate on the record timestamps.
        /// <p>
        /// For each pair of records meeting both join predicates the provided <see cref="Func{V1, V2, VR}"/> will be called to compute
        /// a value (with arbitrary type) for the result record.
        /// The key of the result record is the same as for both joining input records.
        /// If an input record key or value is <code>null</code> the record will not be included in the join operation and thus no
        /// output record will be added to the resulting <see cref="IKStream{K, V}"/>.
        /// </p>
        /// <para>
        /// Both input streams (or to be more precise, their underlying source topics) need to have the same number of
        /// partitions.
        /// If this is not the case, you would need to call {@link #through(String)} (for one input stream) before doing the
        /// join, using a pre-created topic with the "correct" number of partitions.
        /// Furthermore, both input streams need to be co-partitioned on the join key (i.e., use the same partitioner).
        /// If this requirement is not met, Kafka Streams will automatically repartition the data, i.e., it will create an
        /// internal repartitioning topic in Kafka and write and re-read the data via this topic before the actual join.
        /// The repartitioning topic will be named "${applicationId}-&lt;name&gt;-repartition", where "applicationId" is
        /// user-specified in <see cref="StreamConfig"/> via parameter <see cref="StreamConfig.ApplicationId"/>
        /// , "&lt;name&gt;" is an internally generated name, and "-repartition" is a fixed suffix.
        /// </para>
        /// <para>
        /// Repartitioning can happen for one or both of the joining <see cref="IKStream{K, V}"/>s.
        /// For this case, all data of the stream will be redistributed through the repartitioning topic by writing all
        /// records to it, and rereading all records from it, such that the join input  <see cref="IKStream{K, V}"/> is partitiond
        /// correctly on its key.
        /// </para>
        /// <para>
        /// Both of the joining <see cref="IKStream{K, V}"/>s will be materialized in local state stores with auto-generated store names,
        /// unless a name is provided via a <see cref="Materialized{K, V, S}"/> instance.
        /// For failure and recovery each store will be backed by an internal changelog topic that will be created in Kafka.
        /// The changelog topic will be named "${applicationId}-&lt;storename&gt;-changelog", where "applicationId" is user-specified
        /// in <see cref="StreamConfig"/> via parameter <see cref="StreamConfig.ApplicationId"/>, "storeName" is an
        /// internally generated name, and "-changelog" is a fixed suffix.
        /// </para>
        /// You can retrieve all generated internal topic names via <see cref="Topology.Describe"/>.
        /// </summary>
        /// <typeparam name="V0">the value type of the other stream</typeparam>
        /// <typeparam name="VR">the value type of the result stream</typeparam>
        /// <param name="stream">the <see cref="IKStream{K, V0}"/> to be joined with this stream</param>
        /// <param name="valueJoiner">a <see cref="Func{V, V0, VR}"/> joiner function that computes the join result for a pair of matching records</param>
        /// <param name="windows">the specification of the <see cref="JoinWindowOptions"/></param>
        /// <param name="props">a <see cref="StreamJoinProps"/> used to configure join stores</param>
        /// <returns>a <see cref="IKStream{K, VR}"/> that contains join-records for each key and values computed by the given <see cref="IValueJoiner{V, V0, VR}"/> , one for each matched record-pair with the same key and within the joining window intervals</returns>
        IKStream<K, VR> Join<V0, VR>(IKStream<K, V0> stream, Func<V, V0, VR> valueJoiner, JoinWindowOptions windows, StreamJoinProps<K, V, V0> props = null);


        /// <summary>
        /// Join records of this stream with another <code>IKStream</code>'s records using windowed inner equi join using the
        /// <see cref="StreamJoinProps"/> instance for configuration of the <see cref="ISerDes{K}"/> key serde, <see cref="ISerDes{V}"/> stream's value
        /// serde, <see cref="ISerDes{V0}"/> the other stream's value serde, and used state stores.
        /// The join is computed on the records' key with join attribute {@code thisKStream.key == otherKStream.key}.
        /// Furthermore, two records are only joined if their timestamps are close to each other as defined by the given
        /// <see cref="JoinWindowOptions"/>, i.e., the window defines an additional join predicate on the record timestamps.
        /// <p>
        /// For each pair of records meeting both join predicates the provided <see cref="IValueJoiner{V1, V2, VR}"/> will be called to compute
        /// a value (with arbitrary type) for the result record.
        /// The key of the result record is the same as for both joining input records.
        /// If an input record key or value is <code>null</code> the record will not be included in the join operation and thus no
        /// output record will be added to the resulting <see cref="IKStream{K, V}"/>.
        /// </p>
        /// <para>
        /// Both input streams (or to be more precise, their underlying source topics) need to have the same number of
        /// partitions.
        /// If this is not the case, you would need to call {@link #through(String)} (for one input stream) before doing the
        /// join, using a pre-created topic with the "correct" number of partitions.
        /// Furthermore, both input streams need to be co-partitioned on the join key (i.e., use the same partitioner).
        /// If this requirement is not met, Kafka Streams will automatically repartition the data, i.e., it will create an
        /// internal repartitioning topic in Kafka and write and re-read the data via this topic before the actual join.
        /// The repartitioning topic will be named "${applicationId}-&lt;name&gt;-repartition", where "applicationId" is
        /// user-specified in <see cref="StreamConfig"/> via parameter <see cref="StreamConfig.ApplicationId"/>
        /// , "&lt;name&gt;" is an internally generated name, and "-repartition" is a fixed suffix.
        /// </para>
        /// <para>
        /// Repartitioning can happen for one or both of the joining <see cref="IKStream{K, V}"/>s.
        /// For this case, all data of the stream will be redistributed through the repartitioning topic by writing all
        /// records to it, and rereading all records from it, such that the join input  <see cref="IKStream{K, V}"/> is partitiond
        /// correctly on its key.
        /// </para>
        /// <para>
        /// Both of the joining <see cref="IKStream{K, V}"/>s will be materialized in local state stores with auto-generated store names,
        /// unless a name is provided via a <see cref="Materialized{K, V, S}"/> instance.
        /// For failure and recovery each store will be backed by an internal changelog topic that will be created in Kafka.
        /// The changelog topic will be named "${applicationId}-&lt;storename&gt;-changelog", where "applicationId" is user-specified
        /// in <see cref="StreamConfig"/> via parameter <see cref="StreamConfig.ApplicationId"/>, "storeName" is an
        /// internally generated name, and "-changelog" is a fixed suffix.
        /// </para>
        /// You can retrieve all generated internal topic names via <see cref="Topology.Describe"/>.
        /// </summary>
        /// <typeparam name="V0">the value type of the other stream</typeparam>
        /// <typeparam name="VR">the value type of the result stream</typeparam>
        /// <typeparam name="V0S">the serdes value type of the other stream</typeparam>
        /// <param name="stream">the <see cref="IKStream{K, V0}"/> to be joined with this stream</param>
        /// <param name="valueJoiner">a <see cref="IValueJoiner{V, V0, VR}"/> that computes the join result for a pair of matching records</param>
        /// <param name="windows">the specification of the <see cref="JoinWindowOptions"/></param>
        /// <param name="props">a <see cref="StreamJoinProps"/> used to configure join stores</param>
        /// <returns>a <see cref="IKStream{K, VR}"/> that contains join-records for each key and values computed by the given <see cref="IValueJoiner{V, V0, VR}"/> , one for each matched record-pair with the same key and within the joining window intervals</returns>
        IKStream<K, VR> Join<V0, VR, V0S>(IKStream<K, V0> stream, IValueJoiner<V, V0, VR> valueJoiner, JoinWindowOptions windows, StreamJoinProps props = null)
            where V0S : ISerDes<V0>, new();


        /// <summary>
        /// Join records of this stream with another <code>IKStream</code>'s records using windowed inner equi join using the
        /// <see cref="StreamJoinProps"/> instance for configuration of the <see cref="ISerDes{K}"/> key serde, <see cref="ISerDes{V}"/> stream's value
        /// serde, <see cref="ISerDes{V0}"/> the other stream's value serde, and used state stores.
        /// The join is computed on the records' key with join attribute {@code thisKStream.key == otherKStream.key}.
        /// Furthermore, two records are only joined if their timestamps are close to each other as defined by the given
        /// <see cref="JoinWindowOptions"/>, i.e., the window defines an additional join predicate on the record timestamps.
        /// <p>
        /// For each pair of records meeting both join predicates the provided <see cref="IValueJoiner{V1, V2, VR}"/> will be called to compute
        /// a value (with arbitrary type) for the result record.
        /// The key of the result record is the same as for both joining input records.
        /// If an input record key or value is <code>null</code> the record will not be included in the join operation and thus no
        /// output record will be added to the resulting <see cref="IKStream{K, V}"/>.
        /// </p>
        /// <para>
        /// Both input streams (or to be more precise, their underlying source topics) need to have the same number of
        /// partitions.
        /// If this is not the case, you would need to call {@link #through(String)} (for one input stream) before doing the
        /// join, using a pre-created topic with the "correct" number of partitions.
        /// Furthermore, both input streams need to be co-partitioned on the join key (i.e., use the same partitioner).
        /// If this requirement is not met, Kafka Streams will automatically repartition the data, i.e., it will create an
        /// internal repartitioning topic in Kafka and write and re-read the data via this topic before the actual join.
        /// The repartitioning topic will be named "${applicationId}-&lt;name&gt;-repartition", where "applicationId" is
        /// user-specified in <see cref="StreamConfig"/> via parameter <see cref="StreamConfig.ApplicationId"/>
        /// , "&lt;name&gt;" is an internally generated name, and "-repartition" is a fixed suffix.
        /// </para>
        /// <para>
        /// Repartitioning can happen for one or both of the joining <see cref="IKStream{K, V}"/>s.
        /// For this case, all data of the stream will be redistributed through the repartitioning topic by writing all
        /// records to it, and rereading all records from it, such that the join input  <see cref="IKStream{K, V}"/> is partitiond
        /// correctly on its key.
        /// </para>
        /// <para>
        /// Both of the joining <see cref="IKStream{K, V}"/>s will be materialized in local state stores with auto-generated store names,
        /// unless a name is provided via a <see cref="Materialized{K, V, S}"/> instance.
        /// For failure and recovery each store will be backed by an internal changelog topic that will be created in Kafka.
        /// The changelog topic will be named "${applicationId}-&lt;storename&gt;-changelog", where "applicationId" is user-specified
        /// in <see cref="StreamConfig"/> via parameter <see cref="StreamConfig.ApplicationId"/>, "storeName" is an
        /// internally generated name, and "-changelog" is a fixed suffix.
        /// </para>
        /// You can retrieve all generated internal topic names via <see cref="Topology.Describe"/>.
        /// </summary>
        /// <typeparam name="V0">the value type of the other stream</typeparam>
        /// <typeparam name="VR">the value type of the result stream</typeparam>
        /// <param name="stream">the <see cref="IKStream{K, V0}"/> to be joined with this stream</param>
        /// <param name="valueJoiner">a <see cref="IValueJoiner{V, V0, VR}"/> that computes the join result for a pair of matching records</param>
        /// <param name="windows">the specification of the <see cref="JoinWindowOptions"/></param>
        /// <param name="props">a <see cref="StreamJoinProps"/> used to configure join stores</param>
        /// <returns>a <see cref="IKStream{K, VR}"/> that contains join-records for each key and values computed by the given <see cref="IValueJoiner{V, V0, VR}"/> , one for each matched record-pair with the same key and within the joining window intervals</returns>
        IKStream<K, VR> Join<V0, VR>(IKStream<K, V0> stream, IValueJoiner<V, V0, VR> valueJoiner, JoinWindowOptions windows, StreamJoinProps<K, V, V0> props = null);

        /// <summary>
        /// Join records of this stream with <see cref="IKTable{K, VT}"/>'s records using non-windowed left equi join with default
        /// serializers and deserializers.
        /// In contrast to <see cref="IKStream{K, V}.Join{V0, VR, V0S}(IKTable{K, V0}, Func{V, V0, VR}, string)"/> (inner-join), all records from this stream will produce an
        /// output record (cf. below).
        /// The join is a primary key table lookup join with join attribute <code> stream.key == table.key</code>.
        /// "Table lookup join" means, that results are only computed if KStream records are processed.
        /// This is done by performing a lookup for matching records in the <em>current</em> (i.e., processing time) internal
        /// <see cref="IKTable{K, VT}"/> state.
        /// In contrast, processing <see cref="IKTable{K, VT}"/> input records will only update the internal <see cref="IKTable{K, VT}"/>  state and
        /// will not produce any result records.
        /// <para>
        /// For each <see cref="IKStream{K, V}"/> record whether or not it finds a corresponding record in <see cref="IKTable{K, VT}"/>  the provided
        /// function joiner <see cref="Func{V, V0, VR}"/> will be called to compute a value (with arbitrary type) for the result record.
        /// If no <see cref="IKTable{K, VT}"/> record was found during lookup, a null value will be provided to joiner function.
        /// The key of the result record is the same as for both joining input records.
        /// If an <see cref="IKStream{K, V}"/> input record key or value is null the record will not be included in the join
        /// operation and thus no output record will be added to the resulting <see cref="IKStream{K, V}"/>.
        /// </para>
        /// <para>
        /// Both input streams (or to be more precise, their underlying source topics) need to have the same number of
        /// partitions.
        /// </para>
        /// </summary>
        /// <typeparam name="VT">the value type of the table</typeparam>
        /// <typeparam name="VR">the value type of the result stream</typeparam>
        /// <typeparam name="VTS">serdes's value of table</typeparam>
        /// <param name="table">the <see cref="IKTable{K, VT}"/> to be joined with this stream</param>
        /// <param name="valueJoiner">a function joiner that computes the join result for a pair of matching records</param>
        /// <param name="named">a name config used to name the processor in the topology</param>
        /// <returns>a <see cref="IKStream{K, V}"/> that contains join-records for each key and values computed by the given joiner, one output for each input <see cref= "IKStream{K, V}" /> record </returns>
        IKStream<K, VR> LeftJoin<VT, VR, VTS>(IKTable<K, VT> table, Func<V, VT, VR> valueJoiner, string named = null)
            where VTS : ISerDes<VT>, new();

        /// <summary>
        /// Join records of this stream with <see cref="IKTable{K, VT}"/>'s records using non-windowed left equi join with default
        /// serializers and deserializers.
        /// In contrast to <see cref="IKStream{K, V}.Join{V0, VR, V0S}(IKTable{K, V0}, Func{V, V0, VR}, string)"/> (inner-join), all records from this stream will produce an
        /// output record (cf. below).
        /// The join is a primary key table lookup join with join attribute <code> stream.key == table.key</code>.
        /// "Table lookup join" means, that results are only computed if KStream records are processed.
        /// This is done by performing a lookup for matching records in the <em>current</em> (i.e., processing time) internal
        /// <see cref="IKTable{K, VT}"/> state.
        /// In contrast, processing <see cref="IKTable{K, VT}"/> input records will only update the internal <see cref="IKTable{K, VT}"/>  state and
        /// will not produce any result records.
        /// <para>
        /// For each <see cref="IKStream{K, V}"/> record whether or not it finds a corresponding record in <see cref="IKTable{K, VT}"/>  the provided
        /// <see cref="IValueJoiner{V, VT, VR}"/> joiner <see cref="Func{V, V0, VR}"/> will be called to compute a value (with arbitrary type) for the result record.
        /// If no <see cref="IKTable{K, VT}"/> record was found during lookup, a null value will be provided to joiner function.
        /// The key of the result record is the same as for both joining input records.
        /// If an <see cref="IKStream{K, V}"/> input record key or value is null the record will not be included in the join
        /// operation and thus no output record will be added to the resulting <see cref="IKStream{K, V}"/>.
        /// </para>
        /// <para>
        /// Both input streams (or to be more precise, their underlying source topics) need to have the same number of
        /// partitions.
        /// </para>
        /// </summary>
        /// <typeparam name="VT">the value type of the table</typeparam>
        /// <typeparam name="VR">the value type of the result stream</typeparam>
        /// <typeparam name="VTS">serdes's value of table</typeparam>
        /// <param name="table">the <see cref="IKTable{K, VT}"/> to be joined with this stream</param>
        /// <param name="valueJoiner">a <see cref="IValueJoiner{V, VT, VR}"/> joiner that computes the join result for a pair of matching records</param>
        /// <param name="named">a name config used to name the processor in the topology</param>
        /// <returns>a <see cref="IKStream{K, V}"/> that contains join-records for each key and values computed by the given joiner, one output for each input <see cref= "IKStream{K, V}" /> record </returns>
        IKStream<K, VR> LeftJoin<VT, VR, VTS>(IKTable<K, VT> table, IValueJoiner<V, VT, VR> valueJoiner, string named = null)
            where VTS : ISerDes<VT>, new();

        
        /// <summary>
        /// Join records of this stream with <see cref="IKTable{K, VT}"/>'s records using non-windowed left equi join with default
        /// serializers and deserializers.
        /// In contrast to <see cref="IKStream{K, V}.Join{V0, VR, V0S}(IKTable{K, V0}, Func{V, V0, VR}, string)"/> (inner-join), all records from this stream will produce an
        /// output record (cf. below).
        /// The join is a primary key table lookup join with join attribute <code> stream.key == table.key</code>.
        /// "Table lookup join" means, that results are only computed if KStream records are processed.
        /// This is done by performing a lookup for matching records in the <em>current</em> (i.e., processing time) internal
        /// <see cref="IKTable{K, VT}"/> state.
        /// In contrast, processing <see cref="IKTable{K, VT}"/> input records will only update the internal <see cref="IKTable{K, VT}"/>  state and
        /// will not produce any result records.
        /// <para>
        /// For each <see cref="IKStream{K, V}"/> record whether or not it finds a corresponding record in <see cref="IKTable{K, VT}"/>  the provided
        /// function joiner <see cref="Func{V, V0, VR}"/> will be called to compute a value (with arbitrary type) for the result record.
        /// If no <see cref="IKTable{K, VT}"/> record was found during lookup, a null value will be provided to joiner function.
        /// The key of the result record is the same as for both joining input records.
        /// If an <see cref="IKStream{K, V}"/> input record key or value is null the record will not be included in the join
        /// operation and thus no output record will be added to the resulting <see cref="IKStream{K, V}"/>.
        /// </para>
        /// <para>
        /// Both input streams (or to be more precise, their underlying source topics) need to have the same number of
        /// partitions.
        /// </para>
        /// </summary>
        /// <typeparam name="VT">the value type of the table</typeparam>
        /// <typeparam name="VR">the value type of the result stream</typeparam>
        /// <param name="table">the <see cref="IKTable{K, VT}"/> to be joined with this stream</param>
        /// <param name="valueJoiner">a function joiner that computes the join result for a pair of matching records</param>
        /// <param name="named">a name config used to name the processor in the topology</param>
        /// <returns>a <see cref="IKStream{K, V}"/> that contains join-records for each key and values computed by the given joiner, one output for each input <see cref= "IKStream{K, V}" /> record </returns>
        IKStream<K, VR> LeftJoin<VT, VR>(IKTable<K, VT> table, Func<V, VT, VR> valueJoiner, string named = null);

        
        /// <summary>
        /// Join records of this stream with <see cref="IKTable{K, VT}"/>'s records using non-windowed left equi join with default
        /// serializers and deserializers.
        /// In contrast to <see cref="IKStream{K, V}.Join{V0, VR, V0S}(IKTable{K, V0}, Func{V, V0, VR}, string)"/> (inner-join), all records from this stream will produce an
        /// output record (cf. below).
        /// The join is a primary key table lookup join with join attribute <code> stream.key == table.key</code>.
        /// "Table lookup join" means, that results are only computed if KStream records are processed.
        /// This is done by performing a lookup for matching records in the <em>current</em> (i.e., processing time) internal
        /// <see cref="IKTable{K, VT}"/> state.
        /// In contrast, processing <see cref="IKTable{K, VT}"/> input records will only update the internal <see cref="IKTable{K, VT}"/>  state and
        /// will not produce any result records.
        /// <para>
        /// For each <see cref="IKStream{K, V}"/> record whether or not it finds a corresponding record in <see cref="IKTable{K, VT}"/>  the provided
        /// <see cref="IValueJoiner{V, VT, VR}"/> joiner <see cref="Func{V, V0, VR}"/> will be called to compute a value (with arbitrary type) for the result record.
        /// If no <see cref="IKTable{K, VT}"/> record was found during lookup, a null value will be provided to joiner function.
        /// The key of the result record is the same as for both joining input records.
        /// If an <see cref="IKStream{K, V}"/> input record key or value is null the record will not be included in the join
        /// operation and thus no output record will be added to the resulting <see cref="IKStream{K, V}"/>.
        /// </para>
        /// <para>
        /// Both input streams (or to be more precise, their underlying source topics) need to have the same number of
        /// partitions.
        /// </para>
        /// </summary>
        /// <typeparam name="VT">the value type of the table</typeparam>
        /// <typeparam name="VR">the value type of the result stream</typeparam>
        /// <param name="table">the <see cref="IKTable{K, VT}"/> to be joined with this stream</param>
        /// <param name="valueJoiner">a <see cref="IValueJoiner{V, VT, VR}"/> joiner that computes the join result for a pair of matching records</param>
        /// <param name="named">a name config used to name the processor in the topology</param>
        /// <returns>a <see cref="IKStream{K, V}"/> that contains join-records for each key and values computed by the given joiner, one output for each input <see cref= "IKStream{K, V}" /> record </returns>
        IKStream<K, VR> LeftJoin<VT, VR>(IKTable<K, VT> table, IValueJoiner<V, VT, VR> valueJoiner, string named = null);

        /// <summary>
        /// Join records of this stream with <see cref="IGlobalKTable{K0, V0}"/>'s records using non-windowed left equi join.
        /// In contrast to <see cref="IKStream{K, V}.Join{K0, V0, VR}(IGlobalKTable{K0, V0}, Func{K, V, K0}, Func{V, V0, VR}, string)"/> (inner-join), all records from this stream
        /// will produce an output record (cf. below).
        /// The join is a primary key table lookup join with join attribute
        /// <code>keyValueMapper.map(stream.keyValue) == table.key</code>.
        /// "Table lookup join" means, that results are only computed if <see cref="IKStream{K, V}"/> records are processed.
        /// This is done by performing a lookup for matching records in the <em>current</em> internal <see cref="IGlobalKTable{K0, V0}"/>
        /// state.
        /// In contrast, processing <see cref="IGlobalKTable{K0, V0}"/> input records will only update the internal <see cref="IGlobalKTable{K0, V0}"/>
        /// state and will not produce any result records.
        /// <para>
        /// For each <see cref="IKStream{K, V}"/> record whether or not it finds a corresponding record in <see cref="IGlobalKTable{K0, V0}"/> the
        /// provided <see cref="Func{V, V0, VR}"/> joiner will be called to compute a value (with arbitrary type) for the result record.
        /// The key of the result record is the same as this <see cref="IKStream{K, V}"/>.
        /// If a <see cref="IKStream{K, V}"/> input record key or value is null, the record will not be included in the join
        /// operation and thus no output record will be added to the resulting <see cref="IKStream{K, V}"/>.
        /// If mapper returns null implying no match exists, a null value will be
        /// provided to <see cref="Func{V, V0, VR}"/> joiner.
        /// If no <see cref="IGlobalKTable{K0, V0}"/> record was found during lookup, a null value will be provided to
        /// <see cref="Func{V, V0, VR}"/> joiner.
        /// </para>
        /// </summary>
        /// <typeparam name="K0">the key type of <see cref="IGlobalKTable{K0, V0}"/></typeparam>
        /// <typeparam name="V0">the value type of <see cref="IGlobalKTable{K0, V0}"/></typeparam>
        /// <typeparam name="VR">the value type of the resulting</typeparam>
        /// <param name="globalTable">the global table to be joined with this stream</param>
        /// <param name="keyMapper">instance of mapper used to map from the (key, value) of this stream to the key of the global table</param>
        /// <param name="valueJoiner">a function joiner that computes the join result for a pair of matching records</param>
        /// <param name="named">a name config used to name the processor in the topology</param>
        /// <returns>a <see cref="IKStream{K, V}"/> that contains join-records for each key and values computed by the given joiner, one output for each input stream's record</returns>
        IKStream<K, VR> LeftJoin<K0, V0, VR>(IGlobalKTable<K0, V0> globalTable, Func<K, V, K0> keyMapper, Func<V, V0, VR> valueJoiner, string named = null);

      
        /// <summary>
        /// Join records of this stream with <see cref="IGlobalKTable{K0, V0}"/>'s records using non-windowed left equi join.
        /// In contrast to <see cref="IKStream{K, V}.Join{K0, V0, VR}(IGlobalKTable{K0, V0}, Func{K, V, K0}, Func{V, V0, VR}, string)"/> (inner-join), all records from this stream
        /// will produce an output record (cf. below).
        /// The join is a primary key table lookup join with join attribute
        /// <code>keyValueMapper.map(stream.keyValue) == table.key</code>.
        /// "Table lookup join" means, that results are only computed if <see cref="IKStream{K, V}"/> records are processed.
        /// This is done by performing a lookup for matching records in the <em>current</em> internal <see cref="IGlobalKTable{K0, V0}"/>
        /// state.
        /// In contrast, processing <see cref="IGlobalKTable{K0, V0}"/> input records will only update the internal <see cref="IGlobalKTable{K0, V0}"/>
        /// state and will not produce any result records.
        /// <para>
        /// For each <see cref="IKStream{K, V}"/> record whether or not it finds a corresponding record in <see cref="IGlobalKTable{K0, V0}"/> the
        /// provided <see cref="IValueJoiner{V, V0, VR}"/> joiner will be called to compute a value (with arbitrary type) for the result record.
        /// The key of the result record is the same as this <see cref="IKStream{K, V}"/>.
        /// If a <see cref="IKStream{K, V}"/> input record key or value is null, the record will not be included in the join
        /// operation and thus no output record will be added to the resulting <see cref="IKStream{K, V}"/>.
        /// If mapper returns null implying no match exists, a null value will be
        /// provided to <see cref="IValueJoiner{V, V0, VR}"/> joiner.
        /// If no <see cref="IGlobalKTable{K0, V0}"/> record was found during lookup, a null value will be provided to
        /// <see cref="IValueJoiner{V, V0, VR}"/> joiner.
        /// </para>
        /// </summary>
        /// <typeparam name="K0">the key type of <see cref="IGlobalKTable{K0, V0}"/></typeparam>
        /// <typeparam name="V0">the value type of <see cref="IGlobalKTable{K0, V0}"/></typeparam>
        /// <typeparam name="VR">the value type of the resulting</typeparam>
        /// <param name="globalTable">the global table to be joined with this stream</param>
        /// <param name="keyMapper">instance of mapper used to map from the (key, value) of this stream to the key of the global table</param>
        /// <param name="valueJoiner">a function joiner that computes the join result for a pair of matching records</param>
        /// <param name="named">a name config used to name the processor in the topology</param>
        /// <returns>a <see cref="IKStream{K, V}"/> that contains join-records for each key and values computed by the given joiner, one output for each input stream's record</returns>
        IKStream<K, VR> LeftJoin<K0, V0, VR>(IGlobalKTable<K0, V0> globalTable, IKeyValueMapper<K, V, K0> keyMapper, IValueJoiner<V, V0, VR> valueJoiner, string named = null);

        /// <summary>
        /// Join records of this stream with another <see cref="IKStream{K, V0}"/> 's records using windowed left equi join with default
        /// serializers and deserializers.
        /// In contrast to <see cref="IKStream{K, V}.Join{V0, VR, V0S}(IKStream{K, V0}, Func{V, V0, VR}, JoinWindowOptions, StreamJoinProps)"/> (inner-join), all records from this stream will
        /// produce at least one output record (cf. below).
        /// The join is computed on the records' key with join attribute <code>thisKStream.key == otherKStream.key</code>.
        /// Furthermore, two records are only joined if their timestamps are close to each other as defined by the given
        /// <see cref="JoinWindowOptions"/>, i.e., the window defines an additional join predicate on the record timestamps.
        /// <para>
        /// For each pair of records meeting both join predicates the provided function joiner will be called to compute
        /// a value (with arbitrary type) for the result record.
        /// The key of the result record is the same as for both joining input records.
        /// Furthermore, for each input record of this <see cref="IKStream{K, V}"/> that does not satisfy the join predicate the provided
        /// function joiner will be called with a null value for the other stream.
        /// If an input record key or value is null the record will not be included in the join operation and thus no
        /// output record will be added to the resulting <see cref="IKStream{K, VR}"/>.
        /// </para>
        /// Both input streams (or to be more precise, their underlying source topics) need to have the same number of
        /// partitions.
        /// <para>
        /// Both of the joining IKStreams will be materialized in local state stores with auto-generated store names.
        /// For failure and recovery each store will be backed by an internal changelog topic that will be created in Kafka.
        /// The changelog topic will be named "${applicationId}-&lt;storename&gt;-changelog", where "applicationId" is user-specified
        /// in <see cref="StreamConfig.ApplicationId"/>, "storeName" is an internally generated name, and "-changelog" is a fixed suffix.
        /// </para>
        /// </summary>
        /// <typeparam name="V0">the value type of the other stream</typeparam>
        /// <typeparam name="VR">the value type of the result stream</typeparam>
        /// <typeparam name="V0S">the serdes value type of other stream</typeparam>
        /// <param name="stream">the <see cref="IKStream{K, V0}"/> to be joined with this stream</param>
        /// <param name="valueJoiner">a <see cref="Func{V, V0, VR}"/> that computes the join result for a pair of matching records</param>
        /// <param name="windows">the specification of the <see cref="JoinWindowOptions"/></param>
        /// <param name="props">a <see cref="StreamJoinProps"/> instance to configure serdes and state stores</param>
        /// <returns>a <see cref="IKStream{K, VR}"/>that contains join-records for each key and values computed by the given
        /// <see cref="Func{V, V0, VR}"/> joiner , one for each matched record-pair with the same key plus one for each non-matching record of
        /// this <see cref="IKStream{K, V0}"/> and within the joining window intervals</returns>
        IKStream<K, VR> LeftJoin<V0, VR, V0S>(IKStream<K, V0> stream, Func<V, V0, VR> valueJoiner, JoinWindowOptions windows, StreamJoinProps props = null)
            where V0S : ISerDes<V0>, new();

        /// <summary>
        /// Join records of this stream with another <see cref="IKStream{K, V0}"/> 's records using windowed left equi join with default
        /// serializers and deserializers.
        /// In contrast to <see cref="IKStream{K, V}.Join{V0, VR, V0S}(IKStream{K, V0}, Func{V, V0, VR}, JoinWindowOptions, StreamJoinProps)"/> (inner-join), all records from this stream will
        /// produce at least one output record (cf. below).
        /// The join is computed on the records' key with join attribute <code>thisKStream.key == otherKStream.key</code>.
        /// Furthermore, two records are only joined if their timestamps are close to each other as defined by the given
        /// <see cref="JoinWindowOptions"/>, i.e., the window defines an additional join predicate on the record timestamps.
        /// <para>
        /// For each pair of records meeting both join predicates the provided function joiner will be called to compute
        /// a value (with arbitrary type) for the result record.
        /// The key of the result record is the same as for both joining input records.
        /// Furthermore, for each input record of this <see cref="IKStream{K, V}"/> that does not satisfy the join predicate the provided
        /// function joiner will be called with a null value for the other stream.
        /// If an input record key or value is null the record will not be included in the join operation and thus no
        /// output record will be added to the resulting <see cref="IKStream{K, VR}"/>.
        /// </para>
        /// Both input streams (or to be more precise, their underlying source topics) need to have the same number of
        /// partitions.
        /// <para>
        /// Both of the joining IKStreams will be materialized in local state stores with auto-generated store names.
        /// For failure and recovery each store will be backed by an internal changelog topic that will be created in Kafka.
        /// The changelog topic will be named "${applicationId}-&lt;storename&gt;-changelog", where "applicationId" is user-specified
        /// in <see cref="StreamConfig.ApplicationId"/>, "storeName" is an internally generated name, and "-changelog" is a fixed suffix.
        /// </para>
        /// </summary>
        /// <typeparam name="V0">the value type of the other stream</typeparam>
        /// <typeparam name="VR">the value type of the result stream</typeparam>
        /// <param name="stream">the <see cref="IKStream{K, V0}"/> to be joined with this stream</param>
        /// <param name="valueJoiner">a <see cref="Func{V, V0, VR}"/> that computes the join result for a pair of matching records</param>
        /// <param name="windows">the specification of the <see cref="JoinWindowOptions"/></param>
        /// <param name="props">a <see cref="StreamJoinProps"/> instance to configure serdes and state stores</param>
        /// <returns>a <see cref="IKStream{K, VR}"/>that contains join-records for each key and values computed by the given
        /// <see cref="Func{V, V0, VR}"/> joiner , one for each matched record-pair with the same key plus one for each non-matching record of
        /// this <see cref="IKStream{K, V0}"/> and within the joining window intervals</returns>
        IKStream<K, VR> LeftJoin<V0, VR>(IKStream<K, V0> stream, Func<V, V0, VR> valueJoiner, JoinWindowOptions windows, StreamJoinProps<K, V, V0> props = null);

        /// <summary>
        /// Join records of this stream with another <see cref="IKStream{K, V0}"/> 's records using windowed left equi join with default
        /// serializers and deserializers.
        /// In contrast to <see cref="IKStream{K, V}.Join{V0, VR, V0S}(IKStream{K, V0}, Func{V, V0, VR}, JoinWindowOptions, StreamJoinProps)"/> (inner-join), all records from this stream will
        /// produce at least one output record (cf. below).
        /// The join is computed on the records' key with join attribute <code>thisKStream.key == otherKStream.key</code>.
        /// Furthermore, two records are only joined if their timestamps are close to each other as defined by the given
        /// <see cref="JoinWindowOptions"/>, i.e., the window defines an additional join predicate on the record timestamps.
        /// <para>
        /// For each pair of records meeting both join predicates the provided function joiner will be called to compute
        /// a value (with arbitrary type) for the result record.
        /// The key of the result record is the same as for both joining input records.
        /// Furthermore, for each input record of this <see cref="IKStream{K, V}"/> that does not satisfy the join predicate the provided
        /// function joiner will be called with a null value for the other stream.
        /// If an input record key or value is null the record will not be included in the join operation and thus no
        /// output record will be added to the resulting <see cref="IKStream{K, VR}"/>.
        /// </para>
        /// Both input streams (or to be more precise, their underlying source topics) need to have the same number of
        /// partitions.
        /// <para>
        /// Both of the joining IKStreams will be materialized in local state stores with auto-generated store names.
        /// For failure and recovery each store will be backed by an internal changelog topic that will be created in Kafka.
        /// The changelog topic will be named "${applicationId}-&lt;storename&gt;-changelog", where "applicationId" is user-specified
        /// in <see cref="StreamConfig.ApplicationId"/>, "storeName" is an internally generated name, and "-changelog" is a fixed suffix.
        /// </para>
        /// </summary>
        /// <typeparam name="V0">the value type of the other stream</typeparam>
        /// <typeparam name="VR">the value type of the result stream</typeparam>
        /// <typeparam name="V0S">the serdes value type of other stream</typeparam>
        /// <param name="stream">the <see cref="IKStream{K, V0}"/> to be joined with this stream</param>
        /// <param name="valueJoiner">a <see cref="IValueJoiner{V, V0, VR}"/> that computes the join result for a pair of matching records</param>
        /// <param name="windows">the specification of the <see cref="JoinWindowOptions"/></param>
        /// <param name="props">a <see cref="StreamJoinProps"/> instance to configure serdes and state stores</param>
        /// <returns>a <see cref="IKStream{K, VR}"/>that contains join-records for each key and values computed by the given
        /// <see cref="IValueJoiner{V, V0, VR}"/> joiner , one for each matched record-pair with the same key plus one for each non-matching record of
        /// this <see cref="IKStream{K, V0}"/> and within the joining window intervals</returns>
        IKStream<K, VR> LeftJoin<V0, VR, V0S>(IKStream<K, V0> stream, IValueJoiner<V, V0, VR> valueJoiner, JoinWindowOptions windows, StreamJoinProps props = null)
            where V0S : ISerDes<V0>, new();

        /// <summary>
        /// Join records of this stream with another <see cref="IKStream{K, V0}"/> 's records using windowed left equi join with default
        /// serializers and deserializers.
        /// In contrast to <see cref="IKStream{K, V}.Join{V0, VR, V0S}(IKStream{K, V0}, Func{V, V0, VR}, JoinWindowOptions, StreamJoinProps)"/> (inner-join), all records from this stream will
        /// produce at least one output record (cf. below).
        /// The join is computed on the records' key with join attribute <code>thisKStream.key == otherKStream.key</code>.
        /// Furthermore, two records are only joined if their timestamps are close to each other as defined by the given
        /// <see cref="JoinWindowOptions"/>, i.e., the window defines an additional join predicate on the record timestamps.
        /// <para>
        /// For each pair of records meeting both join predicates the provided function joiner will be called to compute
        /// a value (with arbitrary type) for the result record.
        /// The key of the result record is the same as for both joining input records.
        /// Furthermore, for each input record of this <see cref="IKStream{K, V}"/> that does not satisfy the join predicate the provided
        /// function joiner will be called with a null value for the other stream.
        /// If an input record key or value is null the record will not be included in the join operation and thus no
        /// output record will be added to the resulting <see cref="IKStream{K, VR}"/>.
        /// </para>
        /// Both input streams (or to be more precise, their underlying source topics) need to have the same number of
        /// partitions.
        /// <para>
        /// Both of the joining IKStreams will be materialized in local state stores with auto-generated store names.
        /// For failure and recovery each store will be backed by an internal changelog topic that will be created in Kafka.
        /// The changelog topic will be named "${applicationId}-&lt;storename&gt;-changelog", where "applicationId" is user-specified
        /// in <see cref="StreamConfig.ApplicationId"/>, "storeName" is an internally generated name, and "-changelog" is a fixed suffix.
        /// </para>
        /// </summary>
        /// <typeparam name="V0">the value type of the other stream</typeparam>
        /// <typeparam name="VR">the value type of the result stream</typeparam>
        /// <param name="stream">the <see cref="IKStream{K, V0}"/> to be joined with this stream</param>
        /// <param name="valueJoiner">a <see cref="IValueJoiner{V, V0, VR}"/> that computes the join result for a pair of matching records</param>
        /// <param name="windows">the specification of the <see cref="JoinWindowOptions"/></param>
        /// <param name="props">a <see cref="StreamJoinProps"/> instance to configure serdes and state stores</param>
        /// <returns>a <see cref="IKStream{K, VR}"/>that contains join-records for each key and values computed by the given
        /// <see cref="IValueJoiner{V, V0, VR}"/> joiner , one for each matched record-pair with the same key plus one for each non-matching record of
        /// this <see cref="IKStream{K, V0}"/> and within the joining window intervals</returns>
        IKStream<K, VR> LeftJoin<V0, VR>(IKStream<K, V0> stream, IValueJoiner<V, V0, VR> valueJoiner, JoinWindowOptions windows, StreamJoinProps<K, V, V0> props = null);

        /// <summary>
        /// Join records of this stream with another <see cref="IKStream{K, V0}"/> 's records using windowed left equi join with default
        /// serializers and deserializers.
        /// In contrast to <see cref="IKStream{K, V}.Join{V0, VR, V0S}(IKStream{K, V0}, Func{V, V0, VR}, JoinWindowOptions, StreamJoinProps)"/> (inner-join) or <see cref="IKStream{K, V}.LeftJoin{V0, VR, V0S}(IKStream{K, V0}, Func{V, V0, VR}, JoinWindowOptions, StreamJoinProps)"/> (left-join),all records from both streams will produce at
        /// least one output record (cf.below).
        /// The join is computed on the records' key with join attribute <code>thisKStream.key == otherKStream.key</code>.
        /// Furthermore, two records are only joined if their timestamps are close to each other as defined by the given
        /// <see cref="JoinWindowOptions"/>, i.e., the window defines an additional join predicate on the record timestamps.
        /// <para>
        /// For each pair of records meeting both join predicates the provided function joiner will be called to compute
        /// a value (with arbitrary type) for the result record.
        /// The key of the result record is the same as for both joining input records.
        /// Furthermore, for each input record of both IKStream that does not satisfy the join predicate the provided
        /// <see cref="Func{V, V0, VR}"/> joiner will be called with a null value for this/other stream, respectively.
        /// If an input record key or value is null, the record will not be included in the join operation and thus no
        /// output record will be added to the resulting <see cref="IKStream{K, VR}"/>.
        /// </para>
        /// Both input streams (or to be more precise, their underlying source topics) need to have the same number of
        /// partitions.
        /// <para>
        /// Both of the joining IKStreams will be materialized in local state stores with auto-generated store names.
        /// For failure and recovery each store will be backed by an internal changelog topic that will be created in Kafka.
        /// The changelog topic will be named "${applicationId}-&lt;storename&gt;-changelog", where "applicationId" is user-specified
        /// in <see cref="StreamConfig.ApplicationId"/>, "storeName" is an internally generated name, and "-changelog" is a fixed suffix.
        /// </para>
        /// </summary>
        /// <typeparam name="V0">the value type of the other stream</typeparam>
        /// <typeparam name="VR">the value type of the result stream</typeparam>
        /// <typeparam name="V0S">the serdes value type of other stream</typeparam>
        /// <param name="stream">the <see cref="IKStream{K, V0}"/> to be joined with this stream</param>
        /// <param name="valueJoiner">a <see cref="Func{V, V0, VR}"/> that computes the join result for a pair of matching records</param>
        /// <param name="windows">the specification of the <see cref="JoinWindowOptions"/></param>
        /// <param name="props">a <see cref="StreamJoinProps"/> instance to configure serdes and state stores</param>
        /// <returns>a <see cref="IKStream{K, VR}"/>that contains join-records for each key and values computed by the given
        /// <see cref="Func{V, V0, VR}"/> joiner , one for each matched record-pair with the same key plus one for each non-matching record of
        /// this <see cref="IKStream{K, V0}"/> and within the joining window intervals</returns>
        IKStream<K, VR> OuterJoin<V0, VR, V0S>(IKStream<K, V0> stream, Func<V, V0, VR> valueJoiner, JoinWindowOptions windows, StreamJoinProps props = null)
            where V0S : ISerDes<V0>, new();

        /// <summary>
        /// Join records of this stream with another <see cref="IKStream{K, V0}"/> 's records using windowed left equi join with default
        /// serializers and deserializers.
        /// In contrast to <see cref="IKStream{K, V}.Join{V0, VR, V0S}(IKStream{K, V0}, Func{V, V0, VR}, JoinWindowOptions, StreamJoinProps)"/> (inner-join) or <see cref="IKStream{K, V}.LeftJoin{V0, VR, V0S}(IKStream{K, V0}, Func{V, V0, VR}, JoinWindowOptions, StreamJoinProps)"/> (left-join),all records from both streams will produce at
        /// least one output record (cf.below).
        /// The join is computed on the records' key with join attribute <code>thisKStream.key == otherKStream.key</code>.
        /// Furthermore, two records are only joined if their timestamps are close to each other as defined by the given
        /// <see cref="JoinWindowOptions"/>, i.e., the window defines an additional join predicate on the record timestamps.
        /// <para>
        /// For each pair of records meeting both join predicates the provided function joiner will be called to compute
        /// a value (with arbitrary type) for the result record.
        /// The key of the result record is the same as for both joining input records.
        /// Furthermore, for each input record of both IKStream that does not satisfy the join predicate the provided
        /// <see cref="Func{V, V0, VR}"/> joiner will be called with a null value for this/other stream, respectively.
        /// If an input record key or value is null, the record will not be included in the join operation and thus no
        /// output record will be added to the resulting <see cref="IKStream{K, VR}"/>.
        /// </para>
        /// Both input streams (or to be more precise, their underlying source topics) need to have the same number of
        /// partitions.
        /// <para>
        /// Both of the joining IKStreams will be materialized in local state stores with auto-generated store names.
        /// For failure and recovery each store will be backed by an internal changelog topic that will be created in Kafka.
        /// The changelog topic will be named "${applicationId}-&lt;storename&gt;-changelog", where "applicationId" is user-specified
        /// in <see cref="StreamConfig.ApplicationId"/>, "storeName" is an internally generated name, and "-changelog" is a fixed suffix.
        /// </para>
        /// </summary>
        /// <typeparam name="V0">the value type of the other stream</typeparam>
        /// <typeparam name="VR">the value type of the result stream</typeparam>
        /// <param name="stream">the <see cref="IKStream{K, V0}"/> to be joined with this stream</param>
        /// <param name="valueJoiner">a <see cref="Func{V, V0, VR}"/> that computes the join result for a pair of matching records</param>
        /// <param name="windows">the specification of the <see cref="JoinWindowOptions"/></param>
        /// <param name="props">a <see cref="StreamJoinProps"/> instance to configure serdes and state stores</param>
        /// <returns>a <see cref="IKStream{K, VR}"/>that contains join-records for each key and values computed by the given
        /// <see cref="Func{V, V0, VR}"/> joiner , one for each matched record-pair with the same key plus one for each non-matching record of
        /// this <see cref="IKStream{K, V0}"/> and within the joining window intervals</returns>
        IKStream<K, VR> OuterJoin<V0, VR>(IKStream<K, V0> stream, Func<V, V0, VR> valueJoiner, JoinWindowOptions windows, StreamJoinProps<K, V, V0> props = null);

        /// <summary>
        /// Join records of this stream with another <see cref="IKStream{K, V0}"/> 's records using windowed left equi join with default
        /// serializers and deserializers.
        /// In contrast to <see cref="IKStream{K, V}.Join{V0, VR, V0S}(IKStream{K, V0}, Func{V, V0, VR}, JoinWindowOptions, StreamJoinProps)"/> (inner-join) or <see cref="IKStream{K, V}.LeftJoin{V0, VR, V0S}(IKStream{K, V0}, Func{V, V0, VR}, JoinWindowOptions, StreamJoinProps)"/> (left-join),all records from both streams will produce at
        /// least one output record (cf.below).
        /// The join is computed on the records' key with join attribute <code>thisKStream.key == otherKStream.key</code>.
        /// Furthermore, two records are only joined if their timestamps are close to each other as defined by the given
        /// <see cref="JoinWindowOptions"/>, i.e., the window defines an additional join predicate on the record timestamps.
        /// <para>
        /// For each pair of records meeting both join predicates the provided function joiner will be called to compute
        /// a value (with arbitrary type) for the result record.
        /// The key of the result record is the same as for both joining input records.
        /// Furthermore, for each input record of both IKStream that does not satisfy the join predicate the provided
        /// <see cref="IValueJoiner{V, V0, VR}"/> joiner will be called with a null value for this/other stream, respectively.
        /// If an input record key or value is null, the record will not be included in the join operation and thus no
        /// output record will be added to the resulting <see cref="IKStream{K, VR}"/>.
        /// </para>
        /// Both input streams (or to be more precise, their underlying source topics) need to have the same number of
        /// partitions.
        /// <para>
        /// Both of the joining IKStreams will be materialized in local state stores with auto-generated store names.
        /// For failure and recovery each store will be backed by an internal changelog topic that will be created in Kafka.
        /// The changelog topic will be named "${applicationId}-&lt;storename&gt;-changelog", where "applicationId" is user-specified
        /// in <see cref="StreamConfig.ApplicationId"/>, "storeName" is an internally generated name, and "-changelog" is a fixed suffix.
        /// </para>
        /// </summary>
        /// <typeparam name="V0">the value type of the other stream</typeparam>
        /// <typeparam name="VR">the value type of the result stream</typeparam>
        /// <typeparam name="V0S">the serdes value type of other stream</typeparam>
        /// <param name="stream">the <see cref="IKStream{K, V0}"/> to be joined with this stream</param>
        /// <param name="valueJoiner">a <see cref="IValueJoiner{V, V0, VR}"/> that computes the join result for a pair of matching records</param>
        /// <param name="windows">the specification of the <see cref="JoinWindowOptions"/></param>
        /// <param name="props">a <see cref="StreamJoinProps"/> instance to configure serdes and state stores</param>
        /// <returns>a <see cref="IKStream{K, VR}"/>that contains join-records for each key and values computed by the given
        /// <see cref="IValueJoiner{V, V0, VR}"/> joiner , one for each matched record-pair with the same key plus one for each non-matching record of
        /// this <see cref="IKStream{K, V0}"/> and within the joining window intervals</returns>
        IKStream<K, VR> OuterJoin<V0, VR, V0S>(IKStream<K, V0> stream, IValueJoiner<V, V0, VR> valueJoiner, JoinWindowOptions windows, StreamJoinProps props = null)
            where V0S : ISerDes<V0>, new();

        /// <summary>
        /// Join records of this stream with another <see cref="IKStream{K, V0}"/> 's records using windowed left equi join with default
        /// serializers and deserializers.
        /// In contrast to <see cref="IKStream{K, V}.Join{V0, VR, V0S}(IKStream{K, V0}, Func{V, V0, VR}, JoinWindowOptions, StreamJoinProps)"/> (inner-join) or <see cref="IKStream{K, V}.LeftJoin{V0, VR, V0S}(IKStream{K, V0}, Func{V, V0, VR}, JoinWindowOptions, StreamJoinProps)"/> (left-join),all records from both streams will produce at
        /// least one output record (cf.below).
        /// The join is computed on the records' key with join attribute <code>thisKStream.key == otherKStream.key</code>.
        /// Furthermore, two records are only joined if their timestamps are close to each other as defined by the given
        /// <see cref="JoinWindowOptions"/>, i.e., the window defines an additional join predicate on the record timestamps.
        /// <para>
        /// For each pair of records meeting both join predicates the provided function joiner will be called to compute
        /// a value (with arbitrary type) for the result record.
        /// The key of the result record is the same as for both joining input records.
        /// Furthermore, for each input record of both IKStream that does not satisfy the join predicate the provided
        /// <see cref="IValueJoiner{V, V0, VR}"/> joiner will be called with a null value for this/other stream, respectively.
        /// If an input record key or value is null, the record will not be included in the join operation and thus no
        /// output record will be added to the resulting <see cref="IKStream{K, VR}"/>.
        /// </para>
        /// Both input streams (or to be more precise, their underlying source topics) need to have the same number of
        /// partitions.
        /// <para>
        /// Both of the joining IKStreams will be materialized in local state stores with auto-generated store names.
        /// For failure and recovery each store will be backed by an internal changelog topic that will be created in Kafka.
        /// The changelog topic will be named "${applicationId}-&lt;storename&gt;-changelog", where "applicationId" is user-specified
        /// in <see cref="StreamConfig.ApplicationId"/>, "storeName" is an internally generated name, and "-changelog" is a fixed suffix.
        /// </para>
        /// </summary>
        /// <typeparam name="V0">the value type of the other stream</typeparam>
        /// <typeparam name="VR">the value type of the result stream</typeparam>
        /// <param name="stream">the <see cref="IKStream{K, V0}"/> to be joined with this stream</param>
        /// <param name="valueJoiner">a <see cref="IValueJoiner{V, V0, VR}"/> that computes the join result for a pair of matching records</param>
        /// <param name="windows">the specification of the <see cref="JoinWindowOptions"/></param>
        /// <param name="props">a <see cref="StreamJoinProps"/> instance to configure serdes and state stores</param>
        /// <returns>a <see cref="IKStream{K, VR}"/>that contains join-records for each key and values computed by the given
        /// <see cref="IValueJoiner{V, V0, VR}"/> joiner , one for each matched record-pair with the same key plus one for each non-matching record of
        /// this <see cref="IKStream{K, V0}"/> and within the joining window intervals</returns>
        IKStream<K, VR> OuterJoin<V0, VR>(IKStream<K, V0> stream, IValueJoiner<V, V0, VR> valueJoiner, JoinWindowOptions windows, StreamJoinProps<K, V, V0> props = null);

        /// <summary>
        /// Convert this stream to a <see cref="IKTable{K, V}"/>.
        /// Note that this is a logical operation and only changes the "interpretation" of the stream, i.e., each record of
        /// it was a "fact/event" and is re-interpreted as update now (cf. IKStream vs IKTable
        /// </summary>
        /// <returns>a <see cref="IKTable{K, V}"/> that contains the same records as this <see cref="IKStream{K, V}"/></returns>
        IKTable<K, V> ToTable();

        /// <summary>
        /// Convert this stream to a <see cref="IKTable{K, V}"/>.
        /// Note that this is a logical operation and only changes the "interpretation" of the stream, i.e., each record of
        /// it was a "fact/event" and is re-interpreted as update now (cf. IKStream vs IKTable
        /// </summary>
        /// <param name="materialized">an instance of <see cref="Materialized{K, V, S}"/> used to describe how the state store of the
        /// resulting table should be materialized.</param>
        /// <param name="named">a name config used to name the processor in the topology</param>
        /// <returns>a <see cref="IKTable{K, V}"/> that contains the same records as this <see cref="IKStream{K, V}"/></returns>>
        IKTable<K, V> ToTable(Materialized<K, V, IKeyValueStore<Bytes, byte[]>> materialized, string named = null);
    }
}
