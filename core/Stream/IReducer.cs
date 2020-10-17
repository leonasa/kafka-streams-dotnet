using System;

namespace Streamiz.Kafka.Net.Stream
{
    /// <summary>
    /// The <see cref="IReducer{V}"/> interface for combining two values of the same type into a new value.
    /// In contrast to <see cref="IAggregator{K,V,VA}" />
    /// the result type must be the same as the input type.
    /// <p>
    /// The provided values can be either original values from input keyvalue
    /// pair records or be a previously
    /// computed result from <see cref="IReducer{V}.Apply(V, V)"/>
    /// </p>
    /// <see cref="IReducer{V}"/> can be used to implement aggregation functions like sum, min, or max.
    /// </summary>
    /// <typeparam name="V">value type</typeparam>
    public interface IReducer<V>
    {
        /// <summary>
        /// Aggregate the two given values into a single one.
        /// </summary>
        /// <param name="value1">the first value for the aggregation</param>
        /// <param name="value2">the second value for the aggregation</param>
        /// <returns>the aggregated value</returns>
        V Apply(V value1, V value2);
    }

    internal class WrappedReducer<V> : IReducer<V>
    {
        private readonly Func<V, V, V> function;
        public WrappedReducer(Func<V, V, V> function)
        {
            this.function = function ?? throw new ArgumentNullException(nameof(function), $"IReducer function can't be null");
        }
        public V Apply(V value1, V value2) => function.Invoke(value1, value2);
    }
}
