﻿using System;

namespace Streamiz.Kafka.Net.Stream
{
    /// <summary>
    /// The initializer interface for creating an initial value in aggregations.
    /// Initializer is used in combination with Aggregator.
    /// </summary>
    /// <typeparam name="VA">aggregate value type</typeparam>
    public interface Initializer<out VA>
    {
        /// <summary>
        /// Return the initial value for an aggregation.
        /// </summary>
        /// <returns>the initial value for an aggregation</returns>
        VA Apply();
    }

    internal class WrappedInitializer<T> : Initializer<T>
    {
        private readonly Func<T> function;

        public WrappedInitializer(Func<T> function)
        {
            this.function = function ?? throw new ArgumentNullException(nameof(function), $"Initializer function can't be null");
        }

        public T Apply() => function.Invoke();
    }
}
