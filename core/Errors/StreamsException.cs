﻿using System;
using System.Runtime.Serialization;

namespace Streamiz.Kafka.Net.Errors
{
    /// <summary>
    /// <see cref="StreamsException"/> is the top-level exception type generated by Kafka Streams.
    /// </summary>
    [Serializable]
    public class StreamsException : Exception
    {
        /// <summary>
        /// Constructor with exception message
        /// </summary>
        /// <param name="message">Exception message</param>
        public StreamsException(string message) 
            : base(message)
        {
        }

        /// <summary>
        /// Constructor with inner exception
        /// </summary>
        /// <param name="innerException">Inner exception</param>
        public StreamsException(Exception innerException) 
            : this(innerException.Message, innerException)
        {
        }

        /// <summary>
        /// Constructor with exception message and inner exception
        /// </summary>
        /// <param name="message">Exception message</param>
        /// <param name="innerException">Inner exception</param>
        public StreamsException(string message, Exception innerException) 
            : base(message, innerException)
        {
        }

        /// <summary>
        /// Constructor for Serialization
        /// </summary>
        /// <param name="info"></param>
        /// <param name="context"></param>
        protected StreamsException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }
    }
}
