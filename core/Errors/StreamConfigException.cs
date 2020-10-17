using System;
using System.Runtime.Serialization;

namespace Streamiz.Kafka.Net.Errors
{
    /// <summary>
    /// Signals that the configuration in your stream is incorrect or maybe a property is missing
    /// </summary>
    [Serializable]
    public class StreamConfigException : Exception
    {
        /// <summary>
        /// Constructor with exception message
        /// </summary>
        /// <param name="message">Message</param>
        public StreamConfigException(string message)
            : base(message)
        {
        }

        /// <summary>
        /// Constructor for Serialization
        /// </summary>
        /// <param name="info"></param>
        /// <param name="context"></param>
        protected StreamConfigException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }
    }
}
