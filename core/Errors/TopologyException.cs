using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Stream.Internal;
using System;
using System.Runtime.Serialization;

namespace Streamiz.Kafka.Net.Errors
{
    /// <summary>
    /// Indicates a pre run time error occurred while parsing the <see cref="Topology"/> logical topology
    /// to construct the <see cref="ProcessorTopology"/> physical processor topology.
    /// </summary>
    [Serializable]
    public class TopologyException : Exception
    {
        /// <summary>
        /// Constructor with exception message
        /// </summary>
        /// <param name="message">Exception message</param>
        public TopologyException(string message) 
            : base(message)
        {
        }


        /// <summary>
        /// Constructor for Serialization
        /// </summary>
        /// <param name="info"></param>
        /// <param name="context"></param>
        protected TopologyException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }
    }

    /// <summary>
    /// Indicates a pre run time error occurred while parsing the <see cref="Topology"/> logical topology
    /// to construct the <see cref="ProcessorTopology"/> physical processor topology.
    /// </summary>
    [Serializable]
    public class MissingTaskForPartitionException : Exception
    {
        /// <summary>
        /// Constructor with exception message
        /// </summary>
        /// <param name="message">Exception message</param>
        public MissingTaskForPartitionException(string message) 
            : base(message)
        {
        }


        /// <summary>
        /// Constructor for Serialization
        /// </summary>
        /// <param name="info"></param>
        /// <param name="context"></param>
        protected MissingTaskForPartitionException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }
    }
}
