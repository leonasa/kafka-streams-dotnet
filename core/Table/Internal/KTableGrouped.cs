using System.Collections.Generic;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream.Internal;
using Streamiz.Kafka.Net.Stream.Internal.Graph.Nodes;

namespace Streamiz.Kafka.Net.Table.Internal
{
    internal class KTableGrouped<K, KR, V, VR> : KTable<KR, V, VR>
    {
        private readonly IProcessorSupplier<K, V> processorSupplier;

        internal KTableGrouped(string name, ISerDes<KR> keySerde, ISerDes<VR> valSerde, List<string> sourceNodes, IProcessorSupplier<K, V> processorSupplier, StreamGraphNode streamsGraphNode, InternalStreamBuilder builder)
            : base(name, keySerde, valSerde, sourceNodes, (IProcessorSupplier<KR, V>)null, streamsGraphNode, builder)
        {
            this.processorSupplier = processorSupplier;
        }

        public override IKTableValueGetterSupplier<KR, VR> ValueGetterSupplier
        {
            get
            {
                return processorSupplier switch
                {
                    null => base.ValueGetterSupplier, 
                    IKStreamAggProcessorSupplier<K, KR, V, VR> supplier => supplier.View(),
                    _ => null
                };
            }
        }

        public override void EnableSendingOldValues()
        {
            if (!SendOldValues)
            {
                if (processorSupplier == null)
                    base.EnableSendingOldValues();
                else
                {
                    if (processorSupplier is IKStreamAggProcessorSupplier<KR, VR> supplier)
                    {
                        supplier.EnableSendingOldValues();
                    }
                    SendOldValues = true;
                }
            }
        }
    }
}
