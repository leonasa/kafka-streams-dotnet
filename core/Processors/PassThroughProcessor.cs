namespace Streamiz.Kafka.Net.Processors
{
    internal class PassThroughProcessor<K, V> : AbstractProcessor<K, V>
    {
        public override void Process(K key, V value)
        {
            Forward(key, value);
        }
    }
}
