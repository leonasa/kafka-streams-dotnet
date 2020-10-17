using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.SerDes;

namespace Streamiz.Kafka.Net.SchemaRegistry.SerDes.Avro
{
    /// <summary>
    /// SerDes for avro beans
    /// </summary>
    /// <typeparam name="T">type of avro bean
    /// </typeparam>
    public class SchemaAvroSerDes<T> : AbstractSerDes<T>
    {
        private AvroSerializer<T> avroSerializer;
        private AvroDeserializer<T> avroDeserializer;

        private SchemaRegistryConfig GetConfig(ISchemaRegistryConfig config)
        {
            SchemaRegistryConfig c = new SchemaRegistryConfig();
            c.Url = config.SchemaRegistryUrl;
            if (config.SchemaRegistryMaxCachedSchemas.HasValue)
            {
                c.MaxCachedSchemas = config.SchemaRegistryMaxCachedSchemas;
            }

            if (config.SchemaRegistryRequestTimeoutMs.HasValue)
            {
                c.RequestTimeoutMs = config.SchemaRegistryRequestTimeoutMs;
            }

            return c;
        }

        private AvroSerializerConfig GetSerializerConfig(ISchemaRegistryConfig config)
        {
            AvroSerializerConfig c = new AvroSerializerConfig();
            if (config.AutoRegisterSchemas.HasValue)
            {
                c.AutoRegisterSchemas = config.AutoRegisterSchemas;
            }

            return c;
        }

        /// <summary>
        /// Initialize method with a current context which contains <see cref="IStreamConfig"/>.
        /// Can be used to initialize the serdes according to some parameters present in the configuration such as the schema.registry.url
        /// </summary>
        /// <param name="context">SerDesContext with stream configuration</param>
        public override void Initialize(SerDesContext context)
        {
            if (!isInitialized)
            {
                if (context.Config is ISchemaRegistryConfig schemaConfig)
                {
                    var registryClient = GetSchemaRegistryClient(GetConfig(schemaConfig));
                    avroDeserializer = new AvroDeserializer<T>(registryClient);
                    avroSerializer = new AvroSerializer<T>(registryClient, GetSerializerConfig(schemaConfig));

                    isInitialized = true;
                }
                else
                {
                    throw new StreamConfigException($"Configuration must inherited from ISchemaRegistryConfig for SchemaAvroSerDes<{typeof(T).Name}");
                }
            }
        }

        /// <summary>
        /// Deserialize a record value from a byte array into <typeparamref name="T"/> value
        /// </summary>
        /// <param name="data">serialized bytes.</param>
        /// <param name="context">serialization context</param>
        /// <returns>deserialized <typeparamref name="T"/> using data; may be null</returns>

        public override T Deserialize(byte[] data, SerializationContext context)
        {
            if (!isInitialized)
            {
                throw new StreamsException($"SchemaAvroSerDes<{typeof(T).Name} is not initialized !");
            }

            return avroDeserializer
                   .AsSyncOverAsync()
                   .Deserialize(data, data == null, context);
        }

        /// <summary>
        /// Convert <typeparamref name="T"/> <code>data</code> into a byte array.
        /// </summary>
        /// <param name="data"><typeparamref name="T"/> data</param>
        /// <param name="context">serialization context</param>
        /// <returns>serialized bytes</returns>
        public override byte[] Serialize(T data, SerializationContext context)
        {
            if (!isInitialized)
            {
                throw new StreamsException($"SchemaAvroSerDes<{typeof(T).Name} is not initialized !");
            }

            return avroSerializer.AsSyncOverAsync().Serialize(data, context);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="config"></param>
        /// <returns></returns>
        protected virtual ISchemaRegistryClient GetSchemaRegistryClient(SchemaRegistryConfig config)
        {
            return new CachedSchemaRegistryClient(config);
        }
    }
}
