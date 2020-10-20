﻿using Confluent.Kafka;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Processors;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.SerDes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;

namespace Streamiz.Kafka.Net
{
    /// <summary>
    /// Processing Guarantee enumeration used in <see cref="IStreamConfig.Guarantee"/>
    /// </summary>
    public enum ProcessingGuarantee
    {
        /// <summary>
        /// Config value for parameter <see cref="IStreamConfig.Guarantee"/> for at-least-once processing guarantees.
        /// </summary>
        AT_LEAST_ONCE,
        /// <summary>
        /// Config value for parameter <see cref="IStreamConfig.Guarantee"/> for exactly-once processing guarantees.
        /// </summary>
        EXACTLY_ONCE
    }

    /// <summary>
    /// Interface stream configuration for a <see cref="KafkaStream"/> instance.
    /// See <see cref="StreamConfig"/> to obtain implementation about this interface.
    /// You could develop your own implementation and get it in your <see cref="KafkaStream"/> instance.
    /// </summary>
    public interface IStreamConfig : ICloneable<IStreamConfig>
    {
        #region AddConfig

        /// <summary>
        /// Add keyvalue configuration for producer, consumer and admin client.
        /// [WARNING] : Maybe will change
        /// </summary>
        /// <param name="key">New key</param>
        /// <param name="value">New value</param>
        void AddConfig(string key, string value);

        /// <summary>
        /// Add keyvalue configuration for admin client
        /// [WARNING] : Maybe will change
        /// </summary>
        /// <param name="key">New key</param>
        /// <param name="value">New value</param>
        void AddAdminConfig(string key, string value);

        /// <summary>
        /// Add keyvalue configuration for consumer
        /// [WARNING] : Maybe will change
        /// </summary>
        /// <param name="key">New key</param>
        /// <param name="value">New value</param>
        void AddConsumerConfig(string key, string value);

        /// <summary>
        /// Add keyvalue configuration for producer
        /// [WARNING] : Maybe will change
        /// </summary>
        /// <param name="key">New key</param>
        /// <param name="value">New value</param>
        void AddProducerConfig(string key, string value);

        #endregion

        #region Methods 

        /// <summary>
        /// Get the configs to the <see cref="IProducer{TKey, TValue}"/>
        /// </summary>
        /// <returns>Return <see cref="ProducerConfig"/> for building <see cref="IProducer{TKey, TValue}"/> instance.</returns>
        ProducerConfig ToProducerConfig();

        /// <summary>
        /// Get the configs to the <see cref="IProducer{TKey, TValue}"/> with specific <paramref name="clientId"/>
        /// </summary>
        /// <param name="clientId">Producer client ID</param>
        /// <returns>Return <see cref="ProducerConfig"/> for building <see cref="IProducer{TKey, TValue}"/> instance.</returns>
        ProducerConfig ToProducerConfig(string clientId);

        /// <summary>
        /// Get the configs to the <see cref="IConsumer{TKey, TValue}"/>
        /// </summary>
        /// <returns>Return <see cref="ConsumerConfig"/> for building <see cref="IConsumer{TKey, TValue}"/> instance.</returns>
        ConsumerConfig ToConsumerConfig();

        /// <summary>
        /// Get the configs to the <see cref="IConsumer{TumerKey, TValue}"/> with specific <paramref name="clientId"/>
        /// </summary>
        /// <param name="clientId">Consumer client ID</param>
        /// <returns>Return <see cref="ConsumerConfig"/> for building <see cref="IConsumer{TKey, TValue}"/> instance.</returns>
        ConsumerConfig ToConsumerConfig(string clientId);

        /// <summary>
        /// Get the configs to the restore <see cref="IConsumer{TKey, TValue}"/> with specific <paramref name="clientId"/>.
        /// Restore consumer is using to restore persistent state store.
        /// </summary>
        /// <param name="clientId">Consumer client ID</param>
        /// <returns>Return <see cref="ConsumerConfig"/> for building <see cref="IConsumer{TKey, TValue}"/> instance.</returns>
        ConsumerConfig ToGlobalConsumerConfig(string clientId);

        /// <summary>
        /// Get the configs to the <see cref="IAdminClient"/> with specific <paramref name="clientId"/>
        /// </summary>
        /// <param name="clientId">Admin client ID</param>
        /// <returns>Return <see cref="AdminClientConfig"/> for building <see cref="IAdminClient"/> instance.</returns>
        AdminClientConfig ToAdminConfig(string clientId);

        #endregion

        #region Stream Config Property

        /// <summary>
        /// Maximum allowed time between calls to consume messages (e.g., rd_kafka_consumer_poll())
        /// for high-level consumers. If this interval is exceeded the consumer is considered
        /// failed and the group will rebalance in order to reassign the partitions to another
        /// consumer group member. Warning: Offset commits may be not possible at this point.
        /// Note: It is recommended to set `enable.auto.offset.store=false` for long-time
        /// processing applications and then explicitly store offsets (using offsets_store())
        /// *after* message processing, to make sure offsets are not auto-committed prior
        /// to processing has finished. The interval is checked two times per second. See
        /// KIP-62 for more information. default: 300000 importance: high
        /// </summary>
        int? MaxPollIntervalMs { get; set; }

        /// <summary>
        /// The maximum number of records returned in a single call to poll(). (Default: 500)
        /// </summary>
        long MaxPollRecords { get; set; }

        /// <summary>
        /// The amount of time in milliseconds to block waiting for input.
        /// </summary>
        long PollMs { get; set; }

        /// <summary>
        /// The frequency with which to save the position of the processor. (Note, if <see cref="IStreamConfig.Guarantee"/> is set to <see cref="ProcessingGuarantee.EXACTLY_ONCE"/>, the default value is <code>" + EOS_DEFAULT_COMMIT_INTERVAL_MS + "</code>,"
        /// otherwise the default value is <code>" + DEFAULT_COMMIT_INTERVAL_MS + "</code>.
        /// </summary>
        long CommitIntervalMs { get; set; }

        /// <summary>
        /// The amount of time to wait for response from cluster when getting matadata.
        /// </summary>
        int MetadataRequestTimeoutMs { get; set; }

        /// <summary>
        /// Timeout used for transaction related operations. (Default : 10 seconds).
        /// </summary>
        TimeSpan TransactionTimeout { get; set; }

        /// <summary>
        /// Enables the transactional producer. The transactional.id is used to identify the same transactional producer instance across process restarts.
        /// </summary>
        string TransactionalId { get; set; }

        /// <summary>
        /// An identifier for the stream processing application. Must be unique within the Kafka cluster. It is used as 1) the default client-id prefix, 2) the group-id for membership management, 3) the changelog topic prefix.
        /// </summary>
        string ApplicationId { get; set; }

        /// <summary>
        /// An ID prefix string used for the client IDs of internal consumer, producer and restore-consumer, with pattern '&lt;client.id&gt;-StreamThread-&lt;threadSequenceNumber&gt;-&lt;consumer|producer|restore-consumer&gt;'.
        /// </summary>
        string ClientId { get; set; }

        /// <summary>
        /// The number of threads to execute stream processing.
        /// </summary>
        int NumStreamThreads { get; set; }

        /// <summary>
        /// Default key serdes for consumer and materialized state store
        /// </summary>
        ISerDes DefaultKeySerDes { get; set; }

        /// <summary>
        /// Default value serdes for consumer and materialized state store
        /// </summary>
        ISerDes DefaultValueSerDes { get; set; }

        /// <summary>
        /// Default timestamp extractor class that implements the <see cref="ITimestampExtractor"/> interface.
        /// </summary>
        ITimestampExtractor DefaultTimestampExtractor { get; set; }

        /// <summary>
        /// The processing guarantee that should be used. Possible values are <see cref="ProcessingGuarantee.AT_LEAST_ONCE"/> (default) and <see cref="ProcessingGuarantee.EXACTLY_ONCE"/>. Note that exactly-once processing requires a cluster of at least three brokers by default what is the recommended setting for production; for development you can change this, by adjusting broker setting 'transaction.state.log.replication.factor' and 'transaction.state.log.min.isr'.
        /// </summary>
        ProcessingGuarantee Guarantee { get; set; }

        /// <summary>
        /// Initial list of brokers as a CSV list of broker host or host:port.
        /// </summary>
        string BootstrapServers { get; set; }

        /// <summary>
        /// Maximum amount of time a stream task will stay idle when not all of its partition buffers contain records, to avoid potential out-of-order record processing across multiple input streams. (Default: 0)
        /// </summary>
        long MaxTaskIdleMs { get; set; }

        /// <summary>
        /// Maximum number of records to buffer per partition. (Default: 1000)
        /// </summary>
        long BufferedRecordsPerPartition { get; set; }

        #endregion
    }

    /// <summary>
    /// Implementation of <see cref="IStreamConfig"/>. Contains all configuration for your stream.
    /// By default, Kafka Streams does not allow users to overwrite the following properties (Streams setting shown in parentheses)
    ///    - EnableAutoCommit = (false) - Streams client will always disable/turn off auto committing
    ///    - PartitionAssignmentStrategy = <see cref="Range"/> - Streams application must have a partition assignment stategy to RANGE for join processing
    /// If <see cref="IStreamConfig.Guarantee"/> is set to <see cref="ProcessingGuarantee.EXACTLY_ONCE"/>, Kafka Streams does not allow users to overwrite the following properties (Streams setting shown in parentheses):
    ///    - <see cref="IsolationLevel"/> (<see cref="IsolationLevel.ReadCommitted"/>) - Consumers will always read committed data only
    ///    - <see cref="EnableIdempotence"/> (true) - Producer will always have idempotency enabled
    ///    - <see cref="MaxInFlight"/> (5) - Producer will always have one in-flight request per connection
    /// If <see cref="IStreamConfig.Guarantee"/> is set to <see cref="ProcessingGuarantee.EXACTLY_ONCE"/>, Kafka Streams initialize the following properties :
    ///    - <see cref="CommitIntervalMs"/> (<see cref="EOS_DEFAULT_COMMIT_INTERVAL_MS"/>
    /// <exemple>
    /// <code>
    /// var config = new StreamConfig();
    /// config.ApplicationId = "test-app";
    /// config.BootstrapServers = "localhost:9092";
    /// </code>
    /// </exemple>
    /// </summary>
    [Serializable]
    public class StreamConfig : Dictionary<string, dynamic>, IStreamConfig, ISchemaRegistryConfig
    {
        #region Not used for moment

        /// private string applicationServerCst = "application.server";
        /// private string topologyOptimizationCst = "topology.optimization";
        /// private string cacheMaxBytesBufferingCst = "cache.max.bytes.buffering";
        /// private string rocksdbConfigSetterCst = "rocksdb.config.setter";
        /// private string stateCleanupDelayMsCst = "state.cleanup.delay.ms";
        /// private string pollMsCst = "poll.ms";
        /// private string processingGuaranteeCst = "processing.guarantee";

        /// public static readonly string AT_LEAST_ONCE = "at_least_once";
        /// public static readonly string EXACTLY_ONCE = "exactly_once";

        /// private string Optimize
        /// {
        ///     get => this[topologyOptimizationCst];
        ///     set => this.AddOrUpdate(topologyOptimizationCst, value);
        /// }

        /// private string ApplicationServer
        /// {
        ///     get => this[applicationServerCst];
        ///     set => this.AddOrUpdate(applicationServerCst, value);
        /// }

        /// private string ProcessingGuaranteeConfig
        /// {
        ///     get => this[processingGuaranteeCst];
        ///     set
        ///     {
        ///         if (value.Equals(AT_LEAST_ONCE) || value.Equals(EXACTLY_ONCE))
        ///             this.AddOrUpdate(processingGuaranteeCst, value);
        ///         else
        ///             throw new InvalidOperationException($"ProcessingGuaranteeConfig value must equal to {AT_LEAST_ONCE} or {EXACTLY_ONCE}");
        ///     }
        /// }

        /// private long PollMsConfig
        /// {
        ///     get => Convert.ToInt64(this[pollMsCst]);
        ///     set => this.AddOrUpdate(pollMsCst, value.ToString());
        /// }

        /// private long StateCleanupDelayMs
        /// {
        ///     get => Convert.ToInt64(this[stateCleanupDelayMsCst]);
        ///     set => this.AddOrUpdate(stateCleanupDelayMsCst, value.ToString());
        /// }

        /// private long CacheMaxBytesBuffering
        /// {
        ///     get => Convert.ToInt64(this[cacheMaxBytesBufferingCst]);
        ///     set => this.AddOrUpdate(cacheMaxBytesBufferingCst, value.ToString());
        /// }

        #endregion

        #region Config constants

        internal static readonly string schemaRegistryUrlCst = "schema.registry.url";
        internal static readonly string schemaRegistryAutoRegisterCst = "schema.registry.auto.register.schema";
        internal static readonly string schemaRegistryRequestTimeoutMsCst = "schema.registry.request.timeout.ms";
        internal static readonly string schemaRegistryMaxCachedSchemasCst = "schema.registry.max.cached.schemas";
        internal static readonly string applicatonIdCst = "application.id";
        internal static readonly string clientIdCst = "client.id";
        internal static readonly string numStreamThreadsCst = "num.stream.threads";
        internal static readonly string defaultKeySerDesCst = "default.key.serdes";
        internal static readonly string defaultValueSerDesCst = "default.value.serdes";
        internal static readonly string defaultTimestampExtractorCst = "default.timestamp.extractor";
        internal static readonly string processingGuaranteeCst = "processing.guarantee";
        internal static readonly string transactionTimeoutCst = "transaction.timeout";
        internal static readonly string commitIntervalMsCst = "commit.interval.ms";
        internal static readonly string pollMsCst = "poll.ms";
        internal static readonly string maxPollRecordsCst = "max.poll.records.ms";
        internal static readonly string maxTaskIdleCst = "max.task.idle.ms";
        internal static readonly string bufferedRecordsPerPartitionCst = "buffered.records.per.partition";

        /// <summary>
        /// Default commit interval in milliseconds when exactly once is not enabled
        /// </summary>
        public static readonly long DEFAULT_COMMIT_INTERVAL_MS = 30000L;

        /// <summary>
        /// Default commit interval in milliseconds when exactly once is enabled
        /// </summary>
        public static readonly long EOS_DEFAULT_COMMIT_INTERVAL_MS = 100L;

        #endregion

        private ConsumerConfig _consumerConfig;
        private ProducerConfig _producerConfig;
        private AdminClientConfig _adminClientConfig;
        private ClientConfig _config;

        private IDictionary<string, string> _internalConsumerConfig = new Dictionary<string, string>();
        private IDictionary<string, string> _internalProducerConfig = new Dictionary<string, string>();
        private IDictionary<string, string> _internalAdminConfig = new Dictionary<string, string>();

        private bool changeGuarantee;

        #region ClientConfig

        /// <summary>
        /// Add keyvalue configuration for producer, consumer and admin client.
        /// [WARNING] : Maybe will change
        /// </summary>
        /// <param name="key">New key</param>
        /// <param name="value">New value</param>
        public void AddConfig(string key, string value)
        {
            AddConsumerConfig(key, value);
            AddAdminConfig(key, value);
            AddProducerConfig(key, value);
        }

        /// <summary>
        /// Timeout for broker API version requests. default: 10000 importance: low
        /// </summary>
        public int? ApiVersionRequestTimeoutMs
        {
            get => _config.ApiVersionRequestTimeoutMs;
            set
            {
                _config.ApiVersionRequestTimeoutMs = value;
                _consumerConfig.ApiVersionRequestTimeoutMs = value;
                _producerConfig.ApiVersionRequestTimeoutMs = value;
                _adminClientConfig.ApiVersionRequestTimeoutMs = value;
            }
        }

        /// <summary>
        /// Dictates how long the `broker.version.fallback` fallback is used in the case
        /// the ApiVersionRequest fails. **NOTE**: The ApiVersionRequest is only issued when
        /// a new connection to the broker is made (such as after an upgrade). default: 0
        /// importance: medium
        /// </summary>
        public int? ApiVersionFallbackMs
        {
            get => _config.ApiVersionFallbackMs;
            set
            {
                _config.ApiVersionFallbackMs = value;
                _consumerConfig.ApiVersionFallbackMs = value;
                _producerConfig.ApiVersionFallbackMs = value;
                _adminClientConfig.ApiVersionFallbackMs = value;
            }
        }

        /// <summary>
        /// Older broker versions (before 0.10.0) provide no way for a client to query for
        /// supported protocol features (ApiVersionRequest, see `api.version.request`) making
        /// it impossible for the client to know what features it may use. As a workaround
        /// a user may set this property to the expected broker version and the client will
        /// automatically adjust its feature set accordingly if the ApiVersionRequest fails
        /// (or is disabled). The fallback broker version will be used for `api.version.fallback.ms`.
        /// Valid values are: 0.9.0, 0.8.2, 0.8.1, 0.8.0. Any other value >= 0.10, such as
        /// 0.10.2.1, enables ApiVersionRequests. default: 0.10.0 importance: medium
        /// </summary>
        public string BrokerVersionFallback
        {
            get => _config.BrokerVersionFallback;
            set
            {
                _config.BrokerVersionFallback = value;
                _consumerConfig.BrokerVersionFallback = value;
                _producerConfig.BrokerVersionFallback = value;
                _adminClientConfig.BrokerVersionFallback = value;
            }
        }

        /// <summary>
        /// Protocol used to communicate with brokers. default: plaintext importance: high
        /// </summary>
        public SecurityProtocol? SecurityProtocol
        {
            get => _config.SecurityProtocol;
            set
            {
                _config.SecurityProtocol = value;
                _consumerConfig.SecurityProtocol = value;
                _producerConfig.SecurityProtocol = value;
                _adminClientConfig.SecurityProtocol = value;
            }
        }

        /// <summary>
        /// A cipher suite is a named combination of authentication, encryption, MAC and
        /// key exchange algorithm used to negotiate the security settings for a network
        /// connection using TLS or SSL network protocol. See manual page for `ciphers(1)`
        /// and `SSL_CTX_set_cipher_list(3). default: '' importance: low
        /// </summary>
        public string SslCipherSuites
        {
            get => _config.SslCipherSuites;
            set
            {
                _config.SslCipherSuites = value;
                _consumerConfig.SslCipherSuites = value;
                _producerConfig.SslCipherSuites = value;
                _adminClientConfig.SslCipherSuites = value;
            }
        }

        /// <summary>
        /// The supported-curves extension in the TLS ClientHello message specifies the curves
        /// (standard/named, or 'explicit' GF(2^k) or GF(p)) the client is willing to have
        /// the server use. See manual page for `SSL_CTX_set1_curves_list(3)`. OpenSSL >=
        /// 1.0.2 required. default: '' importance: low
        /// </summary>
        public string SslCurvesList
        {
            get => _config.SslCurvesList;
            set
            {
                _config.SslCurvesList = value;
                _consumerConfig.SslCurvesList = value;
                _producerConfig.SslCurvesList = value;
                _adminClientConfig.SslCurvesList = value;
            }
        }

        /// <summary>
        /// The client uses the TLS ClientHello signature_algorithms extension to indicate
        /// to the server which signature/hash algorithm pairs may be used in digital signatures.
        /// See manual page for `SSL_CTX_set1_sigalgs_list(3)`. OpenSSL >= 1.0.2 required.
        /// default: '' importance: low
        /// </summary>
        public string SslSigalgsList
        {
            get => _config.SslSigalgsList;
            set
            {
                _config.SslSigalgsList = value;
                _consumerConfig.SslSigalgsList = value;
                _producerConfig.SslSigalgsList = value;
                _adminClientConfig.SslSigalgsList = value;
            }
        }

        /// <summary>
        /// Path to client's private key (PEM) used for authentication. default: '' importance:
        /// low
        /// </summary>
        public string SslKeyLocation
        {
            get => _config.SslKeyLocation;
            set
            {
                _config.SslKeyLocation = value;
                _consumerConfig.SslKeyLocation = value;
                _producerConfig.SslKeyLocation = value;
                _adminClientConfig.SslKeyLocation = value;
            }
        }

        /// <summary>
        /// Private key passphrase (for use with `ssl.key.location` and `set_ssl_cert()`)
        /// default: '' importance: low
        /// </summary>
        public string SslKeyPassword
        {
            get => _config.SslKeyPassword;
            set
            {
                _config.SslKeyPassword = value;
                _consumerConfig.SslKeyPassword = value;
                _producerConfig.SslKeyPassword = value;
                _adminClientConfig.SslKeyPassword = value;
            }
        }

        /// <summary>
        /// Client's private key string (PEM format) used for authentication. default: ''
        /// importance: low
        /// </summary>
        public string SslKeyPem
        {
            get => _config.SslKeyPem;
            set
            {
                _config.SslKeyPem = value;
                _consumerConfig.SslKeyPem = value;
                _producerConfig.SslKeyPem = value;
                _adminClientConfig.SslKeyPem = value;
            }
        }

        /// <summary>
        /// Path to client's public key (PEM) used for authentication. default: '' importance:
        /// low
        /// </summary>
        public string SslCertificateLocation
        {
            get => _config.SslCertificateLocation;
            set
            {
                _config.SslCertificateLocation = value;
                _consumerConfig.SslCertificateLocation = value;
                _producerConfig.SslCertificateLocation = value;
                _adminClientConfig.SslCertificateLocation = value;
            }
        }

        /// <summary>
        /// Client's public key string (PEM format) used for authentication. default: ''
        /// importance: low
        /// </summary>
        public string SslCertificatePem
        {
            get => _config.SslCertificatePem;
            set
            {
                _config.SslCertificatePem = value;
                _consumerConfig.SslCertificatePem = value;
                _producerConfig.SslCertificatePem = value;
                _adminClientConfig.SslCertificatePem = value;
            }
        }

        /// <summary>
        /// File or directory path to CA certificate(s) for verifying the broker's key. default:
        /// '' importance: low
        /// </summary>
        public string SslCaLocation
        {
            get => _config.SslCaLocation;
            set
            {
                _config.SslCaLocation = value;
                _consumerConfig.SslCaLocation = value;
                _producerConfig.SslCaLocation = value;
                _adminClientConfig.SslCaLocation = value;
            }
        }

        /// <summary>
        /// Path to client's keystore (PKCS#12) used for authentication. default: '' importance:
        /// low
        /// </summary>
        public string SslKeystoreLocation
        {
            get => _config.SslKeystoreLocation;
            set
            {
                _config.SslKeystoreLocation = value;
                _consumerConfig.SslKeystoreLocation = value;
                _producerConfig.SslKeystoreLocation = value;
                _adminClientConfig.SslKeystoreLocation = value;
            }
        }

        /// <summary>
        /// Request broker's supported API versions to adjust functionality to available
        /// protocol features. If set to false, or the ApiVersionRequest fails, the fallback
        /// version `broker.version.fallback` will be used. **NOTE**: Depends on broker version
        /// >=0.10.0. If the request is not supported by (an older) broker the `broker.version.fallback`
        /// fallback is used. default: true importance: high
        /// </summary>
        public bool? ApiVersionRequest
        {
            get => _config.ApiVersionRequest;
            set
            {
                _config.ApiVersionRequest = value;
                _consumerConfig.ApiVersionRequest = value;
                _producerConfig.ApiVersionRequest = value;
                _adminClientConfig.ApiVersionRequest = value;
            }
        }

        /// <summary>
        /// Client's keystore (PKCS#12) password. default: '' importance: low
        /// </summary>
        public string SslKeystorePassword
        {
            get => _config.SslKeystorePassword;
            set
            {
                _config.SslKeystorePassword = value;
                _consumerConfig.SslKeystorePassword = value;
                _producerConfig.SslKeystorePassword = value;
                _adminClientConfig.SslKeystorePassword = value;
            }
        }

        /// <summary>
        /// Enable OpenSSL's builtin broker (server) certificate verification. default:
        /// true importance: low
        /// </summary>
        public bool? EnableSslCertificateVerification
        {
            get => _config.EnableSslCertificateVerification;
            set
            {
                _config.EnableSslCertificateVerification = value;
                _consumerConfig.EnableSslCertificateVerification = value;
                _producerConfig.EnableSslCertificateVerification = value;
                _adminClientConfig.EnableSslCertificateVerification = value;
            }
        }

        /// <summary>
        /// Endpoint identification algorithm to validate broker hostname using broker certificate.
        /// https - Server (broker) hostname verification as specified in RFC2818. none -
        /// No endpoint verification. OpenSSL >= 1.0.2 required. default: none importance:
        /// low
        /// </summary>
        public SslEndpointIdentificationAlgorithm? SslEndpointIdentificationAlgorithm
        {
            get => _config.SslEndpointIdentificationAlgorithm;
            set
            {
                _config.SslEndpointIdentificationAlgorithm = value;
                _consumerConfig.SslEndpointIdentificationAlgorithm = value;
                _producerConfig.SslEndpointIdentificationAlgorithm = value;
                _adminClientConfig.SslEndpointIdentificationAlgorithm = value;
            }
        }

        /// <summary>
        /// Kerberos principal name that Kafka runs as, not including /hostname@REALM default:
        /// kafka importance: low
        /// </summary>
        public string SaslKerberosServiceName
        {
            get => _config.SaslKerberosServiceName;
            set
            {
                _config.SaslKerberosServiceName = value;
                _consumerConfig.SaslKerberosServiceName = value;
                _producerConfig.SaslKerberosServiceName = value;
                _adminClientConfig.SaslKerberosServiceName = value;
            }
        }

        /// <summary>
        /// This client's Kerberos principal name. (Not supported on Windows, will use the
        /// logon user's principal). default: kafkaclient importance: low
        /// </summary>
        public string SaslKerberosPrincipal
        {
            get => _config.SaslKerberosPrincipal;
            set
            {
                _config.SaslKerberosPrincipal = value;
                _consumerConfig.SaslKerberosPrincipal = value;
                _producerConfig.SaslKerberosPrincipal = value;
                _adminClientConfig.SaslKerberosPrincipal = value;
            }
        }

        /// <summary>
        /// Shell command to refresh or acquire the client's Kerberos ticket. This command
        /// is executed on client creation and every sasl.kerberos.min.time.before.relogin
        /// (0=disable). %{config.prop.name} is replaced by corresponding config object value.
        /// default: kinit -R -t "%{sasl.kerberos.keytab}" -k %{sasl.kerberos.principal}
        /// || kinit -t "%{sasl.kerberos.keytab}" -k %{sasl.kerberos.principal} importance:
        /// low
        /// </summary>
        public string SaslKerberosKinitCmd
        {
            get => _config.SaslKerberosKinitCmd;
            set
            {
                _config.SaslKerberosKinitCmd = value;
                _consumerConfig.SaslKerberosKinitCmd = value;
                _producerConfig.SaslKerberosKinitCmd = value;
                _adminClientConfig.SaslKerberosKinitCmd = value;
            }
        }

        /// <summary>
        /// Path to Kerberos keytab file. This configuration property is only used as a variable
        /// in `sasl.kerberos.kinit.cmd` as ` ... -t "%{sasl.kerberos.keytab}"`. default:
        /// '' importance: low
        /// </summary>
        public string SaslKerberosKeytab
        {
            get => _config.SaslKerberosKeytab;
            set
            {
                _config.SaslKerberosKeytab = value;
                _consumerConfig.SaslKerberosKeytab = value;
                _producerConfig.SaslKerberosKeytab = value;
                _adminClientConfig.SaslKerberosKeytab = value;
            }
        }

        /// <summary>
        /// Minimum time in milliseconds between key refresh attempts. Disable automatic
        /// key refresh by setting this property to 0. default: 60000 importance: low
        /// </summary>
        public int? SaslKerberosMinTimeBeforeRelogin
        {
            get => _config.SaslKerberosMinTimeBeforeRelogin;
            set
            {
                _config.SaslKerberosMinTimeBeforeRelogin = value;
                _consumerConfig.SaslKerberosMinTimeBeforeRelogin = value;
                _producerConfig.SaslKerberosMinTimeBeforeRelogin = value;
                _adminClientConfig.SaslKerberosMinTimeBeforeRelogin = value;
            }
        }

        /// <summary>
        /// SASL username for use with the PLAIN and SASL-SCRAM-.. mechanisms default: ''
        /// importance: high
        /// </summary>
        public string SaslUsername
        {
            get => _config.SaslUsername;
            set
            {
                _config.SaslUsername = value;
                _consumerConfig.SaslUsername = value;
                _producerConfig.SaslUsername = value;
                _adminClientConfig.SaslUsername = value;
            }
        }

        /// <summary>
        /// SASL password for use with the PLAIN and SASL-SCRAM-.. mechanism default: ''
        /// importance: high
        /// </summary>
        public string SaslPassword
        {
            get => _config.SaslPassword;
            set
            {
                _config.SaslPassword = value;
                _consumerConfig.SaslPassword = value;
                _producerConfig.SaslPassword = value;
                _adminClientConfig.SaslPassword = value;
            }
        }

        /// <summary>
        /// SASL/OAUTHBEARER configuration. The format is implementation-dependent and must
        /// be parsed accordingly. The default unsecured token implementation (see https://tools.ietf.org/html/rfc7515#appendix-A.5)
        /// recognizes space-separated name=value pairs with valid names including principalClaimName,
        /// principal, scopeClaimName, scope, and lifeSeconds. The default value for principalClaimName
        /// is "sub", the default value for scopeClaimName is "scope", and the default value
        /// for lifeSeconds is 3600. The scope value is CSV format with the default value
        /// being no/empty scope. For example: `principalClaimName=azp principal=admin scopeClaimName=roles
        /// scope=role1,role2 lifeSeconds=600`. In addition, SASL extensions can be communicated
        /// to the broker via `extension_NAME=value`. For example: `principal=admin extension_traceId=123`
        /// default: '' importance: low
        /// </summary>
        public string SaslOauthbearerConfig
        {
            get => _config.SaslOauthbearerConfig;
            set
            {
                _config.SaslOauthbearerConfig = value;
                _consumerConfig.SaslOauthbearerConfig = value;
                _producerConfig.SaslOauthbearerConfig = value;
                _adminClientConfig.SaslOauthbearerConfig = value;
            }
        }

        /// <summary>
        /// Enable the builtin unsecure JWT OAUTHBEARER token handler if no oauthbearer_refresh_cb
        /// has been set. This builtin handler should only be used for development or testing,
        /// and not in production. default: false importance: low
        /// </summary>
        public bool? EnableSaslOauthbearerUnsecureJwt
        {
            get => _config.EnableSaslOauthbearerUnsecureJwt;
            set
            {
                _config.EnableSaslOauthbearerUnsecureJwt = value;
                _consumerConfig.EnableSaslOauthbearerUnsecureJwt = value;
                _producerConfig.EnableSaslOauthbearerUnsecureJwt = value;
                _adminClientConfig.EnableSaslOauthbearerUnsecureJwt = value;
            }
        }

        /// <summary>
        ///  Path to CRL for verifying broker's certificate validity. default: '' importance:
        ///  low
        ///  </summary>
        public string SslCrlLocation
        {
            get => _config.SslCrlLocation;
            set
            {
                _config.SslCrlLocation = value;
                _consumerConfig.SslCrlLocation = value;
                _producerConfig.SslCrlLocation = value;
                _adminClientConfig.SslCrlLocation = value;
            }
        }

        /// <summary>
        /// Signal that librdkafka will use to quickly terminate on rd_kafka_destroy(). If
        /// this signal is not set then there will be a delay before rd_kafka_wait_destroyed()
        /// returns true as internal threads are timing out their system calls. If this signal
        /// is set however the delay will be minimal. The application should mask this signal
        /// as an internal signal handler is installed. default: 0 importance: low
        /// </summary>
        public int? InternalTerminationSignal
        {
            get => _config.InternalTerminationSignal;
            set
            {
                _config.InternalTerminationSignal = value;
                _consumerConfig.InternalTerminationSignal = value;
                _producerConfig.InternalTerminationSignal = value;
                _adminClientConfig.InternalTerminationSignal = value;
            }
        }

        /// <summary>
        /// Log broker disconnects. It might be useful to turn this off when interacting
        /// with 0.9 brokers with an aggressive `connection.max.idle.ms` value. default:
        /// true importance: low
        /// </summary>
        public bool? LogConnectionClose
        {
            get => _config.LogConnectionClose;
            set
            {
                _config.LogConnectionClose = value;
                _consumerConfig.LogConnectionClose = value;
                _producerConfig.LogConnectionClose = value;
                _adminClientConfig.LogConnectionClose = value;
            }
        }

        /// <summary>
        /// Print internal thread name in log messages (useful for debugging librdkafka internals)
        /// default: true importance: low
        /// </summary>
        public bool? LogThreadName
        {
            get => _config.LogThreadName;
            set
            {
                _config.LogThreadName = value;
                _consumerConfig.LogThreadName = value;
                _producerConfig.LogThreadName = value;
                _adminClientConfig.LogThreadName = value;
            }
        }

        /// <summary>
        /// SASL mechanism to use for authentication. Supported: GSSAPI, PLAIN, SCRAM-SHA-256,
        /// SCRAM-SHA-512. **NOTE**: Despite the name, you may not configure more than one
        /// mechanism.
        /// </summary>
        public SaslMechanism? SaslMechanism
        {
            get => _config.SaslMechanism;
            set
            {
                _config.SaslMechanism = value;
                _consumerConfig.SaslMechanism = value;
                _producerConfig.SaslMechanism = value;
                _adminClientConfig.SaslMechanism = value;
            }
        }

        /// <summary>
        /// This field indicates the number of acknowledgements the leader broker must receive
        /// from ISR brokers before responding to the request: Zero=Broker does not send
        /// any response/ack to client, One=The leader will write the record to its local
        /// log but will respond without awaiting full acknowledgement from all followers.
        /// All=Broker will block until message is committed by all in sync replicas (ISRs).
        /// If there are less than min.insync.replicas (broker configuration) in the ISR
        /// set the produce request will fail.
        /// </summary>
        public Acks? Acks
        {
            get => _config.Acks;
            set
            {
                _config.Acks = value;
                _consumerConfig.Acks = value;
                _producerConfig.Acks = value;
                _adminClientConfig.Acks = value;
            }
        }

        /// <summary>
        /// Maximum Kafka protocol request message size. Due to differing framing overhead
        /// between protocol versions the producer is unable to reliably enforce a strict
        /// max message limit at produce time and may exceed the maximum size by one message
        /// in protocol ProduceRequests, the broker will enforce the the topic's `max.message.bytes`
        /// limit (see Apache Kafka documentation). default: 1000000 importance: medium
        /// </summary>
        public int? MessageMaxBytes
        {
            get => _config.MessageMaxBytes;
            set
            {
                _config.MessageMaxBytes = value;
                _consumerConfig.MessageMaxBytes = value;
                _producerConfig.MessageMaxBytes = value;
                _adminClientConfig.MessageMaxBytes = value;
            }
        }

        /// <summary>
        /// Maximum size for message to be copied to buffer. Messages larger than this will
        /// be passed by reference (zero-copy) at the expense of larger iovecs. default:
        /// 65535 importance: low
        /// </summary>
        public int? MessageCopyMaxBytes
        {
            get => _config.MessageCopyMaxBytes;
            set
            {
                _config.MessageCopyMaxBytes = value;
                _consumerConfig.MessageCopyMaxBytes = value;
                _producerConfig.MessageCopyMaxBytes = value;
                _adminClientConfig.MessageCopyMaxBytes = value;
            }
        }

        /// <summary>
        /// Maximum Kafka protocol response message size. This serves as a safety precaution
        /// to avoid memory exhaustion in case of protocol hickups. This value must be at
        /// least `fetch.max.bytes` + 512 to allow for protocol overhead; the value is adjusted
        /// automatically unless the configuration property is explicitly set. default: 100000000
        /// importance: medium
        /// </summary>
        public int? ReceiveMessageMaxBytes
        {
            get => _config.ReceiveMessageMaxBytes;
            set
            {
                _config.ReceiveMessageMaxBytes = value;
                _consumerConfig.ReceiveMessageMaxBytes = value;
                _producerConfig.ReceiveMessageMaxBytes = value;
                _adminClientConfig.ReceiveMessageMaxBytes = value;
            }
        }

        /// <summary>
        /// Maximum number of in-flight requests per broker connection. This is a generic
        /// property applied to all broker communication, however it is primarily relevant
        /// to produce requests. In particular, note that other mechanisms limit the number
        /// of outstanding consumer fetch request per broker to one. default: 1000000 importance:
        /// low
        /// </summary>
        public int? MaxInFlight
        {
            get => _config.MaxInFlight;
            set
            {
                if (Guarantee != ProcessingGuarantee.EXACTLY_ONCE)
                {
                    _config.MaxInFlight = value;
                    _consumerConfig.MaxInFlight = value;
                    _producerConfig.MaxInFlight = value;
                    _adminClientConfig.MaxInFlight = value;
                }
                else if (!changeGuarantee)
                    throw new StreamsException($"You can't update MaxInFlight because your processing guarantee is exactly-once");
            }
        }

        /// <summary>
        /// Non-topic request timeout in milliseconds. This is for metadata requests, etc.
        /// default: 60000 importance: low
        /// </summary>
        public int MetadataRequestTimeoutMs
        {
            get => _config.MetadataRequestTimeoutMs ?? 1000;
            set
            {
                _config.MetadataRequestTimeoutMs = value;
                _consumerConfig.MetadataRequestTimeoutMs = value;
                _producerConfig.MetadataRequestTimeoutMs = value;
                _adminClientConfig.MetadataRequestTimeoutMs = value;
            }
        }

        /// <summary>
        /// Period of time in milliseconds at which topic and broker metadata is refreshed
        /// in order to proactively discover any new brokers, topics, partitions or partition
        /// leader changes. Use -1 to disable the intervalled refresh (not recommended).
        /// If there are no locally referenced topics (no topic objects created, no messages
        /// produced, no subscription or no assignment) then only the broker list will be
        /// refreshed every interval but no more often than every 10s. default: 300000 importance:
        /// low
        /// </summary>
        public int? TopicMetadataRefreshIntervalMs
        {
            get => _config.TopicMetadataRefreshIntervalMs;
            set
            {
                _config.TopicMetadataRefreshIntervalMs = value;
                _consumerConfig.TopicMetadataRefreshIntervalMs = value;
                _producerConfig.TopicMetadataRefreshIntervalMs = value;
                _adminClientConfig.TopicMetadataRefreshIntervalMs = value;
            }
        }

        /// <summary>
        /// Metadata cache max age. Defaults to topic.metadata.refresh.interval.ms * 3 default:
        /// 900000 importance: low
        /// </summary>
        public int? MetadataMaxAgeMs
        {
            get => _config.MetadataMaxAgeMs;
            set
            {
                _config.MetadataMaxAgeMs = value;
                _consumerConfig.MetadataMaxAgeMs = value;
                _producerConfig.MetadataMaxAgeMs = value;
                _adminClientConfig.MetadataMaxAgeMs = value;
            }
        }

        /// <summary>
        /// When a topic loses its leader a new metadata request will be enqueued with this
        /// initial interval, exponentially increasing until the topic metadata has been
        /// refreshed. This is used to recover quickly from transitioning leader brokers.
        /// default: 250 importance: low
        /// </summary>
        public int? TopicMetadataRefreshFastIntervalMs
        {
            get => _config.TopicMetadataRefreshFastIntervalMs;
            set
            {
                _config.TopicMetadataRefreshFastIntervalMs = value;
                _consumerConfig.TopicMetadataRefreshFastIntervalMs = value;
                _producerConfig.TopicMetadataRefreshFastIntervalMs = value;
                _adminClientConfig.TopicMetadataRefreshFastIntervalMs = value;
            }
        }

        /// <summary>
        /// Sparse metadata requests (consumes less network bandwidth) default: true importance:
        /// low
        /// </summary>
        public bool? TopicMetadataRefreshSparse
        {
            get => _config.TopicMetadataRefreshSparse;
            set
            {
                _config.TopicMetadataRefreshSparse = value;
                _consumerConfig.TopicMetadataRefreshSparse = value;
                _producerConfig.TopicMetadataRefreshSparse = value;
                _adminClientConfig.TopicMetadataRefreshSparse = value;
            }
        }

        /// <summary>
        /// Topic blacklist, a comma-separated list of regular expressions for matching topic
        /// names that should be ignored in broker metadata information as if the topics
        /// did not exist. default: '' importance: low
        /// </summary>
        public string TopicBlacklist
        {
            get => _config.TopicBlacklist;
            set
            {
                _config.TopicBlacklist = value;
                _consumerConfig.TopicBlacklist = value;
                _producerConfig.TopicBlacklist = value;
                _adminClientConfig.TopicBlacklist = value;
            }
        }

        /// <summary>
        /// A comma-separated list of debug contexts to enable. Detailed Producer debugging:
        /// broker,topic,msg. Consumer: consumer,cgrp,topic,fetch default: '' importance:
        /// medium
        /// </summary>
        public string Debug
        {
            get => _config.Debug;
            set
            {
                _config.Debug = value;
                _consumerConfig.Debug = value;
                _producerConfig.Debug = value;
                _adminClientConfig.Debug = value;
            }
        }

        /// <summary>
        /// Default timeout for network requests. Producer: ProduceRequests will use the
        /// lesser value of `socket.timeout.ms` and remaining `message.timeout.ms` for the
        /// first message in the batch. Consumer: FetchRequests will use `fetch.wait.max.ms`
        /// + `socket.timeout.ms`. Admin: Admin requests will use `socket.timeout.ms` or
        /// explicitly set `rd_kafka_AdminOptions_set_operation_timeout()` value. default:
        /// 60000 importance: low
        /// </summary>
        public int? SocketTimeoutMs
        {
            get => _config.SocketTimeoutMs;
            set
            {
                _config.SocketTimeoutMs = value;
                _consumerConfig.SocketTimeoutMs = value;
                _producerConfig.SocketTimeoutMs = value;
                _adminClientConfig.SocketTimeoutMs = value;
            }
        }

        /// <summary>
        /// Broker socket send buffer size. System default is used if 0. default: 0 importance:
        /// low
        /// </summary>
        public int? SocketSendBufferBytes
        {
            get => _config.SocketSendBufferBytes;
            set
            {
                _config.SocketSendBufferBytes = value;
                _consumerConfig.SocketSendBufferBytes = value;
                _producerConfig.SocketSendBufferBytes = value;
                _adminClientConfig.SocketSendBufferBytes = value;
            }
        }

        /// <summary>
        /// Broker socket receive buffer size. System default is used if 0. default: 0 importance:
        /// low
        /// </summary>
        public int? SocketReceiveBufferBytes
        {
            get => _config.SocketReceiveBufferBytes;
            set
            {
                _config.SocketReceiveBufferBytes = value;
                _consumerConfig.SocketReceiveBufferBytes = value;
                _producerConfig.SocketReceiveBufferBytes = value;
                _adminClientConfig.SocketReceiveBufferBytes = value;
            }
        }

        /// <summary>
        /// Enable TCP keep-alives (SO_KEEPALIVE) on broker sockets default: false importance:
        /// low
        /// </summary>
        public bool? SocketKeepaliveEnable
        {
            get => _config.SocketKeepaliveEnable;
            set
            {
                _config.SocketKeepaliveEnable = value;
                _consumerConfig.SocketKeepaliveEnable = value;
                _producerConfig.SocketKeepaliveEnable = value;
                _adminClientConfig.SocketKeepaliveEnable = value;
            }
        }

        /// <summary>
        /// Disable the Nagle algorithm (TCP_NODELAY) on broker sockets. default: false importance:
        /// low
        /// </summary>
        public bool? SocketNagleDisable
        {
            get => _config.SocketNagleDisable;
            set
            {
                _config.SocketNagleDisable = value;
                _consumerConfig.SocketNagleDisable = value;
                _producerConfig.SocketNagleDisable = value;
                _adminClientConfig.SocketNagleDisable = value;
            }
        }

        /// <summary>
        /// Disconnect from broker when this number of send failures (e.g., timed out requests)
        /// is reached. Disable with 0. WARNING: It is highly recommended to leave this setting
        /// at its default value of 1 to avoid the client and broker to become desynchronized
        /// in case of request timeouts. NOTE: The connection is automatically re-established.
        /// default: 1 importance: low
        /// </summary>
        public int? SocketMaxFails
        {
            get => _config.SocketMaxFails;
            set
            {
                _config.SocketMaxFails = value;
                _consumerConfig.SocketMaxFails = value;
                _producerConfig.SocketMaxFails = value;
                _adminClientConfig.SocketMaxFails = value;
            }
        }

        /// <summary>
        /// How long to cache the broker address resolving results (milliseconds). default:
        /// 1000 importance: low
        /// </summary>
        public int? BrokerAddressTtl
        {
            get => _config.BrokerAddressTtl;
            set
            {
                _config.BrokerAddressTtl = value;
                _consumerConfig.BrokerAddressTtl = value;
                _producerConfig.BrokerAddressTtl = value;
                _adminClientConfig.BrokerAddressTtl = value;
            }
        }

        /// <summary>
        /// Allowed broker IP address families: any, v4, v6 default: any importance: low
        /// </summary>
        public BrokerAddressFamily? BrokerAddressFamily
        {
            get => _config.BrokerAddressFamily;
            set
            {
                _config.BrokerAddressFamily = value;
                _consumerConfig.BrokerAddressFamily = value;
                _producerConfig.BrokerAddressFamily = value;
                _adminClientConfig.BrokerAddressFamily = value;
            }
        }

        /// <summary>
        /// The initial time to wait before reconnecting to a broker after the connection
        /// has been closed. The time is increased exponentially until `reconnect.backoff.max.ms`
        /// is reached. -25% to +50% jitter is applied to each reconnect backoff. A value
        /// of 0 disables the backoff and reconnects immediately. default: 100 importance:
        /// medium
        /// </summary>
        public int? ReconnectBackoffMs
        {
            get => _config.ReconnectBackoffMs;
            set
            {
                _config.ReconnectBackoffMs = value;
                _consumerConfig.ReconnectBackoffMs = value;
                _producerConfig.ReconnectBackoffMs = value;
                _adminClientConfig.ReconnectBackoffMs = value;
            }
        }

        /// <summary>
        /// The maximum time to wait before reconnecting to a broker after the connection
        /// has been closed. default: 10000 importance: medium
        /// </summary>
        public int? ReconnectBackoffMaxMs
        {
            get => _config.ReconnectBackoffMaxMs;
            set
            {
                _config.ReconnectBackoffMaxMs = value;
                _consumerConfig.ReconnectBackoffMaxMs = value;
                _producerConfig.ReconnectBackoffMaxMs = value;
                _adminClientConfig.ReconnectBackoffMaxMs = value;
            }
        }

        /// <summary>
        /// librdkafka statistics emit interval. The granularity is 1000ms.
        /// A value of 0 disables statistics. default: 0 importance: high
        /// </summary>
        public int? StatisticsIntervalMs
        {
            get => _config.StatisticsIntervalMs;
            set
            {
                _config.StatisticsIntervalMs = value;
                _consumerConfig.StatisticsIntervalMs = value;
                _producerConfig.StatisticsIntervalMs = value;
                _adminClientConfig.StatisticsIntervalMs = value;
            }
        }

        /// <summary>
        /// Disable spontaneous log_cb from internal librdkafka threads, instead enqueue
        /// log messages on queue set with `rd_kafka_set_log_queue()` and serve log callbacks
        /// or events through the standard poll APIs. **NOTE**: Log messages will linger
        /// in a temporary queue until the log queue has been set. default: false importance:
        /// low
        /// </summary>
        public bool? LogQueue
        {
            get => _config.LogQueue;
            set
            {
                _config.LogQueue = value;
                _consumerConfig.LogQueue = value;
                _producerConfig.LogQueue = value;
                _adminClientConfig.LogQueue = value;
            }
        }

        /// <summary>
        /// List of plugin libraries to load (; separated). The library search path is platform
        /// dependent (see dlopen(3) for Unix and LoadLibrary() for Windows). If no filename
        /// extension is specified the platform-specific extension (such as .dll or .so)
        /// will be appended automatically. default: '' importance: low
        /// </summary>
        public string PluginLibraryPaths
        {
            get => _config.PluginLibraryPaths;
            set
            {
                _config.PluginLibraryPaths = value;
                _consumerConfig.PluginLibraryPaths = value;
                _producerConfig.PluginLibraryPaths = value;
                _adminClientConfig.PluginLibraryPaths = value;
            }
        }

        /// <summary>
        /// A rack identifier for this client. This can be any string value which indicates
        /// where this client is physically located. It corresponds with the broker config
        /// `broker.rack`. default: '' importance: low
        /// </summary>
        public string ClientRack
        {
            get => _config.ClientRack;
            set
            {
                _config.ClientRack = value;
                _consumerConfig.ClientRack = value;
                _producerConfig.ClientRack = value;
                _adminClientConfig.ClientRack = value;
            }
        }

        #endregion

        #region ConsumerConfig

        /// <summary>
        /// Add keyvalue configuration for consumer
        /// [WARNING] : Maybe will change
        /// </summary>
        /// <param name="key">New key</param>
        /// <param name="value">New value</param>
        public void AddConsumerConfig(string key, string value) => _internalConsumerConfig.AddOrUpdate(key, value);

        /// <summary>
        /// Controls how to read messages written transactionally: `read_committed` - only
        /// return transactional messages which have been committed. `read_uncommitted` -
        /// return all messages, even transactional messages which have been aborted. default:
        /// read_committed importance: high
        /// </summary>
        public IsolationLevel? IsolationLevel
        {
            get { return _consumerConfig.IsolationLevel; }
            set
            {
                if (Guarantee != ProcessingGuarantee.EXACTLY_ONCE)
                    _consumerConfig.IsolationLevel = value;
                else if (!changeGuarantee)
                    throw new StreamsException($"You can't update IsolationLevel because your processing guarantee is exactly-once");
            }
        }

        /// <summary>
        /// How long to postpone the next fetch request for a topic+partition in case of
        /// a fetch error. default: 500 importance: medium
        /// </summary>
        public int? FetchErrorBackoffMs { get { return _consumerConfig.FetchErrorBackoffMs; } set { _consumerConfig.FetchErrorBackoffMs = value; } }

        /// <summary>
        /// Minimum number of bytes the broker responds with. If fetch.wait.max.ms expires
        /// the accumulated data will be sent to the client regardless of this setting. default:
        /// 1 importance: low
        /// </summary>
        public int? FetchMinBytes { get { return _consumerConfig.FetchMinBytes; } set { _consumerConfig.FetchMinBytes = value; } }

        /// <summary>
        /// Maximum amount of data the broker shall return for a Fetch request. Messages
        /// are fetched in batches by the consumer and if the first message batch in the
        /// first non-empty partition of the Fetch request is larger than this value, then
        /// the message batch will still be returned to ensure the consumer can make progress.
        /// The maximum message batch size accepted by the broker is defined via `message.max.bytes`
        /// (broker config) or `max.message.bytes` (broker topic config). `fetch.max.bytes`
        /// is automatically adjusted upwards to be at least `message.max.bytes` (consumer
        /// config). default: 52428800 importance: medium
        /// </summary>
        public int? FetchMaxBytes { get { return _consumerConfig.FetchMaxBytes; } set { _consumerConfig.FetchMaxBytes = value; } }

        /// <summary>
        /// Initial maximum number of bytes per topic+partition to request when fetching
        /// messages from the broker. If the client encounters a message larger than this
        /// value it will gradually try to increase it until the entire message can be fetched.
        /// default: 1048576 importance: medium
        /// </summary>
        public int? MaxPartitionFetchBytes { get { return _consumerConfig.MaxPartitionFetchBytes; } set { _consumerConfig.MaxPartitionFetchBytes = value; } }

        /// <summary>
        /// Maximum time the broker may wait to fill the response with fetch.min.bytes. default:
        /// 100 importance: low
        /// </summary>
        public int? FetchWaitMaxMs { get { return _consumerConfig.FetchWaitMaxMs; } set { _consumerConfig.FetchWaitMaxMs = value; } }

        /// <summary>
        /// Maximum number of kilobytes per topic+partition in the local consumer queue.
        /// This value may be overshot by fetch.message.max.bytes. This property has higher
        /// priority than queued.min.messages. default: 1048576 importance: medium
        /// </summary>
        public int? QueuedMaxMessagesKbytes { get { return _consumerConfig.QueuedMaxMessagesKbytes; } set { _consumerConfig.QueuedMaxMessagesKbytes = value; } }

        /// <summary>
        /// Minimum number of messages per topic+partition librdkafka tries to maintain in
        /// the local consumer queue. default: 100000 importance: medium
        /// </summary>
        public int? QueuedMinMessages { get { return _consumerConfig.QueuedMinMessages; } set { _consumerConfig.QueuedMinMessages = value; } }

        /// <summary>
        /// Automatically store offset of last message provided to application. The offset
        /// store is an in-memory store of the next offset to (auto-)commit for each partition.
        /// default: true importance: high
        /// </summary>
        public bool? EnableAutoOffsetStore { get { return _consumerConfig.EnableAutoOffsetStore; } set { _consumerConfig.EnableAutoOffsetStore = value; } }

        /// <summary>
        /// Automatically and periodically commit offsets in the background. Note: setting
        /// this to false does not prevent the consumer from fetching previously committed
        /// start offsets. To circumvent this behaviour set specific start offsets per partition
        /// in the call to assign(). default: true importance: high
        /// </summary>
        public bool? EnableAutoCommit { get { return _consumerConfig.EnableAutoCommit; } private set { _consumerConfig.EnableAutoCommit = value; } }

        /// <summary>
        /// Maximum allowed time between calls to consume messages (e.g., rd_kafka_consumer_poll())
        /// for high-level consumers. If this interval is exceeded the consumer is considered
        /// failed and the group will rebalance in order to reassign the partitions to another
        /// consumer group member. Warning: Offset commits may be not possible at this point.
        /// Note: It is recommended to set `enable.auto.offset.store=false` for long-time
        /// processing applications and then explicitly store offsets (using offsets_store())
        /// *after* message processing, to make sure offsets are not auto-committed prior
        /// to processing has finished. The interval is checked two times per second. See
        /// KIP-62 for more information. default: 300000 importance: high
        /// </summary>
        public int? MaxPollIntervalMs { get { return _consumerConfig.MaxPollIntervalMs; } set { _consumerConfig.MaxPollIntervalMs = value; } }

        /// <summary>
        /// How often to query for the current client group coordinator. If the currently
        /// assigned coordinator is down the configured query interval will be divided by
        /// ten to more quickly recover in case of coordinator reassignment. default: 600000
        /// importance: low
        /// </summary>
        public int? CoordinatorQueryIntervalMs { get { return _consumerConfig.CoordinatorQueryIntervalMs; } set { _consumerConfig.CoordinatorQueryIntervalMs = value; } }

        /// <summary>
        /// Group protocol type default: consumer importance: low
        /// </summary>
        public string GroupProtocolType { get { return _consumerConfig.GroupProtocolType; } set { _consumerConfig.GroupProtocolType = value; } }

        /// <summary>
        /// Group session keepalive heartbeat interval. default: 3000 importance: low
        /// </summary>
        public int? HeartbeatIntervalMs { get { return _consumerConfig.HeartbeatIntervalMs; } set { _consumerConfig.HeartbeatIntervalMs = value; } }

        /// <summary>
        /// Client group session and failure detection timeout. The consumer sends periodic
        /// heartbeats (heartbeat.interval.ms) to indicate its liveness to the broker. If
        /// no hearts are received by the broker for a group member within the session timeout,
        /// the broker will remove the consumer from the group and trigger a rebalance. The
        /// allowed range is configured with the **broker** configuration properties `group.min.session.timeout.ms`
        /// and `group.max.session.timeout.ms`. Also see `max.poll.interval.ms`. default:
        /// 10000 importance: high
        /// </summary>
        public int? SessionTimeoutMs { get { return _consumerConfig.SessionTimeoutMs; } set { _consumerConfig.SessionTimeoutMs = value; } }

        /// <summary>
        /// Name of partition assignment strategy to use when elected group leader assigns
        /// partitions to group members. default: range,roundrobin importance: medium
        /// </summary>
        public PartitionAssignmentStrategy? PartitionAssignmentStrategy { get { return _consumerConfig.PartitionAssignmentStrategy; } private set { _consumerConfig.PartitionAssignmentStrategy = value; } }

        /// <summary>
        /// Action to take when there is no initial offset in offset store or the desired
        /// offset is out of range: 'smallest','earliest' - automatically reset the offset
        /// to the smallest offset, 'largest','latest' - automatically reset the offset to
        /// the largest offset, 'error' - trigger an error which is retrieved by consuming
        /// messages and checking 'message->err'. default: largest importance: high
        /// </summary>
        public AutoOffsetReset? AutoOffsetReset { get { return _consumerConfig.AutoOffsetReset; } set { _consumerConfig.AutoOffsetReset = value; } }

        /// <summary>
        /// A comma separated list of fields that may be optionally set in Confluent.Kafka.ConsumeResult`2
        /// objects returned by the Confluent.Kafka.Consumer`2.Consume(System.TimeSpan) method.
        /// Disabling fields that you do not require will improve throughput and reduce memory
        /// consumption. Allowed values: headers, timestamp, topic, all, none default: all
        /// importance: low
        /// </summary>
        public string ConsumeResultFields { set { _consumerConfig.ConsumeResultFields = value; } }

        /// <summary>
        /// Emit RD_KAFKA_RESP_ERR__PARTITION_EOF event whenever the consumer reaches the
        /// end of a partition. default: false importance: low
        /// </summary>
        public bool? EnablePartitionEof { get { return _consumerConfig.EnablePartitionEof; } set { _consumerConfig.EnablePartitionEof = value; } }

        /// <summary>
        /// Verify CRC32 of consumed messages, ensuring no on-the-wire or on-disk corruption
        /// to the messages occurred. This check comes at slightly increased CPU usage. default:
        /// false importance: medium
        /// </summary>
        public bool? CheckCrcs { get { return _consumerConfig.CheckCrcs; } set { _consumerConfig.CheckCrcs = value; } }

        #endregion

        #region ProducerConfig

        /// <summary>
        /// Add keyvalue configuration for producer
        /// [WARNING] : Maybe will change
        /// </summary>
        /// <param name="key">New key</param>
        /// <param name="value">New value</param>
        public void AddProducerConfig(string key, string value) => _internalProducerConfig.AddOrUpdate(key, value);

        /// <summary>
        /// The threshold of outstanding not yet transmitted broker requests needed to backpressure
        /// the producer's message accumulator. If the number of not yet transmitted requests
        /// equals or exceeds this number, produce request creation that would have otherwise
        /// been triggered (for example, in accordance with linger.ms) will be delayed. A
        /// lower number yields larger and more effective batches. A higher value can improve
        /// latency when using compression on slow machines. default: 1 importance: low
        /// </summary>     
        public int? QueueBufferingBackpressureThreshold { get { return _producerConfig.QueueBufferingBackpressureThreshold; } set { _producerConfig.QueueBufferingBackpressureThreshold = value; } }

        /// <summary>
        /// The backoff time in milliseconds before retrying a protocol request. default:
        /// 100 importance: medium
        /// </summary>
        public int? RetryBackoffMs { get { return _producerConfig.RetryBackoffMs; } set { _producerConfig.RetryBackoffMs = value; } }

        /// <summary>
        /// How many times to retry sending a failing Message. **Note:** retrying may cause
        /// reordering unless `enable.idempotence` is set to true. default: 2 importance:
        /// high
        /// </summary>
        public int? MessageSendMaxRetries { get { return _producerConfig.MessageSendMaxRetries; } set { _producerConfig.MessageSendMaxRetries = value; } }

        /// <summary>
        /// Delay in milliseconds to wait for messages in the producer queue to accumulate
        /// before constructing message batches (MessageSets) to transmit to brokers. A higher
        /// value allows larger and more effective (less overhead, improved compression)
        /// batches of messages to accumulate at the expense of increased message delivery
        /// latency. default: 0.5 importance: high
        /// </summary>
        public double? LingerMs { get { return _producerConfig.LingerMs; } set { _producerConfig.LingerMs = value; } }

        /// <summary>
        /// Maximum total message size sum allowed on the producer queue. This queue is shared
        /// by all topics and partitions. This property has higher priority than queue.buffering.max.messages.
        /// default: 1048576 importance: high
        /// </summary>
        public int? QueueBufferingMaxKbytes { get { return _producerConfig.QueueBufferingMaxKbytes; } set { _producerConfig.QueueBufferingMaxKbytes = value; } }

        /// <summary>
        /// Maximum number of messages allowed on the producer queue. This queue is shared
        /// by all topics and partitions. default: 100000 importance: high
        /// </summary>
        public int? QueueBufferingMaxMessages { get { return _producerConfig.QueueBufferingMaxMessages; } set { _producerConfig.QueueBufferingMaxMessages = value; } }

        /// <summary>
        /// **EXPERIMENTAL**: subject to change or removal. When set to `true`, any error
        /// that could result in a gap in the produced message series when a batch of messages
        /// fails, will raise a fatal error (ERR__GAPLESS_GUARANTEE) and stop the producer.
        /// Messages failing due to `message.timeout.ms` are not covered by this guarantee.
        /// Requires `enable.idempotence=true`. default: false importance: low
        /// </summary>
        public bool? EnableGaplessGuarantee { get { return _producerConfig.EnableGaplessGuarantee; } set { _producerConfig.EnableGaplessGuarantee = value; } }

        /// <summary>
        /// When set to `true`, the producer will ensure that messages are successfully produced
        /// exactly once and in the original produce order. The following configuration properties
        /// are adjusted automatically (if not modified by the user) when idempotence is
        /// enabled: `max.in.flight.requests.per.connection=5` (must be less than or equal
        /// to 5), `retries=INT32_MAX` (must be greater than 0), `acks=all`, `queuing.strategy=fifo`.
        /// Producer instantation will fail if user-supplied configuration is incompatible.
        /// default: false importance: high
        /// </summary>
        public bool? EnableIdempotence
        {
            get { return _producerConfig.EnableIdempotence; }
            set
            {
                if (Guarantee != ProcessingGuarantee.EXACTLY_ONCE)
                    _producerConfig.EnableIdempotence = value;
                else if (!changeGuarantee)
                    throw new StreamsException($"You can't update EnableIdempotence because your processing guarantee is exactly-once");
            }
        }

        /// <summary>
        /// Enables the transactional producer. The transactional.id is used to identify
        /// the same transactional producer instance across process restarts. It allows the
        /// producer to guarantee that transactions corresponding to earlier instances of
        /// the same producer have been finalized prior to starting any new transactions,
        /// and that any zombie instances are fenced off. If no transactional.id is provided,
        /// then the producer is limited to idempotent delivery (if enable.idempotence is
        /// set). Requires broker version >= 0.11.0. default: '' importance: high
        /// </summary>
        public string TransactionalId { get { return _producerConfig.TransactionalId; } set { _producerConfig.TransactionalId = value; } }

        /// <summary>
        /// compression codec to use for compressing message sets. This is the default value
        /// for all topics, may be overridden by the topic configuration property `compression.codec`.
        /// default: none importance: medium
        /// </summary>
        public CompressionType? CompressionType { get { return _producerConfig.CompressionType; } set { _producerConfig.CompressionType = value; } }

        ///  <summary>
        ///  Compression level parameter for algorithm selected by configuration property
        ///  `compression.codec`. Higher values will result in better compression at the cost
        ///  of more CPU usage. Usable range is algorithm-dependent: [0-9] for gzip; [0-12]
        ///  for lz4; only 0 for snappy; -1 = codec-dependent default compression level. default:
        ///  -1 importance: medium
        ///  </summary>
        public int? CompressionLevel { get { return _producerConfig.CompressionLevel; } set { _producerConfig.CompressionLevel = value; } }

        /// <summary>
        /// Partitioner: `random` - random distribution, `consistent` - CRC32 hash of key
        /// (Empty and NULL keys are mapped to single partition), `consistent_random` - CRC32
        /// hash of key (Empty and NULL keys are randomly partitioned), `murmur2` - Java
        /// Producer compatible Murmur2 hash of key (NULL keys are mapped to single partition),
        /// `murmur2_random` - Java Producer compatible Murmur2 hash of key (NULL keys are
        /// randomly partitioned. This is functionally equivalent to the default partitioner
        /// in the Java Producer.). default: consistent_random importance: high
        /// </summary>
        public Partitioner? Partitioner { get { return _producerConfig.Partitioner; } set { _producerConfig.Partitioner = value; } }

        /// <summary>
        /// Local message timeout. This value is only enforced locally and limits the time
        /// a produced message waits for successful delivery. A time of 0 is infinite. This
        /// is the maximum time librdkafka may use to deliver a message (including retries).
        /// Delivery error occurs when either the retry count or the message timeout are
        /// exceeded. The message timeout is automatically adjusted to `transaction.timeout.ms`
        /// if `transactional.id` is configured. default: 300000 importance: high
        /// </summary>
        public int? MessageTimeoutMs { get { return _producerConfig.MessageTimeoutMs; } set { _producerConfig.MessageTimeoutMs = value; } }

        /// <summary>
        /// The ack timeout of the producer request in milliseconds. This value is only enforced
        /// by the broker and relies on `request.required.acks` being != 0. default: 5000
        /// importance: medium
        /// </summary>
        public int? RequestTimeoutMs { get { return _producerConfig.RequestTimeoutMs; } set { _producerConfig.RequestTimeoutMs = value; } }

        /// <summary>
        /// A comma separated list of fields that may be optionally set in delivery reports.
        /// Disabling delivery report fields that you do not require will improve maximum
        /// throughput and reduce memory usage. Allowed values: key, value, timestamp, headers,
        /// all, none. default: all importance: low
        /// </summary>
        public string DeliveryReportFields { get { return _producerConfig.DeliveryReportFields; } set { _producerConfig.DeliveryReportFields = value; } }

        /// <summary>
        /// Specifies whether to enable notification of delivery reports. Typically you should
        /// set this parameter to true. Set it to false for "fire and forget" semantics and
        /// a small boost in performance. default: true importance: low
        /// </summary>
        public bool? EnableDeliveryReports { get { return _producerConfig.EnableDeliveryReports; } set { _producerConfig.EnableDeliveryReports = value; } }

        /// <summary>
        /// Specifies whether or not the producer should start a background poll thread to
        /// receive delivery reports and event notifications. Generally, this should be set
        /// to true. If set to false, you will need to call the Poll function manually. default:
        /// true importance: low
        /// </summary>
        public bool? EnableBackgroundPoll { get { return _producerConfig.EnableBackgroundPoll; } set { _producerConfig.EnableBackgroundPoll = value; } }

        /// <summary>
        /// The maximum amount of time in milliseconds that the transaction coordinator will
        /// wait for a transaction status update from the producer before proactively aborting
        /// the ongoing transaction. If this value is larger than the `transaction.max.timeout.ms`
        /// setting in the broker, the init_transactions() call will fail with ERR_INVALID_TRANSACTION_TIMEOUT.
        /// The transaction timeout automatically adjusts `message.timeout.ms` and `socket.timeout.ms`,
        /// unless explicitly configured in which case they must not exceed the transaction
        /// timeout (`socket.timeout.ms` must be at least 100ms lower than `transaction.timeout.ms`).
        /// default: 60000 importance: medium
        /// </summary>
        public int? TransactionTimeoutMs { get { return _producerConfig.TransactionTimeoutMs; } set { _producerConfig.TransactionTimeoutMs = value; } }

        /// <summary>
        /// Maximum number of messages batched in one MessageSet. The total MessageSet size
        /// is also limited by message.max.bytes. default: 10000 importance: medium
        /// </summary>
        public int? BatchNumMessages { get { return _producerConfig.BatchNumMessages; } set { _producerConfig.BatchNumMessages = value; } }

        #endregion

        #region AdminConfig

        /// <summary>
        /// Add keyvalue configuration for admin client
        /// [WARNING] : Maybe will change
        /// </summary>
        /// <param name="key">New key</param>
        /// <param name="value">New value</param>
        public void AddAdminConfig(string key, string value) => _internalAdminConfig.AddOrUpdate(key, value);

        #endregion

        #region Ctor

        /// <summary>
        /// Constructor empty
        /// </summary>
        public StreamConfig()
            : this(null)
        {

        }

        /// <summary>
        /// Constructor with a dictionary of properties.
        /// This is stream properties. 
        /// <para>
        /// If you want to set specific properties for consumer, producer, admin client or global.
        /// Please use <see cref="IStreamConfig.AddConsumerConfig(string, string)"/>, <see cref="IStreamConfig.AddProducerConfig(string, string)"/>, 
        /// <see cref="IStreamConfig.AddAdminConfig(string, string)"/> or <see cref="IStreamConfig.AddConfig(string, string)"/>. WARNING : MAYBE WILL CHANGE
        /// </para>
        /// </summary>
        /// <param name="properties">Dictionary of stream properties</param>
        public StreamConfig(IDictionary<string, dynamic> properties)
        {
            ClientId = null;
            NumStreamThreads = 1;
            DefaultKeySerDes = new ByteArraySerDes();
            DefaultValueSerDes = new ByteArraySerDes();
            DefaultTimestampExtractor = new FailOnInvalidTimestamp();
            Guarantee = ProcessingGuarantee.AT_LEAST_ONCE;
            TransactionTimeout = TimeSpan.FromSeconds(10);
            PollMs = 100;
            MaxPollRecords = 500;
            MaxTaskIdleMs = 0;
            BufferedRecordsPerPartition = 1000;

            if (properties != null)
            {
                foreach (var k in properties)
                    DictionaryExtensions.AddOrUpdate(this, k.Key, k.Value);
            }

            _consumerConfig = new ConsumerConfig();
            _producerConfig = new ProducerConfig();
            _adminClientConfig = new AdminClientConfig();
            _config = new ClientConfig();

            MaxPollIntervalMs = 300000;
            EnableAutoCommit = false;
            PartitionAssignmentStrategy = Confluent.Kafka.PartitionAssignmentStrategy.Range;
        }

        /// <summary>
        /// Constructor for Serialization
        /// </summary>
        /// <param name="info"></param>
        /// <param name="context"></param>
        protected StreamConfig(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }

        #endregion

        #region IStreamConfig Impl

        /// <summary>
        /// The number of threads to execute stream processing.
        /// </summary>
        public int NumStreamThreads
        {
            get => this[numStreamThreadsCst];
            set
            {
                if (value >= 1)
                    this.AddOrUpdate(numStreamThreadsCst, value);
                else
                    throw new StreamConfigException($"NumStreamThreads value must always be greather than 1");
            }
        }

        /// <summary>
        /// An ID prefix string used for the client IDs of internal consumer, producer and restore-consumer, with pattern '&lt;client.id&gt;-StreamThread-&lt;threadSequenceNumber&gt;-&lt;consumer|producer|restore-consumer&gt;'.
        /// </summary>
        public string ClientId
        {
            get => this[clientIdCst];
            set => this.AddOrUpdate(clientIdCst, value);
        }

        /// <summary>
        /// An identifier for the stream processing application. Must be unique within the Kafka cluster. It is used as 1) the default client-id prefix, 2) the group-id for membership management, 3) the changelog topic prefix.
        /// </summary>
        public string ApplicationId
        {
            get => this[applicatonIdCst];
            set => this.AddOrUpdate(applicatonIdCst, value);
        }

        /// <summary>
        /// Default key serdes for consumer and materialized state store
        /// </summary>
        public ISerDes DefaultKeySerDes
        {
            get => this[defaultKeySerDesCst];
            set => this.AddOrUpdate(defaultKeySerDesCst, value);
        }

        /// <summary>
        /// Default value serdes for consumer and materialized state store
        /// </summary>
        public ISerDes DefaultValueSerDes
        {
            get => this[defaultValueSerDesCst];
            set => this.AddOrUpdate(defaultValueSerDesCst, value);
        }

        /// <summary>
        /// Default timestamp extractor class that implements the <see cref="ITimestampExtractor"/> interface.
        /// </summary>
        public ITimestampExtractor DefaultTimestampExtractor
        {
            get => this[defaultTimestampExtractorCst];
            set => this.AddOrUpdate(defaultTimestampExtractorCst, value);
        }

        /// <summary>
        /// Initial list of brokers as a CSV list of broker host or host:port. default:
        /// '' importance: high
        /// </summary>
        public string BootstrapServers
        {
            get => _config.BootstrapServers;
            set
            {
                _config.BootstrapServers = value;
                _consumerConfig.BootstrapServers = value;
                _producerConfig.BootstrapServers = value;
                _adminClientConfig.BootstrapServers = value;
            }
        }

        /// <summary>
        /// The processing guarantee that should be used. Possible values are <see cref="ProcessingGuarantee.AT_LEAST_ONCE"/> (default) and <see cref="ProcessingGuarantee.EXACTLY_ONCE"/>
        /// Note that exactly-once processing requires a cluster of at least three brokers by default what is the recommended setting for production; for development you can change this, by adjusting broker setting
        /// <code>transaction.state.log.replication.factor</code> and <code>transaction.state.log.min.isr</code>.
        /// </summary>
        public ProcessingGuarantee Guarantee
        {
            get => this[processingGuaranteeCst];
            set
            {
                changeGuarantee = true;
                if (value == ProcessingGuarantee.EXACTLY_ONCE)
                {
                    IsolationLevel = Confluent.Kafka.IsolationLevel.ReadCommitted;
                    EnableIdempotence = true;
                    MaxInFlight = 5;
                    MessageSendMaxRetries = Int32.MaxValue;
                    CommitIntervalMs = EOS_DEFAULT_COMMIT_INTERVAL_MS;
                }
                else if (value == ProcessingGuarantee.AT_LEAST_ONCE)
                    CommitIntervalMs = DEFAULT_COMMIT_INTERVAL_MS;

                this.AddOrUpdate(processingGuaranteeCst, value);
                changeGuarantee = false;
            }
        }

        /// <summary>
        /// Timeout used for transaction related operations. (Default : 10 seconds).
        /// </summary>
        public TimeSpan TransactionTimeout
        {
            get => this[transactionTimeoutCst];
            set => this.AddOrUpdate(transactionTimeoutCst, value);
        }

        /// <summary>
        /// The frequency with which to save the position of the processor. (Note, if <see cref="IStreamConfig.Guarantee"/> is set to <see cref="ProcessingGuarantee.EXACTLY_ONCE"/>, the default value is <code>" + EOS_DEFAULT_COMMIT_INTERVAL_MS + "</code>,"
        /// otherwise the default value is <code>" + DEFAULT_COMMIT_INTERVAL_MS + "</code>.
        /// </summary>
        public long CommitIntervalMs
        {
            get => this[commitIntervalMsCst];
            set => this.AddOrUpdate(commitIntervalMsCst, value);
        }

        /// <summary>
        /// The amount of time in milliseconds to block waiting for input. (Default : 100)
        /// </summary>
        public long PollMs
        {
            get => this[pollMsCst];
            set => this.AddOrUpdate(pollMsCst, value);
        }

        /// <summary>
        /// The maximum number of records returned in a single call to poll(). (Default: 500)
        /// </summary>
        public long MaxPollRecords
        {
            get => this[maxPollRecordsCst];
            set => this.AddOrUpdate(maxPollRecordsCst, value);
        }

        /// <summary>
        /// Maximum amount of time a stream task will stay idle when not all of its partition buffers contain records, to avoid potential out-of-order record processing across multiple input streams. (Default: 0)
        /// </summary>
        public long MaxTaskIdleMs
        {
            get => this[maxTaskIdleCst];
            set => this.AddOrUpdate(maxTaskIdleCst, value);
        }

        /// <summary>
        /// Maximum number of records to buffer per partition. (Default: 1000)
        /// </summary>
        public long BufferedRecordsPerPartition
        {
            get => this[bufferedRecordsPerPartitionCst];
            set => this.AddOrUpdate(bufferedRecordsPerPartitionCst, value);
        }

        /// <summary>
        /// Get the configs to the <see cref="IProducer{TKey, TValue}"/>
        /// </summary>
        /// <returns>Return <see cref="ProducerConfig"/> for building <see cref="IProducer{TKey, TValue}"/> instance.</returns>
        public ProducerConfig ToProducerConfig() => ToProducerConfig(ClientId);

        /// <summary>
        /// Get the configs to the <see cref="IProducer{TKey, TValue}"/> with specific <paramref name="clientId"/>
        /// </summary>
        /// <param name="clientId">Producer client ID</param>
        /// <returns>Return <see cref="ProducerConfig"/> for building <see cref="IProducer{TKey, TValue}"/> instance.</returns>
        public ProducerConfig ToProducerConfig(string clientId)
        {
            var c = _producerConfig.Union(_internalProducerConfig).ToDictionary();
            ProducerConfig config = new ProducerConfig(c);
            config.ClientId = clientId;
            return config;
        }

        /// <summary>
        /// Get the configs to the <see cref="IConsumer{TKey, TValue}"/>
        /// </summary>
        /// <returns>Return <see cref="ConsumerConfig"/> for building <see cref="IConsumer{TKey, TValue}"/> instance.</returns>
        public ConsumerConfig ToConsumerConfig() => ToConsumerConfig(ClientId);

        /// <summary>
        /// Get the configs to the <see cref="IConsumer{TumerKey, TValue}"/> with specific <paramref name="clientId"/>
        /// </summary>
        /// <param name="clientId">Consumer client ID</param>
        /// <returns>Return <see cref="ConsumerConfig"/> for building <see cref="IConsumer{TKey, TValue}"/> instance.</returns>
        public ConsumerConfig ToConsumerConfig(string clientId)
        {
            if (!ContainsKey(applicatonIdCst))
                throw new StreamConfigException($"Key {applicatonIdCst} was not found. She is mandatory for getting consumer config");

            var c = _consumerConfig.Union(_internalConsumerConfig).ToDictionary();
            var config = new ConsumerConfig(c);
            config.GroupId = ApplicationId;
            config.ClientId = clientId;
            return config;
        }

        /// <summary>
        /// Get the configs to the restore <see cref="IConsumer{TKey, TValue}"/> with specific <paramref name="clientId"/>.
        /// Restore consumer is using to restore persistent state store.
        /// </summary>
        /// <param name="clientId">Consumer client ID</param>
        /// <returns>Return <see cref="ConsumerConfig"/> for building <see cref="IConsumer{TKey, TValue}"/> instance.</returns>
        public ConsumerConfig ToGlobalConsumerConfig(string clientId)
        {
            var config = ToConsumerConfig(clientId);
            config.GroupId = $"{ApplicationId}-Global-{Guid.NewGuid()}";
            config.AutoOffsetReset = Confluent.Kafka.AutoOffsetReset.Earliest;
            return config;
        }

        /// <summary>
        /// Get the configs to the <see cref="IAdminClient"/> with specific <paramref name="clientId"/>
        /// </summary>
        /// <param name="clientId">Admin client ID</param>
        /// <returns>Return <see cref="AdminClientConfig"/> for building <see cref="IAdminClient"/> instance.</returns>
        public AdminClientConfig ToAdminConfig(string clientId)
        {
            var c = _adminClientConfig.Union(_internalAdminConfig).ToDictionary();
            var config = new AdminClientConfig(c);
            config.ClientId = clientId;
            return config;
        }

        /// <summary>
        /// Return new instance of <see cref="StreamConfig"/>.
        /// </summary>
        /// <returns>Return new instance of <see cref="StreamConfig"/>.</returns>
        public IStreamConfig Clone()
        {
            var config = new StreamConfig(this);
            config._internalConsumerConfig = new Dictionary<string, string>(_internalConsumerConfig);
            config._internalAdminConfig = new Dictionary<string, string>(_internalAdminConfig);
            config._internalProducerConfig = new Dictionary<string, string>(_internalProducerConfig);

            config._consumerConfig = new ConsumerConfig(_consumerConfig);
            config._producerConfig = new ProducerConfig(_producerConfig);
            config._adminClientConfig = new AdminClientConfig(_adminClientConfig);
            config._config = new ClientConfig(_config);

            return config;
        }

        #endregion

        #region ISchemaRegistryConfig Impl

        /// <summary>
        /// Specifies the timeout for requests to Confluent Schema Registry. default: 30000
        /// </summary>
        public int? SchemaRegistryRequestTimeoutMs
        {
            get => ContainsKey(schemaRegistryRequestTimeoutMsCst) ? this[schemaRegistryRequestTimeoutMsCst] : null;
            set => this.AddOrUpdate(schemaRegistryRequestTimeoutMsCst, value);
        }

        /// <summary>
        /// Specifies the maximum number of schemas CachedSchemaRegistryClient should cache locally. default: 1000
        /// </summary>
        public int? SchemaRegistryMaxCachedSchemas
        {
            get => ContainsKey(schemaRegistryMaxCachedSchemasCst) ? this[schemaRegistryMaxCachedSchemasCst] : null;
            set => this.AddOrUpdate(schemaRegistryMaxCachedSchemasCst, value);
        }

        /// <summary>
        /// A comma-separated list of URLs for schema registry instances that are used register or lookup schemas.
        /// </summary>
        public string SchemaRegistryUrl
        {
            get => ContainsKey(schemaRegistryUrlCst) ? this[schemaRegistryUrlCst] : null;
            set => this.AddOrUpdate(schemaRegistryUrlCst, value);
        }

        /// <summary>
        /// Specifies whether or not the Avro serializer should attempt to auto-register unrecognized schemas with Confluent Schema Registry. default: true
        /// </summary>
        public bool? AutoRegisterSchemas
        {
            get => ContainsKey(schemaRegistryAutoRegisterCst) ? this[schemaRegistryAutoRegisterCst] : null;
            set => this.AddOrUpdate(schemaRegistryAutoRegisterCst, value);
        }

        #endregion

        #region ToString()

        /// <summary>
        /// Override ToString method
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            string replaceValue = "********";
            // todo
            List<string> keysToNotDisplay = new List<string> {
                "sasl.password"
            };

            List<string> keysAlreadyPrint = new List<string>();

            StringBuilder sb = new StringBuilder();

            // stream config property
            sb.AppendLine();
            sb.AppendLine("\tStream property:");
            foreach (var kp in this)
                sb.AppendLine($"\t\t{kp.Key}: \t{kp.Value}");

            // client config property
            sb.AppendLine("\tClient property:");
            var configs = _config.Intercept((kp) => keysToNotDisplay.Contains(kp.Key), replaceValue);
            foreach (var kp in configs)
            {
                sb.AppendLine($"\t\t{kp.Key}: \t{kp.Value}");
                keysAlreadyPrint.Add(kp.Key);
            }

            // consumer config property
            sb.AppendLine("\tConsumer property:");
            var consumersConfig = _consumerConfig
                                    .Union(_internalConsumerConfig)
                                    .Except((i) => keysAlreadyPrint.Contains(i.Key))
                                    .Intercept((kp) => keysToNotDisplay.Contains(kp.Key), replaceValue);
            if (consumersConfig.Any())
            {
                foreach (var kp in consumersConfig)
                    sb.AppendLine($"\t\t{kp.Key}: \t{kp.Value}");
            }
            else
                sb.AppendLine($"\t\tNone");

            // producer config property
            sb.AppendLine("\tProducer property:");
            var producersConfig = _producerConfig
                                    .Union(_internalProducerConfig)
                                    .Except((i) => keysAlreadyPrint.Contains(i.Key))
                                    .Intercept((kp) => keysToNotDisplay.Contains(kp.Key), replaceValue);
            if (producersConfig.Any())
            {
                foreach (var kp in producersConfig)
                    sb.AppendLine($"\t\t{kp.Key}: \t{kp.Value}");
            }
            else
                sb.AppendLine($"\t\tNone");


            // admin config property
            sb.AppendLine("\tAdmin client property:");
            var adminsConfig = _adminClientConfig
                                    .Union(_internalAdminConfig)
                                    .Except((i) => keysAlreadyPrint.Contains(i.Key))
                                    .Intercept((kp) => keysToNotDisplay.Contains(kp.Key), replaceValue);
            if (adminsConfig.Any())
            {
                foreach (var kp in adminsConfig)
                    sb.AppendLine($"\t\t{kp.Key}: \t{kp.Value}");
            }
            else
                sb.AppendLine($"\t\tNone");

            return sb.ToString();
        }

        #endregion
    }

    /// <summary>
    /// Implementation of <see cref="IStreamConfig"/>. Contains all configuration for your stream.
    /// By default, Kafka Streams does not allow users to overwrite the following properties (Streams setting shown in parentheses)
    ///    - EnableAutoCommit = (false) - Streams client will always disable/turn off auto committing
    /// If <see cref="IStreamConfig.Guarantee"/> is set to <see cref="ProcessingGuarantee.EXACTLY_ONCE"/>, Kafka Streams does not allow users to overwrite the following properties (Streams setting shown in parentheses):
    ///    - <see cref="IsolationLevel"/> (<see cref="IsolationLevel.ReadCommitted"/>) - Consumers will always read committed data only
    ///    - <see cref="StreamConfig.EnableIdempotence"/> (true) - Producer will always have idempotency enabled
    ///    - <see cref="StreamConfig.MaxInFlight"/> (5) - Producer will always have one in-flight request per connection
    /// If <see cref="IStreamConfig.Guarantee"/> is set to <see cref="ProcessingGuarantee.EXACTLY_ONCE"/>, Kafka Streams initialize the following properties :
    ///    - <see cref="StreamConfig.CommitIntervalMs"/> (<see cref="StreamConfig.EOS_DEFAULT_COMMIT_INTERVAL_MS"/>
    /// <exemple>
    /// <code>
    /// var config = new StreamConfig&lt;StringSerDes, StringSerDes&gt;();
    /// config.ApplicationId = "test-app";
    /// config.BootstrapServers = "localhost:9092";
    /// </code>
    /// </exemple>
    /// </summary>
    /// <typeparam name="KS">Default key serdes</typeparam>
    /// <typeparam name="VS">Default value serdes</typeparam>
    public class StreamConfig<KS, VS> : StreamConfig
        where KS : ISerDes, new()
        where VS : ISerDes, new()
    {
        /// <summary>
        /// Constructor empty
        /// </summary>
        public StreamConfig()
            : this(null)
        {

        }

        /// <summary>
        /// Constructor with properties. 
        /// See <see cref="StreamConfig"/>
        /// <para>
        /// <see cref="IStreamConfig.DefaultKeySerDes"/> is set to <code>new KS();</code>
        /// <see cref="IStreamConfig.DefaultValueSerDes"/> is set to <code>new VS();</code>
        /// </para>
        /// </summary>
        /// <param name="properties">Dictionary of stream properties</param>
        public StreamConfig(IDictionary<string, dynamic> properties)
            : base(properties)
        {
            DefaultKeySerDes = new KS();
            DefaultValueSerDes = new VS();
        }
    }
}