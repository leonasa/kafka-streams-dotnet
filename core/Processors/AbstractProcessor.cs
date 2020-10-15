using Confluent.Kafka;
using log4net;
using Streamiz.Kafka.Net.Crosscutting;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Processors.Internal;
using Streamiz.Kafka.Net.SerDes;
using System.Collections.Generic;

namespace Streamiz.Kafka.Net.Processors
{
    internal abstract class AbstractProcessor<K, V> : IProcessor<K, V>
    {
        protected ILog log = null;
        protected string logPrefix = "";

        public ProcessorContext Context { get; protected set; }

        public string Name { get; set; }
        public IList<string> StateStores { get; protected set; }

        public ISerDes<K> KeySerDes => Key as ISerDes<K>;

        public ISerDes<V> ValueSerDes => Value as ISerDes<V>;

        public ISerDes Key { get; internal set; } = null;

        public ISerDes Value { get; internal set; } = null;

        public IList<IProcessor> Next { get; } = new List<IProcessor>();

        #region Ctor

        protected AbstractProcessor()
            : this(null)
        {

        }

        protected AbstractProcessor(string name)
            : this(name, null, null)
        {
        }

        protected AbstractProcessor(string name, ISerDes<K> keySerdes, ISerDes<V> valueSerdes)
            : this(name, keySerdes, valueSerdes, null)
        {
        }

        protected AbstractProcessor(string name, ISerDes<K> keySerdes, ISerDes<V> valueSerdes, List<string> stateStores)
        {
            Name = name;
            Key = keySerdes;
            Value = valueSerdes;
            StateStores = stateStores != null ? new List<string>(stateStores) : new List<string>();
            log = Logger.GetLogger(GetType());
        }

        #endregion

        public virtual void Close()
        {
            foreach (var n in Next)
            {
                n.Close();
            }
        }

        #region Forward

        public virtual void Forward<K1, V1>(K1 key, V1 value, long ts)
        {
            Context.ChangeTimestamp(ts);
            Forward<K1, V1>(key, value);
        }

        public virtual void Forward<K1, V1>(K1 key, V1 value)
        {
            log.Debug($"{logPrefix}Forward<{typeof(K1).Name},{typeof(V1).Name}> message with key {key} and value {value} to each next processor");
            foreach (var n in Next)
                if (n is IProcessor<K1, V1>)
                    (n as IProcessor<K1, V1>).Process(key, value);
        }

        public virtual void Forward<K1, V1>(K1 key, V1 value, string name)
        {
            foreach (var n in Next)
            {
                if (n is IProcessor<K1, V1> && n.Name.Equals(name))
                {
                    log.Debug($"{logPrefix}Forward<{typeof(K1).Name},{typeof(V1).Name}> message with key {key} and value {value} to processor {name}");
                    (n as IProcessor<K1, V1>).Process(key, value);
                }
            }
        }

        public virtual void Forward(K key, V value)
        {
            log.Debug($"{logPrefix}Forward<{typeof(K).Name},{typeof(V).Name}> message with key {key} and value {value} to each next processor");
            foreach (var n in Next)
            {
                if (n is IProcessor<K, V>)
                    (n as IProcessor<K, V>).Process(key, value);
                else
                    n.Process(key, value);
            }
        }

        public virtual void Forward(K key, V value, string name)
        {
            foreach (var n in Next)
            {
                if (n.Name.Equals(name))
                {
                    log.Debug($"{logPrefix}Forward<{typeof(K).Name},{typeof(V).Name}> message with key {key} and value {value} to processor {name}");
                    if (n is IProcessor<K, V>)
                        (n as IProcessor<K, V>).Process(key, value);
                    else
                        n.Process(key, value);
                }
            }
        }

        #endregion

        public virtual void Init(ProcessorContext context)
        {
            log.Debug($"{logPrefix}Initializing process context");
            Context = context;
            foreach (var n in Next)
            {
                n.Init(context);
                Key?.Initialize(Context.SerDesContext);
                Value?.Initialize(Context.SerDesContext);
            }
            log.Debug($"{logPrefix}Process context initialized");
        }

        protected void LogProcessingKeyValue(K key, V value) => log.Debug($"{logPrefix}Process<{typeof(K).Name},{typeof(V).Name}> message with key {key} and {value} with record metadata [topic:{Context.RecordContext.Topic}|partition:{Context.RecordContext.Partition}|offset:{Context.RecordContext.Offset}]");

        protected SerializationContext GetSerializationContext(bool isKey)
        {
            return new SerializationContext(isKey ? MessageComponentType.Key : MessageComponentType.Value,
                Context?.RecordContext?.Topic,
                Context?.RecordContext?.Headers);
        }

        #region Setter

        internal void SetTaskId(TaskId id)
        {
            logPrefix = $"stream-task[{id.Id}|{id.Partition}]|processor[{Name}]- ";
        }

        public void AddNextProcessor(IProcessor next)
        {
            if (next != null && !Next.Contains(next as IProcessor))
                Next.Add(next);
        }

        #endregion

        #region Process object

        public void Process(object key, object value)
        {
            key = DeserializeKey(key);

            value = DeserializeValue(value);

            if ((key == null || key is K) && (value == null || value is V))
                Process((K)key, (V)value);
        }

        private object DeserializeValue(object value)
        {
            if (value != null && value is byte[] valueBytes)
            {
                if (ValueSerDes != null)
                    value = Value.DeserializeObject(valueBytes, GetSerializationContext(false));
                else
                {
                    log.Error($"{logPrefix}Impossible to receive source data because keySerdes and/or valueSerdes is not set ! KeySerdes : {(KeySerDes != null ? KeySerDes.GetType().Name : "NULL")} | ValueSerdes : {(ValueSerDes != null ? ValueSerDes.GetType().Name : "NULL")}.");
                    throw new StreamsException($"{logPrefix}The value serdes is not compatible to the actual value for this processor. Change the default value serdes in StreamConfig or provide correct Serdes via method parameters(using the DSL)");
                }
            }

            return value;
        }

        private object DeserializeKey(object key)
        {
            if (key != null && key is byte[] keyBytes)
            {
                if (KeySerDes != null)
                    key = Key.DeserializeObject(keyBytes, GetSerializationContext(true));
                else
                {
                    log.Error($"{logPrefix}Impossible to receive source data because keySerdes and/or valueSerdes is not set ! KeySerdes : {(KeySerDes != null ? KeySerDes.GetType().Name : "NULL")} | ValueSerdes : {(ValueSerDes != null ? ValueSerDes.GetType().Name : "NULL")}.");
                    throw new StreamsException($"{logPrefix}The key serdes is not compatible to the actual key for this processor. Change the default key serdes in StreamConfig or provide correct Serdes via method parameters(using the DSL)");
                }
            }

            return key;
        }

        #endregion

        #region Abstract

        public abstract void Process(K key, V value);

        #endregion

        public override bool Equals(object obj)
        {
            return obj is AbstractProcessor<K, V> processor && processor.Name.Equals(Name);
        }

        public override int GetHashCode()
        {
            return Name.GetHashCode();
        }
    }
}
