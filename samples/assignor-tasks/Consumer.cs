using System;
using System.Threading;
using Confluent.Kafka;

namespace assignor_tasks
{
    internal class Consumer : IDisposable
    {
        private readonly IConsumer<string, string> consumer;
        private readonly int timeout;
        private readonly CancellationToken token;
        private readonly Thread thread;

        public Consumer(IConsumer<string, string> c1, string topic, int v, CancellationToken token)
        {
            consumer = c1;
            timeout = v;
            this.token = token;

            thread = new Thread(Run);
            consumer.Subscribe(topic);
        }

        private void Run()
        {
            var random = new Random();
            while (!token.IsCancellationRequested)
            {
                consumer.Consume(timeout);
                if (consumer.Assignment.Count > 0 && random.Next(0, 100) >= 99)
                {
                    // P = 1/100
                    consumer.Unassign();
                    var p = random.Next(0, 8);
                    Console.WriteLine($"Manually consumer {consumer.MemberId} assigned to partition {p}");
                    consumer.Assign(new TopicPartition("test", p));
                }
            }
        }

        public void Start()
        {
            thread.Start();
        }

        public void Dispose()
        {
            consumer?.Dispose();
        }
    }
}
