using Confluent.Kafka;
using ConsumerCSW;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Threading;

namespace ConsumerEurope
{
    class Program
    {
        public static void Main(string[] args)
        {
            var conf = new ConsumerConfig
            {
                GroupId = "consumer-group-europe",
                BootstrapServers = "localhost:9092",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            using (var c = new ConsumerBuilder<Ignore, string>(conf).Build())
            {
                var topics = new List<string>();
                topics.Add("csw-topic-portugal");
                topics.Add("csw-topic-espanha");
                c.Subscribe(topics);
                CancellationTokenSource cts = new CancellationTokenSource();
                Console.CancelKeyPress += (_, e) => {
                    e.Cancel = true; // prevent the process from terminating.
                    cts.Cancel();
                };

                try
                {
                    while (true)
                    {
                        try
                        {
                            var cr = c.Consume(cts.Token);
                            var order = JsonConvert.DeserializeObject<OrderMessage>(cr.Value);

                            Console.WriteLine($"Consumed message '{order}' at: '{cr.TopicPartitionOffset}'.");
                            foreach (PropertyDescriptor descriptor in TypeDescriptor.GetProperties(order))
                            {
                                string name = descriptor.Name;
                                object value = descriptor.GetValue(order);
                                Console.WriteLine("{0}={1}", name, value);
                            }



                        }
                        catch (ConsumeException e)
                        {
                            Console.WriteLine($"Error occured: {e.Error.Reason}");
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    c.Close();
                }
            }
        }
    }
}
