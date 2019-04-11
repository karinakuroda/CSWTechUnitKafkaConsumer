using Confluent.Kafka;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Threading;

namespace ConsumerCSW
{
    class Program
    {
        public static void Main(string[] args)
        {
            Console.WriteLine("Type the option:");
            Console.WriteLine("1 - Run GroupId consumer-group-all");
            Console.WriteLine("2 - Run GroupId consumer-group-europe");
            Console.WriteLine("3 - Run GroupId consumer-group-europe-test");
            var opt = Console.ReadLine();
            
            var conf = GetConfigConsumerGroup(opt);
            
            using (var c = new ConsumerBuilder<Ignore, string>(conf).Build())
            {
                c.Subscribe(GetTopicsConsumerGroup(opt));

                CancellationTokenSource cts = new CancellationTokenSource();
                Console.CancelKeyPress += (_, e) =>
                {
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

        private static ConsumerConfig GetConfigConsumerGroup(string opt)
        {
            string groupId = string.Empty;

            switch (opt)
            {
                case "1":
                    groupId = "consumer-group-all";
                    break;
                case "2":
                    groupId = "consumer-group-europe";
                    break;
                case "3":
                    groupId = "consumer-group-europe-test";
                    break;
                default:
                    break;
            }

            var conf = new ConsumerConfig
            {
                GroupId = groupId,
                BootstrapServers = "localhost:9092",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            return conf;
        }

        private static List<string> GetTopicsConsumerGroup(string opt)
        {
            var topics = new List<string>();

            if (opt == "1")
            {
                topics.Add("csw-topic");
                topics.Add("csw-topic-portugal");
                topics.Add("csw-topic-espanha");
            }
            else
            {
                topics.Add("csw-topic-portugal");
                topics.Add("csw-topic-espanha");
            }

            return topics;
        }
    }
}
