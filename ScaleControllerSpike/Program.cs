using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.Configuration;

namespace ScaleControllerSpike
{
    class Program
    {
        private static IConfiguration configration;
        static void Main(string[] args)
        {
            var builder = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json");
            configration = builder.Build();
            TestBehavior().GetAwaiter().GetResult();
        }

        private static async Task TestBehavior()
        {
            string topicName = "singletopic";  //reproduce2, singletopic
            int partitionToWrite = 0;
            // Produce one. 
            var producer = GetProducer();
            var consumer = GetConsumer();
            consumer.Subscribe(new List<string>{topicName});
            var adminClient = GetAdminClient();
            var topicPartition = new TopicPartition(topicName, new Partition(partitionToWrite));
            await producer.ProduceAsync(topicPartition, new Message<string, string>
            {
                Key = "Key",
                Value = "Message - One"
            });
            Console.WriteLine("** Produce - One");
            await PrintMetrics(adminClient, consumer, topicName);
            await producer.ProduceAsync(topicPartition, new Message<string, string>
            {
                Key = "Key",
                Value = "Message - Two"
            });
            Console.WriteLine("** Produce - Two");
            await PrintMetrics(adminClient, consumer, topicName);

            var result = consumer.Consume(TimeSpan.FromSeconds(3));
            Console.WriteLine($"*** Consume key: {result.Message.Key} value: {result.Message.Value}");
            consumer.Commit(result);

            await producer.ProduceAsync(topicPartition, new Message<string, string>
            {
                Key = "Key",
                Value = "Message - Three"
            });
            await PrintMetrics(adminClient, consumer, topicName);
            result = consumer.Consume(TimeSpan.FromSeconds(3));
            Console.WriteLine($"*** Consume key: {result.Message.Key} value: {result.Message.Value}");
            consumer.Commit(result);
            await PrintMetrics(adminClient, consumer, topicName);

            result = consumer.Consume(TimeSpan.FromSeconds(3));
            Console.WriteLine($"*** Consume key: {result.Message.Key} value: {result.Message.Value}");
            consumer.Commit(result);
            await PrintMetrics(adminClient, consumer, topicName);

            producer.Dispose();
            consumer.Dispose();
            adminClient.Dispose();
        }

        public static async Task PrintMetrics(IAdminClient adminClient, IConsumer<string, string> consumer,
            string topicName)
        {
            var topicPartitions = LoadTopicPartitions(adminClient, topicName);
            var metrics = await  GetMetricsAsync(topicPartitions, consumer, topicName);
            Console.WriteLine(metrics.ToString());
        }

        public static async Task<KafkaTriggerMetrics> GetMetricsAsync(List<TopicPartition> topicPartitions, IConsumer<string, string> consumer, string topicName)
        {
            var operationTimeout = TimeSpan.FromSeconds(5);
            var allPartitions = topicPartitions;
            if (allPartitions == null)
            {
                return new KafkaTriggerMetrics(0L, 0);
            }

            var ownedCommittedOffset = consumer.Committed(allPartitions, operationTimeout);
            var partitionWithHighestLag = Partition.Any;
            long highestPartitionLag = 0L;
            long totalLag = 0L;
            foreach (var topicPartition in allPartitions)
            {
                // This call goes to the server always which probably yields the most accurate results. It blocks.
                // Alternatively we could use consumer.GetWatermarkOffsets() that returns cached values, without blocking.
                var watermark = consumer.QueryWatermarkOffsets(topicPartition, operationTimeout);
                Console.WriteLine($"Partition: {topicPartition.Partition} High Watermark: {watermark.High} : Low Watermark: {watermark.Low}");
                var commited = ownedCommittedOffset.FirstOrDefault(x => x.Partition == topicPartition.Partition);
                if (commited != null)
                {
                    Console.WriteLine($"Commited : {commited.Offset.Value}");
                    long diff;
                    if (commited.Offset == Offset.Unset)
                    {
                        diff = watermark.High.Value;
                    }
                    else
                    {
                        diff = watermark.High.Value - commited.Offset.Value;
                    }

                    totalLag += diff;

                    if (diff > highestPartitionLag)
                    {
                        highestPartitionLag = diff;
                        partitionWithHighestLag = topicPartition.Partition;
                    }
                }
            }

            if (partitionWithHighestLag != Partition.Any)
            {
                Console.WriteLine($"Total lag in '{topicName}' is {totalLag}, highest partition lag found in {partitionWithHighestLag.Value} with value of {highestPartitionLag}");
            }

            return new KafkaTriggerMetrics(totalLag, allPartitions.Count);
        }

        protected static List<TopicPartition> LoadTopicPartitions(IAdminClient adminClient, string topicName)
        {
            try
            {
                var timeout = TimeSpan.FromSeconds(5);
                var metadata = adminClient.GetMetadata(topicName, timeout);
                if (metadata.Topics == null || metadata.Topics.Count == 0)
                {
                    Console.WriteLine($"Could not load metadata information about topic '{topicName}'");
                    return new List<TopicPartition>();
                }

                var topicMetadata = metadata.Topics[0];
                var partitions = topicMetadata.Partitions;
                if (partitions == null || partitions.Count == 0)
                {
                    Console.WriteLine($"Could not load partition information about topic '{topicName}'");
                    return new List<TopicPartition>();
                }

                return partitions.Select(x => new TopicPartition(topicMetadata.Topic, new Partition(x.PartitionId))).ToList();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Failed to load partition information from topic '{topicName}' {ex.Message}: {ex.StackTrace}");
            }

            return new List<TopicPartition>();
        }

        private static IProducer<string, string> GetProducer()
        {
            var config = new ProducerConfig
            {
                BootstrapServers = configration["BootstrapServers"],
                SaslMechanism = SaslMechanism.Plain,
                SecurityProtocol = SecurityProtocol.SaslSsl,
                //Authentication
                SaslUsername = configration["SaslUsername"],
                SaslPassword = configration["SaslPassword"]
                //Debug = "all"
            };

            return new ProducerBuilder<string, string>(config)
                .SetLogHandler((k, v) => Console.WriteLine($"MyLogger {k.ToString()}:{v?.Message}")).Build();
        }

        private static IConsumer<string, string> GetConsumer()
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = configration["BootstrapServers"],
                SaslMechanism = SaslMechanism.Plain,
                SecurityProtocol = SecurityProtocol.SaslSsl,
                GroupId = "$Default",
                AutoOffsetReset = AutoOffsetReset.Earliest,
                //Authentication
                SaslUsername = configration["SaslUsername"],
                SaslPassword = configration["SaslPassword"]
            };

            return new ConsumerBuilder<string, string>(config)
                .SetLogHandler((k, v) => Console.WriteLine($"MyLogger {k.ToString()}:{v?.Message}")).Build();
        }

        private static IAdminClient GetAdminClient()
        {
            var config = new AdminClientConfig
            {
                BootstrapServers = configration["BootstrapServers"],
                SaslMechanism = SaslMechanism.Plain,
                SecurityProtocol = SecurityProtocol.SaslSsl,
                //Authentication
                SaslUsername = configration["SaslUsername"],
                SaslPassword = configration["SaslPassword"]
            };
            return new AdminClientBuilder(config)
                .SetLogHandler((k, v) => Console.WriteLine($"MyLogger {k.ToString()}:{v?.Message}")).Build();

        }
    }

    public class KafkaTriggerMetrics {
    public long TotalLag { get; set; }

    /// <summary>
    /// The number of partitions.
    /// </summary>
    public long PartitionCount { get; set; }

    public KafkaTriggerMetrics(long totalLag, int partitionCount)
    {
        TotalLag = totalLag;
        PartitionCount = partitionCount;
    }

    public string ToString()
    {
        return $"TotalLag: {TotalLag} PartitionCount: {PartitionCount}";
    }
}
}