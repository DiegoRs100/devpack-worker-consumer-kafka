using Confluent.Kafka;
using MediatR;

namespace Devpack.WorkerConsumer.Kafka
{
    public class WorkerOptionsBuilder<TMessage> where TMessage : IRequest
    {
        public string BootstrapServer { get; private set; }
        public string ApplicationName { get; private set; }

        public string ConsumerTopicName { get; private set; } = default!;
        public ConsumerBuilder<string, TMessage> ConsumerBuilder { get; private set; } = default!;

        public string DeadLetterTopicName { get; private set; } = default!;
        public ProducerBuilder<string, TMessage> DeadLetterProducerBuilder { get; private set; } = default!;

        public WorkerOptionsBuilder(string applicationName, string bootstrapServer)
        {
            ApplicationName = applicationName;
            BootstrapServer = bootstrapServer;
        }

        public WorkerOptionsBuilder<TMessage> AddConsumer(string topicName, ConsumerConfig config)
        {
            ConsumerTopicName = topicName;

            config.BootstrapServers = BootstrapServer;
            config.EnableAutoCommit = false;
            config.EnablePartitionEof = true;

            config.GroupId ??= ApplicationName;

            ConsumerBuilder = new ConsumerBuilder<string, TMessage>(config);

            return this;
        }

        public WorkerOptionsBuilder<TMessage> AddConsumer(string topicName, ConsumerConfig config, Action<ConsumerBuilder<string, TMessage>> options)
        {
            var consumerBuilder = AddConsumer(topicName, config).ConsumerBuilder;
            options.Invoke(consumerBuilder);

            return this;
        }

        public WorkerOptionsBuilder<TMessage> AddDeadLetter(string topicName, ProducerConfig config)
        {
            DeadLetterTopicName = topicName;
            config.BootstrapServers = BootstrapServer;

            DeadLetterProducerBuilder = new ProducerBuilder<string, TMessage>(config);

            return this;
        }

        public WorkerOptionsBuilder<TMessage> AddDeadLetter(string topicName, ProducerConfig config, Action<ProducerBuilder<string, TMessage>> options)
        {
            var deadLetterTopicName = AddDeadLetter(topicName, config).DeadLetterProducerBuilder;
            options.Invoke(deadLetterTopicName);

            return this;
        }
    }
}