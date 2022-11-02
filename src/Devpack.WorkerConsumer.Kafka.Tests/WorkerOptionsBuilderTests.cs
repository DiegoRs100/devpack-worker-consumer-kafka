using Confluent.Kafka;
using Devpack.WorkerConsumer.Kafka.Serializers;
using Devpack.WorkerConsumer.Kafka.Tests.Common;
using FluentAssertions;
using System;
using Xunit;

namespace Devpack.WorkerConsumer.Kafka.Tests
{
    public class WorkerOptionsBuilderTests
    {
        [Fact(DisplayName = "Deve popular as informações obrigatórias quando o objeto for instanciado.")]
        [Trait("Category", "Services")]
        public void Constructor()
        {
            var applicationName = Guid.NewGuid().ToString();
            var bootstrapServer = Guid.NewGuid().ToString();

            var builder = new WorkerOptionsBuilder<EventTest>(applicationName, bootstrapServer);

            builder.ApplicationName.Should().Be(applicationName);
            builder.BootstrapServer.Should().Be(bootstrapServer);
        }

        [Fact(DisplayName = "Deve adicionar a informação do consumer quando o método for chamado.")]
        [Trait("Category", "Services")]
        public void AddConsumer()
        {
            var topicName = Guid.NewGuid().ToString();

            var consumerConfig = new ConsumerConfig()
            {
                EnableAutoCommit = true,
                EnablePartitionEof = false
            };

            var builder = CreateWorkerOptionsBuilder();
            builder.AddConsumer(topicName, consumerConfig);

            builder.ConsumerTopicName.Should().Be(topicName);
            builder.Invoking(b => b.ConsumerBuilder.Build()).Should().Throw<InvalidOperationException>();

            consumerConfig.BootstrapServers.Should().Be(builder.BootstrapServer);
            consumerConfig.EnableAutoCommit.Should().BeFalse();
            consumerConfig.EnablePartitionEof.Should().BeTrue();
            consumerConfig.GroupId.Should().Be(builder.ApplicationName);
        }

        [Fact(DisplayName = "Deve adicionar a informação do consumer quando o método for chamado passando options de deserialização.")]
        [Trait("Category", "Services")]
        public void AddConsumer_WithOptions()
        {
            var topicName = Guid.NewGuid().ToString();

            var consumerConfig = new ConsumerConfig()
            {
                EnableAutoCommit = true,
                EnablePartitionEof = false
            };

            var builder = CreateWorkerOptionsBuilder();
            builder.AddConsumer(topicName, consumerConfig, o => o.SetValueDeserializer(new JsonGzipDeserializer<EventTest>()));

            builder.ConsumerTopicName.Should().Be(topicName);
            builder.Invoking(b => b.ConsumerBuilder.Build()).Should().NotThrow();

            consumerConfig.BootstrapServers.Should().Be(builder.BootstrapServer);
            consumerConfig.EnableAutoCommit.Should().BeFalse();
            consumerConfig.EnablePartitionEof.Should().BeTrue();
            consumerConfig.GroupId.Should().Be(builder.ApplicationName);
        }

        [Fact(DisplayName = "Deve adicionar a informação da DeadLetter quando o método for chamado.")]
        [Trait("Category", "Services")]
        public void AddDeadLetter()
        {
            var topicName = Guid.NewGuid().ToString();
            var producerConfig = new ProducerConfig();

            var builder = CreateWorkerOptionsBuilder();
            builder.AddDeadLetter(topicName, producerConfig);

            builder.DeadLetterTopicName.Should().Be(topicName);
            builder.Invoking(b => b.DeadLetterProducerBuilder.Build()).Should().Throw<ArgumentNullException>();

            producerConfig.BootstrapServers.Should().Be(builder.BootstrapServer);
        }

        [Fact(DisplayName = "Deve adicionar a informação da DeadLetter quando o método for chamado passando options de serialização.")]
        [Trait("Category", "Services")]
        public void AddDeadLetter_WithOptions()
        {
            var topicName = Guid.NewGuid().ToString();
            var producerConfig = new ProducerConfig();

            var builder = CreateWorkerOptionsBuilder();
            builder.AddDeadLetter(topicName, producerConfig, o => o.SetValueSerializer(new JsonGzipSerializer<EventTest>()));

            builder.DeadLetterTopicName.Should().Be(topicName);
            builder.Invoking(b => b.DeadLetterProducerBuilder.Build()).Should().NotThrow();

            producerConfig.BootstrapServers.Should().Be(builder.BootstrapServer);
        }

        private static WorkerOptionsBuilder<EventTest> CreateWorkerOptionsBuilder()
        {
            return new WorkerOptionsBuilder<EventTest>(Guid.NewGuid().ToString(), Guid.NewGuid().ToString());
        }
    }
}