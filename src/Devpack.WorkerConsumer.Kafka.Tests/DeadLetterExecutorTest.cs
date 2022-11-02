using Confluent.Kafka;
using Devpack.Extensions.Types;
using Devpack.Testability.Extensions;
using Devpack.WorkerConsumer.Kafka.DeadLetter;
using Devpack.WorkerConsumer.Kafka.Serializers;
using Devpack.WorkerConsumer.Kafka.Subscriber;
using Devpack.WorkerConsumer.Kafka.Tests.Common;
using FluentAssertions;
using Microsoft.Extensions.Logging;
using Moq;
using System;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Devpack.WorkerConsumer.Kafka.Tests
{
    public class DeadLetterExecutorTest
    {
        [Fact(DisplayName = "Deve instanciar corretamente as propriedades quando o objeto for instanciado.")]
        [Trait("Category", "Services")]
        public void Constructor()
        {
            var topicName = Guid.NewGuid().ToString();
            var logger = new Logger<WorkerSubscriber>(new LoggerFactory());

            var producerBuilder = new ProducerBuilder<string, EventTest>(new ProducerConfig())
                .SetValueSerializer(new JsonGzipSerializer<EventTest>());

            var executor = new DeadLetterExecutor<EventTest>(topicName, producerBuilder, logger);

            (executor.GetFieldValue("_deadLetterTopicName") as string).Should().Be(topicName);
            (executor.GetFieldValue("_deadLetterProducer") as IProducer<string, EventTest>).Should().NotBeNull();
            (executor.GetFieldValue("_logger") as ILogger<WorkerSubscriber>).Should().Be(logger);
        }

        [Fact(DisplayName = "Deve instanciar corretamente as propriedades quando o objeto for instanciado.")]
        [Trait("Category", "Services")]
        public async Task ProcessDeadLetter()
        {
            var topicName = Guid.NewGuid().ToString();
            var exception = new InvalidCastException();
            var logger = new Mock<ILogger<WorkerSubscriber>>();
            var cancellationToken = new CancellationToken();

            var consumerResult = new ConsumeResult<string, EventTest>()
            {
                Message = new Message<string, EventTest>()
                {
                    Key = Guid.NewGuid().ToString(),
                    Value = new EventTest()
                }
            };

            var producerBuilder = new ProducerBuilder<string, EventTest>(new ProducerConfig()
            {
                BootstrapServers = "localhost:8080",
                MessageTimeoutMs = 100
            }).SetValueSerializer(new JsonGzipSerializer<EventTest>());

            var executor = new DeadLetterExecutor<EventTest>(topicName, producerBuilder, logger.Object);

            await executor.Invoking(e => e.ProcessDeadLetter(exception, consumerResult, cancellationToken))
                .Should().ThrowAsync<ProduceException<string, EventTest>>();

            logger.VerifyLogHasCalled(LogLevel.Error, "Failed trying to process a message from the queue.", Times.Once);
        }
    }
}