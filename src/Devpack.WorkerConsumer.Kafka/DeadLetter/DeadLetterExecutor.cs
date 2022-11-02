using Confluent.Kafka;
using Devpack.Extensions.Types;
using Devpack.WorkerConsumer.Kafka.Subscriber;
using MediatR;
using Microsoft.Extensions.Logging;
using System.Text.Json;

namespace Devpack.WorkerConsumer.Kafka.DeadLetter
{
    public class DeadLetterExecutor<TMessage> : IDeadLetterExecutor<TMessage> where TMessage : IRequest
    {
        private readonly string _deadLetterTopicName;
        private readonly IProducer<string, TMessage> _deadLetterProducer;
        private readonly ILogger<WorkerSubscriber> _logger;

        public DeadLetterExecutor(string topicName,
                                  ProducerBuilder<string, TMessage> producerBuilder,
                                  ILogger<WorkerSubscriber> logger)
        {
            _deadLetterTopicName = topicName;
            _deadLetterProducer = producerBuilder.Build();
            _logger = logger;
        }

        public Task ProcessDeadLetter(Exception ex, ConsumeResult<string, TMessage> consumeResult, CancellationToken cancellation)
        {
            var logId = LogInvalidMessage(ex);
            return SendMessageToDeadLetter(consumeResult, logId, cancellation);
        }

        private Guid LogInvalidMessage(Exception exception)
        {
            _logger.LogError("Failed trying to process a message from the queue.", exception.ToDictionary());
            return Guid.NewGuid();
        }

        private Task SendMessageToDeadLetter(ConsumeResult<string, TMessage> consumeResult, Guid logId, CancellationToken cancellation)
        {
            var message = new Message<string, TMessage>
            {
                Key = consumeResult.Message.Key,
                Value = consumeResult.Message.Value
            };

            message.Headers ??= new Headers();
            message.Headers.Add("LogId", JsonSerializer.SerializeToUtf8Bytes(logId));

            return _deadLetterProducer.ProduceAsync(_deadLetterTopicName, message, cancellation);
        }
    }
}