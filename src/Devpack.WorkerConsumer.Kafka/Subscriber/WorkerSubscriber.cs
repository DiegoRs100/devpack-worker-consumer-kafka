using Confluent.Kafka;
using Devpack.Extensions.Types;
using Devpack.WorkerConsumer.Kafka.DeadLetter;
using MediatR;
using Microsoft.Extensions.Logging;
using System.Diagnostics.CodeAnalysis;

namespace Devpack.WorkerConsumer.Kafka.Subscriber
{
    public class WorkerSubscriber : IWorkerSubscriber
    {
        private readonly IServiceProvider _serviceProvider;
        private readonly ILogger<WorkerSubscriber> _logger;

        public WorkerSubscriber(IServiceProvider serviceProvider, ILogger<WorkerSubscriber> logger)
        {
            _serviceProvider = serviceProvider;
            _logger = logger;
        }

        [ExcludeFromCodeCoverage]
        public Task Subscribe<TMessage>(WorkerOptionsBuilder<TMessage> workerOptions, CancellationToken cancellation)
            where TMessage : IRequest
        {
            _ = Task.Factory.StartNew(async () =>
            {
                var deadLetter = new DeadLetterExecutor<TMessage>(
                    workerOptions.DeadLetterTopicName,
                    workerOptions.DeadLetterProducerBuilder, _logger);

                var consumer = workerOptions.ConsumerBuilder.Build();
                var executor = new WorkerExecutor<TMessage>(deadLetter, _serviceProvider);

                consumer.Subscribe(workerOptions.ConsumerTopicName);

                ConsumeResult<string, TMessage> consumeResult = default!;

                while (!cancellation.IsCancellationRequested)
                {
                    try
                    {
                        consumeResult = consumer.Consume();

                        if (consumeResult.IsPartitionEOF)
                            continue;

                        await executor.ExecuteAsync(consumeResult, cancellation);
                        consumer.Commit();
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError("Failed trying to read a message from the queue.", ex.ToDictionary());
                    }
                }
            }, cancellation, TaskCreationOptions.LongRunning, TaskScheduler.Default);

            return Task.CompletedTask;
        }
    }
}