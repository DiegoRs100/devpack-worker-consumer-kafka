using Confluent.Kafka;
using Devpack.WorkerConsumer.Kafka.DeadLetter;
using MediatR;
using Microsoft.Extensions.DependencyInjection;

namespace Devpack.WorkerConsumer.Kafka
{
    public class WorkerExecutor<TMessage> where TMessage : IRequest
    {
        private readonly IDeadLetterExecutor<TMessage> _deadLetterExecutor;
        private readonly IServiceProvider _serviceProvider;

        public WorkerExecutor(IDeadLetterExecutor<TMessage> deadLetterExecutor,
                              IServiceProvider serviceProvider)
        {
            _deadLetterExecutor = deadLetterExecutor;
            _serviceProvider = serviceProvider;
        }

        public async Task ExecuteAsync(ConsumeResult<string, TMessage> consumeResult, CancellationToken cancellation)
        {
            try
            {
                using var scope = _serviceProvider.CreateScope();
                var mediator = scope.ServiceProvider.GetRequiredService<IMediator>();

                await mediator.Send(consumeResult.Message.Value!, cancellation);
            }
            catch (Exception ex)
            {
                await _deadLetterExecutor.ProcessDeadLetter(ex, consumeResult, cancellation);
            }
        }
    }
}