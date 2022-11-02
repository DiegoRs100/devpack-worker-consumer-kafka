using Confluent.Kafka;
using MediatR;

namespace Devpack.WorkerConsumer.Kafka.DeadLetter
{
    public interface IDeadLetterExecutor<TMessage> where TMessage : IRequest
    {
        Task ProcessDeadLetter(Exception ex, ConsumeResult<string, TMessage> consumeResult, CancellationToken cancellation);
    }
}