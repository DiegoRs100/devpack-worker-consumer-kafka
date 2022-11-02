using MediatR;

namespace Devpack.WorkerConsumer.Kafka.Subscriber
{
    public interface IWorkerSubscriber
    {
        Task Subscribe<TMessage>(WorkerOptionsBuilder<TMessage> workerOptions, CancellationToken cancellation) where TMessage : IRequest;
    }
}