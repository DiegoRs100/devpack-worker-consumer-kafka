using Devpack.WorkerConsumer.Kafka.Subscriber;
using MediatR;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Devpack.WorkerConsumer.Kafka
{
    public static class DependencyInjection
    {
        public static IServiceCollection AddWorker<TWorker>(this IServiceCollection services) where TWorker : BackgroundService
        {
            services.AddSingleton<IWorkerSubscriber, WorkerSubscriber>();
            services.AddMediatR(typeof(TWorker));
            services.AddHostedService<TWorker>();

            return services;
        }
    }
}