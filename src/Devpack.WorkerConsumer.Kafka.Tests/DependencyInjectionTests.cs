using Devpack.WorkerConsumer.Kafka.Subscriber;
using Devpack.WorkerConsumer.Kafka.Tests.Common;
using FluentAssertions;
using MediatR;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Devpack.WorkerConsumer.Kafka.Tests
{
    public class DependencyInjectionTests
    {
        [Fact(DisplayName = "Deve injetar corretamente as instâncias quando o método for chamado.")]
        [Trait("Category", "Extensions")]
        public void AddWorker()
        {
            var serviceCollection = new ServiceCollection();
            serviceCollection.AddWorker<BackgroundServiceTest>();
            serviceCollection.AddSingleton<ILogger<WorkerSubscriber>>(new Logger<WorkerSubscriber>(new LoggerFactory()));
  
            var serviceProvider = serviceCollection.BuildServiceProvider();

            var workerSubscriber = serviceProvider.GetService<IWorkerSubscriber>();
            var mediator = serviceProvider.GetService<IMediator>();

            workerSubscriber.Should().NotBeNull();
            mediator.Should().NotBeNull();
        }
    }
}