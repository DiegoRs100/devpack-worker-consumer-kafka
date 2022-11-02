using Confluent.Kafka;
using Devpack.WorkerConsumer.Kafka.DeadLetter;
using Devpack.WorkerConsumer.Kafka.Tests.Common;
using MediatR;
using Microsoft.Extensions.DependencyInjection;
using Moq;
using System;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Devpack.WorkerConsumer.Kafka.Tests
{
    public class WorkerExecutorTests
    {
        private readonly Mock<IDeadLetterExecutor<EventTest>> _deadLetterMock;
        private readonly Mock<IMediator> _mediatorMock;

        private readonly IServiceProvider _serviceProvider;

        public WorkerExecutorTests()
        {
            _deadLetterMock = new Mock<IDeadLetterExecutor<EventTest>>();
            _mediatorMock = new Mock<IMediator>();

            var serviceCollection = new ServiceCollection()
                .AddSingleton(_mediatorMock.Object);

            _serviceProvider = serviceCollection.BuildServiceProvider();
        }

        [Fact(DisplayName = "Deve executar o serviço sem chamar a DeadLetter quando o método for chamado com sucesso.")]
        [Trait("Category", "Helper")]
        public async Task AddWorker_WhenSuccess()
        {
            var consumeResult = new ConsumeResult<string, EventTest>()
            {
                Message = new Message<string, EventTest>() { Value = new EventTest() }
            };

            var cancellationToken = new CancellationToken();

            var executor = new WorkerExecutor<EventTest>(_deadLetterMock.Object, _serviceProvider);
            await executor.ExecuteAsync(consumeResult, cancellationToken);

            _mediatorMock.Verify(m => m.Send(consumeResult.Message.Value!, cancellationToken), Times.Once);

            _deadLetterMock.Verify(m => m.ProcessDeadLetter(It.IsAny<Exception>(), 
                It.IsAny<ConsumeResult<string, EventTest>>(), It.IsAny<CancellationToken>()), Times.Never);
        }

        [Fact(DisplayName = "Deve executar o serviço de DeadLetter quando o método lançar uma exception.")]
        [Trait("Category", "Helper")]
        public async Task AddWorker_WhenException()
        {
            var consumeResult = new ConsumeResult<string, EventTest>();
            var cancellationToken = new CancellationToken();

            var executor = new WorkerExecutor<EventTest>(_deadLetterMock.Object, _serviceProvider);
            await executor.ExecuteAsync(consumeResult, cancellationToken);

            _deadLetterMock.Verify(m => m.ProcessDeadLetter(It.IsAny<NullReferenceException>(), consumeResult, cancellationToken), Times.Once);
        }
    }
}