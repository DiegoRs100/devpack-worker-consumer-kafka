# What does it do?

Facilitator that allows the creation of a background kafka-based microservice.

# How to use it?

To use the library, just perform the following configuration:

**Create mediator event class**

This class represents the message that will travel via kafka.

```csharp
    public class ProductCreatedEvent : IRequest
    { 
        public Guid ProductId { get; set; };
    }
```

**Create mediator handler**

Here is where the business rule should be written.

```csharp
    public class ProductCreatedHandler : IRequestHandler<ProductCreatedEvent>
    {
        public Task<Unit> Handle(ProductCreatedEvent request, CancellationToken cancellationToken)
        {
            // Code Here
            return Task.FromResult(Unit.Value);
        }
    }
```

**Create a BackgroundService**

Here, the configuration of the kafka consumer is performed.

```csharp
    public class ProductWorker : BackgroundService
    {
        private readonly IWorkerSubscriber _workerSubscriber;

        public ReplicasetWorker(IWorkerSubscriber workerSubscriber)
        {
            _workerSubscriber = workerSubscriber;
        }

        protected async override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var applicationName = "product-worker";
            var bootstrapServer = "localhost:9094";

            var workerBuilder = new WorkerOptionsBuilder<ProductCreatedEvent>(applicationName, bootstrapServer);

            workerBuilder
                .AddConsumer(
                    "product-created-topic", new ConsumerConfig
                    {
                        AutoOffsetReset = AutoOffsetReset.Earliest
                    }, x => x.SetValueDeserializer(new JsonGzipDeserializer<ProductCreatedEvent>()))
                .AddDeadLetter(
                    "product-created-deadletter-topic", new ProducerConfig()
                    {
                        Acks = Acks.All,
                        EnableIdempotence = true,
                        MaxInFlight = 1,
                        MessageSendMaxRetries = 2
                    }, x => x.SetValueSerializer(new JsonGzipSerializer<ProductCreatedEvent>()));

            await _workerSubscriber.Subscribe(workerBuilder, new CancellationToken());
        }
    }
```

*OBS:* It is possible to overwrite more than one different topic in the same background service or even in different background services.

**In Startup.cs**

```csharp
    public void ConfigureServices(IServiceCollection services)
    {
        ..
        services.AddWorker<ProductWorker>();
    }
```