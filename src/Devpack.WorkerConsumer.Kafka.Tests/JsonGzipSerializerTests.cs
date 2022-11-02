using Confluent.Kafka;
using Devpack.WorkerConsumer.Kafka.Serializers;
using FluentAssertions;
using System;
using Xunit;

namespace Devpack.WorkerConsumer.Kafka.Tests
{
    public class JsonGzipSerializerTests
    {
        // Esse teste valida inevitavelmente o método de serialização e deserialização 
        [Fact(DisplayName = "Deve serializar um objeto usando compressão Gzip quando um objeto válido for passado.")]
        [Trait("Category", "Services")]
        public void SerializeAndDeserialize()
        {
            var data = Guid.NewGuid();

            var serializedData = new JsonGzipSerializer<Guid>().Serialize(data, new SerializationContext());
            var deserializedData = new JsonGzipDeserializer<Guid>().Deserialize(serializedData, false, new SerializationContext());

            deserializedData.Should().Be(data);
        }
    }
}