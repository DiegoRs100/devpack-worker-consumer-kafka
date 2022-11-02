using Confluent.Kafka;
using System.IO.Compression;
using System.Text.Json;

namespace Devpack.WorkerConsumer.Kafka.Serializers
{
    public class JsonGzipSerializer<TObject> : ISerializer<TObject>
    {
        public byte[] Serialize(TObject data, SerializationContext context)
        {
            var bytes = JsonSerializer.SerializeToUtf8Bytes(data);

            using var memoryStream = new MemoryStream();
            using var zipStream = new GZipStream(memoryStream, CompressionMode.Compress, true);

            zipStream.Write(bytes, 0, bytes.Length);
            zipStream.Close();

            return memoryStream.ToArray();
        }
    }

    public class JsonGzipDeserializer<TObject> : IDeserializer<TObject>
    {
        public TObject Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
        {
            using var memoryStream = new MemoryStream(data.ToArray());
            using var zipStream = new GZipStream(memoryStream, CompressionMode.Decompress, true);

            return JsonSerializer.Deserialize<TObject>(zipStream)!;
        }
    }
}