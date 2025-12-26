using RabbitMQ.Client;
using System.Text;
using System.Text.Json;

namespace Payments.Api.Messaging;

public class RabbitMqPublisher
{
    private readonly ConnectionFactory _factory;
    private const string QueueName = "payments.requested";

    public RabbitMqPublisher()
    {
        _factory = new ConnectionFactory
        {
            HostName = Environment.GetEnvironmentVariable("RABBITMQ_HOST") ?? "rabbitmq",
            UserName = Environment.GetEnvironmentVariable("RABBITMQ_USER") ?? "guest",
            Password = Environment.GetEnvironmentVariable("RABBITMQ_PASS") ?? "guest"
        };
    }

    public async Task PublishAsync(object message)
    {
        // Criar conexão assíncrona
        var connection = await _factory.CreateConnectionAsync();

        await using var conn = connection;
        await using var channel = await conn.CreateChannelAsync();

        // Declarar fila
        await channel.QueueDeclareAsync(
            queue: QueueName,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: null
        );

        var json = JsonSerializer.Serialize(message);
        var bytes = Encoding.UTF8.GetBytes(json);

        var props = new BasicProperties
        {
            Persistent = true
        };

        // Publicar
        await channel.BasicPublishAsync(
            exchange: "",
            routingKey: QueueName,
            mandatory: false,
            basicProperties: props,
            body: bytes
        );

        Console.WriteLine($"[RabbitMQ] Evento publicado: {json}");
    }
}
