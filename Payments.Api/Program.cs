using Dapper;
using Npgsql;
using Payments.Api.Messaging;
using Prometheus;
using System.Text.Json;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

// registra publisher corretamente
builder.Services.AddSingleton<RabbitMqPublisher>();

var connectionString = builder.Configuration.GetConnectionString("DefaultConnection");

// ----------------------------------------------------------------------
// 1. Criar banco automaticamente se não existir (gamesdb já deve existir)
// ----------------------------------------------------------------------

async Task EnsureTablesExist()
{
    using var con = new NpgsqlConnection(connectionString);
    await con.OpenAsync();

    // Tabela de eventos
    await con.ExecuteAsync("""
        CREATE TABLE IF NOT EXISTS events (
            id UUID PRIMARY KEY,
            type TEXT NOT NULL,
            data TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL
        );
    """);

    // Tabela de pagamentos
    await con.ExecuteAsync("""
        CREATE TABLE IF NOT EXISTS payments (
            id UUID PRIMARY KEY,
            user_id UUID NOT NULL,
            status TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL
        );
    """);
}

await EnsureTablesExist();

// ----------------------------------------------------------------------
// 2. Construção do app
// ----------------------------------------------------------------------

var app = builder.Build();

app.UseSwagger();
app.UseSwaggerUI();

// ----------------------------------------------------------------------
// 3. Endpoints
// ----------------------------------------------------------------------

app.MapPost("/payments", async (CreatePayment dto, RabbitMqPublisher publisher) =>
{
    using var con = new NpgsqlConnection(connectionString);
    await con.OpenAsync();

    var id = Guid.NewGuid();
    var now = DateTime.UtcNow;

    // Cria pagamento
    await con.ExecuteAsync("""
        INSERT INTO payments (id, user_id, status, created_at)
        VALUES (@id, @user_id, 'pending', @created_at)
    """, new { id, user_id = dto.UserId, created_at = now });

    // Cria evento
    var evt = new { PaymentId = id, dto.UserId, dto.Items };

    await con.ExecuteAsync("""
        INSERT INTO events (id, type, data, created_at)
        VALUES (@id, 'PaymentRequested', @data, @ts)
    """, new
    {
        id = Guid.NewGuid(),
        data = JsonSerializer.Serialize(evt),
        ts = now
    });

    //  PUBLICAR EVENTO
    await publisher.PublishAsync(new
    {
        PaymentId = id,
        UserId = dto.UserId,
        Items = dto.Items,
        Timestamp = now
    });


    return Results.Created($"/payments/{id}", new { id, status = "pending" });
});

app.MapGet("/payments/{id}", async (Guid id) =>
{
    using var con = new NpgsqlConnection(connectionString);
    await con.OpenAsync();

    var p = await con.QuerySingleOrDefaultAsync("""
        SELECT id, user_id, status, created_at
        FROM payments WHERE id = @id
    """, new { id });

    return p is null ? Results.NotFound() : Results.Ok(p);
});

app.MapPut("/payments/{id}/approve", async (Guid id) =>
{
    using var con = new NpgsqlConnection(connectionString);
    await con.OpenAsync();

    var rows = await con.ExecuteAsync("""
        UPDATE payments SET status = 'approved'
        WHERE id = @id
    """, new { id });

    return rows == 0 ? Results.NotFound() : Results.NoContent();
});

app.MapGet("/payments", async () =>
{
    using var con = new NpgsqlConnection(connectionString);
    var list = await con.QueryAsync("SELECT id,user_id,status,created_at FROM payments ORDER BY created_at DESC");
    return Results.Ok(list);
});

app.MapPut("/payments/{id}/reject", async (Guid id) =>
{
    using var con = new NpgsqlConnection(connectionString);
    await con.OpenAsync();

    var rows = await con.ExecuteAsync("""
        UPDATE payments SET status = 'rejected'
        WHERE id = @id
    """, new { id });

    return rows == 0 ? Results.NotFound() : Results.NoContent();
});

app.MapPut("/payments/{id}/cancel", async (Guid id) =>
{
    using var con = new NpgsqlConnection(connectionString);
    await con.OpenAsync();

    var rows = await con.ExecuteAsync("""
        UPDATE payments SET status = 'canceled'
        WHERE id = @id
    """, new { id });

    return rows == 0 ? Results.NotFound() : Results.NoContent();
});

app.MapDelete("/payments/{id}", async (Guid id) =>
{
    using var con = new NpgsqlConnection(connectionString);
    await con.OpenAsync();

    var rows = await con.ExecuteAsync("""
        DELETE FROM payments WHERE id = @id
    """, new { id });

    return rows == 0 ? Results.NotFound() : Results.Ok(new { deleted = true });
});


app.MapGet("/payments/{id}/events", async (Guid id) =>
{
    using var con = new NpgsqlConnection(connectionString);

    var events = await con.QueryAsync("""
        SELECT id, type, data, created_at
        FROM events
        WHERE data LIKE @p
        ORDER BY created_at DESC
    """, new { p = $"%\"PaymentId\":\"{id}\"%" });

    return Results.Ok(events);
});

app.MapGet("/payments/full", async () =>
{
    using var con = new NpgsqlConnection(connectionString);

    var sql = """
        SELECT 
            p.id,
            p.user_id,
            u.name AS user_name,
            u.email,
            p.status,
            p.created_at
        FROM payments p
        LEFT JOIN users u ON p.user_id = u.id
        ORDER BY p.created_at DESC
    """;

    var list = await con.QueryAsync(sql);
    return Results.Ok(list);
});




// endpoint de métricas do Prometheus
app.MapMetrics();


app.Run();

record CreatePayment(Guid UserId, string[] Items);
