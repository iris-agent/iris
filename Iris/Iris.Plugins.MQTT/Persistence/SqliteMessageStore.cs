using Iris.Persistence;
using Microsoft.Extensions.Logging;

namespace Iris.Plugins.MQTT.Persistence;

/// <summary>
/// SQLite-based implementation of the message store for MQTT messages.
/// </summary>
public sealed class SqliteMessageStore : IMessageStore<MqttMessage>, IDisposable
{
    private readonly Iris.Persistence.SqliteMessageStore<MqttMessage> _genericStore;

    public SqliteMessageStore(MessageStoreOptions options, ILoggerFactory loggerFactory)
    {
        var serializationStrategy = new MqttMessageSerializationStrategy();
        var genericLogger = loggerFactory.CreateLogger<Iris.Persistence.SqliteMessageStore<MqttMessage>>();
        _genericStore = new Iris.Persistence.SqliteMessageStore<MqttMessage>(options, genericLogger, serializationStrategy);
    }

    public Task InitializeAsync(CancellationToken cancellationToken)
        => _genericStore.InitializeAsync(cancellationToken);

    public Task<bool> HasBeenProcessedAsync(string messageId, CancellationToken cancellationToken)
        => _genericStore.HasBeenProcessedAsync(messageId, cancellationToken);

    public Task<bool> TryStoreNewMessageAsync(MqttMessage message, CancellationToken cancellationToken)
        => _genericStore.TryStoreNewMessageAsync(message, cancellationToken);

    public Task<List<MqttMessage>> GetPendingMessagesAsync(int limit, CancellationToken cancellationToken)
        => _genericStore.GetPendingMessagesAsync(limit, cancellationToken);

    public Task MarkAsProcessingAsync(string messageId, CancellationToken cancellationToken)
        => _genericStore.MarkAsProcessingAsync(messageId, cancellationToken);

    public Task MarkAsCompletedAsync(string messageId, CancellationToken cancellationToken)
        => _genericStore.MarkAsCompletedAsync(messageId, cancellationToken);

    public Task MarkAsFailedAsync(string messageId, string error, int attemptCount, CancellationToken cancellationToken)
        => _genericStore.MarkAsFailedAsync(messageId, error, attemptCount, cancellationToken);

    public Task<int> CleanupOldMessagesAsync(TimeSpan retention, CancellationToken cancellationToken)
        => _genericStore.CleanupOldMessagesAsync(retention, cancellationToken);

    public Task<MessageStoreStats> GetStatsAsync(CancellationToken cancellationToken)
        => _genericStore.GetStatsAsync(cancellationToken);

    public void Dispose()
        => _genericStore.Dispose();
}
