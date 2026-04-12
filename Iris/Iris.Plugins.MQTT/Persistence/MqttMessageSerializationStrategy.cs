using Iris.Persistence;
using Microsoft.Data.Sqlite;

namespace Iris.Plugins.MQTT.Persistence;

/// <summary>
/// Serialization strategy for MQTT messages.
/// </summary>
public sealed class MqttMessageSerializationStrategy : IMessageSerializationStrategy<MqttMessage>
{
    public string TableName => "mqtt_messages";

    public string GetCreateTableSql()
    {
        return @"
            CREATE TABLE IF NOT EXISTS mqtt_messages (
                message_id TEXT PRIMARY KEY,
                topic TEXT NOT NULL,
                payload TEXT NOT NULL,
                received_at INTEGER NOT NULL,
                status INTEGER NOT NULL,
                attempt_count INTEGER DEFAULT 0,
                last_attempt_at INTEGER,
                error_message TEXT,
                metadata_json TEXT,
                processed_at INTEGER,
                created_at INTEGER NOT NULL DEFAULT (strftime('%s','now') * 1000)
            );

            CREATE INDEX IF NOT EXISTS idx_status ON mqtt_messages(status);
            CREATE INDEX IF NOT EXISTS idx_received_at ON mqtt_messages(received_at);
            CREATE INDEX IF NOT EXISTS idx_topic ON mqtt_messages(topic);
        ";
    }

    public string GetInsertSql()
    {
        return @"
            INSERT INTO mqtt_messages (message_id, topic, payload, received_at, status, attempt_count, metadata_json)
            VALUES (@messageId, @topic, @payload, @receivedAt, @status, @attemptCount, @metadataJson)
        ";
    }

    public void AddInsertParameters(SqliteCommand command, MqttMessage message)
    {
        command.Parameters.AddWithValue("@topic", message.Topic);
    }

    public MqttMessage MapFromReader(SqliteDataReader reader)
    {
        return new MqttMessage
        {
            MessageId = reader.GetString(reader.GetOrdinal("message_id")),
            Topic = reader.GetString(reader.GetOrdinal("topic")),
            Payload = reader.GetString(reader.GetOrdinal("payload")),
            ReceivedAt = DateTimeOffset.FromUnixTimeMilliseconds(reader.GetInt64(reader.GetOrdinal("received_at"))),
            Status = (MessageStatus)reader.GetInt32(reader.GetOrdinal("status")),
            AttemptCount = reader.GetInt32(reader.GetOrdinal("attempt_count")),
            LastAttemptAt = reader.IsDBNull(reader.GetOrdinal("last_attempt_at"))
                ? null
                : DateTimeOffset.FromUnixTimeMilliseconds(reader.GetInt64(reader.GetOrdinal("last_attempt_at"))),
            ErrorMessage = reader.IsDBNull(reader.GetOrdinal("error_message"))
                ? null
                : reader.GetString(reader.GetOrdinal("error_message")),
            MetadataJson = reader.IsDBNull(reader.GetOrdinal("metadata_json"))
                ? null
                : reader.GetString(reader.GetOrdinal("metadata_json")),
            ProcessedAt = reader.IsDBNull(reader.GetOrdinal("processed_at"))
                ? null
                : DateTimeOffset.FromUnixTimeMilliseconds(reader.GetInt64(reader.GetOrdinal("processed_at")))
        };
    }
}
