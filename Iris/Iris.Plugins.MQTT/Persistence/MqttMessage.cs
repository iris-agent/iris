using Iris.Persistence;

namespace Iris.Plugins.MQTT.Persistence;

/// <summary>
/// Represents an MQTT message stored in the persistent message store.
/// Used for deduplication and buffering.
/// </summary>
public sealed class MqttMessage : IPersistedMessage
{
    /// <summary>Unique identifier for this message (from payload or generated).</summary>
    public string MessageId { get; set; } = string.Empty;

    /// <summary>MQTT topic the message was received from.</summary>
    public string Topic { get; set; } = string.Empty;

    /// <summary>Raw message payload/body.</summary>
    public string Payload { get; set; } = string.Empty;

    /// <summary>When the message was first received.</summary>
    public DateTimeOffset ReceivedAt { get; set; }

    /// <summary>Current processing status.</summary>
    public MessageStatus Status { get; set; }

    /// <summary>Number of delivery attempts made.</summary>
    public int AttemptCount { get; set; }

    /// <summary>Timestamp of the last delivery attempt.</summary>
    public DateTimeOffset? LastAttemptAt { get; set; }

    /// <summary>Error message from the last failed attempt.</summary>
    public string? ErrorMessage { get; set; }

    /// <summary>When the message was successfully processed and delivered.</summary>
    public DateTimeOffset? ProcessedAt { get; set; }

    /// <summary>Additional metadata as JSON string.</summary>
    public string? MetadataJson { get; set; }
}
