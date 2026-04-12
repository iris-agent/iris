using System.Security.Cryptography;
using System.Security.Cryptography;
using System.Text;

namespace Iris.Plugins.MQTT.Persistence;

/// <summary>
/// Application-level message identification for MQTT deduplication.
///
/// IMPORTANT: Message IDs are NOT part of the MQTT protocol specification.
/// MQTT only defines Packet Identifiers (for QoS 1/2 flow control, session-scoped only).
///
/// This implementation provides application-level deduplication using:
/// 1. MQTT 5.0 Correlation Data (when provided by publisher) - from MQTT spec
/// 2. Content-based hashing (SHA256 of topic:payload) - our application logic
/// 3. MQTT metadata (QoS, DUP flag, Packet ID) - for logging/diagnostics only
/// </summary>
public static class MessageIdExtractor
{
    /// <summary>
    /// Generate an application-level message ID for deduplication.
    /// Priority:
    /// 1. MQTT 5.0 Correlation Data (if present)
    /// 2. Content Hash (SHA256 of topic:payload)
    /// </summary>
    public static string GetMessageId(string payload, string topic, Dictionary<string, string>? metadata = null)
    {
        if (metadata != null &&
            metadata.TryGetValue("MqttCorrelationData", out var correlationData) &&
            !string.IsNullOrWhiteSpace(correlationData))
        {
            try
            {
                var bytes = Convert.FromBase64String(correlationData);
                return Convert.ToHexString(bytes).ToLowerInvariant();
            }
            catch
            {
                // Fall through to content hash
            }
        }

        return GenerateContentHash(payload, topic);
    }

    /// <summary>
    /// Get MQTT protocol metadata for diagnostics and logging.
    /// </summary>
    public static (bool IsMqttDuplicate, string? PacketId, int QoS) GetMqttProtocolInfo(Dictionary<string, string>? metadata)
    {
        if (metadata == null)
            return (false, null, 0);

        var isDup = metadata.TryGetValue("MqttDuplicate", out var dup) && dup == "True";
        metadata.TryGetValue("MqttPacketId", out var packetId);
        var qos = metadata.TryGetValue("MqttQoS", out var qosStr) && int.TryParse(qosStr, out var qosVal) ? qosVal : 0;

        return (isDup, packetId, qos);
    }

    /// <summary>
    /// Generate a deterministic content-based message ID using SHA256 of topic + payload.
    /// </summary>
    private static string GenerateContentHash(string payload, string topic)
    {
        var combined = $"{topic}:{payload}";
        var hashBytes = SHA256.HashData(Encoding.UTF8.GetBytes(combined));
        return Convert.ToHexString(hashBytes).ToLowerInvariant();
    }
}
