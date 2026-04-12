using Iris.Persistence;
using Iris.Persistence;

namespace Iris.Plugins.MQTT.Configuration;

public enum TransportDirection
{
    Both,
    Receive,
    Send
}

public sealed class MqttOptions
{
    public string Name { get; set; } = "mqtt";
    public string BrokerHost { get; set; } = string.Empty;
    public int BrokerPort { get; set; } = 1883;
    public string Topic { get; set; } = string.Empty;
    public string? Username { get; set; }
    public string? Password { get; set; }

    [System.Text.Json.Serialization.JsonIgnore]
    public TransportDirection DirectionEnum { get; set; } = TransportDirection.Both;

    public string Direction
    {
        get => DirectionEnum.ToString();
        set
        {
            if (Enum.TryParse<TransportDirection>(value?.Trim(), true, out var d))
            {
                DirectionEnum = d;
            }
        }
    }

    // Listening options
    public bool Enabled { get; set; } = false;
    public MessageStoreOptions? MessageStore { get; set; }
}
