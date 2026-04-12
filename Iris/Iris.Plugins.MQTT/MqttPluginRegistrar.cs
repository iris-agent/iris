using Iris.Core.Plugins;
using Iris.Plugins.MQTT.Transports;

namespace Iris.Plugins.MQTT;

/// <summary>
/// Registers MQTT plugin types with the host factory.
/// Discovered automatically at startup via IPluginRegistrar.
/// </summary>
public sealed class MqttPluginRegistrar : IPluginRegistrar
{
    public void RegisterPlugins(IPluginFactory factory)
    {
        factory.RegisterTransportType("Mqtt", typeof(MqttTransport));
    }
}
