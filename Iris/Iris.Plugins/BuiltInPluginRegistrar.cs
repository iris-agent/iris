using Iris.Core.Plugins;
using Iris.Plugins.Connectors;

namespace Iris.Plugins;

/// <summary>
/// Registers all built-in plugin types from Iris.Plugins with the host factory.
/// Called by PluginBootstrapService at startup via IPluginRegistrar discovery.
/// </summary>
/// <remarks>
/// Connectors model domain integrations (<em>what</em> the agent talks to).
/// Transports model protocol delivery channels (<em>how</em> data moves).
/// </remarks>
public sealed class BuiltInPluginRegistrar : IPluginRegistrar
{
    public void RegisterPlugins(IPluginFactory factory)
    {
        // Connectors — domain integrations (what the agent talks to)
        factory.RegisterConnectorType("FilesystemWatcher", typeof(FilesystemWatcherConnector));
        factory.RegisterConnectorType("FileWriter",    typeof(FileWriterConnector));
    }
}
