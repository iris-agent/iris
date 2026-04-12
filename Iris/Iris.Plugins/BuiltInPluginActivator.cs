using Iris.Core;
using Iris.Core.Plugins;
using Iris.Plugins.Configuration;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace Iris.Plugins;

/// <summary>
/// Activates built-in plugin instances based on their own configuration options.
/// Binds plugin option types directly from IConfiguration so the core never references them.
/// </summary>
public sealed class BuiltInPluginActivator : IPluginActivator
{
    private readonly IConfiguration _configuration;
    private readonly ILogger<BuiltInPluginActivator> _logger;

    public BuiltInPluginActivator(IConfiguration configuration, ILogger<BuiltInPluginActivator> logger)
    {
        _configuration = configuration;
        _logger = logger;
    }

    public async Task ActivatePluginsAsync(
        IPluginFactory factory,
        IPluginRegistry registry,
        IServiceProvider services,
        CancellationToken cancellationToken)
    {
        var connectorsSection = _configuration.GetSection("Connectors");
        foreach (var child in connectorsSection.GetChildren())
        {
            var type = child["type"] ?? child.Key;

            var transport = CreateTransport(child.GetSection("transport"), factory, services, child.Key);

            if (string.Equals(type, "FilesystemWatcher", StringComparison.OrdinalIgnoreCase))
            {
                var options = child.Get<FilesystemWatcherOptions>() ?? new FilesystemWatcherOptions();
                options.Name = child.Key;
                if (options.Enabled)
                {
                    var connector = factory.CreateConnector("FilesystemWatcher", services, options, transport);
                    if (connector != null) registry.RegisterConnector(connector);
                }
            }
            else if (string.Equals(type, "FileWriter", StringComparison.OrdinalIgnoreCase))
            {
                var options = child.Get<FileWriterOptions>() ?? new FileWriterOptions();
                options.Name = child.Key;
                if (!string.IsNullOrWhiteSpace(options.OutputPath))
                {
                    var connector = factory.CreateConnector("FileWriter", services, options, transport);
                    if (connector != null) registry.RegisterConnector(connector);
                }
            }
        }

        await Task.CompletedTask;
    }

    private ITransport? CreateTransport(IConfigurationSection section, IPluginFactory factory, IServiceProvider services, string connectorName)
    {
        var transportType = section["type"];
        if (string.IsNullOrWhiteSpace(transportType))
            return null;

        var transport = factory.CreateTransport(transportType, services, section);
        if (transport == null)
            _logger.LogWarning("Could not create transport of type '{TransportType}' for connector '{ConnectorName}'.", transportType, connectorName);

        return transport;
    }
}
