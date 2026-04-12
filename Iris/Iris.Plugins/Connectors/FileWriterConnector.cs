using Iris.Core;
using Iris.Core.Plugins;
using Iris.Plugins.Configuration;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace Iris.Plugins.Connectors;

/// <summary>
/// Writes each incoming <see cref="DataMessage"/> body to a timestamped file
/// in a configured output directory.
/// </summary>
[Plugin("FileWriter", "1.0.0", PluginType.Connector,
    Author = "Iris Team",
    Description = "Writes messages to timestamped files in a local directory")]
public sealed class FileWriterConnector : IConnector
{
    private readonly FileWriterOptions _options;
    private readonly ILogger<FileWriterConnector> _logger;
    private bool _initialized;

    public string Name => _options.Name;
    public ITransport? Transport { get; }
    public event Func<DataMessage, Task>? MessageReceived;

    public FileWriterConnector(FileWriterOptions options, ILogger<FileWriterConnector> logger, ITransport? transport)
    {
        _options = options;
        Transport = transport;
        _logger = logger;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }

    public async Task SendAsync(DataMessage message, CancellationToken cancellationToken)
    {
        if (!_initialized)
        {
            if (string.IsNullOrWhiteSpace(_options.OutputPath))
            {
                _logger.LogWarning("Cannot send message {Id} - FileWriterConnector OutputPath is not configured.", message.Id);
                return;
            }

            if (string.IsNullOrWhiteSpace(_options.FileExtension))
            {
                _logger.LogWarning("Cannot send message {Id} - FileWriterConnector FileExtension is not configured.", message.Id);
                return;
            }

            Directory.CreateDirectory(_options.OutputPath);
            _initialized = true;
        }

        var extension = _options.FileExtension.StartsWith('.')
            ? _options.FileExtension
            : $".{_options.FileExtension}";

        var fileName = $"{DateTimeOffset.UtcNow:yyyyMMdd_HHmmss_fff}_{message.Id}{extension}";
        var filePath = Path.Combine(_options.OutputPath, fileName);

        await File.WriteAllTextAsync(filePath, message.Body, cancellationToken);
        _logger.LogInformation("Message {Id} written to {File}.", message.Id, filePath);
    }
}