using Iris.Core;
using Iris.Core.Plugins;
using Iris.Plugins.Configuration;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace Iris.Plugins.Connectors;

/// <summary>
/// Reads new files from a local directory, raises <see cref="MessageReceived"/>
/// for each one, then deletes the file.
/// </summary>
[Plugin("FilesystemWatcher", "1.0.0", PluginType.Connector, 
    Author = "Iris Team", 
    Description = "Monitors a directory for new files and processes them")]
public sealed class FilesystemWatcherConnector : IConnector, IDisposable
{
    private readonly FilesystemWatcherOptions _options;
    private readonly ILogger<FilesystemWatcherConnector> _logger;
    private FileSystemWatcher? _watcher;

    public string Name => _options.Name;
    public ITransport? Transport { get; }
    public event Func<DataMessage, Task>? MessageReceived;

    public FilesystemWatcherConnector(FilesystemWatcherOptions options, ILogger<FilesystemWatcherConnector> logger, ITransport? transport)
    {
        _options = options;
        Transport = transport;
        _logger = logger;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        if (!_options.Enabled)
        {
            _logger.LogInformation("FilesystemWatcherConnector is disabled.");
            return Task.CompletedTask;
        }

        if (string.IsNullOrWhiteSpace(_options.ReadPath))
        {
            _logger.LogWarning("FilesystemWatcherConnector is enabled but ReadPath is not configured. Skipping.");
            return Task.CompletedTask;
        }

        Directory.CreateDirectory(_options.ReadPath);

        _watcher = new FileSystemWatcher(_options.ReadPath, _options.Filter)
        {
            NotifyFilter = NotifyFilters.FileName | NotifyFilters.LastWrite,
            EnableRaisingEvents = true
        };

        _watcher.Created += OnFileCreated;

        _logger.LogInformation("FilesystemWatcherConnector reading from {Path} for {Filter}.",
            _options.ReadPath, _options.Filter);

        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        if (_watcher is not null)
        {
            _watcher.EnableRaisingEvents = false;
            _watcher.Created -= OnFileCreated;
        }

        _logger.LogInformation("FilesystemWatcherConnector stopped.");
        return Task.CompletedTask;
    }

    private void OnFileCreated(object sender, FileSystemEventArgs e)
    {
        _ = ProcessFileAsync(e.FullPath);
    }

    private async Task ProcessFileAsync(string filePath)
    {
        // Brief delay to allow the writing process to finish flushing.
        await Task.Delay(250);

        try
        {
            _logger.LogInformation("File detected: {File}", filePath);

            string content = await ReadWithRetryAsync(filePath);

            var message = new DataMessage
            {
                Body = content,
                Metadata = new Dictionary<string, string>
                {
                    ["SourceFile"] = Path.GetFileName(filePath),
                    ["SourcePath"] = filePath,
                    ["DetectedAt"] = DateTimeOffset.UtcNow.ToString("O")
                }
            };

            if (MessageReceived is not null)
                await MessageReceived(message);

            if (_options.DeleteAfterProcessing)
            {
                File.Delete(filePath);
                _logger.LogInformation("Deleted processed file: {File}.", filePath);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error processing file {File}.", filePath);
        }
    }

    /// <summary>Retries the read a few times to handle files still being written.</summary>
    private static async Task<string> ReadWithRetryAsync(string filePath, int attempts = 5)
    {
        for (int i = 0; i < attempts; i++)
        {
            try
            {
                return await File.ReadAllTextAsync(filePath);
            }
            catch (IOException) when (i < attempts - 1)
            {
                await Task.Delay(200 * (i + 1));
            }
        }

        return await File.ReadAllTextAsync(filePath);
    }

    public void Dispose()
    {
        _watcher?.Dispose();
    }
}