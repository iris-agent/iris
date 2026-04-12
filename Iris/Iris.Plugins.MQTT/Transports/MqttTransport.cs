using Iris.Core;
using Iris.Core.Plugins;
using Iris.Persistence;
using Iris.Plugins.MQTT.Configuration;
using Iris.Plugins.MQTT.Messaging;
using Iris.Plugins.MQTT.Persistence;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace Iris.Plugins.MQTT.Transports;

/// <summary>
/// A unified MQTT transport that can both publish and subscribe to MQTT topics.
/// </summary>
[Plugin("Mqtt", "1.0.0", PluginType.Transport,
    Author = "Iris Team",
    Description = "Publishes and subscribes to messages from an MQTT broker")]
public sealed class MqttTransport : ITransport, IDisposable
{
    private readonly MqttOptions _options;
    private readonly ILogger<MqttTransport> _logger;
    private readonly ILoggerFactory _loggerFactory;
    private IMessageStore<MqttMessage>? _messageStore;
    private MqttMessageQueueClient? _mqttSenderClient;
    private MqttMessageQueueClient? _mqttReceiverClient;
    private CancellationTokenSource? _cts;

    public string Name => _options.Name;
    public bool CanSend => _options.DirectionEnum != TransportDirection.Receive;
    public bool CanReceive => _options.DirectionEnum != TransportDirection.Send;
    public event Func<DataMessage, Task>? MessageReceived;

    public MqttTransport(IConfigurationSection section, ILogger<MqttTransport> logger, ILoggerFactory loggerFactory)
        : this(section.Get<MqttOptions>() ?? new MqttOptions(), logger, loggerFactory)
    {
        _options.Name = string.IsNullOrWhiteSpace(_options.Name) || _options.Name == "mqtt"
            ? section.Key
            : _options.Name;
    }

    public MqttTransport(MqttOptions options, ILogger<MqttTransport> logger, ILoggerFactory loggerFactory)
    {
        _options = options;
        _logger = logger;
        _loggerFactory = loggerFactory;

        if (_options.MessageStore?.Enabled == true)
        {
            _messageStore = new SqliteMessageStore(_options.MessageStore, loggerFactory);
        }
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("MqttTransport ({Name}) starting with Direction = {Direction}", Name, _options.DirectionEnum);

        if (!CanReceive)
        {
            _logger.LogInformation("MqttTransport ({Name}) skipping listener start because it is Send-only.", Name);
            return;
        }

        if (!_options.Enabled || string.IsNullOrWhiteSpace(_options.Topic))
        {
            return;
        }

        if (string.IsNullOrWhiteSpace(_options.BrokerHost))
        {
            _logger.LogWarning("Cannot listen to MQTT - BrokerHost is not configured.");
            return;
        }

        _logger.LogInformation("MqttTransport starting listener on {Host}:{Port}, topic: {Topic}",
            _options.BrokerHost, _options.BrokerPort, _options.Topic);

        await ConnectReceiverAsync(cancellationToken);

        _cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        await _mqttReceiverClient!.SubscribeAsync(OnMqttMessageReceived, cancellationToken);
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (_cts != null)
        {
            _cts.Cancel();
        }

        await Task.CompletedTask;
    }

    public async Task SendAsync(DataMessage message, CancellationToken cancellationToken)
    {
        if (!CanSend)
        {
            return;
        }

        if (string.IsNullOrWhiteSpace(_options.BrokerHost))
        {
            _logger.LogWarning("Cannot send message {Id} - MQTT BrokerHost is not configured.", message.Id);
            return;
        }

        await ConnectSenderAsync(cancellationToken);

        await _mqttSenderClient!.PublishAsync(message, cancellationToken);
        _logger.LogInformation("Message {Id} sent to MQTT topic {Topic}.", message.Id, _options.Topic);
    }

    private async Task ConnectSenderAsync(CancellationToken cancellationToken)
    {
        if (_mqttSenderClient == null)
        {
            _mqttSenderClient = new MqttMessageQueueClient(
                _options.BrokerHost,
                _options.BrokerPort,
                _options.Topic,
                _options.Username,
                _options.Password,
                _loggerFactory.CreateLogger<MqttMessageQueueClient>());

            await _mqttSenderClient.ConnectAsync(cancellationToken);
        }
    }

    private async Task ConnectReceiverAsync(CancellationToken cancellationToken)
    {
        if (!CanReceive)
        {
            _logger.LogInformation("MqttTransport ({Name}) receiver connection skipped because direction is Send.", Name);
            return;
        }

        if (_mqttReceiverClient == null)
        {
            _mqttReceiverClient = new MqttMessageQueueClient(
                _options.BrokerHost,
                _options.BrokerPort,
                _options.Topic,
                _options.Username,
                _options.Password,
                _loggerFactory.CreateLogger<MqttMessageQueueClient>());

            await _mqttReceiverClient.ConnectAsync(cancellationToken);
        }
    }

    private async Task OnMqttMessageReceived(DataMessage message)
    {
        if (!CanReceive)
        {
            return;
        }

        if (MessageReceived != null)
        {
            try
            {
                await MessageReceived(message);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to route message from MQTT listener.");
            }
        }
    }

    public void Dispose()
    {
        _mqttSenderClient?.Dispose();
        _mqttReceiverClient?.Dispose();
        _cts?.Dispose();
    }
}
