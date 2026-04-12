using System.Net;
using System.Text;
using Iris.Core;
using Microsoft.Extensions.Logging;
using MQTTnet;
using MQTTnet.Client;
using MQTTnet.Protocol;

namespace Iris.Plugins.MQTT.Messaging;

/// <summary>
/// Implements message sending and receiving using MQTT.
/// </summary>
public sealed class MqttMessageQueueClient : IDisposable
{
    private readonly IMqttClient _mqttClient;
    private readonly MqttClientOptions _options;
    private readonly ILogger _logger;
    private readonly string _topic;
    private readonly string _brokerHost;
    private readonly int _brokerPort;
    private readonly SemaphoreSlim _connectionLock = new SemaphoreSlim(1, 1);
    private bool _isReconnecting;
    private DateTime _lastConnectionTime;
    private string? _connectedToIpAddress;

    public MqttMessageQueueClient(string brokerHost, int brokerPort, string topic, string? username, string? password, ILogger logger)
    {
        _logger = logger;
        _topic = topic;
        _brokerHost = brokerHost;
        _brokerPort = brokerPort;
        var factory = new MqttFactory();
        _mqttClient = factory.CreateMqttClient();
        var builder = new MqttClientOptionsBuilder()
            .WithTcpServer(brokerHost, brokerPort)
            .WithCleanSession()
            .WithKeepAlivePeriod(TimeSpan.FromSeconds(15))
            .WithTimeout(TimeSpan.FromSeconds(10));
        if (!string.IsNullOrEmpty(username) && !string.IsNullOrEmpty(password))
            builder = builder.WithCredentials(username, password);
        _options = builder.Build();
    }

    public async Task ConnectAsync(CancellationToken cancellationToken)
    {
        _mqttClient.ConnectedAsync += async e =>
        {
            _lastConnectionTime = DateTime.UtcNow;

            try
            {
                var hostEntry = await Dns.GetHostEntryAsync(_brokerHost);
                _logger.LogInformation("Connected! Broker DNS '{BrokerHost}' resolved to {Count} IP address(es):",
                    _brokerHost, hostEntry.AddressList.Length);
                foreach (var addr in hostEntry.AddressList)
                {
                    _logger.LogInformation("  - {Address} (Family: {AddressFamily})", addr, addr.AddressFamily);
                }

                await Task.Delay(100);
                _connectedToIpAddress = await GetConnectedEndpointAsync();
                if (_connectedToIpAddress != null)
                {
                    _logger.LogInformation("*** ACTUAL CONNECTED TO: {IpAddress}:{Port} ***", _connectedToIpAddress, _brokerPort);
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Could not resolve broker DNS for '{BrokerHost}'", _brokerHost);
            }
        };

        _mqttClient.DisconnectedAsync += async e =>
        {
            var connectionDuration = _lastConnectionTime != default
                ? DateTime.UtcNow - _lastConnectionTime
                : TimeSpan.Zero;

            if (e.Exception != null)
            {
                _logger.LogError(e.Exception, "MQTT client disconnected with error. Reason: {Reason}, " +
                    "WasConnected: {WasConnected}, Duration: {Duration:hh\\:mm\\:ss}, ReasonString: {ReasonString}",
                    e.Reason,
                    e.ClientWasConnected,
                    connectionDuration,
                    e.ReasonString ?? "(none)");
            }
            else
            {
                _logger.LogWarning("MQTT client disconnected. Reason: {Reason}, " +
                    "WasConnected: {WasConnected}, Duration: {Duration:hh\\:mm\\:ss}, ReasonString: {ReasonString}",
                    e.Reason,
                    e.ClientWasConnected,
                    connectionDuration,
                    e.ReasonString ?? "(none)");
            }

            if (e.ConnectResult != null)
            {
                _logger.LogInformation("Disconnected from: Server: {Server}, ClientId: {ClientId}, " +
                    "SessionWasPresent: {SessionPresent}, ConnectedIP: {ConnectedIP}",
                    e.ConnectResult.ServerReference ?? "(none)",
                    e.ConnectResult.AssignedClientIdentifier ?? "(none)",
                    e.ConnectResult.IsSessionPresent,
                    _connectedToIpAddress ?? "(unknown)");
            }

            if (!e.ClientWasConnected || cancellationToken.IsCancellationRequested)
            {
                return;
            }

            await TryReconnectAsync(cancellationToken);
        };

        var result = await _mqttClient.ConnectAsync(_options, cancellationToken);
        if (result.ResultCode == MqttClientConnectResultCode.Success)
        {
            LogBrokerMetadata(result, "Connected to MQTT broker successfully");
        }
        else
        {
            _logger.LogError("Failed to connect to MQTT broker. Result: {ResultCode}, Reason: {Reason}",
                result.ResultCode, result.ReasonString);
            throw new Exception($"MQTT connection failed: {result.ResultCode} - {result.ReasonString}");
        }
    }

    private async Task TryReconnectAsync(CancellationToken cancellationToken)
    {
        if (_isReconnecting)
        {
            return;
        }

        await _connectionLock.WaitAsync(cancellationToken);
        try
        {
            if (_isReconnecting || _mqttClient.IsConnected)
            {
                return;
            }

            _isReconnecting = true;
            _logger.LogInformation("Attempting to reconnect to MQTT broker...");

            const int maxRetries = 5;
            for (int attempt = 1; attempt <= maxRetries; attempt++)
            {
                try
                {
                    var delay = TimeSpan.FromSeconds(Math.Min(Math.Pow(2, attempt), 60));
                    _logger.LogInformation("Reconnection attempt {Attempt}/{MaxRetries} in {Delay} seconds...",
                        attempt, maxRetries, delay.TotalSeconds);
                    await Task.Delay(delay, cancellationToken);

                    var result = await _mqttClient.ConnectAsync(_options, cancellationToken);
                    if (result.ResultCode == MqttClientConnectResultCode.Success)
                    {
                        LogBrokerMetadata(result, "Successfully reconnected to MQTT broker");
                        return;
                    }
                    else
                    {
                        _logger.LogWarning("Reconnection attempt {Attempt} failed: {ResultCode}",
                            attempt, result.ResultCode);
                    }
                }
                catch (Exception ex) when (!cancellationToken.IsCancellationRequested)
                {
                    _logger.LogError(ex, "Error during reconnection attempt {Attempt}", attempt);
                }
            }

            _logger.LogError("Failed to reconnect after {MaxRetries} attempts", maxRetries);
        }
        finally
        {
            _isReconnecting = false;
            _connectionLock.Release();
        }
    }

    public async Task PublishAsync(DataMessage message, CancellationToken cancellationToken)
    {
        if (!_mqttClient.IsConnected)
        {
            _logger.LogWarning("MQTT client is not connected. Attempting to reconnect before publishing message {Id}...", message.Id);
            await TryReconnectAsync(cancellationToken);

            if (!_mqttClient.IsConnected)
            {
                throw new InvalidOperationException("Cannot publish message - MQTT client is not connected after reconnection attempt");
            }
        }

        var mqttMessage = new MqttApplicationMessageBuilder()
            .WithTopic(_topic)
            .WithPayload(message.Body)
            .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce)
            .Build();
        await _mqttClient.PublishAsync(mqttMessage, cancellationToken);
        _logger.LogInformation("Published message with Id {Id} to topic {Topic}", message.Id, _topic);
    }

    public async Task SubscribeAsync(Func<DataMessage, Task> onMessage, CancellationToken cancellationToken)
    {
        if (!_mqttClient.IsConnected)
        {
            _logger.LogError("Cannot subscribe to topic {Topic} - MQTT client is not connected", _topic);
            throw new InvalidOperationException("MQTT client is not connected. Call ConnectAsync first.");
        }

        _mqttClient.ApplicationMessageReceivedAsync += async e =>
        {
            try
            {
                var payloadBytes = e.ApplicationMessage.Payload ?? Array.Empty<byte>();
                var body = Encoding.UTF8.GetString(payloadBytes);
                var msg = new DataMessage
                {
                    Body = body,
                    Metadata = new Dictionary<string, string>
                    {
                        ["MqttTopic"] = e.ApplicationMessage.Topic,
                        ["SubscribedTopic"] = _topic,
                        ["MqttQoS"] = ((int)e.ApplicationMessage.QualityOfServiceLevel).ToString(),
                        ["MqttRetain"] = e.ApplicationMessage.Retain.ToString(),
                        ["MqttDuplicate"] = e.ApplicationMessage.Dup.ToString(),
                        ["MqttPacketId"] = e.PacketIdentifier.ToString()
                    }
                };

                if (e.ApplicationMessage.CorrelationData?.Length > 0)
                {
                    msg.Metadata["MqttCorrelationData"] = Convert.ToBase64String(e.ApplicationMessage.CorrelationData);
                }

                _logger.LogInformation("Received message from topic {ReceivedTopic} (subscribed to {SubscribedTopic}), QoS: {QoS}, PacketId: {PacketId}, Dup: {Dup}, Length: {Length} bytes",
                    e.ApplicationMessage.Topic, _topic, e.ApplicationMessage.QualityOfServiceLevel,
                    e.PacketIdentifier, e.ApplicationMessage.Dup, payloadBytes.Length);
                await onMessage(msg);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error processing received MQTT message from topic {Topic}", e.ApplicationMessage.Topic);
            }
        };

        var subscribeOptions = new MqttClientSubscribeOptionsBuilder()
            .WithTopicFilter(f => f.WithTopic(_topic).WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce))
            .Build();

        var result = await _mqttClient.SubscribeAsync(subscribeOptions, cancellationToken);

        foreach (var subscription in result.Items)
        {
            if (subscription.ResultCode == MqttClientSubscribeResultCode.GrantedQoS0 ||
                subscription.ResultCode == MqttClientSubscribeResultCode.GrantedQoS1 ||
                subscription.ResultCode == MqttClientSubscribeResultCode.GrantedQoS2)
            {
                _logger.LogInformation("Successfully subscribed to topic filter: {TopicFilter} with QoS: {ResultCode}",
                    subscription.TopicFilter.Topic, subscription.ResultCode);
            }
            else
            {
                _logger.LogError("Failed to subscribe to topic filter: {TopicFilter}. Result: {ResultCode}",
                    subscription.TopicFilter.Topic, subscription.ResultCode);
                throw new Exception($"MQTT subscription failed: {subscription.ResultCode}");
            }
        }
    }

    private void LogBrokerMetadata(MqttClientConnectResult result, string message)
    {
        _logger.LogInformation("{Message}. Server: {Server}, ClientId: {ClientId}, SessionPresent: {SessionPresent}, " +
            "ReasonString: {ReasonString}, ResponseInfo: {ResponseInfo}, ReceiveMax: {ReceiveMax}, " +
            "MaxPacketSize: {MaxPacketSize}, MaxQoS: {MaxQoS}, RetainAvailable: {RetainAvailable}, " +
            "WildcardAvailable: {WildcardAvailable}, SharedSubAvailable: {SharedSubAvailable}, " +
            "SubIdsAvailable: {SubIdsAvailable}, TopicAliasMax: {TopicAliasMax}",
            message,
            result.ServerReference ?? "(none)",
            result.AssignedClientIdentifier ?? "(none)",
            result.IsSessionPresent,
            result.ReasonString ?? "(none)",
            result.ResponseInformation ?? "(none)",
            result.ReceiveMaximum,
            result.MaximumPacketSize,
            result.MaximumQoS,
            result.RetainAvailable,
            result.WildcardSubscriptionAvailable,
            result.SharedSubscriptionAvailable,
            result.SubscriptionIdentifiersAvailable,
            result.TopicAliasMaximum);

        if (result.UserProperties?.Count > 0)
        {
            _logger.LogInformation("Broker User Properties (may contain node/cluster info):");
            foreach (var prop in result.UserProperties)
            {
                _logger.LogInformation("  {Key} = {Value}", prop.Name, prop.Value);
            }
        }
        else
        {
            _logger.LogInformation("No User Properties received from broker");
        }
    }

    private async Task<string?> GetConnectedEndpointAsync()
    {
        try
        {
            var processId = Environment.ProcessId;
            var startInfo = new System.Diagnostics.ProcessStartInfo
            {
                FileName = "netstat",
                Arguments = $"-ano",
                RedirectStandardOutput = true,
                UseShellExecute = false,
                CreateNoWindow = true
            };

            using var process = System.Diagnostics.Process.Start(startInfo);
            if (process == null) return null;

            var output = await process.StandardOutput.ReadToEndAsync();
            await process.WaitForExitAsync();

            var lines = output.Split('\n');
            foreach (var line in lines)
            {
                if (line.Contains("ESTABLISHED") && line.Contains($":{_brokerPort}") && line.Contains(processId.ToString()))
                {
                    var parts = line.Split(new[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);
                    if (parts.Length >= 3)
                    {
                        var remoteEndpoint = parts[2];
                        var ipPart = remoteEndpoint.Split(':')[0];
                        return ipPart;
                    }
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Could not determine connected endpoint via netstat");
        }
        return null;
    }

    public void Dispose()
    {
        _connectionLock?.Dispose();
        _mqttClient?.Dispose();
    }
}
