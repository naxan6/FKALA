using System;
using System.Text;
using FKala.Api.Settings;
using FKala.Core.Interfaces;
using Microsoft.Extensions.Options;
using MQTTnet;
using MQTTnet.Client;
using MQTTnet.Client.Options;

namespace FKala.Api.Worker;

public class MqttWorker : IHostedService, IDisposable
{

    private IMqttClient? mqttClient;
    private IMqttClientOptions? mqttOptions;
    private readonly MqttSettings settings;
    private readonly IDataLayer? fkalaDataLayer;
    private readonly ILogger<MqttWorker> _logger;

    public MqttWorker(IOptions<MqttSettings> settings, IDataLayer fkalaDataLayer, ILogger<MqttWorker> logger)
    {
        this.settings = settings.Value;
        this._logger = logger;
        if (string.IsNullOrWhiteSpace(this.settings.Url))
        {
            return;
        }

        var factory = new MqttFactory();
        mqttClient = factory.CreateMqttClient();

        mqttOptions = new MqttClientOptionsBuilder()
            .WithClientId($"Fkala-{Environment.MachineName}")
            .WithTcpServer(this.settings.Url, this.settings.Port)
            .WithCleanSession()
            .Build();
        this.fkalaDataLayer = fkalaDataLayer;
    }


    public async Task StartAsync(CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(this.settings.Url))
        {
            return;
        }

        try
        {
            _logger.LogInformation("Connecting to MQTT broker...");

            // Event-Handler für eingehende Nachrichten
            mqttClient.UseConnectedHandler(async e =>
            {
                _logger.LogInformation("Connected to MQTT broker successfully.");

                // Alle Topics aus der Liste abonnieren
                foreach (var topic in this.settings.Topics)
                {
                    await mqttClient.SubscribeAsync(new MQTTnet.Client.Subscribing.MqttClientSubscribeOptionsBuilder()
                        .WithTopicFilter(topic)
                        .Build());

                    _logger.LogInformation("Subscribed to topic '{Topic}'", topic);
                }
            });

            mqttClient.UseDisconnectedHandler(async e =>
            {
                _logger.LogWarning("Disconnected from MQTT broker. Reconnecting...");
                await Task.Delay(5000, cancellationToken);
                try
                {
                    await mqttClient.ReconnectAsync(cancellationToken);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Failed to reconnect to MQTT broker");
                }
            });

            mqttClient.UseApplicationMessageReceivedHandler(e =>
            {
                var topic = e.ApplicationMessage.Topic;
                if (e.ApplicationMessage.Payload == null) return;
                var payload = Encoding.UTF8.GetString(e.ApplicationMessage.Payload);

                // Nachrichten von Topics auf der Blacklist ignorieren
                if (!this.IsTopicBlacklisted(topic))
                {
                    var fkalaData = $"{topic.Replace(' ', '_')} {DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ss.fffffff")} {payload}";
                    this.fkalaDataLayer!.Insert(fkalaData);
                }
                    
            });

            await mqttClient!.ConnectAsync(mqttOptions, cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error connecting mqtt {Url}:{Port}", this.settings.Url, this.settings.Port);
        }
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Disconnecting from MQTT broker...");

        if (mqttClient != null) 
        {
            await mqttClient!.DisconnectAsync(cancellationToken);
        }
    }

    public void Dispose()
    {
        if (mqttClient != null) 
        {
            mqttClient.Dispose();
        }
    }

     private bool IsTopicBlacklisted(string topic)
    {
        foreach (var blacklistedTopic in this.settings.Blacklist)
        {
            if (IsMatch(topic, blacklistedTopic))
            {
                return true;
            }
        }

        return false;
    }

    private bool IsMatch(string topic, string blacklistPattern)
    {
        var topicLevels = topic.Split('/');
        var patternLevels = blacklistPattern.Split('/');

        for (int i = 0; i < patternLevels.Length; i++)
        {
            if (patternLevels[i] == "#")
            {
                return true;
            }

            if (patternLevels[i] == "+")
            {
                continue;
            }

            if (i >= topicLevels.Length || patternLevels[i] != topicLevels[i])
            {
                return false;
            }
        }

        return topicLevels.Length == patternLevels.Length;
    }
}
