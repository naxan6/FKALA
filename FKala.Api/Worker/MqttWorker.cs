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

    private IMqttClient mqttClient;
    private IMqttClientOptions mqttOptions;
    private readonly MqttSettings settings;
    private readonly IDataLayer fkalaDataLayer;

    public MqttWorker(IOptions<MqttSettings> settings, IDataLayer fkalaDataLayer)
    {
        this.settings = settings.Value;
        if (string.IsNullOrWhiteSpace(this.settings.Url))
        {
            return;
        }

        var factory = new MqttFactory();
        mqttClient = factory.CreateMqttClient();

        mqttOptions = new MqttClientOptionsBuilder()
            .WithClientId("Fkala")
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
            Console.WriteLine("Connecting to MQTT broker...");

            // Event-Handler für eingehende Nachrichten
            mqttClient.UseConnectedHandler(async e =>
            {
                Console.WriteLine("Connected to MQTT broker successfully.");

                // Alle Topics aus der Liste abonnieren
                foreach (var topic in this.settings.Topics)
                {
                    await mqttClient.SubscribeAsync(new MQTTnet.Client.Subscribing.MqttClientSubscribeOptionsBuilder()
                        .WithTopicFilter(topic)
                        .Build());

                    Console.WriteLine($"Subscribed to topic '{topic}'");
                }
            });

            mqttClient.UseDisconnectedHandler(e =>
            {
                Console.WriteLine("Disconnected from MQTT broker.");
            });

            mqttClient.UseApplicationMessageReceivedHandler(e =>
            {
                var topic = e.ApplicationMessage.Topic;
                var payload = Encoding.UTF8.GetString(e.ApplicationMessage.Payload);

                // Nachrichten von Topics auf der Blacklist ignorieren
                if (!this.IsTopicBlacklisted(topic))
                {
                    var fkalaData = $"{topic.Replace(' ', '_')} {DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ss.fffffff")} {payload}";
                    this.fkalaDataLayer.Insert(fkalaData, false);
                    // Console.WriteLine($"Received message from topic '{topic}': {payload}");
                }
                    
            });

            await mqttClient.ConnectAsync(mqttOptions, cancellationToken);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error connecting mqtt {this.settings.Url}:{this.settings.Port} - {ex.ToString()}");
        }
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        Console.WriteLine("Disconnecting from MQTT broker...");

        if (mqttClient != null) 
        {
            await mqttClient.DisconnectAsync(cancellationToken);
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
