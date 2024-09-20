using System;

namespace FKala.Api.Settings
{
    public class MqttSettings {
        public const string ConfigurationSection = "Mqtt";

        public string Url { get; set; }

        public int? Port { get; set; }
        
        public IList<string> Topics { get; set; } = [];

        public IList<string> Blacklist { get; set; } = [];

         public void Configure(IConfiguration configuration)
        {
            configuration.GetSection(ConfigurationSection).Bind(this);            
        }        
    }
}
