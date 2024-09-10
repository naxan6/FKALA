using System;
using System.Globalization;
using System.Text.RegularExpressions;

namespace FKala.TestConsole
{
    public class InfluxLineProtocolImporter
    {
        private readonly DataLayer _dataLayer;
        // Regex to match the InfluxDB Line Protocol with optional tags and required fields and timestamp
        private static readonly Regex LineProtocolRegex = new Regex(@"^(?<measurement>[^,]+)(?:,(?<tags>[^ ]+))? (?<fields>[^ ]+) (?<timestamp>\d+)$");

        public InfluxLineProtocolImporter(DataLayer dataLayer)
        {
            _dataLayer = dataLayer;
        }

        public void Import(string lineProtocolData)
        {
            var lines = lineProtocolData.Split(new[] { '\n', '\r' }, StringSplitOptions.RemoveEmptyEntries);
            foreach (var line in lines)
            {
                var match = LineProtocolRegex.Match(line);
                if (!match.Success)
                {
                    throw new FormatException($"Ungültiges InfluxDB Line Protocol Format: {line}");
                }

                var measurement = match.Groups["measurement"].Value;
                var tags = match.Groups["tags"].Value;
                var fields = match.Groups["fields"].Value;
                var timestamp = match.Groups["timestamp"].Value;

                DateTime parsedTimestamp = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
                parsedTimestamp = parsedTimestamp.AddTicks(long.Parse(timestamp) / 100);

                // Check for the 'topic' tag
                string topic = null;
                if (!string.IsNullOrEmpty(tags))
                {
                    var tagParts = tags.Split(',');
                    foreach (var tag in tagParts)
                    {
                        var keyValue = tag.Split('=');
                        if (keyValue.Length == 2 && keyValue[0] == "topic")
                        {
                            topic = keyValue[1];
                            break;
                        }
                    }
                }

                // Parsing fields
                var fieldParts = fields.Split(',');

                foreach (var fieldPart in fieldParts)
                {
                    var fieldKeyValue = fieldPart.Split('=');
                    if (fieldKeyValue.Length != 2)
                    {
                        throw new FormatException($"Ungültiges Feldformat: {fieldPart}");
                    }

                    var fieldName = fieldKeyValue[0];
                    var fieldValue = fieldKeyValue[1];

                    if (!decimal.TryParse(fieldValue, NumberStyles.Any, CultureInfo.InvariantCulture, out var parsedValue))
                    {
                        throw new FormatException($"Ungültiger Feldwert: {fieldValue}");
                    }

                    var newMeasurement = string.IsNullOrEmpty(topic) ? measurement : topic;
                    var rawData = $"{newMeasurement} {parsedTimestamp:yyyy-MM-ddTHH:mm:ss.ffffff} {parsedValue.ToString(CultureInfo.InvariantCulture)}";

                    _dataLayer.Insert(rawData);
                }
            }
        }
    }
}
