using System;
using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;
using FKala.TestConsole.DataLayers;
using FKala.TestConsole.Interfaces;

namespace FKala.TestConsole.Migration
{
    public class InfluxLineProtocolImporter
    {
        private static CultureInfo ci = CultureInfo.InvariantCulture;

        private readonly IDataLayer _dataLayer;
        // Regex to match the InfluxDB Line Protocol with optional tags and required fields and timestamp
        private static readonly Regex LineProtocolRegex = new Regex(@"^(?<measurement>[^,]+)(?:,(?<tags>[^ ]+))? (?<fields>[^ ]+) (?<timestamp>\d+)$", RegexOptions.Compiled);

        public InfluxLineProtocolImporter(IDataLayer dataLayer)
        {
            _dataLayer = dataLayer;
        }
        StringBuilder sb = new StringBuilder();

        public void Import(string line, bool locking = true)
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

                var fieldExt = fieldName == "value" ? "" : $"_{fieldName}";

                if (!decimal.TryParse(fieldValue, NumberStyles.Any, ci, out var parsedValue))
                {
                    throw new FormatException($"Ungültiger Feldwert: {fieldValue}");
                }

                var newMeasurement = string.IsNullOrEmpty(topic) ? measurement : topic;

                sb.Clear();
                sb.Append(newMeasurement);
                sb.Append(fieldExt);
                sb.Append(' ');
                sb.Append(parsedTimestamp.ToString("yyyy-MM-ddTHH:mm:ss.fffffff"));
                sb.Append(' ');
                sb.Append(parsedValue.ToString(ci));
                //var rawData = $"{newMeasurement}{fieldExt} {parsedTimestamp:yyyy-MM-ddTHH:mm:ss.fffffff} {parsedValue.ToString(ci)}";

                _dataLayer.Insert(sb.ToString(), locking);
            }

        }
    }
}
