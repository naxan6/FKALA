﻿using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Diagnostics;
using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;
using FKala.Core.Interfaces;
using NodaTime.Calendars;
using System.Reflection.Metadata.Ecma335;

namespace FKala.Core.Migration
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

#pragma warning disable CS1998 // Bei der asynchronen Methode fehlen "await"-Operatoren. Die Methode wird synchron ausgeführt.
        public async IAsyncEnumerable<Dictionary<string, object?>> Import(string filePath)
#pragma warning restore CS1998 // Bei der asynchronen Methode fehlen "await"-Operatoren. Die Methode wird synchron ausgeführt.
        {
            StreamReader sr = new StreamReader(filePath, Encoding.UTF8, false, new FileStreamOptions() { BufferSize = 262144, Access = FileAccess.Read, Mode = FileMode.Open, Share = FileShare.Read });
            var gesamt = sr.BaseStream.Length;
            string? line;
            decimal percent;
            while ((line = sr.ReadLine()) != null) {
                ImportLine(line);
                percent = 1.0m * sr.BaseStream.Position / gesamt;
                if (percent * 100 % 1 == 0)
                {
                    var msg = $"Progress {percent}% for Import {filePath}";
                    Console.WriteLine(msg);
                    var retRow = new Dictionary<string, object?>
                        {
                            { "progress", $"{percent.ToString("F2")}%" },
                            { "msg", msg }

                        };
                    yield return retRow;
                }
            }
        }

        public void ImportLine(string line)
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
            string? topic = null;
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

                _dataLayer.Insert(sb.ToString());
            }

        }
    }
}