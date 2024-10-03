using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Diagnostics;
using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;
using FKala.Core.Interfaces;
using NodaTime.Calendars;
using System.Reflection.Metadata.Ecma335;
using FKala.Core.Migrations.Influx;

namespace FKala.Core.Migration
{
    public class InfluxLineProtocolImporter
    {
        private static CultureInfo ci = CultureInfo.InvariantCulture;

        private readonly IDataLayer _dataLayer;
        private DateTime _start;
        private DateTime _end;

        // Regex to match the InfluxDB Line Protocol with optional tags and required fields and timestamp
        private static readonly Regex LineProtocolRegex = new Regex(@"^(?<measurement>[^,]+)(?:,(?<tags>[^ ]+))? (?<fields>[^ ]+) (?<timestamp>\d+)$", RegexOptions.Compiled);

        public InfluxLineProtocolImporter(IDataLayer dataLayer)
        {
            _dataLayer = dataLayer;
        }
        StringBuilder sb = new StringBuilder();

#pragma warning disable CS1998 // Bei der asynchronen Methode fehlen "await"-Operatoren. Die Methode wird synchron ausgeführt.
        public async IAsyncEnumerable<Dictionary<string, object?>> Import(string stringParams)
#pragma warning restore CS1998 // Bei der asynchronen Methode fehlen "await"-Operatoren. Die Methode wird synchron ausgeführt.
        {
            var parts = stringParams.Split(";");
            string filePath = "";
            if (parts.Count() == 1)
            {
                this._start = DateTime.MinValue;
                this._end = DateTime.MaxValue;
                filePath = parts[0];
            } 
            else if (parts.Count() == 3)
            {
                this._start = DateTime.Parse(parts[0]);
                this._end = DateTime.Parse(parts[1]);
                filePath = parts[2];
            }
            else
            {
                throw new Exception("wrong parameter count. only <path> or <from> <until> <path>!");
            }
            StreamReader sr = new StreamReader(filePath, Encoding.UTF8, false, new FileStreamOptions() { BufferSize = 262144, Access = FileAccess.Read, Mode = FileMode.Open, Share = FileShare.Read });
            var gesamt = sr.BaseStream.Length;
            string? line;
            int tenthmillipercent;
            decimal previousPercent = 0;
            DateTime start = DateTime.Now;
            var preRow = new Dictionary<string, object?>
                        {
                            { "progress", $"0.000%" },
                            { "eta", $"-" },
                            { "msg", "Started InfluxImporter" }
                        };
            yield return preRow;

            while ((line = sr.ReadLine()) != null)
            {
                ImportLine(line);
                decimal percent = 1.0m * sr.BaseStream.Position / gesamt;
                tenthmillipercent = (int)(100000.0m * sr.BaseStream.Position / gesamt);
                if (previousPercent + 0.001M < percent)
                {
                    previousPercent = percent;
                    var msg = $"processing Import {filePath} ...";
                    Console.WriteLine(msg);

                    var elapsed = DateTime.Now.Ticks - start.Ticks;
                    var eta = (elapsed / percent) - elapsed;
                    var etaTs = new TimeSpan((long)eta);
                    var retRow = new Dictionary<string, object?>
                        {
                            { "progress", $"{(1.0m * tenthmillipercent / 1000).ToString("F3")}%" },
                            { "eta", $"{etaTs.ToString()}" },
                            { "msg", msg }

                        };
                    yield return retRow;
                }
            }
        }

        InfluxLineParser ilp = new InfluxLineParser();

        public void ImportLine(string line)
        {
            ilp.Read(line);

            string measurement = ilp.measurement!;
            var tags = ilp.tags;
            var fields = ilp.fields;
            var time = ilp.time;

            if (!(time >= _start && time < _end))
            {
                return;
            }

            foreach (var field in fields)
            {
                var fieldName = field.Key;
                var fieldValue = field.Value;

                if (fieldValue.EndsWith('i'))
                {
                    fieldValue = fieldValue.Substring(0, fieldValue.Length - 1);
                }
                string kalaLineProtValue;
                //if (!decimal.TryParse(fieldValue, NumberStyles.Any, ci, out var parsedValue))
                //{
                //    kalaLineProtValue = parsedValue.ToString(ci);
                //}
                //else
                //{
                    kalaLineProtValue = fieldValue;
                //}
                var newMeasurement = measurement;
                if (tags.ContainsKey("topic"))
                {
                    newMeasurement = tags["topic"];
                } 
                else if (tags.ContainsKey("sensorname"))
                {
                    newMeasurement = measurement + "/" + tags["sensorname"].Replace(" ", "_");
                }


                    var fieldExt = fieldName == "value" ? "" : $"/{fieldName}";

                sb.Clear();
                sb.Append(newMeasurement);
                sb.Append(fieldExt);
                sb.Append(' ');
                sb.Append(time!.Value.ToString("yyyy-MM-ddTHH:mm:ss.fffffff"));
                sb.Append(' ');
                sb.Append(kalaLineProtValue);
                //var rawData = $"{newMeasurement}{fieldExt} {parsedTimestamp:yyyy-MM-ddTHH:mm:ss.fffffff} {parsedValue.ToString(ci)}";

                _dataLayer.Insert(sb.ToString());
            }
        }
    }
}