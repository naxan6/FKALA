using FKala.Core.Model;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FKala.Core.DataLayer.Infrastructure
{
    public static class DatFileParser
    {
        public static DataPoint ParseLine(int fileyear, int filemonth, int fileday, string? line)
        {
            ReadOnlySpan<char> span = line.AsSpan();
            var dateTime = new DateTime(fileyear, filemonth, fileday, int.Parse(span.Slice(0, 2)), int.Parse(span.Slice(3, 2)), int.Parse(span.Slice(6, 2)), DateTimeKind.Utc);

            dateTime.AddTicks(int.Parse(span.Slice(9, 7)));
            span = span.Slice(17);

            var valueRaw = span.Slice(0);
            decimal? value = null;
            string? valuetext = null;
            try
            {
                value = decimal.Parse(valueRaw, CultureInfo.InvariantCulture);
            }
            catch (Exception)
            {
                valuetext = valueRaw.ToString();
            }

            return new DataPoint
            {
                Time = dateTime,
                Value = value
                // ValueText = valuetext
            };
        }
    }
}
