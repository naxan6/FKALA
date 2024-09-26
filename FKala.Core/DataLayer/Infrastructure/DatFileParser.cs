using FKala.Core.Logic;
using FKala.Core.Model;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace FKala.Core.DataLayer.Infrastructure
{
    public static class DatFileParser
    {
        public static DataPoint ParseLine(int fileyear, int filemonth, int fileday, string? line, string filepath, int lineIdx, KalaQl.KalaQlContext context)
        {
            try
            {
                ReadOnlySpan<char> span = line.AsSpan();
                int hh, mm, ss;
                int.TryParse(span.Slice(0, 2), out hh);
                int.TryParse(span.Slice(3, 2), out mm);
                int.TryParse(span.Slice(6, 2), out ss);
                var dateTime = new DateTime(fileyear, filemonth, fileday, hh, mm, ss, DateTimeKind.Utc);

                dateTime = dateTime.AddTicks(int.Parse(span.Slice(9, 7)));
                span = span.Slice(17);

                var valueRaw = span.Slice(0);
                decimal value;

                var dp = Pools.DataPoint.Get();
                dp.Time = dateTime;
                var success = decimal.TryParse(valueRaw, CultureInfo.InvariantCulture, out value);

                
                if (success)
                {
                    dp.Value = value;
                }
                else
                {
                    dp.ValueText = valueRaw.ToString();
                }

                return dp;
            }
            catch (Exception)
            {
                string msg = $"Error while parsing line {lineIdx} # {line} # in {filepath}";
                context.AddError(msg);
                
                throw;
            }
        }
    }
}
