using FKala.TestConsole.Interfaces;
using FKala.TestConsole.KalaQl.Windowing;
using FKala.TestConsole.KalaQl;
using FKala.TestConsole.Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FKala.TestConsole.Logic;
using System.Globalization;
using FKala.Core.Interfaces;

namespace FKala.Core.DataLayers
{
    public class Cache_Minutely : Cache_Base, ICache
    {
        public IDataLayer DataLayer { get; }
        public override string CacheSubdir { get { return "Minutely"; } }
        public override string GetTimeFormat()
        {
            return "MM-ddTHH:mm";
        }
        public Cache_Minutely(IDataLayer dataLayer)
        {
            DataLayer = dataLayer;
        }

        public override IEnumerable<DataPoint> GetAggregateForCaching(string measurement, DateTime start, DateTime end, AggregateFunction aggrFunc)
        {
            KalaResult aggResult = KalaQuery
               .Start()
               .Add(new Op_BaseQuery("fullRes", measurement, start, end, CacheResolutionPredefinedes.NoCache))
               .Add(new Op_Aggregate("minutely", "fullRes", Window.Aligned_1Minute, aggrFunc, false, false))
               .Add(new Op_Publish(new List<string>() { "minutely" }, PublishMode.MultipleResultsets))
               .Execute(DataLayer);
            var rs = aggResult.ResultSets.First().Resultset;
            //return EnumerableHelpers.SkipLast(rs);
            return rs;
        }

        public override DataPoint? ReadLine(int fileyear, string? line)
        {
            ReadOnlySpan<char> span = line.AsSpan();
            //06-15T23:26 55.654105
            var dateTime = new DateTime(fileyear, int.Parse(span.Slice(0, 2)), int.Parse(span.Slice(3, 2)), int.Parse(span.Slice(6, 2)), int.Parse(span.Slice(9, 2)), 0, DateTimeKind.Utc);

            //dateTime.AddTicks(int.Parse(span.Slice(15, 7)));
            span = span.Slice(12);
            var value = decimal.Parse(span, CultureInfo.InvariantCulture);
            return new DataPoint
            {
                Time = dateTime,
                Value = value
            };
        }
        public override DateTime ShouldUpdateFromWhere(DataPoint? newestInCache, DataPoint newestInRaw)
        {
            // 5 Minute Alterung erlauben
            if (newestInCache.Time.Add(new TimeSpan(0, 1, 0)) > newestInRaw.Time)
            {
                return DateTime.MaxValue;
            }
            
            // die letzten 5 Minuten refreshen
            return newestInCache.Time.Subtract(new TimeSpan(0, 5, 0));
        }
    }
}
