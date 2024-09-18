using FKala.Core.Interfaces;
using FKala.Core.KalaQl;
using FKala.Core.KalaQl.Windowing;
using FKala.Core.Model;
using System.Globalization;

namespace FKala.Core.DataLayer.Cache
{
    public class Cache_Hourly : Cache_Base, ICache
    {
        public override string CacheSubdir { get { return "Hourly"; } }

        public override string GetTimeFormat()
        {
            return "MM-ddTHH";
        }
        public Cache_Hourly(IDataLayer dataLayer) : base(dataLayer)
        {            
        }

        public override IEnumerable<DataPoint> GetAggregateForCaching(string measurement, DateTime start, DateTime end, AggregateFunction aggrFunc)
        {
            KalaResult aggResult = KalaQuery
                            .Start()
                            .Add(new Op_BaseQuery(null, "fullRes", measurement, start, end, CacheResolutionPredefined.NoCache))
                            .Add(new Op_Aggregate(null, "hourly", "fullRes", Window.Aligned_1Hour, aggrFunc, false, false))
                            .Add(new Op_Publish(null, new List<string>() { "hourly" }, PublishMode.MultipleResultsets))
                            .Execute(DataLayer);
            if (aggResult?.ResultSets == null)
            {
                throw new Exception("could not aquire aggregate for caching");
            }
            var rs = aggResult!.ResultSets.First().Resultset;
            return rs;
        }

        public override DataPoint ReadLine(int fileyear, string? line)
        {
            ReadOnlySpan<char> span = line.AsSpan();
            //06-15T23 55.654105
            var dateTime = new DateTime(fileyear, int.Parse(span.Slice(0, 2)), int.Parse(span.Slice(3, 2)), int.Parse(span.Slice(6, 2)), 0, 0, DateTimeKind.Utc);

            //dateTime.AddTicks(int.Parse(span.Slice(15, 7)));
            span = span.Slice(9);
            var value = decimal.Parse(span, CultureInfo.InvariantCulture);

            return new DataPoint
            {
                Time = dateTime,
                Value = value
            };
        }

        public override DateTime ShouldUpdateFromWhere(DataPoint? newestInCache, DataPoint? newestInRaw)
        {
            // no refresh for non-existent cache
            if (newestInCache == null || newestInRaw == null)
            {
                return DateTime.MaxValue;
            }

            // allow 30 minutes aging against raw data
            if (newestInCache.Time.Add(new TimeSpan(1, 30, 0)) > newestInRaw.Time)
            {
                return DateTime.MaxValue;
            }

            // ..dann so viel refreshen: die letzten 2 Stunden
            return newestInCache.Time.Subtract(new TimeSpan(2, 0, 0));
        }
    }
}
