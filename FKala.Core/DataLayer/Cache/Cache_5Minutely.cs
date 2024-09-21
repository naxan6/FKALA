using FKala.Core.DataLayer.Infrastructure;
using FKala.Core.Interfaces;
using FKala.Core.KalaQl;
using FKala.Core.KalaQl.Windowing;
using FKala.Core.Model;
using System.Globalization;

namespace FKala.Core.DataLayer.Cache
{
    public class Cache_5Minutely : Cache_Base, ICache
    {
        public override string CacheSubdir { get { return "5Minutely"; } }
        public override string GetTimeFormat()
        {
            return "MM-ddTHH:mm";
        }
        public Cache_5Minutely(IDataLayer dataLayer) : base(dataLayer)
        {
        }

        public override IEnumerable<DataPoint> GetAggregateForCaching(string measurement, DateTime start, DateTime end, AggregateFunction aggrFunc)
        {
            KalaResult aggResult = KalaQuery
               .Start()
               .Add(new Op_BaseQuery(null, "fullRes", measurement, start, end, CacheResolutionPredefined.NoCache))
               .Add(new Op_Aggregate(null, "5minutely", "fullRes", Window.Aligned_5Minutes, aggrFunc, false, false))
               .Add(new Op_Publish(null, new List<string>() { "5minutely" }, PublishMode.MultipleResultsets))
               .Execute(DataLayer);

            if (aggResult?.ResultSets == null)
            {
                throw new Exception("could not aquire aggregate for caching");
            }
            var rs = aggResult.ResultSets.First().Resultset;
            //return EnumerableHelpers.SkipLast(rs);
            return rs;
        }

        public override DataPoint ReadLine(int fileyear, string? line)
        {
            ReadOnlySpan<char> span = line.AsSpan();
            //06-15T23:26 55.654105
            var dateTime = new DateTime(fileyear, int.Parse(span.Slice(0, 2)), int.Parse(span.Slice(3, 2)), int.Parse(span.Slice(6, 2)), int.Parse(span.Slice(9, 2)), 0, DateTimeKind.Utc);

            //dateTime.AddTicks(int.Parse(span.Slice(15, 7)));
            span = span.Slice(12);
            var value = decimal.Parse(span, CultureInfo.InvariantCulture);
            var dp = Pools.DataPoint.Get();
            dp.Time = dateTime;
            dp.Value = value;
            return dp;
        }
        public override DateTime ShouldUpdateFromWhere(int cacheYear, DataPoint? newestInCache, DataPoint? newestInRaw)
        {
            // no refresh for non-existent cache
            if (newestInCache == null || newestInRaw == null)
            {
                return DateTime.MaxValue;
            }

            // allow 10 minutes from windowstart aging against raw-data
            if (newestInCache.Time.Add(new TimeSpan(0, 10, 0)) > newestInRaw.Time)
            {
                return DateTime.MaxValue;
            }

            // then refresh the last 15 (3 windows) existent minutes in the cache
            DateTime calculated = newestInCache.Time.Subtract(new TimeSpan(0, 15, 0));
            DateTime maxValueInCacheFile = new DateTime(cacheYear, 12, 31, 0, 0, 0, DateTimeKind.Utc);
            return calculated < maxValueInCacheFile ? calculated : maxValueInCacheFile;
        }
    }
}
