﻿using FKala.Core.DataLayer.Infrastructure;
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
        public Window Window { get; } = Window.Aligned_1Hour;
        public Cache_Hourly(IDataLayer dataLayer) : base(dataLayer)
        {            
        }

        public override IEnumerable<DataPoint> GetAggregateForCaching(string measurement, DateTime start, DateTime end, AggregateFunction aggrFunc)
        {
            var baseQuery = new Op_Load(null, "fullRes", measurement, start, end, CacheResolutionPredefined.NoCache);
            baseQuery.DontInvalidateCache_ForUseWhileCacheRebuild = true;
            KalaResult aggResult = KalaQuery
                            .Start()
                            .Add(baseQuery)
                            .Add(new Op_Aggregate(null, "hourly", "fullRes", Window, aggrFunc, false, false))
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

            var dp = Pools.DataPoint.Get();
            dp.StartTime = dateTime;
            dp.EndTime = dateTime.Add(Window.Interval);
            dp.Value = value;
            return dp;
        }

        public override DateTime ShouldUpdateFromWhere(int cacheYear,  DataPoint? newestInCache, DataPoint? newestInRaw)
        {
            // no refresh for non-existent cache
            if (newestInCache == null || newestInRaw == null)
            {
                return DateTime.MaxValue;
            }

            // allow 30 minutes aging against raw data
            if (newestInCache.StartTime.Add(new TimeSpan(1, 30, 0)) > newestInRaw.StartTime)
            {
                return DateTime.MaxValue;
            }

            // ..dann so viel refreshen: die letzten 2 Stunden
            DateTime calculated = newestInCache.StartTime.Subtract(new TimeSpan(2, 0, 0));
            DateTime maxValueInCacheFile = new DateTime(cacheYear, 12, 31, 0, 0, 0, DateTimeKind.Utc);
            return calculated < maxValueInCacheFile ? calculated : maxValueInCacheFile;
        }
    }
}
