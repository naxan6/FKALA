using FKala.TestConsole.KalaQl.Windowing;
using FKala.TestConsole.KalaQl;
using FKala.TestConsole.Logic;
using FKala.TestConsole.Model;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FKala.TestConsole.Interfaces;

namespace FKala.TestConsole.DataLayers
{
    public class CachingLayer
    {
        public IDataLayer DataLayer { get; }
        public string CacheDirectory { get; }

        public CachingLayer(IDataLayer dataLayer, string storagePath) {
            DataLayer = dataLayer;
            this.CacheDirectory = Path.Combine(storagePath, "data");
            Directory.CreateDirectory(CacheDirectory);
        }


        public IEnumerable<DataPoint> LoadMinutelyResolution(string measurement, DateTime startTime, DateTime endTime, CacheResolution cacheResolution)
        {
            var measurementPath = PathSanitizer.SanitizePath(measurement);
            var measurementCachePath = Path.Combine(CacheDirectory, "Minutely");
            Directory.CreateDirectory(measurementCachePath);
            var years = Enumerable.Range(startTime.Year, endTime.Year - startTime.Year + 1);
            var maxSeen = DateTime.MinValue;
            years = FilterYearsForExistingRawData(measurement, years);
            foreach (int year in years)
            {
                var yearFilePath = Path.Combine(measurementCachePath, $"{measurementPath}_{year}_{cacheResolution.AggregateFunction}.dat");
                if (cacheResolution.ForceRebuild)
                {
                    if (File.Exists(yearFilePath)) File.Delete(yearFilePath);
                }
                if (!File.Exists(yearFilePath))
                {
                    PrepareMinutelyData(measurement, year, yearFilePath, cacheResolution.AggregateFunction);
                }
                var yearEnumerable = LoadMinutelyData(startTime, endTime, year, yearFilePath);
                foreach (var item in yearEnumerable)
                {
                    maxSeen = maxSeen > item.Time ? maxSeen : item.Time;
                    yield return item;
                }
            }


            //// Hm, nein das soltle async sein - ausserdem ist das hier unscharf da vom übergebenen Interval abhängig
            //var newestDatapointRaw = this.LoadNewestDatapoint(measurement);
            //var newestAggregateDatapointCached = new DateTime(maxSeen.Year, maxSeen.Month, maxSeen.Day, maxSeen.Hour, maxSeen.Minute, 0, DateTimeKind.Utc);
            //if (maxSeen < newestDatapointRaw.Time && maxSeen < endTime.Subtract(new TimeSpan(0,1,0)))
            //{
            //    //INVALIDATE CACHE
            //    var yearFilePath = Path.Combine(measurementCachePath, $"{measurementPath}_{maxSeen.Year}_{cacheResolution.AggregateFunction}.dat");
            //    File.Delete(yearFilePath);
            //}
        }
        public void PrepareMinutelyData(string measurement, int year, string yearFilePath, AggregateFunction aggrFunc)
        {
            KalaResult aggResult = KalaQuery
               .Start()
               .Add(new Op_BaseQuery("fullRes", measurement, new DateTime(year, 1, 1), new DateTime(year + 1, 1, 1), PredefinedCacheResolutions.NoCache))
               .Add(new Op_Aggregate("minutely", "fullRes", Window.Aligned_1Minute, aggrFunc, false, false))
               .Add(new Op_Publish(new List<string>() { "minutely" }, PublishMode.MultipleResultsets))
               .Execute(DataLayer);
            var rs = aggResult.ResultSets.First().Resultset;
            if (rs.Any())
            {
                using var bw = new BufferedWriter_Locking(yearFilePath);
                foreach (var dp in EnumerableHelpers.SkipLast(rs))
                {
                    bw.Append(dp.Time.ToString("MM-ddTHH:mm"));
                    bw.Append(" ");
                    bw.Append(dp.Value.ToString());
                    bw.AppendNewline();
                }
                bw.Dispose();
            }
        }

        public IEnumerable<DataPoint> LoadMinutelyData(DateTime startTime, DateTime endTime, int year, string yearFilePath)
        {
            if (!File.Exists(yearFilePath)) yield break;
            int fileyear = year;
            using var _fs = new FileStream(yearFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            using var sr = new StreamReader(_fs, Encoding.UTF8, false, 16384);
            string? line;
            while ((line = sr.ReadLine()) != null)
            {
                ReadOnlySpan<char> span = line.AsSpan();
                //06-15T23:26 55.654105
                var dateTime = new DateTime(fileyear, int.Parse(span.Slice(0, 2)), int.Parse(span.Slice(3, 2)), int.Parse(span.Slice(6, 2)), int.Parse(span.Slice(9, 2)), 0, DateTimeKind.Utc);

                //dateTime.AddTicks(int.Parse(span.Slice(15, 7)));
                span = span.Slice(12);
                var value = decimal.Parse(span, CultureInfo.InvariantCulture);
                if (dateTime >= startTime && dateTime < endTime)
                {
                    yield return new DataPoint
                    {
                        Time = dateTime,
                        Value = value
                    };
                }
            }
        }

        public IEnumerable<DataPoint> LoadHourlyResolution(string measurement, DateTime startTime, DateTime endTime, CacheResolution cacheResolution)
        {
            var measurementPath = PathSanitizer.SanitizePath(measurement);
            var measurementCachePath = Path.Combine(CacheDirectory, "Hourly");
            Directory.CreateDirectory(measurementCachePath);
            var years = Enumerable.Range(startTime.Year, endTime.Year - startTime.Year + 1);
            years = FilterYearsForExistingRawData(measurement, years);

            foreach (int year in years)
            {
                var yearFilePath = Path.Combine(measurementCachePath, $"{measurementPath}_{year}_{cacheResolution.AggregateFunction}.dat");
                if (cacheResolution.ForceRebuild)
                {
                    if (File.Exists(yearFilePath)) File.Delete(yearFilePath);
                }
                if (!File.Exists(yearFilePath))
                {
                    PrepareHourlyData(measurement, year, yearFilePath, cacheResolution.AggregateFunction);
                }
                var yearEnumerable = LoadHourlyData(startTime, endTime, year, yearFilePath);
                foreach (var item in yearEnumerable)
                {
                    yield return item;
                }
            }
        }

        public void PrepareHourlyData(string measurement, int year, string yearFilePath, AggregateFunction aggrFunc)
        {
            KalaResult aggResult = KalaQuery
                .Start()
                .Add(new Op_BaseQuery("fullRes", measurement, new DateTime(year, 1, 1), new DateTime(year + 1, 1, 1), PredefinedCacheResolutions.NoCache))
                .Add(new Op_Aggregate("hourly", "fullRes", Window.Aligned_1Hour, aggrFunc, false, false))
                .Add(new Op_Publish(new List<string>() { "hourly" }, PublishMode.MultipleResultsets))
                .Execute(DataLayer);

            var rs = aggResult.ResultSets.First().Resultset;
            if (rs.Any())
            {
                using var bw = new BufferedWriter_Locking(yearFilePath);
                foreach (var dp in rs)
                {
                    bw.Append(dp.Time.ToString("MM-ddTHH"));
                    bw.Append(" ");
                    bw.Append(dp.Value.ToString());
                    bw.AppendNewline();
                }
                bw.Dispose();
            }
        }

        public static IEnumerable<DataPoint> LoadHourlyData(DateTime startTime, DateTime endTime, int year, string yearFilePath)
        {
            if (!File.Exists(yearFilePath)) yield break;
            int fileyear = year;
            using var _fs = new FileStream(yearFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            using var sr = new StreamReader(_fs, Encoding.UTF8, false, 16384);
            string? line;
            while ((line = sr.ReadLine()) != null)
            {
                ReadOnlySpan<char> span = line.AsSpan();
                //06-15T23 55.654105
                var dateTime = new DateTime(fileyear, int.Parse(span.Slice(0, 2)), int.Parse(span.Slice(3, 2)), int.Parse(span.Slice(6, 2)), 0, 0, DateTimeKind.Utc);

                //dateTime.AddTicks(int.Parse(span.Slice(15, 7)));
                span = span.Slice(9);
                var value = decimal.Parse(span, CultureInfo.InvariantCulture);
                if (dateTime >= startTime && dateTime < endTime)
                {
                    yield return new DataPoint
                    {
                        Time = dateTime,
                        Value = value
                    };
                }
            }
        }
        private IEnumerable<int> FilterYearsForExistingRawData(string measurement, IEnumerable<int> years)
        {
            var rawYears = DataLayer.LoadAvailableYears(measurement);
            years = years.Where(year => rawYears.Contains(year));
            return years;
        }
    }
}
