using FKala.Core.KalaQl.Windowing;
using FKala.Core.Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FKala.Core.Interfaces;
using FKala.Core.Logic;
using System.Diagnostics.Metrics;
using System.Globalization;
using System.Net.WebSockets;

namespace FKala.Core.DataLayer.Cache
{
    public abstract class Cache_Base : ICache
    {
        protected Cache_Base(IDataLayer dataLayer)
        {
            DataLayer = dataLayer;
        }

        public abstract IEnumerable<DataPoint> GetAggregateForCaching(string measurement, DateTime start, DateTime end, AggregateFunction aggrFunc);
        public abstract string GetTimeFormat();
        public abstract DataPoint ReadLine(int fileyear, string? line);
        public abstract string CacheSubdir { get; }
        public IDataLayer DataLayer { get; }

        public abstract DateTime ShouldUpdateFromWhere(DataPoint? newestInCache, DataPoint? newestInRaw);

        public IEnumerable<DataPoint?> LoadNewestDatapoint(string newestFile)
        {
            var parts = newestFile.Split('_');
            var fileYear = int.Parse(parts[parts.Length - 2]);
            var lastLine = LastLineReader.ReadLastLine(newestFile);
            var datapoint = ReadLine(fileYear, lastLine);
            yield return datapoint;
        }
        public void GenerateWholeYearCache(string measurement, int year, string cacheFilePath, AggregateFunction aggrFunc, bool forceRebuild)
        {
            if (forceRebuild)
            {
                if (File.Exists(cacheFilePath)) File.Delete(cacheFilePath);
            }

            IEnumerable<DataPoint> rs = GetAggregateForCaching(measurement, new DateTime(year, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(year + 1, 1, 1, 0, 0, 0, DateTimeKind.Utc), aggrFunc);
            WriteCacheFile(cacheFilePath, rs);
        }

        private void WriteCacheFile(string cacheFilePath, IEnumerable<DataPoint> rs)
        {
            if (rs.Any())
            {
                var timeFormat = GetTimeFormat();
                var writerSvc = DataLayer.WriterSvc;
                writerSvc.CreateWriteDispose(cacheFilePath, (writer) =>
                {
                    foreach (var dp in rs)
                    {
                        if (dp.Value != null)
                        {

                            writer.Append(dp.Time.ToString(timeFormat));
                            writer.Append(" ");
                            writer.Append(dp.Value.Value.ToString(CultureInfo.InvariantCulture));
                            writer.AppendNewline();
                        };
                    }
                });
            }
        }

        public IEnumerable<DataPoint> LoadCache(DateTime startTime, DateTime endTime, int year, string yearFilePath)
        {
            if (!File.Exists(yearFilePath)) yield break;
            int fileyear = year;
            using var _fs = new FileStream(yearFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            using var sr = new StreamReader(_fs, Encoding.UTF8, false, 16384);
            string? line;
            while ((line = sr.ReadLine()) != null)
            {
                var datapoint = ReadLine(fileyear, line);
                if (datapoint.Time >= startTime && datapoint.Time < endTime)
                {
                    yield return datapoint;
                }
                //TODO für später: breaken, sobald endtime überschritten, nicht mehr komplett durchlaufen
            }
        }

        public string EnsureDirectory(string cacheDirectory)
        {
            var measurementCachePath = Path.Combine(cacheDirectory, CacheSubdir);
            Directory.CreateDirectory(measurementCachePath);
            return measurementCachePath;
        }

        public void UpdateData(string measurement, DateTime rebuildFromDateTime, AggregateFunction aggrFunc, string newestCacheFile)
        {
            var parts = newestCacheFile.Split('_');
            var fileYear = int.Parse(parts[parts.Length - 2]);
            IEnumerable<DataPoint> updateData = GetAggregateForCaching(measurement, rebuildFromDateTime, DateTime.MaxValue, aggrFunc);

            FileFromEndProcessor.ProcessFileFromEnd(newestCacheFile, line => line != "" && ReadLine(fileYear, line).Time <= rebuildFromDateTime, "");

            //var newFileContent = validCacheEntries.Concat(updateData);

            WriteCacheFile(newestCacheFile, updateData);


            //var tempFile = Path.GetTempFileName();
            //File.Move(newestCacheFile, newestCacheFile + ".TODEL");
            //File.Move(tempFile, newestCacheFile);
            //File.Delete(newestCacheFile + ".TODEL");
        }
    }
}
