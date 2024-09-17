using FKala.TestConsole.KalaQl.Windowing;
using FKala.TestConsole.Model;
using FKala.TestConsole;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FKala.Core.Interfaces;
using FKala.TestConsole.Logic;
using System.Diagnostics.Metrics;
using FKala.Core.Logic;
using System.Globalization;

namespace FKala.Core.DataLayers
{
    public abstract class Cache_Base : ICache
    {
        public abstract IEnumerable<DataPoint> GetAggregateForCaching(string measurement, DateTime start, DateTime end, AggregateFunction aggrFunc);
        public abstract string GetTimeFormat();
        public abstract DataPoint? ReadLine(int fileyear, string? line);
        public abstract string CacheSubdir {  get; }
        public abstract DateTime ShouldUpdateFromWhere(DataPoint? newestInCache, DataPoint newestInRaw);
        
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

            IEnumerable<DataPoint> rs = GetAggregateForCaching(measurement, new DateTime(year, 1, 1), new DateTime(year + 1, 1, 1), aggrFunc);
            WriteCacheFile(cacheFilePath, rs);
        }

        private void WriteCacheFile(string cacheFilePath, IEnumerable<DataPoint> rs)
        {
            NumberFormatInfo nfi = new NumberFormatInfo();
            nfi.NumberDecimalSeparator = ".";
            if (rs.Any())
            {
                var timeFormat = GetTimeFormat();
                using var bw = new BufferedWriter_NonLocking(cacheFilePath);
                foreach (var dp in rs)
                {
                    bw.Append(dp.Time.ToString(timeFormat));
                    bw.Append(" ");
                    bw.Append(dp.Value.Value.ToString(nfi));
                    bw.AppendNewline();
                }
                bw.Dispose();
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

            FileFromEndProcessor.ProcessFileFromEnd(newestCacheFile, line => line != "" && this.ReadLine(fileYear, line).Time <= rebuildFromDateTime, "");

            //var newFileContent = validCacheEntries.Concat(updateData);
            
            WriteCacheFile(newestCacheFile, updateData);


            //var tempFile = Path.GetTempFileName();
            //File.Move(newestCacheFile, newestCacheFile + ".TODEL");
            //File.Move(tempFile, newestCacheFile);
            //File.Delete(newestCacheFile + ".TODEL");
        }
    }
}
