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

        public abstract DateTime ShouldUpdateFromWhere(int cacheYear, DataPoint? newestInCache, DataPoint? newestInRaw);

        public IEnumerable<DataPoint?> LoadNewestDatapoint(string newestFile)
        {
            var parts = newestFile.Split('_');
            var fileYear = int.Parse(parts[parts.Length - 2]);
            var lastLine = LastLineReader.ReadLastLine(newestFile);
            var datapoint = ReadLine(fileYear, lastLine);
            datapoint.Source = $"{newestFile}, Last Line";
            yield return datapoint;
        }
        public void GenerateWholeYearCache(string measurement, int year, string cacheFilePath, AggregateFunction aggrFunc, bool forceRebuild)
        {
            if (forceRebuild)
            {
                if (File.Exists(cacheFilePath)) File.Delete(cacheFilePath);
            }

            IEnumerable<DataPoint> rs = GetAggregateForCaching(measurement, new DateTime(year, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(year + 1, 1, 1, 0, 0, 0, DateTimeKind.Utc), aggrFunc);
            WriteCacheFile(cacheFilePath, rs, false);
        }

        private void WriteCacheFile(string cacheFilePath, IEnumerable<DataPoint> rs, bool append)
        {
            if (rs.Any())
            {
                var timeFormat = GetTimeFormat();
                var writerSvc = DataLayer.WriterSvc;
                writerSvc.CreateWriteDispose(cacheFilePath, append, (writer) =>
                {
                    foreach (var dp in rs)
                    {
                        if (dp.Value != null)
                        {

                            writer.Append(dp.StartTime.ToString(timeFormat));
                            writer.Append(" ");
                            writer.Append(dp.Value.Value.ToString(CultureInfo.InvariantCulture));
                            writer.AppendNewline();
                        };
                    }
                });
            }
        }

        public IEnumerable<DataPoint> LoadCache(DateTime startTime, DateTime endTime, int year, string yearFilePath, int readBuffer)
        {
            if (!File.Exists(yearFilePath)) yield break;
            int fileyear = year;
            using var _fs = new FileStream(yearFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            using var sr = new StreamReader(_fs, Encoding.UTF8, false, readBuffer);
            string? line;
            NavigateTo(fileyear, sr, startTime);
            while ((line = sr.ReadLine()) != null)
            {
                var datapoint = ReadLine(fileyear, line);
                datapoint.Source = $"Cache {yearFilePath} Line {line}";
                if (datapoint.StartTime >= startTime && datapoint.StartTime < endTime)
                {
                    yield return datapoint;
                }
                //TODO für später: breaken, sobald endtime überschritten, nicht mehr komplett durchlaufen
            }
        }

        private void NavigateTo(int fileyear, StreamReader sr, DateTime startTime)
        {
            DataPoint? first = ReadNextLine(fileyear, sr);
            if (first == null || first.StartTime >= startTime)
            {
                return;
            }


            long fullLength = sr.BaseStream.Length;
            long position = fullLength / 2;
            long jumpintervall = fullLength / 4;
            DateTime lineTime = DateTime.MinValue;
            while (true)
            {
                sr.BaseStream.Position = position;
                sr.DiscardBufferedData();
                DataPoint? current = ReadNextLine(fileyear, sr);
                if (current == null || 
                    jumpintervall == 0 ||
                    (current.StartTime < startTime.AddMinutes(-1) && jumpintervall < 1024)) //BUG/HACK: will only work in some cases
                    //(current.StartTime < startTime.AddMinutes(-1))) //BUG/HACK: will only work in some cases
                {
                    break;
                } 
                else
                {
                    if (current.StartTime < startTime) // position is too early in the file
                    {
                        Console.WriteLine("jump forwards");
                        jumpintervall = jumpintervall / 2;
                        position = position + jumpintervall;
                    } 
                    else if (current.StartTime >= startTime) // position is to late in the file
                    {
                        Console.WriteLine("jump backwards");
                        position = position - jumpintervall;
                    }
                }
            }
        }

        private DataPoint? ReadNextLine(int fileyear, StreamReader sr)
        {
            sr.ReadLine();
            string? line;
            if ((line = sr.ReadLine()) != null) {
                return ReadLine(fileyear, line);
            } 
            else
            {
                return null;
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
            IEnumerable<DataPoint> updateData = GetAggregateForCaching(measurement, rebuildFromDateTime, new DateTime(fileYear, 12, 31,0,0,0, DateTimeKind.Utc), aggrFunc);

            FileFromEndProcessor.ProcessFileFromEnd(newestCacheFile, line => line != "" && ReadLine(fileYear, line).StartTime <= rebuildFromDateTime, "");

            //var newFileContent = validCacheEntries.Concat(updateData);

            WriteCacheFile(newestCacheFile, updateData, true);


            //var tempFile = Path.GetTempFileName();
            //File.Move(newestCacheFile, newestCacheFile + ".TODEL");
            //File.Move(tempFile, newestCacheFile);
            //File.Delete(newestCacheFile + ".TODEL");
        }
    }
}
