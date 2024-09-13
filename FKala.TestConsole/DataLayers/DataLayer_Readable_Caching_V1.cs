using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using FKala.TestConsole.Interfaces;
using FKala.TestConsole.KalaQl;
using FKala.TestConsole.KalaQl.Windowing;
using FKala.TestConsole.Logic;
using FKala.TestConsole.Model;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using static System.Net.Mime.MediaTypeNames;

namespace FKala.TestConsole
{
    //NOT FUNCTIONAL AND TOO BIG FILES
    public class DataLayer_Readable_Caching_V1 : IDataLayer, IDisposable
    {
        private string DataDirectory = "data";
        private string CacheDirectory = "cache";
        private static readonly ReaderWriterLockSlim Locker = new ReaderWriterLockSlim();
        private readonly ConcurrentDictionary<string, IBufferedWriter> _bufferedWriters = new ConcurrentDictionary<string, IBufferedWriter>();
        HashSet<string> CreatedDirectories = new HashSet<string>();
        StringBuilder sb = new StringBuilder();


        public DataLayer_Readable_Caching_V1(string datapath = null, string cachepath = null)
        {
            this.DataDirectory = datapath ?? DataDirectory;
            this.CacheDirectory = cachepath ?? CacheDirectory;
            Directory.CreateDirectory(this.DataDirectory);
            Directory.CreateDirectory(this.CacheDirectory);
            Task.Run(() => FlushBuffersPeriodically());
        }

        public IEnumerable<DataPoint> ReadData(string measurement, DateTime startTime, DateTime endTime, CacheResolution cacheResolution = null)
        {
            if (cacheResolution?.Resolution == Resolution.Hourly)
            {
                return LoadHourlyResolution(measurement, startTime, endTime, cacheResolution);
            }
            else if (cacheResolution?.Resolution == Resolution.Minutely)
            {
                return LoadMinutelyResolution(measurement, startTime, endTime, cacheResolution);
            }
            else
            {
                return LoadFullResolution(measurement, startTime, endTime);
            }
        }

        private IEnumerable<DataPoint> LoadMinutelyResolution(string measurement, DateTime startTime, DateTime endTime, CacheResolution cacheResolution)
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

        private IEnumerable<int> FilterYearsForExistingRawData(string measurement, IEnumerable<int> years)
        {
            var rawYears = LoadRawYears(measurement);
            years = years.Where(year => rawYears.Contains(year));
            return years;
        }

        private List<int> LoadRawYears(string measurement)
        {
            var measurementSubPath = PathSanitizer.SanitizePath(measurement);
            var measurementPath = Path.Combine(DataDirectory, measurementSubPath);
            var years = Directory.GetDirectories(measurementPath);
            var yearsInt = years.Select (dd => int.Parse(Path.GetFileName(dd))).ToList();
            return yearsInt;
        }

        private void PrepareMinutelyData(string measurement, int year, string yearFilePath, AggregateFunction aggrFunc)
        {
            KalaResult aggResult = KalaQuery
               .Start()
               .Add(new Op_BaseQuery("fullRes", measurement, new DateTime(year, 1, 1), new DateTime(year + 1, 1, 1), PredefinedCacheResolutions.NoCache))
               .Add(new Op_Aggregate("minutely", "fullRes", Window.Aligned_1Minute, aggrFunc, false, false))
               .Add(new Op_Publish(new List<string>() { "minutely" }, PublishMode.MultipleResultsets))
               .Execute(this);
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

        private IEnumerable<DataPoint> LoadMinutelyData(DateTime startTime, DateTime endTime, int year, string yearFilePath)
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
                        Value = value,
                        Text = null
                    };
                }
            }
        }

        private IEnumerable<DataPoint> LoadHourlyResolution(string measurement, DateTime startTime, DateTime endTime, CacheResolution cacheResolution)
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

        private void PrepareHourlyData(string measurement, int year, string yearFilePath, AggregateFunction aggrFunc)
        {
            KalaResult aggResult = KalaQuery
                .Start()
                .Add(new Op_BaseQuery("fullRes", measurement, new DateTime(year, 1, 1), new DateTime(year + 1, 1, 1), PredefinedCacheResolutions.NoCache))
                .Add(new Op_Aggregate("hourly", "fullRes", Window.Aligned_1Hour, aggrFunc, false, false))
                .Add(new Op_Publish(new List<string>() { "hourly" }, PublishMode.MultipleResultsets))
                .Execute(this);

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

        private static IEnumerable<DataPoint> LoadHourlyData(DateTime startTime, DateTime endTime, int year, string yearFilePath)
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
                        Value = value,
                        Text = null
                    };
                }
            }
        }

        private DataPoint LoadNewestDatapoint(string measurement)
        {
            var measurementSubPath = PathSanitizer.SanitizePath(measurement);
            var measurementPath = Path.Combine(DataDirectory, measurementSubPath);
            foreach (var yearPath in Directory.GetDirectories(measurementPath).OrderDescending())
            {
                foreach (var monthDir in Directory.GetDirectories(yearPath).OrderDescending())
                {
                    var month = int.Parse(Path.GetFileName(monthDir));

                    foreach (var file in Directory.GetFiles(monthDir, $"{measurementSubPath}*.dat").OrderDescending())
                    {
                        var lastLine = LastLineReader.ReadLastLine(file);
                        if (string.IsNullOrEmpty(lastLine))
                        {
                            return null;
                        }
                        var fn = Path.GetFileNameWithoutExtension(file);
                        var datePart = fn.Substring(fn.Length - 10, 10);
                        ReadOnlySpan<char> dateSpan = datePart.AsSpan();
                        int fileyear = int.Parse(dateSpan.Slice(0, 4));
                        int filemonth = int.Parse(dateSpan.Slice(5, 2));
                        int fileday = int.Parse(dateSpan.Slice(8, 2));
                        var dp = ParseLine(DateTime.MinValue, DateTime.MaxValue, fileyear, filemonth, fileday, lastLine);
                        return dp;
                    }
                }
            }
            throw new Exception("No Latest Datapoint found.");
        }
        private IEnumerable<DataPoint> LoadFullResolution(string measurement, DateTime startTime, DateTime endTime)
        {

            var measurementPath = PathSanitizer.SanitizePath(measurement);
            var startYear = startTime.Year;
            var endYear = endTime.Year;

            for (int year = startYear; year <= endYear; year++)
            {
                var yearPath = Path.Combine(DataDirectory, measurementPath, year.ToString());
                if (!Directory.Exists(yearPath)) continue;

                foreach (var monthDir in Directory.GetDirectories(yearPath))
                {
                    var month = int.Parse(Path.GetFileName(monthDir));
                    if (month < startTime.Month && year == startYear) continue;
                    if (month > endTime.Month && year == endYear) continue;

                    foreach (var file in Directory.GetFiles(monthDir, $"{measurementPath}*.dat"))
                    {
                        if (File.Exists(file))
                        {
                            var fn = Path.GetFileNameWithoutExtension(file);
                            var datePart = fn.Substring(fn.Length - 10, 10);
                            ReadOnlySpan<char> dateSpan = datePart.AsSpan();
                            // DateOnly dt = new DateOnly(int.Parse(dateSpan.Slice(0, 4)), int.Parse(dateSpan.Slice(5, 2)), int.Parse(dateSpan.Slice(8, 2)));
                            int fileyear = int.Parse(dateSpan.Slice(0, 4));
                            int filemonth = int.Parse(dateSpan.Slice(5, 2));
                            int fileday = int.Parse(dateSpan.Slice(8, 2));

                            using var _fs = new FileStream(file, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
                            using var sr = new StreamReader(_fs, Encoding.UTF8, false, 16384);
                            string? line;
                            while ((line = sr.ReadLine()) != null)
                            {
                                var ret = ParseLine(startTime, endTime, fileyear, filemonth, fileday, line);
                                if (ret != null)
                                {
                                    yield return ret;
                                }
                            }
                        }
                    }
                }
            }

        }

        private static DataPoint ParseLine(DateTime startTime, DateTime endTime, int fileyear, int filemonth, int fileday, string? line)
        {
            ReadOnlySpan<char> span = line.AsSpan();

            //var time = span.Slice(0, 16).ToString();
            //var tt = new TimeOnly(int.Parse(span.Slice(0, 2)), int.Parse(span.Slice(3, 2)), int.Parse(span.Slice(6, 2)));

            //var dateTime = new DateTime(dt, tt);
            var dateTime = new DateTime(fileyear, filemonth, fileday, int.Parse(span.Slice(0, 2)), int.Parse(span.Slice(3, 2)), int.Parse(span.Slice(6, 2)), DateTimeKind.Utc);

            dateTime.AddTicks(int.Parse(span.Slice(9, 7)));
            span = span.Slice(17);

            int index = span.IndexOf(' ');
            decimal value;
            string? text = null;
            if (index != -1)
            {
                var valueRaw = span.Slice(0, index);
                value = decimal.Parse(valueRaw, CultureInfo.InvariantCulture);
                text = span.Slice(index + 1).ToString();
            }
            else
            {
                var valueRaw = span.Slice(0);
                value = decimal.Parse(valueRaw, CultureInfo.InvariantCulture);
            }
            if (dateTime >= startTime && dateTime < endTime)
            {
                return new DataPoint
                {
                    Time = dateTime,
                    Value = value,
                    Text = text
                };
            }
            return null;
        }

        public void Insert(string rawData, bool locking = true)
        {
            try
            {
                if (locking) Locker.EnterWriteLock();

                // Parse the raw data
                ReadOnlySpan<char> span = rawData.AsSpan();

                // TODO: Bug spaces in path?
                int index = span.IndexOf(' ');
                var measurement = span.Slice(0, index).ToString();
                measurement = PathSanitizer.SanitizePath(measurement);

                span = span.Slice(index + 1);
                //index = span.IndexOf(' ');
                index = 27; //Länge von yyyy-MM-ddTHH:mm:ss.fffffff hartkodiert statt Ende suchen
                var datetime = span.Slice(0, index);
                //var timestamp = DateTime.ParseExact(datetime, "yyyy-MM-ddTHH:mm:ss.fffffff", CultureInfo.InvariantCulture);


                span = span.Slice(index + 1);
                index = span.IndexOf(' ');
                decimal value;
                ReadOnlySpan<char> valueRaw = null;
                ReadOnlySpan<char> text = null;
                if (index != -1)
                {
                    valueRaw = span.Slice(0, index);
                    // value = decimal.Parse(valueRaw, CultureInfo.InvariantCulture);
                    text = span.Slice(index + 1);
                }
                else
                {
                    valueRaw = span.Slice(0);
                    // value = decimal.Parse(valueRaw, CultureInfo.InvariantCulture);
                }

                // Create the directory path
                var directoryPath = Path.Combine(DataDirectory, measurement, datetime.Slice(0, 4).ToString(), datetime.Slice(5, 2).ToString());
                if (!CreatedDirectories.Contains(directoryPath))
                {
                    Directory.CreateDirectory(directoryPath);
                    CreatedDirectories.Add(directoryPath);
                }


                // Create the file path
                sb.Clear();
                sb.Append(measurement);
                sb.Append('_');
                sb.Append(datetime.Slice(0, 10));
                sb.Append(".dat");

                var filePath = Path.Combine(directoryPath, sb.ToString());

                // Buffer the line
                lock (filePath)
                {
                    if (!_bufferedWriters.TryGetValue(filePath, out var writer))
                    {
                        if (locking)
                        {
                            writer = new BufferedWriter_Locking(filePath);
                        }
                        else
                        {
                            writer = new BufferedWriter_NonLocking(filePath);
                        }

                        _bufferedWriters[filePath] = writer;
                    }

                    // Format the line to write
                    writer.Append(datetime.Slice(11));
                    writer.Append(" ");
                    writer.Append(valueRaw);

                    if (text != null)
                    {
                        writer.Append(" ");
                        writer.Append(text);
                    }

                    writer.AppendNewline();
                }
            }
            finally
            {
                if (locking) Locker.ExitWriteLock();
            }
        }

        private async Task FlushBuffersPeriodically()
        {
            while (true)
            {
                await Task.Delay(TimeSpan.FromSeconds(10));
                foreach (var writer in _bufferedWriters)
                {
                    lock (writer.Key)
                    {
                        writer.Value.Dispose();
                        _bufferedWriters.Remove(writer.Key, out var removed);
                    }
                }
                CreatedDirectories = new HashSet<string>();
            }
        }

        public void Dispose()
        {
            foreach (var writer in _bufferedWriters.Values)
            {
                writer.Dispose();
            }
        }
    }
}
