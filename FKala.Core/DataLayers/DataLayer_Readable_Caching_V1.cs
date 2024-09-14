using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using FKala.TestConsole.DataLayers;
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
    public class DataLayer_Readable_Caching_V1 : IDataLayer, IDisposable
    {
        private string DataDirectory;

        public CachingLayer CachingLayer { get; }

        private string CacheDirectory = "cache";
        private static readonly ReaderWriterLockSlim Locker = new ReaderWriterLockSlim();
        private readonly ConcurrentDictionary<string, IBufferedWriter> _bufferedWriters = new ConcurrentDictionary<string, IBufferedWriter>();
        HashSet<string> CreatedDirectories = new HashSet<string>();
        StringBuilder sb = new StringBuilder();


        public DataLayer_Readable_Caching_V1(string storagePath)
        {
            this.DataDirectory = Path.Combine(storagePath, "data");
            Directory.CreateDirectory(this.DataDirectory);
            
            CachingLayer = new CachingLayer(this, storagePath);
            
            Task.Run(() => FlushBuffersPeriodically());
        }

        public IEnumerable<DataPoint> LoadData(string measurement, DateTime startTime, DateTime endTime, CacheResolution cacheResolution, bool newestOnly)
        {
            if (newestOnly)
            {
                return this.LoadNewestDatapoint(measurement);
            } 
            else if (cacheResolution?.Resolution == Resolution.Hourly)
            {
                return CachingLayer.LoadHourlyResolution(measurement, startTime, endTime, cacheResolution);
            }
            else if (cacheResolution?.Resolution == Resolution.Minutely)
            {
                return CachingLayer.LoadMinutelyResolution(measurement, startTime, endTime, cacheResolution);
            }
            else
            {
                return LoadFullResolution(measurement, startTime, endTime);
            }
        }

        public List<int> LoadAvailableYears(string measurement)
        {
            var measurementSubPath = PathSanitizer.SanitizePath(measurement);
            var measurementPath = Path.Combine(DataDirectory, measurementSubPath);
            var years = Directory.GetDirectories(measurementPath);
            var yearsInt = years.Select(dd => int.Parse(Path.GetFileName(dd))).ToList();
            return yearsInt;
        }


        public IEnumerable<DataPoint> LoadNewestDatapoint(string measurement)
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
                            yield return null;
                            yield break;
                        }
                        var fn = Path.GetFileNameWithoutExtension(file);
                        var datePart = fn.Substring(fn.Length - 10, 10);
                        ReadOnlySpan<char> dateSpan = datePart.AsSpan();
                        int fileyear = int.Parse(dateSpan.Slice(0, 4));
                        int filemonth = int.Parse(dateSpan.Slice(5, 2));
                        int fileday = int.Parse(dateSpan.Slice(8, 2));
                        var dp = ParseLine(DateTime.MinValue, DateTime.MaxValue, fileyear, filemonth, fileday, lastLine);
                        yield return dp;
                        yield break;
                    }
                }
            }
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

            var valueRaw = span.Slice(0);
            decimal? value = null;
            string? valuetext = null;
            try
            {
                 value = decimal.Parse(valueRaw, CultureInfo.InvariantCulture);
            } catch (Exception ex)
            {
                valuetext = valueRaw.ToString();
            }
            
            if (dateTime >= startTime && dateTime < endTime)
            {
                return new DataPoint
                {
                    Time = dateTime,
                    Value = value,
                    ValueText = valuetext
                };
            }
            return null;
        }

        /// <summary>
        /// Expects Data in the Form
        /// "<measurement> <timestamp:YYYY-MM-DDTHH:mm:ss.zzzzzzz <value>"
        /// </summary>
        /// <param name="rawData"></param>
        /// <param name="locking"></param>
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
                index = 27; //Länge von yyyy-MM-ddTHH:mm:ss.fffffff hartkodiert statt Ende suchen
                var datetime = span.Slice(0, index);

                span = span.Slice(index + 1);                
                decimal value;
                ReadOnlySpan<char> valueRaw = null;
                ReadOnlySpan<char> text = null;                
                valueRaw = span.Slice(0);                                        
                
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

        public List<string> LoadMeasurementList()
        {
            var measurements = Directory.GetDirectories(DataDirectory);
            return measurements.Select(d => Path.GetFileName(d)).ToList();
        }
    }
}
