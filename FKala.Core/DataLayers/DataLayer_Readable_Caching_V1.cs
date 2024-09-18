using FKala.Core.DataLayers;
using FKala.Core.Interfaces;
using FKala.Core.Logic;
using FKala.Core.Model;
using Microsoft.Extensions.ObjectPool;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Runtime.Intrinsics.Arm;
using System.Text;

namespace FKala.Core
{
    public partial class DataLayer_Readable_Caching_V1 : IDataLayer, IDisposable
    {
        private string DataDirectory;

        public CachingLayer CachingLayer { get; }
        private readonly ConcurrentDictionary<string, IBufferedWriter> _bufferedWriters = new ConcurrentDictionary<string, IBufferedWriter>();
        ConcurrentBag<string> CreatedDirectories = new ConcurrentBag<string>();
        DefaultObjectPool<StringBuilder> stringBuilderPool = new DefaultObjectPool<StringBuilder>(new StringBuilderPooledObjectPolicy());

        Task BufferedWriterFlushTask;


        public DataLayer_Readable_Caching_V1(string storagePath)
        {
            this.DataDirectory = Path.Combine(storagePath, "data");
            Directory.CreateDirectory(this.DataDirectory);

            CachingLayer = new CachingLayer(this, storagePath);

            BufferedWriterFlushTask = Task.Run(() => FlushBuffersPeriodically());
        }

        public IEnumerable<DataPoint> LoadData(string measurement, DateTime startTime, DateTime endTime, CacheResolution cacheResolution, bool newestOnly)
        {
            if (newestOnly)
            {
                return this.LoadNewestDatapoint(measurement);
            }
            else if (cacheResolution.Resolution != Resolution.Full)
            {
                return CachingLayer.LoadDataFromCache(measurement, startTime, endTime, cacheResolution);
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
                            yield break;
                        }
                        var fn = Path.GetFileNameWithoutExtension(file);
                        var datePart = fn.Substring(fn.Length - 10, 10);
                        ReadOnlySpan<char> dateSpan = datePart.AsSpan();
                        int fileyear = int.Parse(dateSpan.Slice(0, 4));
                        int filemonth = int.Parse(dateSpan.Slice(5, 2));
                        int fileday = int.Parse(dateSpan.Slice(8, 2));
                        var dp = ParseLine(fileyear, filemonth, fileday, lastLine);
                        yield return dp;
                        yield break;
                    }
                }
            }
        }

        private IEnumerable<DataPoint> LoadFullResolution(string measurement, DateTime startTime, DateTime endTime)
        {
            var startYear = startTime.Year;
            var endYear = endTime.Year;

            var measurementPathPart = PathSanitizer.SanitizePath(measurement);
            var measurementPath = Path.Combine(DataDirectory, measurementPathPart);

            SortedDictionary<DateTime, ReaderTuple> streamReaders = null;
            try
            {
                streamReaders = StorageHelper.Init(measurementPath, measurementPathPart, startTime, endTime)
                .OpenStreamReaders()
                .GetOpenStreamReaders();

                string? line;
                List<DataPoint> dataPoints = new List<DataPoint>();
                foreach (var streamreaderTuple in streamReaders)
                {
                    StreamReader sr = streamreaderTuple.Value.StreamReader;
                    int fileyear = streamreaderTuple.Key.Year;
                    int filemonth = streamreaderTuple.Key.Month;
                    int fileday = streamreaderTuple.Key.Day;
                    while ((line = sr.ReadLine()) != null)
                    {
                        var ret = ParseLine(fileyear, filemonth, fileday, line);
                        if (ret.Time >= startTime && ret.Time < endTime)
                        {
                            dataPoints.Add(ret);
                        }
                    }
                    sr.Close();

                    dataPoints.Sort((a, b) => a.Time.CompareTo(b.Time));
                    foreach (var dp in dataPoints)
                    {
                        yield return dp;
                    }
                    dataPoints.Clear();
                }
            }
            finally
            {
                streamReaders!.AsParallel().ForAll(sr => sr.Value?.StreamReader?.Dispose());
            }
            yield break;
        }


        private List<int> GetMonthFolders(string yearDir)
        {
            var entries = Directory.GetDirectories(yearDir, "*", new EnumerationOptions() { ReturnSpecialDirectories = false, BufferSize = 16384, });
            return entries.Where(e => Path.GetFileName(e) != ".DS_Store").Select(y => int.Parse(Path.GetFileName(y))).ToList();
        }

        private static DataPoint ParseLine(int fileyear, int filemonth, int fileday, string? line)
        {
            ReadOnlySpan<char> span = line.AsSpan();
            var dateTime = new DateTime(fileyear, filemonth, fileday, int.Parse(span.Slice(0, 2)), int.Parse(span.Slice(3, 2)), int.Parse(span.Slice(6, 2)), DateTimeKind.Utc);

            dateTime.AddTicks(int.Parse(span.Slice(9, 7)));
            span = span.Slice(17);

            var valueRaw = span.Slice(0);
            decimal? value = null;
            string? valuetext = null;
            try
            {
                value = decimal.Parse(valueRaw, CultureInfo.InvariantCulture);
            }
            catch (Exception)
            {
                valuetext = valueRaw.ToString();
            }

            return new DataPoint
            {
                Time = dateTime,
                Value = value
                // ValueText = valuetext
            };
        }

        /// <summary>
        /// Expects Data in the Form
        /// "<measurement> <timestamp:YYYY-MM-DDTHH:mm:ss.zzzzzzz <value>"
        /// </summary>
        /// <param name="rawData"></param>
        /// <param name="locking"></param>
        public void Insert(string rawData, bool locking = true)
        {

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
            StringBuilder sb = stringBuilderPool.Get();
            sb.Clear();
            sb.Append(measurement);
            sb.Append('_');
            sb.Append(datetime.Slice(0, 10));
            sb.Append(".dat");

            var filePath = Path.Combine(directoryPath, sb.ToString());
            stringBuilderPool.Return(sb);

            // Buffer the line
            lock (filePath)
            {
                if (!_bufferedWriters.TryGetValue(filePath, out var writer))
                {
                    try
                    {
                        writer = new BufferedWriter(filePath);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine("error at BufferedWriter with path <{filePath}>");
                        throw;
                    }
                    _bufferedWriters[filePath] = writer;
                }

                lock (writer.LOCK)
                {
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

        }

        private async Task FlushBuffersPeriodically()
        {
            while (true)
            {
                await Task.Delay(TimeSpan.FromSeconds(10));
                ForceFlushWriters();
            }
        }

        public void ForceFlushWriters()
        {
            foreach (var writer in _bufferedWriters)
            {
                lock (writer.Key)
                {
                    writer.Value.Dispose();
                    _bufferedWriters.Remove(writer.Key, out var removed);
                }
            }
            CreatedDirectories = new ConcurrentBag<string>();
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
