using FKala.Core.DataLayer.Cache;
using FKala.Core.DataLayer.Infrastructure;
using FKala.Core.DataLayers;
using FKala.Core.Interfaces;
using FKala.Core.KalaQl;
using FKala.Core.Logic;
using FKala.Core.Model;
using Microsoft.Extensions.ObjectPool;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Globalization;
using System.Linq;
using System.Runtime.Intrinsics.Arm;
using System.Text;

namespace FKala.Core
{
    public class DataLayer_Readable_Caching_V1 : IDataLayer, IDisposable
    {
        public int ReadBuffer { get; } = 131072;
        public int WriteBuffer { get; } = 131072;

        public string DataDirectory { get; private set; }        
        public CachingLayer CachingLayer { get; private set; }
        public BufferedWriterService WriterSvc { get; private set; }

        ConcurrentDictionary<string, byte> CreatedDirectories = new ConcurrentDictionary<string, byte>();
        DefaultObjectPool<StringBuilder> stringBuilderPool = new DefaultObjectPool<StringBuilder>(new StringBuilderPooledObjectPolicy());

        public DataLayer_Readable_Caching_V1(string storagePath)
        {
            Init(storagePath);
        }

        
        public DataLayer_Readable_Caching_V1(string storagePath, int readBuffer, int writeBuffer)
        {
            this.WriteBuffer = writeBuffer;
            this.ReadBuffer = readBuffer;
            Init(storagePath);
        }

        private void Init(string storagePath)
        {
            storagePath = storagePath.Replace('\\', Path.DirectorySeparatorChar)
                                       .Replace('/', Path.DirectorySeparatorChar);

            this.DataDirectory = Path.Combine(storagePath, "data");
            Directory.CreateDirectory(this.DataDirectory);
            CachingLayer = new CachingLayer(this, storagePath);
            WriterSvc = new BufferedWriterService(WriteBuffer);
        }

        public IEnumerable<DataPoint> LoadData(string measurement, DateTime startTime, DateTime endTime, CacheResolution cacheResolution, bool newestOnly, bool doSortRawFiles, KalaQlContext context)
        {
            if (newestOnly)
            {
                return this.LoadNewestDatapoint(measurement, context);
            }
            else if (cacheResolution.Resolution != Resolution.Full)
            {
                return CachingLayer.LoadDataFromCache(measurement, startTime, endTime, cacheResolution, context);
            }
            else
            {
                return LoadFullResolution(measurement, startTime, endTime, doSortRawFiles, context);
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


        public IEnumerable<DataPoint> LoadNewestDatapoint(string measurement, KalaQlContext context)
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
                        var dp = DatFileParser.ParseLine(fileyear, filemonth, fileday, lastLine, file, -1, context);
                        yield return dp;
                        yield break;
                    }
                }
            }
        }

        private IEnumerable<DataPoint> LoadFullResolution(string measurement, DateTime startTime, DateTime endTime, bool doSortRawFiles, KalaQlContext context)
        {
            var measurementPathPart = PathSanitizer.SanitizePath(measurement);
            var measurementPath = Path.Combine(DataDirectory, measurementPathPart);

            using (var sa = StorageAccess.ForRead(measurementPath, measurementPathPart, startTime, endTime, context, doSortRawFiles))
            {
                foreach (var dp in sa.OpenStreamReaders().StreamDataPoints())
                {
                    yield return dp;
                }
            }
        }

        public async IAsyncEnumerable<Dictionary<string, object>> MergeRawFilesFromMeasurementToMeasurement(string measurement, string targetmeasurement, KalaQlContext context)
        {
            var measurementPathPart = PathSanitizer.SanitizePath(measurement);
            var measurementPath = Path.Combine(DataDirectory, measurementPathPart);
            var day = DateTime.MinValue.AddDays(1);
            using (var sa = StorageAccess.ForMerging(measurementPath, measurementPathPart, context))
            {
                foreach (var dp in sa.OpenStreamReaders().StreamMergeDataPoints())
                {
                    var filePath = GetInsertTargetFilepath(targetmeasurement, dp.Time.ToString("yyyy-MM-dd"));
                    if (dp.Source != null && dp.Source.StartsWith(filePath + ","))
                    {
                        continue;
                    }

                    Insert($"{targetmeasurement} {dp.Time.ToString("yyyy-MM-ddTHH:mm:ss.fffffff")} {(dp.Value.HasValue ? dp.Value : dp.ValueText)}", dp.Source, filePath);

                    if (day < dp.Time)
                    {
                        day = dp.Time.AddDays(1);
                        yield return new Dictionary<string, object>() { { "msg", $"rewrite of day {dp.Time.Date} done" } };
                    }
                }
            }
            yield return new Dictionary<string, object>() { { "msg", $"reinserted/merged {measurementPath} to {targetmeasurement}" } };
        }

        public async IAsyncEnumerable<Dictionary<string, object>> Cleanup(string measurement, KalaQlContext context, bool cleanSorted = false)
        {
            var measurementPathPart = PathSanitizer.SanitizePath(measurement);
            var measurementPath = Path.Combine(DataDirectory, measurementPathPart);

            using (var sa = StorageAccess.ForCleanup(measurementPath, measurementPathPart, context))
            {
                var readers = sa.GetReaders();
                foreach (var rt in readers)
                {
                    var measure = new DirectoryInfo(rt.FilePath).Parent!.Parent!.Parent!.Name;
                    var filename = Path.GetFileName(rt.FilePath);
                    var measureInName = filename.Substring(0, filename.Length - "_yyyy-MM-dd.dat".Length);

                    if (measure != measureInName || (rt.FilePath[rt.FilePath.Length - 15] == '#' && cleanSorted))
                    {
                        File.Delete(rt.FilePath);
                        yield return new Dictionary<string, object>() { { "msg", $"cleaned up {rt.FilePath}" } };
                    }
                }
            }
            yield break;
        }


        public async IAsyncEnumerable<Dictionary<string, object>> MoveMeasurement(string measurementOld, string measurementNew, KalaQlContext context)
        {
            var measurementPathPartOld = PathSanitizer.SanitizePath(measurementOld);
            var measurementPathOld = Path.Combine(DataDirectory, measurementPathPartOld);

            var measurementPathPartNew = PathSanitizer.SanitizePath(measurementNew);
            var measurementPathNew = Path.Combine(DataDirectory, measurementPathPartNew);
            var measurementPathNewBak = Path.Combine(DataDirectory, measurementPathPartNew + $".bak_{DateTime.Now.ToString("s")}");

            if (Directory.Exists(measurementPathNew))
            {
                Directory.Move(measurementPathNew, measurementPathNewBak);
            }

            Directory.Move(measurementPathOld, measurementPathNew);
            yield return new Dictionary<string, object>() { { "msg", $"renamed {measurementPathOld} to {measurementPathNew} (backup in {measurementPathNewBak}" } };
        }

        public void Insert(string rawData)
        {
            Insert(rawData, "input");
        }

        /// <summary>
        /// Expects Data in the Form
        /// "<measurement> <timestamp:YYYY-MM-DDTHH:mm:ss.zzzzzzz <value>"
        /// </summary>
        /// <param name="rawData"></param>
        /// <param name="locking"></param>
        public void Insert(string rawData, string source)
        {
            string measurement, datetimeHHmmssfffffff, valueString;
            ReadOnlySpan<char> datetime_yyyy_MM_dd;
            ParseRawData(rawData, out measurement, out datetime_yyyy_MM_dd, out datetimeHHmmssfffffff, out valueString);
            string filePath = GetInsertTargetFilepath(measurement, datetime_yyyy_MM_dd);

            WriterSvc.DoWrite(filePath, (writer) =>
            {
                // Format the line to write
                writer.Append(datetimeHHmmssfffffff);
                writer.Append(" ");
                writer.Append(valueString);
                writer.AppendNewline();
            });
        }

        /// <summary>
        /// Expects Data in the Form
        /// "<measurement> <timestamp:YYYY-MM-DDTHH:mm:ss.zzzzzzz <value>"
        /// </summary>
        /// <param name="rawData"></param>
        /// <param name="locking"></param>
        public void Insert(string rawData, string source, string filePath)
        {
            string measurement, datetimeHHmmssfffffff, valueString;
            ReadOnlySpan<char> datetime;
            ParseRawData(rawData, out measurement, out datetime, out datetimeHHmmssfffffff, out valueString);

            WriterSvc.DoWrite(filePath, (writer) =>
            {
                // Format the line to write
                writer.Append(datetimeHHmmssfffffff);
                writer.Append(" ");
                writer.Append(valueString);
                writer.AppendNewline();
            });
        }

        private string GetInsertTargetFilepath(string measurement, ReadOnlySpan<char> datetime_yyyy_MM_dd)
        {
            // Create the directory path
            var directoryPath = Path.Combine(DataDirectory, measurement, datetime_yyyy_MM_dd.Slice(0, 4).ToString(), datetime_yyyy_MM_dd.Slice(5, 2).ToString());

            if (!CreatedDirectories.ContainsKey(directoryPath))
            {
                Directory.CreateDirectory(directoryPath);
                CreatedDirectories.AddOrUpdate(directoryPath, 0, (string key, byte old) => { return 0; });
            }

            // Create the file path
            var sb = stringBuilderPool.Get();
            sb.Clear();
            sb.Append(measurement);
            sb.Append('_'); //marks new as unsorted
            sb.Append(datetime_yyyy_MM_dd.Slice(0, 10)); //YYYY-MM-dd
            sb.Append(".dat");
            var filePath = Path.Combine(directoryPath, sb.ToString());
            stringBuilderPool.Return(sb);
            return filePath;
        }

        private static void ParseRawData(string rawData, out string measurement, out ReadOnlySpan<char> datetime, out string datetimeHHmmssfffffff, out string valueString)
        {
            // Parse the raw data
            ReadOnlySpan<char> span = rawData.AsSpan();

            int index = span.IndexOf(' ');
            measurement = span.Slice(0, index).ToString();
            measurement = PathSanitizer.SanitizePath(measurement);

            span = span.Slice(index + 1);
            index = 27; //Länge von yyyy-MM-ddTHH:mm:ss.fffffff hartkodiert statt Ende suchen
            datetime = span.Slice(0, index);
            datetimeHHmmssfffffff = datetime.Slice(11).ToString();
            span = span.Slice(index + 1);
            ReadOnlySpan<char> valueRaw = null;
            valueRaw = span.Slice(0);
            valueString = valueRaw.ToString().Replace('\n', '|');
        }

        public List<string> LoadMeasurementList()
        {
            var measurements = Directory.GetDirectories(DataDirectory);
            return measurements.Select(d => Path.GetFileName(d)).ToList();
        }

        public void Dispose()
        {
            this.WriterSvc.Dispose();
        }

        public void Flush()
        {
            this.WriterSvc.ForceFlushWriters();
        }

        public void Flush(string filePath)
        {
            this.WriterSvc.ForceFlushWriter(filePath);
        }
    }
}
