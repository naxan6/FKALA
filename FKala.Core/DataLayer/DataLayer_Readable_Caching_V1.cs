using FKala.Core.DataLayer.Cache;
using FKala.Core.DataLayer.Infrastructure;
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
        public BufferedWriterService WriterSvc { get; } = new BufferedWriterService();

        ConcurrentBag<string> CreatedDirectories = new ConcurrentBag<string>();
        DefaultObjectPool<StringBuilder> stringBuilderPool = new DefaultObjectPool<StringBuilder>(new StringBuilderPooledObjectPolicy());

        public DataLayer_Readable_Caching_V1(string storagePath)
        {
            this.DataDirectory = Path.Combine(storagePath, "data");
            Directory.CreateDirectory(this.DataDirectory);

            CachingLayer = new CachingLayer(this, storagePath);

            
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
                        var dp = DatFileParser.ParseLine(fileyear, filemonth, fileday, lastLine);
                        yield return dp;
                        yield break;
                    }
                }
            }
        }

        private IEnumerable<DataPoint> LoadFullResolution(string measurement, DateTime startTime, DateTime endTime)
        {
            var measurementPathPart = PathSanitizer.SanitizePath(measurement);
            var measurementPath = Path.Combine(DataDirectory, measurementPathPart);

            using (var sa = StorageAccess.Init(measurementPath, measurementPathPart, startTime, endTime))
            {
                foreach (var dp in sa.OpenStreamReaders().StreamDataPoints()) {
                    yield return dp;
                }
            }
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

            int index = span.IndexOf(' ');
            var measurement = span.Slice(0, index).ToString();
            measurement = PathSanitizer.SanitizePath(measurement);

            span = span.Slice(index + 1);
            index = 27; //Länge von yyyy-MM-ddTHH:mm:ss.fffffff hartkodiert statt Ende suchen
            var datetime = span.Slice(0, index);
            string datetimeHHmmss = datetime.Slice(11).ToString();

            span = span.Slice(index + 1);
            ReadOnlySpan<char> valueRaw = null;            
            valueRaw = span.Slice(0);
            var valueString = valueRaw.ToString();

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
            WriterSvc.DoWrite(filePath, (writer) =>
            {
                // Format the line to write
                writer.Append(datetimeHHmmss);
                writer.Append(" ");
                writer.Append(valueString);
                writer.AppendNewline();
            });
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
    }
}
