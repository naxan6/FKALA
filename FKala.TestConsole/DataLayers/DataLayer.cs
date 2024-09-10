using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using FKala.TestConsole.Model;
using Newtonsoft.Json;

namespace FKala.TestConsole.DataLayers
{

    public class DataLayer : IDisposable
    {
        private const string DataDirectory = "data";
        private static readonly ReaderWriterLockSlim Locker = new ReaderWriterLockSlim();
        private readonly ConcurrentDictionary<string, BufferedWriter> _bufferedWriters = new ConcurrentDictionary<string, BufferedWriter>();
        //public void Insert(string rawData)
        //{
        //    try
        //    {
        //        //Locker.EnterWriteLock();

        //        // Parse the raw data
        //        var parts = rawData.Split(' ');
        //        var measurement = parts[0];
        //        measurement = PathSanitizer.SanitizePath(measurement);
        //        var timestamp = DateTime.ParseExact(parts[1], "yyyy-MM-ddTHH:mm:ss.ffffff", CultureInfo.InvariantCulture);
        //        var value = decimal.Parse(parts[2], CultureInfo.InvariantCulture);
        //        var text = parts.Length > 3 ? string.Join(" ", parts.Skip(3)) : string.Empty;

        //        // Create the directory path
        //        var measurementPath = measurement.Replace('/', '$');
        //        var directoryPath = Path.Combine(DataDirectory, measurementPath, timestamp.ToString("yyyy"), timestamp.ToString("MM"));

        //        Directory.CreateDirectory(directoryPath);

        //        // Create the file path
        //        var filePath = Path.Combine(directoryPath, $"{measurementPath}_{timestamp:yyyy-MM-dd}.dat");                

        //        // Format the line to write
        //        var line = $"{timestamp:HH:mm:ss.ffffff} {value.ToString(CultureInfo.InvariantCulture)}";
        //        if (!string.IsNullOrEmpty(text))
        //        {
        //            line += $" {text}";
        //        }

        //        // Write the line to the file
        //        using (var fileStream = new FileStream(filePath, FileMode.Append, FileAccess.Write, FileShare.Read))
        //        using (var writer = new StreamWriter(fileStream))
        //        {
        //            writer.WriteLine(line);
        //        }
        //    }
        //    finally
        //    {
        //        //Locker.ExitWriteLock();
        //    }
        //}

        public List<DataPoint> Query(string measurement, DateTime startTime, DateTime endTime)
        {
            var results = ReadData(measurement, startTime, endTime);

            var jsonResults = results.Select(result => new DataPoint
            {
                Time = result.Time,
                Value = result.Value,
                Text = string.IsNullOrEmpty(result.Text) ? null : result.Text
            }).ToList();

            return jsonResults;
        }

        public string SerializeDatapoints(List<DataPoint> results)
        {
            var settings = new JsonSerializerSettings
            {
                NullValueHandling = NullValueHandling.Ignore
            };
            return JsonConvert.SerializeObject(results, settings);
        }

        public List<DataPoint> Aggregate(string measurement, DateTime startTime, DateTime endTime, TimeSpan windowSize, string aggregationFunction, bool includeEmptyIntervals = false, decimal? emptyIntervalValue = null)
        {
            if (windowSize <= TimeSpan.Zero)
            {
                throw new ArgumentException("Das Intervall muss größer als null sein.");
            }

            if (endTime - startTime < windowSize)
            {
                throw new ArgumentException("Das Intervall ist größer als der angegebene Zeitbereich.");
            }

            var dataPoints = ReadData(measurement, startTime, endTime)
                .OrderBy(dp => dp.Time)
                .ToList();

            var results = new List<DataPoint>();
            var currentIntervalStart = startTime;
            var currentIntervalEnd = startTime.Add(windowSize);

            while (currentIntervalStart < endTime)
            {
                var intervalDataPoints = dataPoints
                    .Where(dp => dp.Time >= currentIntervalStart && dp.Time < currentIntervalEnd)
                    .ToList();

                if (intervalDataPoints.Any())
                {
                    decimal? aggregatedValue;

                    switch (aggregationFunction.ToUpper())
                    {
                        case "AVG":
                            aggregatedValue = intervalDataPoints.Average(dp => dp.Value);
                            break;
                        case "FIRST":
                            aggregatedValue = intervalDataPoints.First().Value;
                            break;
                        case "LAST":
                            aggregatedValue = intervalDataPoints.Last().Value;
                            break;
                        case "MIN":
                            aggregatedValue = intervalDataPoints.Min(dp => dp.Value);
                            break;
                        case "MAX":
                            aggregatedValue = intervalDataPoints.Max(dp => dp.Value);
                            break;
                        case "COUNT":
                            aggregatedValue = intervalDataPoints.Count();
                            break;
                        default:
                            throw new ArgumentException("Ungültige Aggregationsfunktion");
                    }

                    results.Add(new DataPoint()
                    {
                        Time = currentIntervalStart,
                        Value = aggregatedValue
                    });
                }
                else if (includeEmptyIntervals)
                {
                    results.Add(new DataPoint()
                    {
                        Time = currentIntervalStart,
                        Value = emptyIntervalValue
                    });
                }

                currentIntervalStart = currentIntervalEnd;
                currentIntervalEnd = currentIntervalStart.Add(windowSize);
            }

            return results;
        }

        private List<DataPoint> ReadData(string measurement, DateTime startTime, DateTime endTime)
        {
            var results = new List<DataPoint>();

            try
            {
                //Locker.EnterReadLock();
                var measurementPath = measurement.Replace('/', '$');
                measurementPath = PathSanitizer.SanitizePath(measurementPath);
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

                        foreach (var file in Directory.GetFiles(monthDir, $"{measurementPath}_*.dat"))
                        {
                            if (File.Exists(file))
                            {
                                var lines = File.ReadAllLines(file);
                                foreach (var line in lines)
                                {
                                    var lineParts = line.Split(' ');
                                    var timePart = lineParts[0];
                                    var datePart = Path.GetFileNameWithoutExtension(file).Split('_')[1];
                                    var dateTimeString = $"{datePart}T{timePart}";
                                    var lineTime = DateTime.ParseExact(dateTimeString, "yyyy-MM-ddTHH:mm:ss.ffffff", CultureInfo.InvariantCulture);

                                    if (lineTime >= startTime && lineTime <= endTime)
                                    {
                                        var value = decimal.Parse(lineParts[1], CultureInfo.InvariantCulture);
                                        var text = lineParts.Length > 2 ? string.Join(" ", lineParts.Skip(2)) : null;

                                        results.Add(new DataPoint
                                        {
                                            Time = lineTime,
                                            Value = value,
                                            Text = text
                                        });
                                    }
                                }
                            }
                        }
                    }
                }
            }
            finally
            {
                //Locker.ExitReadLock();
            }

            return results;
        }


        public List<DataPoint> AddAggregatedMeasurements(string measurement1, string measurement2, DateTime startTime, DateTime endTime, TimeSpan interval, string aggregationFunction)
        {
            var aggregatedData1 = Aggregate(measurement1, startTime, endTime, interval, aggregationFunction);
            var aggregatedData2 = Aggregate(measurement2, startTime, endTime, interval, aggregationFunction);

            var aggregatedResults = new List<DataPoint>();
            var data1Dict = aggregatedData1.ToDictionary(dp => dp.Time);
            var data2Dict = aggregatedData2.ToDictionary(dp => dp.Time);

            var allKeys = data1Dict.Keys.Union(data2Dict.Keys).Distinct().OrderBy(k => k);

            foreach (var key in allKeys)
            {
                var value1 = data1Dict.ContainsKey(key) ? data1Dict[key].Value : 0;
                var value2 = data2Dict.ContainsKey(key) ? data2Dict[key].Value : 0;

                aggregatedResults.Add(new DataPoint()
                {
                    Time = key,
                    Value = value1 + value2
                });
            }

            return aggregatedResults;
        }




        public DataLayer()
        {
            Task.Run(() => FlushBuffersPeriodically());
        }

        public void Insert(string rawData)
        {
            try
            {
                Locker.EnterWriteLock();

                // Parse the raw data
                var parts = rawData.Split(' ');
                var measurement = parts[0];
                measurement = PathSanitizer.SanitizePath(measurement);
                var timestamp = DateTime.ParseExact(parts[1], "yyyy-MM-ddTHH:mm:ss.ffffff", CultureInfo.InvariantCulture);
                var value = decimal.Parse(parts[2], CultureInfo.InvariantCulture);
                var text = parts.Length > 3 ? string.Join(" ", parts.Skip(3)) : string.Empty;

                // Create the directory path
                var measurementPath = measurement.Replace('/', '$');
                var directoryPath = Path.Combine(DataDirectory, measurementPath, timestamp.ToString("yyyy"), timestamp.ToString("MM"));
                Directory.CreateDirectory(directoryPath);

                // Create the file path
                var filePath = Path.Combine(directoryPath, $"{measurementPath}_{timestamp:yyyy-MM-dd}.dat");

                // Format the line to write
                var line = $"{timestamp:HH:mm:ss.ffffff} {value.ToString(CultureInfo.InvariantCulture)}";
                if (!string.IsNullOrEmpty(text))
                {
                    line += $" {text}";
                }

                // Buffer the line
                if (!_bufferedWriters.TryGetValue(filePath, out var writer))
                {
                    writer = new BufferedWriter(filePath);
                    _bufferedWriters[filePath] = writer;
                }
                writer.WriteLine(line);
            }
            finally
            {
                Locker.ExitWriteLock();
            }
        }

        private async Task FlushBuffersPeriodically()
        {
            while (true)
            {
                await Task.Delay(TimeSpan.FromSeconds(10));
                foreach (var writer in _bufferedWriters.Values)
                {
                    writer.Flush();
                }
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

    public class BufferedWriter : IDisposable
    {
        private readonly string _filePath;
        private readonly StringBuilder _buffer = new StringBuilder();
        private readonly object _lock = new object();
        private FileStream _fileStream;
        private StreamWriter _streamWriter;

        public BufferedWriter(string filePath)
        {
            _filePath = filePath;
            _fileStream = new FileStream(filePath, FileMode.Append, FileAccess.Write, FileShare.Read);
            _streamWriter = new StreamWriter(_fileStream);
        }

        public void WriteLine(string line)
        {
            lock (_lock)
            {
                _buffer.AppendLine(line);
            }
        }

        public void Flush()
        {
            lock (_lock)
            {
                if (_buffer.Length == 0) return;

                _streamWriter.Write(_buffer.ToString());
                _buffer.Clear();
                _streamWriter.Flush();
            }
        }

        public void Dispose()
        {
            Flush();
            _streamWriter?.Dispose();
            _fileStream?.Dispose();
        }
    }
}
