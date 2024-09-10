using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using Newtonsoft.Json;

namespace FKala.TestConsole
{
    public class DataLayer
    {
        private const string DataDirectory = "data";
        private static readonly ReaderWriterLockSlim Locker = new ReaderWriterLockSlim();

        public void Insert(string rawData)
        {
            try
            {
                Locker.EnterWriteLock();

                // Parse the raw data
                var parts = rawData.Split(' ');
                var measurement = parts[0];
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

                // Write the line to the file
                using (var fileStream = new FileStream(filePath, FileMode.Append, FileAccess.Write, FileShare.Read))
                using (var writer = new StreamWriter(fileStream))
                {
                    writer.WriteLine(line);
                }
            }
            finally
            {
                Locker.ExitWriteLock();
            }
        }

        public string Query(string measurement, DateTime startTime, DateTime endTime)
        {
            var results = new List<object>();
            try
            {
                Locker.EnterReadLock();

                var measurementPath = measurement.Replace('/', '$');
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

                                        var result = new
                                        {
                                            t = dateTimeString,
                                            v = value,
                                            txt = text
                                        };

                                        results.Add(result);
                                    }
                                }
                            }
                        }
                    }
                }
            }
            finally
            {
                Locker.ExitReadLock();
            }
            var settings = new JsonSerializerSettings
            {
                NullValueHandling = NullValueHandling.Ignore
            };
            return JsonConvert.SerializeObject(results, settings);
        }

        public string Aggregate(string measurement, DateTime startTime, DateTime endTime, TimeSpan interval, string aggregationFunction, bool includeEmptyIntervals = false, decimal? emptyIntervalValue = null)
        {
            var results = new List<object>();
            try
            {
                Locker.EnterReadLock();

                var measurementPath = measurement.Replace('/', '$');
                var startYear = startTime.Year;
                var endYear = endTime.Year;

                var allDataPoints = new List<(DateTime Time, decimal Value)>();

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
                                        allDataPoints.Add((lineTime, value));
                                    }
                                }
                            }
                        }
                    }
                }

                allDataPoints = allDataPoints.OrderBy(dp => dp.Time).ToList();
                var currentIntervalStart = startTime;
                var currentIntervalEnd = startTime.Add(interval);

                while (currentIntervalStart < endTime)
                {
                    var intervalDataPoints = allDataPoints
                        .Where(dp => dp.Time >= currentIntervalStart && dp.Time < currentIntervalEnd)
                        .ToList();

                    if (intervalDataPoints.Any())
                    {
                        decimal aggregatedValue;

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
                            default:
                                throw new ArgumentException("Invalid aggregation function");
                        }

                        results.Add(new
                        {
                            t = currentIntervalStart.ToString("yyyy-MM-ddTHH:mm:ss.ffffff"),
                            v = aggregatedValue
                        });
                    }
                    else if (includeEmptyIntervals)
                    {
                        results.Add(new
                        {
                            t = currentIntervalStart.ToString("yyyy-MM-ddTHH:mm:ss.ffffff"),
                            v = emptyIntervalValue
                        });
                    }

                    currentIntervalStart = currentIntervalEnd;
                    currentIntervalEnd = currentIntervalStart.Add(interval);
                }
            }
            finally
            {
                Locker.ExitReadLock();
            }

            return JsonConvert.SerializeObject(results);
        }
    }
}
