using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using FKala.TestConsole.Interfaces;
using FKala.TestConsole.Model;
using Newtonsoft.Json;

namespace FKala.TestConsole
{

    public class DataLayer_ChatGPT_Buffered : IDataLayer, IDisposable
    {
        private const string DataDirectory = "data";
        private static readonly ReaderWriterLockSlim Locker = new ReaderWriterLockSlim();
        private readonly ConcurrentDictionary<string, BufferedWriter> _bufferedWriters = new ConcurrentDictionary<string, BufferedWriter>();

        public DataLayer_ChatGPT_Buffered()
        {
            Task.Run(() => FlushBuffersPeriodically());
        }

        public List<DataPoint> ReadData(string measurement, DateTime startTime, DateTime endTime)
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

        HashSet<string> CreatedDirectories = new HashSet<string>();
        StringBuilder sb = new StringBuilder();
        public void Insert(string rawData, bool locking = true)
        {
            try
            {
                if (locking) Locker.EnterWriteLock();

                // Parse the raw data
                ReadOnlySpan<char> span = rawData.AsSpan();

                int index = span.IndexOf(' ');
                var measurement = span.Slice(0, index).ToString();
                measurement = PathSanitizer.SanitizePath(measurement);

                span = span.Slice(index + 1);
                index = span.IndexOf(' ');
                var datetime = span.Slice(0, index);
                var timestamp = DateTime.ParseExact(datetime, "yyyy-MM-ddTHH:mm:ss.fffffff", CultureInfo.InvariantCulture);

                span = span.Slice(index + 1);
                index = span.IndexOf(' ');
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

                // Create the directory path
                var directoryPath = Path.Combine(DataDirectory, measurement, timestamp.ToString("yyyy"), timestamp.ToString("MM"));
                if (!CreatedDirectories.Contains(directoryPath))
                {
                    Directory.CreateDirectory(directoryPath);
                    CreatedDirectories.Add(directoryPath);
                }


                // Create the file path
                sb.Clear();
                sb.Append(measurement);
                sb.Append(timestamp.ToString("yyyy-MM-dd"));
                sb.Append(".dat");

                var filePath = Path.Combine(directoryPath, sb.ToString());

                // Buffer the line
                if (!_bufferedWriters.TryGetValue(filePath, out var writer))
                {
                    writer = new BufferedWriter(filePath);
                    _bufferedWriters[filePath] = writer;
                }

                // Format the line to write
                writer.Append(timestamp.ToString("HH:mm:ss.fffffff"));
                writer.Append(" ");
                writer.Append(value.ToString(CultureInfo.InvariantCulture));

                if (!string.IsNullOrEmpty(text))
                {
                    writer.Append(" ");
                    writer.Append(text);
                }

                writer.AppendNewline();
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


        public void Append(string text)
        {
            lock (_lock)
            {
                _buffer.Append(text);
            }
        }
        public void AppendNewline()
        {
            lock (_lock)
            {
                _buffer.Append("\n");
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
