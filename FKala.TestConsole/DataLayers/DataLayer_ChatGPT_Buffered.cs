using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using FKala.TestConsole.Interfaces;
using FKala.TestConsole.Logic;
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

        public IEnumerable<DataPoint> ReadData(string measurement, DateTime startTime, DateTime endTime)
        {
            //var results = new List<DataPoint>();

            try
            {
                //Locker.EnterReadLock();
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
                                DateOnly dt = new DateOnly(int.Parse(dateSpan.Slice(0, 4)), int.Parse(dateSpan.Slice(5, 2)), int.Parse(dateSpan.Slice(8, 2)));
                                
                                var sr = new StreamReader(file, Encoding.UTF8, false, 16384);
                                string? line;
                                while ((line = sr.ReadLine()) != null) {
                                    ReadOnlySpan<char> span = line.AsSpan();

                                    //var time = span.Slice(0, 16).ToString();
                                    var tt = new TimeOnly(int.Parse(span.Slice(0, 2)), int.Parse(span.Slice(3, 2)), int.Parse(span.Slice(6, 2)));
                                    
                                    var dateTime = new DateTime(dt, tt);
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
                                    yield return new DataPoint
                                    {
                                        Time = dateTime,
                                        Value = value,
                                        Text = text
                                    };
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
                lock (filePath)
                {
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
                    lock(writer.Key)
                    {                        
                        writer.Value.Dispose();
                        _bufferedWriters.Remove(writer.Key, out var removed);
                    }
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
            _streamWriter = new StreamWriter(_fileStream, Encoding.UTF8);
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
