using FKala.Core.DataLayer.Cache;
using FKala.Core.DataLayer.Infrastructure;
using FKala.Core.DataLayers;
using FKala.Core.Helper;
using FKala.Core.Interfaces;
using FKala.Core.KalaQl;
using FKala.Core.Logic;
using FKala.Core.Model;
using Microsoft.Extensions.ObjectPool;
using Microsoft.VisualBasic;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Globalization; // Bereits vorhanden, aber zur Sicherheit
using System.IO; // Hinzugefügt
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics.Arm;
using System.Text;

namespace FKala.Core
{
    public class DataLayer_Readable_Caching_V1 : IDataLayer, IDisposable
    {
        private readonly int FilenameDatePatternLength = "yyyy.MM.dd.dat".Length;
        public int ReadBuffer { get; } = 131072;
        public int WriteBuffer { get; } = 131072;

        public string DataDirectory { get; init; }
        public string BlacklistDirectory { get; init; }
        public CachingLayer CachingLayer { get; init; }
        public BufferedWriterService BufferedWriterSvc { get; init; }
        public bool ShuttingDown { get; private set; }

        ConcurrentDictionary<string, bool> MeasurementBlacklist = new ConcurrentDictionary<string, bool>();
        ConcurrentDictionary<string, string> LatestEntries = new ConcurrentDictionary<string, string>();

        ConcurrentDictionary<string, byte> CreatedDirectories = new ConcurrentDictionary<string, byte>();
        DefaultObjectPool<StringBuilder> stringBuilderPool = new DefaultObjectPool<StringBuilder>(new StringBuilderPooledObjectPolicy());
        private LockManager LockManager;

        public DataLayer_Readable_Caching_V1(string storagePath)
        {
            storagePath = storagePath.Replace('\\', Path.DirectorySeparatorChar)
                                       .Replace('/', Path.DirectorySeparatorChar);

            this.DataDirectory = Path.Combine(storagePath, "data");
            this.BlacklistDirectory = Path.Combine(storagePath, "blacklist");
            Directory.CreateDirectory(this.DataDirectory);
            Directory.CreateDirectory(this.BlacklistDirectory);
            CachingLayer = new CachingLayer(this, storagePath);
            BufferedWriterSvc = new BufferedWriterService(WriteBuffer, this);
            LoadMeasureBlacklist();
            this.LockManager = new LockManager();

        }


        public DataLayer_Readable_Caching_V1(string storagePath, int readBuffer, int writeBuffer) : this(storagePath)
        {
            this.WriteBuffer = writeBuffer;
            this.ReadBuffer = readBuffer;
        }

        public IEnumerable<DataPoint> LoadData(string measurement, DateTime startTime, DateTime endTime, CacheResolution cacheResolution, bool newestOnly, KalaQlContext context, bool dontInvalidateCache_ForUseWhileCacheRebuild)
        {
            measurement = PathSanitizer.SanitizePath(measurement);
            if (newestOnly)
            {
                return this.LoadNewestDatapoint(measurement);
            }
            else if (cacheResolution.Resolution != Resolution.Full)
            {
                return CachingLayer.LoadDataFromCache(measurement, startTime, endTime, cacheResolution, context);
            }
            else
            {
                return LoadFullResolution(measurement, startTime, endTime, context, dontInvalidateCache_ForUseWhileCacheRebuild);
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
            if (!Directory.Exists(measurementPath))
            {
                yield break;
            }
            foreach (var yearPath in Directory.GetDirectories(measurementPath).OrderDescending())
            {
                foreach (var monthDir in Directory.GetDirectories(yearPath).OrderDescending())
                {
                    var month = int.Parse(Path.GetFileName(monthDir));

                    foreach (var file in Directory.GetFiles(monthDir, $"{measurementSubPath}*.dat").OrderByDescending(fn => fn.Substring(fn.Length - FilenameDatePatternLength)))
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
                        var dp = DatFileParser.ParseLine(fileyear, filemonth, fileday, lastLine, file, -1);
                        yield return dp;
                        yield break;
                    }
                }
            }
        }

        private IEnumerable<DataPoint> LoadFullResolution(string measurement, DateTime startTime, DateTime endTime, KalaQlContext context, bool dontInvalidateCache_ForUseWhileCacheRebuild)
        {
            (string measurementPathPart, string measurementPath) = GetMeasurementDirectory(measurement);
            using (var sa = StorageAccess.ForReadMultiFile(measurementPath, measurementPathPart, startTime, endTime, context, this.LockManager))
            {
                foreach (var dp in sa.OpenStreamReaders().StreamMergeDataPoints_MaterializeSortIfNeeded(measurement, dontInvalidateCache_ForUseWhileCacheRebuild))
                {
                    yield return dp;
                }
            }
        }

        public IEnumerable<Dictionary<string, object>> CopyFilesFromMeasurementToMeasurement(string measurement, string targetmeasurement, KalaQlContext context)
        {
            BufferedWriterSvc.ForceFlushWriters();

            (string measurementPathPart_Source, string measurementPath_Source) = GetMeasurementDirectory(measurement);
            (string measurementPathPart_Target, string measurementPath_Target) = GetMeasurementDirectory(targetmeasurement);

            FileSystemHelper.DirectoryCopy(measurementPath_Source, measurementPath_Target, true);

            yield return new Dictionary<string, object>() { { "msg", $"copied {measurementPath_Source} into {measurementPath_Target}" } };
        }

        private (string measurementPathPart, string measurementPath) GetMeasurementDirectory(string measurement)
        {
            string measurementPathPart = PathSanitizer.SanitizePath(measurement);
            string measurementPath = Path.Combine(DataDirectory, measurementPathPart);
            return (measurementPathPart, measurementPath);
        }

        public IEnumerable<Dictionary<string, object>> MoveMeasurement(string measurementOld, string measurementNew, KalaQlContext context)
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


        public void Insert(string measurement, DataPoint dataPoint, string? source = "input")
        {
            Insert(dataPoint.AsLineData(measurement), source);
        }

        /// <summary>
        /// Expects Data in the Form
        /// "<measurement> <timestamp:YYYY-MM-DDTHH:mm:ss.zzzzzzz <value>"
        /// </summary>
        /// <param name="rawData"></param>
        /// <param name="locking"></param>
        public void Insert(string rawData, string? source = "input")
        {
            if (ShuttingDown)
            {
                return;
            }
            string measurement, datetimeHHmmssfffffff, valueString;
            ReadOnlySpan<char> datetime_yyyy_MM_ddTHH_mm_ss_fffffff;
            ParseRawData(rawData, out measurement, out datetime_yyyy_MM_ddTHH_mm_ss_fffffff, out datetimeHHmmssfffffff, out valueString);
            if (!IsBlacklisted(measurement, false))
            {
                string filePath = GetInsertTargetFilepath(measurement, datetime_yyyy_MM_ddTHH_mm_ss_fffffff);
                using(this.LockManager.AcquireLock(filePath)) {

                    if (IsDelayedInsert(measurement, datetime_yyyy_MM_ddTHH_mm_ss_fffffff))
                    {
                        DateOnly dt = new DateOnly(int.Parse(datetime_yyyy_MM_ddTHH_mm_ss_fffffff.Slice(0, 4)), int.Parse(datetime_yyyy_MM_ddTHH_mm_ss_fffffff.Slice(5, 2)), int.Parse(datetime_yyyy_MM_ddTHH_mm_ss_fffffff.Slice(8, 2)));
                        CachingLayer.Mark2Invalidate(measurement, dt);
                    }
                    else
                    {
                        filePath = StorageAccess.SetSortMark(filePath, true);
                    }

                    BufferedWriterSvc.DoWrite(filePath, (writer) =>
                    {
                        // Format the line to write
                        writer.Append(datetimeHHmmssfffffff);
                        writer.Append(" ");
                        writer.Append(valueString);
                        writer.AppendNewline();
                    });
                }
            }
        }

        private bool IsDelayedInsert(string measurement, ReadOnlySpan<char> datetime_yyyy_MM_ddTHH_mm_ss_fffffff)
        {
            var toCheckTime = datetime_yyyy_MM_ddTHH_mm_ss_fffffff.ToString();
            if (!this.LatestEntries.ContainsKey(measurement))
            {
                var newest = this.LoadNewestDatapoint(measurement).FirstOrDefault();
                if (newest != null)
                {
                    this.LatestEntries[measurement] = newest.StartTime.ToString("yyyy-MM-ddTHH:mm:ss.fffffff");
                }
                else
                {
                    this.LatestEntries[measurement] = DateTime.MinValue.ToString("yyyy-MM-ddTHH:mm:ss.fffffff");
                }
            }

            if (this.LatestEntries[measurement].CompareTo(toCheckTime) < 0)
            {
                this.LatestEntries[measurement] = toCheckTime;
                return false;
            }
            else
            {
                return true;
            }
        }

        private void LoadMeasureBlacklist()
        {
            var blackListDirs = Directory.GetDirectories(this.BlacklistDirectory, "*", new EnumerationOptions() { BufferSize = ReadBuffer, RecurseSubdirectories = false });
            ConcurrentDictionary<string, bool> newBl = new ConcurrentDictionary<string, bool>();
            foreach (var blDir in blackListDirs)
            {
                newBl.AddOrUpdate(Path.GetFileName(blDir), true, (string dir, bool old) => true);
            }
        }

        public bool IsBlacklisted(string measurement, bool checkOnDisk = true)
        {
            if (MeasurementBlacklist.ContainsKey(measurement))
            {
                return true;
            }
            (string measurementPathPart, string measurementPath) = GetMeasurementDirectory(measurement);
            var blDir = Path.Combine(this.BlacklistDirectory, measurementPathPart);
            if (checkOnDisk && Directory.Exists(blDir))
            {
                MeasurementBlacklist.AddOrUpdate(measurement, true, (string dir, bool old) => true);
                return true;
            }
            return false;
        }

        public IEnumerable<Dictionary<string, object?>> Blacklist(string measurement)
        {
            yield return Msg.Get("msg", $"Live Blacklisting {measurement}");
            MeasurementBlacklist.AddOrUpdate(measurement, true, (string dir, bool old) => true);
            (string measurementPathPart, string measurementPath) = GetMeasurementDirectory(measurement);
            BufferedWriterSvc.ForceFlushWriters();
            yield return Msg.Get("msg", $"Live Blacklisted {measurement}");
            if (Directory.Exists(measurementPath))
            {
                yield return Msg.Get("msg", $"Moving directory to Blacklist-Directory {measurement}");
                Directory.Move(measurementPath, Path.Combine(this.BlacklistDirectory, measurementPathPart));
                yield return Msg.Get("msg", $"Moved directory to Blacklist-Directory {measurement}");
            }
        }

        public IEnumerable<Dictionary<string, object?>> UnBlacklist(string measurement)
        {

            (string measurementPathPart, string measurementPath) = GetMeasurementDirectory(measurement);
            BufferedWriterSvc.ForceFlushWriters();

            var blDir = Path.Combine(this.BlacklistDirectory, measurementPathPart);
            if (Directory.Exists(blDir))
            {
                yield return Msg.Get("msg", $"Moving blacklist directory to data-directory {measurement}");
                Directory.Move(blDir, measurementPath);
                yield return Msg.Get("msg", $"Moved blacklist directory to data-directory {measurement}");
            }

            yield return Msg.Get("msg", $"Live Unblacklisting {measurement}");
            MeasurementBlacklist.Remove(measurement, out bool randombit);
            yield return Msg.Get("msg", $"Live Unblacklisted {measurement}");
        }

        public string GetInsertTargetFilepath(string measurement, ReadOnlySpan<char> datetime_yyyy_MM_dd)
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
            return measurements.Select(d => Path.GetFileName(d)).OrderBy(e => e).ToList();
        }

        public void Dispose()
        {
            this.BufferedWriterSvc.Dispose();
        }

        public void Flush()
        {
            this.BufferedWriterSvc.ForceFlushWriters();
        }

        public void Flush(string filePath)
        {
            this.BufferedWriterSvc.ForceFlushWriter(filePath);
        }

        public void InsertError(string err)
        {
            var line = $"kala/errors {DateTime.Now.ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffffff")} {err.Replace("\n", " | ")}";
            this.Insert(line);
        }
        public void InsertLog(string log)
        {
            var line = $"kala/log {DateTime.Now.ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffffff")} {log.Replace("\n", " | ")}";
            this.Insert(line);
        }

        public IEnumerable<Dictionary<string, object?>> SortRawFiles(string measurement, KalaQlContext context)
        {
            var q = new KalaQuery()
                .Add(new Op_Load("SortRawFiles", "toSort", measurement, DateTime.MinValue, DateTime.MaxValue, CacheResolutionPredefined.NoCache, false))
                .Add(new Op_Publish("SortRawFiles", new List<string>() { "toSort" }, PublishMode.MultipleResultsets));
            var localresult = q.Execute(context.DataLayer).ResultSets!.First().Resultset;

            DateTime day = DateTime.MinValue;
            foreach (var r in localresult) // iterate to load everything
            {
                if (day < r.StartTime)
                {
                    yield return new Dictionary<string, object?>() { { "msg", $"Sort of day {r.StartTime.Date} done" } };
                    day = r.StartTime.AddDays(1);
                }
                Pools.DataPoint.Return(r);
            }
            Console.WriteLine($"Sorted measurement {measurement}.");
            yield return new Dictionary<string, object?>() { { "msg", $"Sorted measurement {measurement}." } };
        }

        public void Shutdown()
        {
            this.ShuttingDown = true;
            this.Dispose();
        }

        public bool DoesMeasurementExist(string measurement)
        {
            (string measurementPathPart, string measurementPath) = GetMeasurementDirectory(measurement);
            return Directory.Exists(measurementPath);
        }

        public void WriteMatViewFile(string viewName, List<string> lines)
        {
            (string measurementPathPart, string measurementPath) = GetMeasurementDirectory(viewName);
            Directory.CreateDirectory(measurementPath); // Sicherstellen, dass das Verzeichnis existiert
            var viewDefFile = Path.Combine(measurementPath, "viewdef.txt");
            File.WriteAllLines(viewDefFile, lines);
        }

        public List<MatView> LoadMatViews()
        {
            EnumerationOptions optionFindFilesRecursive = new EnumerationOptions()
            {
                BufferSize = 131072,
                RecurseSubdirectories = true,
                ReturnSpecialDirectories = false,
                AttributesToSkip = FileAttributes.Hidden | FileAttributes.System // System hinzugefügt
            };
            string filter = "viewdef.txt";
            // Nur Dateien suchen, nicht Verzeichnisse, die zufällig so heißen
            var matViewFiles = Directory.GetFiles(DataDirectory, filter, optionFindFilesRecursive);
            var ret = matViewFiles.Select(f => new MatView(f)).ToList();
            return ret;
        }

        public void DeleteMeasurementAndMatViewDefinition(string measurementName)
        {
            measurementName = PathSanitizer.SanitizePath(measurementName);
            (string measurementPathPart, string measurementPath) = GetMeasurementDirectory(measurementName);

            if (Directory.Exists(measurementPath))
            {
                try
                {
                    Directory.Delete(measurementPath, true); // Rekursives Löschen
                    // Optional: Logging
                    // this.CachingLayer.InvalidateMeasurementCache(measurementName); // Falls Caching betroffen ist
                }
                catch (IOException ex)
                {
                    // Fehlerbehandlung, z.B. Loggen oder spezifischere Exception werfen
                    throw new InvalidOperationException($"Could not delete measurement directory '{measurementPath}'. It might be in use or access denied.", ex);
                }
                catch (UnauthorizedAccessException ex)
                {
                    throw new InvalidOperationException($"Access denied while trying to delete measurement directory '{measurementPath}'.", ex);
                }
            }
            // Optional: Loggen, falls das Verzeichnis nicht existiert
        }

        public class MatView
        {
            public string Query { get; set; } = string.Empty; // Standardwert
            public string ViewdefFilePath { get; }
            public DateTime NewestContent = DateTime.MinValue;

            public MatView(string viewdefFilePath)
            {
                ViewdefFilePath = viewdefFilePath;
                this.Read();
            }

            private void Read()
            {
                if (!File.Exists(ViewdefFilePath))
                {
                    // Datei existiert nicht, Query bleibt leer, NewestContent bleibt MinValue
                    return;
                }

                var lines = File.ReadAllLines(ViewdefFilePath);
                if (lines.Length > 0)
                {
                    DateTime.TryParseExact(lines[0], "yyyy-MM-ddTHH:mm:ss.fffffff", CultureInfo.InvariantCulture, DateTimeStyles.AllowWhiteSpaces, out var result);
                    this.NewestContent = result; // result ist MinValue bei Parse-Fehler, was ok ist
                }

                if (lines.Length >= 2) // Zeitstempel, dann Query
                {
                    // Query beginnt ab der zweiten Zeile (Index 1)
                    this.Query = string.Join(Environment.NewLine, lines.Skip(1));
                }                
                // Wenn lines.Length == 1 (nur Timestamp), ist Query string.Empty
                // Wenn lines.Length == 0 (leere Datei), ist Query string.Empty
            }
        }
    }
}
