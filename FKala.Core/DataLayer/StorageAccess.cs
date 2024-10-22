﻿using FKala.Core.DataLayer.Cache;
using FKala.Core.DataLayer.Infrastructure;
using FKala.Core.Interfaces;
using FKala.Core.KalaQl;
using FKala.Core.Model;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using static FKala.Core.DataLayer_Readable_Caching_V1;

namespace FKala.Core.DataLayers
{
    public class StorageAccess : IDisposable
    {
        FileStreamOptions fileStreamOptions = new FileStreamOptions()
        {
            Access = FileAccess.Read,
            BufferSize = 131072,
            Mode = FileMode.Open,
            Share = FileShare.ReadWrite | FileShare.Delete
        };

        EnumerationOptions optionFindFilesRecursive = new EnumerationOptions()
        {
            BufferSize = 131072,
            RecurseSubdirectories = true,
            ReturnSpecialDirectories = false,
            AttributesToSkip = FileAttributes.Hidden
        };

        public string TimeFormat { get { return "HH:mm:ss.fffffff"; } }
        private SortedDictionary<DateOnly, ReaderTuple> TimeSortedStreamReader;
        private ILookup<DateOnly, ReaderTuple> StreamReaderLookupForMerge;
        private DateTime StartTime;
        private DateTime EndTime;

        public KalaQlContext Context { get; private set; }
        public int ReadBuffer { get; }
        public int WriteBuffer { get; }

        private bool IsActiveAutoSortRawFiles;
        private IDataLayer? DataLayer;
        private TimeOnly AtMidnight = new TimeOnly(0, 0, 0);

        private LockManager LockManager;

        private StorageAccess(IDataLayer dataLayer)
        {
            this.DataLayer = dataLayer;
            fileStreamOptions.BufferSize = dataLayer.ReadBuffer;
            optionFindFilesRecursive.BufferSize = dataLayer.ReadBuffer;
            ReadBuffer = dataLayer.ReadBuffer;
            WriteBuffer = dataLayer.WriteBuffer;
            LockManager = new LockManager();
        }

        public static StorageAccess ForReadMultiFile(string measurementPath, string measurementPathPart, DateTime startTime, DateTime endTime, KalaQl.KalaQlContext context)
        {
            var ret = new StorageAccess(context.DataLayer);
            ret.StartTime = startTime;
            ret.EndTime = endTime;
            ret.Context = context;
            ret.StreamReaderLookupForMerge = ret.QueryFilesForMergingMultipleFiles(measurementPath, measurementPathPart, startTime, endTime);
            return ret;
        }
        public static StorageAccess ForRead(string measurementPath, string measurementPathPart, DateTime startTime, DateTime endTime, KalaQl.KalaQlContext context, bool doSortRawFiles)
        {
            var ret = new StorageAccess(context.DataLayer);
            ret.StartTime = startTime;
            ret.EndTime = endTime;
            ret.Context = context;
            if (doSortRawFiles) { ret.ActivateAutoSortRawFiles(context.DataLayer); }
            ret.TimeSortedStreamReader = ret.GetFilePaths(measurementPath, measurementPathPart, startTime, endTime);
            return ret;
        }

        public static StorageAccess ForSort(string measurementPath, string measurementPathPart, DateTime startTime, DateTime endTime, KalaQl.KalaQlContext context)
        {
            var ret = new StorageAccess(context.DataLayer);
            ret.StartTime = startTime;
            ret.EndTime = endTime;
            ret.Context = context;
            ret.TimeSortedStreamReader = ret.GetFilePaths(measurementPath, measurementPathPart, startTime, endTime);
            return ret;
        }
        public static StorageAccess ForMerging(string measurementPath, string measurementPathPart, KalaQl.KalaQlContext context)
        {
            var ret = new StorageAccess(context.DataLayer);
            ret.StartTime = DateTime.MinValue;
            ret.EndTime = DateTime.MaxValue;
            ret.Context = context;
            ret.StreamReaderLookupForMerge = ret.QueryFilesForMergingAllFiles(measurementPath, measurementPathPart);
            return ret;
        }

        public static StorageAccess ForCleanup(string measurementPath, string measurementPathPart, KalaQl.KalaQlContext context)
        {
            var ret = new StorageAccess(context.DataLayer);
            ret.StartTime = DateTime.MinValue;
            ret.EndTime = DateTime.MaxValue;
            ret.Context = context;
            ret.StreamReaderLookupForMerge = ret.QueryFilesForCleanup(measurementPath, measurementPathPart);
            return ret;
        }

        private SortedDictionary<DateOnly, ReaderTuple> GetFilePaths(string measurementPath, string measurementPathPart, DateTime startTime, DateTime endTime)
        {
            // years
            int startYear = startTime.Year;
            int endYear = endTime.Year;
            var years = GetYearFolders(measurementPath);
            var filteredYears = years.Where(y => y >= startYear && y <= endYear);

            // files
            //string filter = $"{measurementPathPart}_*.dat";
            string filter = $"{measurementPathPart}*.dat";

            var fileCandidates = filteredYears.AsParallel().SelectMany(y => Directory.GetFileSystemEntries(Path.Combine(measurementPath, y.ToString()), filter, optionFindFilesRecursive)).ToList();



            SortedDictionary<DateOnly, ReaderTuple> ret = new SortedDictionary<DateOnly, ReaderTuple>();

            foreach (var candidate in fileCandidates)
            {
                string barename = Path.GetFileNameWithoutExtension(candidate);
                var datePart = barename.Substring(barename.Length - 11, 11);
                ReadOnlySpan<char> dateSpan = datePart.AsSpan();
                // DateOnly dt = new DateOnly(int.Parse(dateSpan.Slice(0, 4)), int.Parse(dateSpan.Slice(5, 2)), int.Parse(dateSpan.Slice(8, 2)));
                bool markedAsSorted = dateSpan[0] == '#' && !IsActiveAutoSortRawFiles;
                int fileyear = int.Parse(dateSpan.Slice(1, 4));
                int filemonth = int.Parse(dateSpan.Slice(6, 2));
                int fileday = int.Parse(dateSpan.Slice(9, 2));

                var fileDateTime = new DateOnly(fileyear, filemonth, fileday);

                if (startTime < fileDateTime.AddDays(1).ToDateTime(AtMidnight) && fileDateTime.ToDateTime(AtMidnight) < endTime)
                {
                    try
                    {
                        ret.Add(fileDateTime, new ReaderTuple() { FileDate = fileDateTime, FilePath = candidate, MarkedAsSorted = markedAsSorted });
                    }
                    catch (ArgumentException)
                    {
                        var msg = $"BUG in raw data: multiple files for date {fileDateTime.ToString("s")} : {candidate}. Skipping file.";
                        Context.AddError(msg);
                    }
                }
            }

            return ret;
        }

        private ILookup<DateOnly, ReaderTuple> QueryFilesForCleanup(string measurementPath, string measurementPathPart)
        {
            // years            
            var years = GetYearFolders(measurementPath);

            // files
            string filter = $"*.*";

            var fileCandidates = years.AsParallel().SelectMany(y => Directory.GetFileSystemEntries(Path.Combine(measurementPath, y.ToString()), filter, optionFindFilesRecursive)).ToList();



            List<(DateOnly, ReaderTuple)> retList = new List<(DateOnly, ReaderTuple)>();

            foreach (var candidate in fileCandidates)
            {
                retList.Add((DateOnly.MinValue, new ReaderTuple() { FileDate = DateOnly.MinValue, FilePath = candidate, MarkedAsSorted = false }));

            }

            var ret = retList.ToLookup(t => t.Item1, t => t.Item2);
            return ret;
        }

        private ILookup<DateOnly, ReaderTuple> QueryFilesForMergingAllFiles(string measurementPath, string measurementPathPart)
        {
            // years            
            var years = GetYearFolders(measurementPath);

            // files
            string filter = $"*.dat";

            var fileCandidates = years.AsParallel().SelectMany(y => Directory.GetFileSystemEntries(Path.Combine(measurementPath, y.ToString()), filter, optionFindFilesRecursive)).ToList();

            List<(DateOnly, ReaderTuple)> retList = new List<(DateOnly, ReaderTuple)>();

            foreach (var candidate in fileCandidates)
            {
                string barename = Path.GetFileNameWithoutExtension(candidate);
                var datePart = barename.Substring(barename.Length - 11, 11);
                ReadOnlySpan<char> dateSpan = datePart.AsSpan();
                int fileyear = int.Parse(dateSpan.Slice(1, 4));
                int filemonth = int.Parse(dateSpan.Slice(6, 2));
                int fileday = int.Parse(dateSpan.Slice(9, 2));
                var fileDateTime = new DateOnly(fileyear, filemonth, fileday);
                retList.Add((fileDateTime, new ReaderTuple() { FileDate = fileDateTime, FilePath = candidate, MarkedAsSorted = false }));

            }
            var ret = retList.ToLookup(t => t.Item1, t => t.Item2);
            return ret;
        }

        private ILookup<DateOnly, ReaderTuple> QueryFilesForMergingMultipleFiles(string measurementPath, string measurementPathPart, DateTime startTime, DateTime endTime)
        {
            // years            
            int startYear = startTime.Year;
            int endYear = endTime.Year;
            var years = GetYearFolders(measurementPath);
            var filteredYears = years.Where(y => y >= startYear && y <= endYear);

            // files
            string filter = $"*.dat";

            var fileCandidates = filteredYears.AsParallel().SelectMany(y => Directory.GetFileSystemEntries(Path.Combine(measurementPath, y.ToString()), filter, optionFindFilesRecursive)).ToList();
            fileCandidates = fileCandidates.Order().ToList();

            List<(DateOnly, ReaderTuple)> retList = new List<(DateOnly, ReaderTuple)>();

            foreach (var candidate in fileCandidates)
            {
                string barename = Path.GetFileNameWithoutExtension(candidate);
                var datePart = barename.Substring(barename.Length - 11, 11);
                ReadOnlySpan<char> dateSpan = datePart.AsSpan();
                int fileyear = int.Parse(dateSpan.Slice(1, 4));
                int filemonth = int.Parse(dateSpan.Slice(6, 2));
                int fileday = int.Parse(dateSpan.Slice(9, 2));
                var fileDateTime = new DateOnly(fileyear, filemonth, fileday);

                if (startTime < fileDateTime.AddDays(1).ToDateTime(AtMidnight) && fileDateTime.ToDateTime(AtMidnight) < endTime)
                {
                    retList.Add((fileDateTime, new ReaderTuple() { FileDate = fileDateTime, FilePath = candidate, MarkedAsSorted = IsSortMarkSet(candidate) }));
                }
            }

            var ret = retList.ToLookup(t => t.Item1, t => t.Item2);
            return ret;
        }

        public StorageAccess OpenStreamReaders()
        {
            if (this.TimeSortedStreamReader != null)
            {
                this.TimeSortedStreamReader.AsParallel().ForAll(t => t.Value.StreamReader = new StreamReader(t.Value.FilePath, Encoding.UTF8, false, fileStreamOptions));
            }
            if (this.StreamReaderLookupForMerge != null)
            {
                DataLayer.BufferedWriterSvc.ForceFlushWriters();
                this.StreamReaderLookupForMerge.AsParallel().ForAll(daySrs => daySrs.ToList().ForEach(sr => sr.StreamReader = new StreamReader(sr.FilePath, Encoding.UTF8, false, fileStreamOptions)));
            }

            return this;
        }

        public IEnumerable<ReaderTuple> GetReaders()
        {
            if (TimeSortedStreamReader != null)
            {
                return TimeSortedStreamReader.Select(r => r.Value);
            }
            if (StreamReaderLookupForMerge != null)
            {
                return StreamReaderLookupForMerge.SelectMany(r => r);
            }
            return new List<ReaderTuple>().AsEnumerable();

        }

        private List<int> GetYearFolders(string measurementDir)
        {
            var entries = Directory.GetDirectories(measurementDir, "*", new EnumerationOptions() { ReturnSpecialDirectories = false, BufferSize = ReadBuffer, });
            return entries.Where(e => Path.GetFileName(e) != ".DS_Store").Select(y => int.Parse(Path.GetFileName(y))).ToList();
        }

        public IEnumerable<DataPoint> StreamMergeDataPoints()
        {
            foreach (var streamreaderTuple in StreamReaderLookupForMerge.OrderBy(k => k.Key))
            {
                int fileyear = streamreaderTuple.Key.Year;
                int filemonth = streamreaderTuple.Key.Month;
                int fileday = streamreaderTuple.Key.Day;
                foreach (var srTuple in streamreaderTuple)
                {
                    DataLayer.Flush(srTuple.FilePath);
                    foreach (var dp in InternalStreamDataPoints(srTuple, fileyear, filemonth, fileday, false))
                    {
                        yield return dp;
                    }
                }
            }
        }

        public static string SetSortMark(string filepath, bool sorted)
        {
            char[] newPath = filepath.ToCharArray();// "measure$aasd[#_]2024-11-02.dat"
            if (sorted)
            {
                newPath[newPath.Length - 15] = '#';   // "<measure$aasd_2024-11-02.dat"
            }
            else
            {
                newPath[newPath.Length - 15] = '_';   // "<measure$aasd_2024-11-02.dat"
            }

            string changePath = new string(newPath);
            return changePath;
        }
        public static bool IsSortMarkSet(string filepath)
        {
            char[] newPath = filepath.ToCharArray();
            return newPath[newPath.Length - 15] == '#';
        }

        public IEnumerable<DataPoint> StreamMergeDataPoints_MaterializeSortIfNeeded(string measurement, bool dontInvalidateCache_ForUseWhileCacheRebuild)
        {
            foreach (var streamreaderDayList in StreamReaderLookupForMerge.OrderBy(srl => srl.Key))
            {
                int fileyear = streamreaderDayList.Key.Year;
                int filemonth = streamreaderDayList.Key.Month;
                int fileday = streamreaderDayList.Key.Day;
                foreach (var srTuple in streamreaderDayList)
                {
                    DataLayer.Flush(srTuple.FilePath);
                }

                // ###### If out of order by multiple files per day or by only single but unsorted file
                if (streamreaderDayList.Count() > 1 ||
                    (streamreaderDayList.Count() == 1 && 
                    (!streamreaderDayList.First().MarkedAsSorted || streamreaderDayList.First().MeasurementFileDiffersToPath())))
                {
                    string genericFilePath = DataLayer.GetInsertTargetFilepath(measurement, $"{fileyear:00}-{filemonth:00}-{fileday:00}");
                    using (LockManager.AcquireLock(genericFilePath))
                    {
                        var allDpsInAllFilesForThisDay = streamreaderDayList.SelectMany(srTuple => InternalStreamDataPoints(srTuple, fileyear, filemonth, fileday, false));

                        Dictionary<DateTime, DataPoint> ret = new Dictionary<DateTime, DataPoint>();
                        // DEDUPLICATE, LAST WINS (has to be sorted in insertion order until here)
                        foreach (var dp in allDpsInAllFilesForThisDay)
                        {
                            ret[dp.StartTime] = dp;
                        }

                        var ordered = ret.OrderBy(k => k.Key).Select(v => v.Value).ToList();

                        var filePath = SetSortMark(genericFilePath, true);


                        WriteSortedFile(filePath, ordered);
                        foreach (var oldFile in streamreaderDayList.Select(sr => sr.FilePath))
                        {
                            if (oldFile != filePath)
                            {
                                File.Delete(oldFile);
                            }
                        }

                        if (!dontInvalidateCache_ForUseWhileCacheRebuild)
                        {
                            DataLayer.CachingLayer.Invalidate(measurement, streamreaderDayList.Key);
                        }

                        foreach (var dp in ordered)
                        {
                            yield return dp;
                        }
                    }
                }
                // ###### If in order
                else
                {
                    var sr = streamreaderDayList.First();
                    if (sr.MarkedAsSorted)
                    {
                        foreach (var dp in InternalStreamDataPoints(sr, fileyear, filemonth, fileday, true))
                        {
                            yield return dp;
                        }
                    }
                }
            }
        }

        public IEnumerable<DataPoint> StreamDataPoints()
        {
            foreach (var streamreaderTuple in TimeSortedStreamReader)
            {
                var srTuple = streamreaderTuple.Value;
                int fileyear = streamreaderTuple.Key.Year;
                int filemonth = streamreaderTuple.Key.Month;
                int fileday = streamreaderTuple.Key.Day;

                if (!streamreaderTuple.Value.MarkedAsSorted)
                {
                    foreach (var dp in InternalStreamDataPointsSort(srTuple, fileyear, filemonth, fileday))
                    {
                        yield return dp;
                    }
                }
                else
                {
                    foreach (var dp in InternalStreamDataPoints(srTuple, fileyear, filemonth, fileday, true))
                    {
                        yield return dp;
                    }
                }
            }
        }

        private IEnumerable<DataPoint> InternalStreamDataPointsSort(ReaderTuple readerTuple, int fileyear, int filemonth, int fileday)
        {
            var comparer = Pools.DataPoint.Get();
            var dataPoints = InternalStreamDataPoints(readerTuple, fileyear, filemonth, fileday, false).ToList();
            bool persistenceIsSorted = IsSortedAndWithoutDuplicates(dataPoints);
            // if not sorted, sort it
            if (!persistenceIsSorted)
            {
                dataPoints = dataPoints.OrderBy(a => a.StartTime).ToList(); // Sort
                dataPoints = dataPoints.Distinct(comparer).ToList(); // Deduplicate

                // persist sorted (if activated)
                // and only if it's at least older than 1-2 days (pathdate is start of day at midnight!)
                if (IsActiveAutoSortRawFiles && readerTuple.FileDate.ToDateTime(AtMidnight) < DateTime.Now.AddDays(-2))
                {
                    WriteSortedFile(readerTuple.FilePath, dataPoints);
                    persistenceIsSorted = true;
                }
            }

            // mark as sorted (if it already was or is now) -
            // and only if it's at least older than 1-2 days (pathdate is start of day at midnight!)
            if (persistenceIsSorted && readerTuple.FileDate.ToDateTime(AtMidnight) < DateTime.Now.AddDays(-2))
            {
                MarkFileAsSorted(readerTuple.FilePath);
            }

            foreach (var dp in dataPoints)
            {
                yield return dp;
            }
        }

        public StorageAccess ActivateAutoSortRawFiles(IDataLayer dataLayer)
        {
            IsActiveAutoSortRawFiles = true;
            DataLayer = dataLayer;
            return this;
        }
        private void WriteSortedFile(string filePath, IEnumerable<DataPoint> rs)
        {
            if (DataLayer == null)
            {
                return;
            }
            if (rs.Any())
            {
                using (this.LockManager.AcquireLock(filePath))
                {
                    var writerSvc = DataLayer.BufferedWriterSvc;
                    writerSvc.CreateWriteDispose(filePath + ".sorted", false, (writer) =>
                    {
                        foreach (var dp in rs)
                        {
                            if (dp.Value != null)
                            {
                                writer.Append(dp.StartTime.ToString(TimeFormat));
                                writer.Append(" ");
                                writer.Append(dp.Value.Value.ToString(CultureInfo.InvariantCulture));
                                writer.AppendNewline();
                            };
                        }
                    });
                    var bakFile = filePath + $".bak_{DateTime.Now.ToString("yyyyMMddHHmmssfff")}";
                    if (File.Exists(filePath))
                    {
                        File.Move(filePath, bakFile);
                        File.Move(filePath + ".sorted", filePath);
                        File.Delete(bakFile);
                    }
                    else
                    {
                        File.Move(filePath + ".sorted", filePath);
                    }
                    Console.WriteLine($"Sorted rewrite of file {filePath}");
                }
            }
        }

        private static void MarkFileAsSorted(string currentPath)
        {
            char[] newPath = currentPath.ToCharArray();// "measure$aasd_2024-11-02.dat"
            newPath[newPath.Length - 15] = '#';   // "<measure$aasd#2024-11-02.dat"
            string sortedMarkedPath = new string(newPath);
            try
            {
                File.Move(currentPath, sortedMarkedPath);
            }
            catch (Exception)
            {
                Console.WriteLine("failed renaming to sorted. maybe already marked sorted by parallel stream");
            }
        }

        //public static void UnMarkFileAsSorted(string currentPath)
        //{

        //    char[] newPath = currentPath.ToCharArray();// "measure$aasd[#_]2024-11-02.dat"
        //    newPath[newPath.Length - 15] = '#';   // "<measure$aasd#2024-11-02.dat"
        //    string sortedMarkedPath = new string(newPath);

        //    char[] newPathUnsorted = currentPath.ToCharArray();// "measure$aasd[#_]2024-11-02.dat"
        //    newPathUnsorted[newPath.Length - 15] = '_';   // "<measure$aasd_2024-11-02.dat"
        //    string unsortedMarkedPath = new string(newPathUnsorted);
        //    try
        //    {
        //        File.Move(sortedMarkedPath, unsortedMarkedPath);
        //    }
        //    catch (Exception)
        //    {
        //        Console.WriteLine("failed renaming to sorted. maybe already marked unsorted by parallel stream");
        //    }
        //}

        static bool IsSorted<T>(List<T> list) where T : IComparable<T>
        {
            for (int i = 1; i < list.Count; i++)
            {
                if (list[i].CompareTo(list[i - 1]) < 0)
                {
                    return false;
                }
            }
            return true;
        }
        static bool IsSortedAndWithoutDuplicates(List<DataPoint> list)
        {
            for (int i = 1; i < list.Count; i++)
            {
                if (list[i].CompareTo(list[i - 1]) < 0 || list[i].Equals(list[i], list[i - 1]))
                {
                    return false;
                }
            }
            return true;
        }

        private IEnumerable<DataPoint> InternalStreamDataPoints(ReaderTuple sr, int fileyear, int filemonth, int fileday, bool checkUnsorted)
        {
            int lineIdx = 0;
            DataPoint? retPrev = null;
            string? dataline;

            while ((dataline = sr.StreamReader!.ReadLine()) != null)
            {
                lineIdx++;
                var ret = DatFileParser.ParseLine(fileyear, filemonth, fileday, dataline, sr.FilePath, lineIdx, Context);
                ret.Source = $"{sr.FilePath}, Line {lineIdx} {sr.MarkedAsSorted}";


                if (retPrev == null) // initial pair or previous pair was fully consumed
                {
                    retPrev = ret;
                    continue;
                }

                if (retPrev.StartTime == ret.StartTime) // combine, if same time and consume both
                {
                    retPrev.Value = retPrev.Value ?? ret.Value;
                    retPrev.ValueText = retPrev.ValueText ?? ret.ValueText;
                    yield return retPrev;

                    retPrev = null;
                    continue;
                }
                else if (retPrev.StartTime >= ret.StartTime && checkUnsorted)
                {
                    string err = $"Marked sorted but unsorted at File {ret.Source} ## {dataline}";
                    DataLayer.InsertError(err);
                    throw new UnexpectedlyUnsortedException(err);
                }

                if (retPrev.StartTime >= StartTime && retPrev.StartTime < EndTime) // send if DataPoint is in window
                {
                    yield return retPrev; //send retPrev
                }
                retPrev = ret; //consume retPrev
            }
            // send last DataPoint is in window
            if (retPrev != null && retPrev.StartTime >= StartTime && retPrev.StartTime < EndTime)
            {
                yield return retPrev;
            }

            sr.StreamReader.Close();
        }

        public void Dispose()
        {
            if (TimeSortedStreamReader != null)
            {
                TimeSortedStreamReader.AsParallel().ForAll(sr => sr.Value?.StreamReader?.Dispose());
            }
            if (StreamReaderLookupForMerge != null)
            {
                StreamReaderLookupForMerge.AsParallel().ForAll(daySrs => daySrs.ToList().ForEach(sr => sr.StreamReader?.Dispose()));
            }
        }
    }
}