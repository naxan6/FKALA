using FKala.Core.DataLayer.Cache;
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

        public static string TimeFormat { get { return "HH:mm:ss.fffffff"; } }
        private ILookup<DateOnly, ReaderTuple>? StreamReaderLookupForMerge;
        private DateTime StartTime;
        private DateTime EndTime;

        public KalaQlContext Context { get; private set; }
        public int ReadBuffer { get; }
        public int WriteBuffer { get; }

        private IDataLayer DataLayer;
        private TimeOnly AtMidnight = new TimeOnly(0, 0, 0);

        private LockManager LockManager;

        private StorageAccess(IDataLayer dataLayer, KalaQlContext context)
        {
            this.DataLayer = dataLayer;
            this.Context = context;

            fileStreamOptions.BufferSize = dataLayer.ReadBuffer;
            optionFindFilesRecursive.BufferSize = dataLayer.ReadBuffer;
            ReadBuffer = dataLayer.ReadBuffer;
            WriteBuffer = dataLayer.WriteBuffer;
            LockManager = new LockManager();
        }

        public static StorageAccess ForReadMultiFile(string measurementPath, string measurementPathPart, DateTime startTime, DateTime endTime, KalaQl.KalaQlContext context)
        {
            var ret = new StorageAccess(context.DataLayer, context);
            ret.StartTime = startTime;
            ret.EndTime = endTime;

            ret.StreamReaderLookupForMerge = ret.QueryFilesForMergingMultipleFiles(measurementPath, measurementPathPart, startTime, endTime);
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
            if (this.StreamReaderLookupForMerge != null)
            {
                //DataLayer.BufferedWriterSvc.ForceFlushWriters(); TRY FOR SPEEDUP
                this.StreamReaderLookupForMerge.AsParallel().ForAll(daySrs => daySrs.ToList().ForEach(sr => sr.StreamReader = new StreamReader(sr.FilePath, Encoding.UTF8, false, fileStreamOptions)));
            }

            return this;
        }

        private List<int> GetYearFolders(string measurementDir)
        {
            var entries = Directory.GetDirectories(measurementDir, "*", new EnumerationOptions() { ReturnSpecialDirectories = false, BufferSize = ReadBuffer, });
            return entries.Where(e => Path.GetFileName(e) != ".DS_Store").Select(y => int.Parse(Path.GetFileName(y))).ToList();
        }

        public static string SetSortMark(string filepath, bool sorted)
        {
            char[] newPath = filepath.ToCharArray();// "measure$aasd[#_]2024-11-02.dat"
            if (sorted)
            {
                newPath[newPath.Length - 15] = '#';   // "<measure$aasd#2024-11-02.dat"
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
            foreach (var streamreaderDayList in StreamReaderLookupForMerge!.OrderBy(srl => srl.Key))
            {
                int fileyear = streamreaderDayList.Key.Year;
                int filemonth = streamreaderDayList.Key.Month;
                int fileday = streamreaderDayList.Key.Day;
                foreach (var srTuple in streamreaderDayList)
                {
                    DataLayer.Flush(srTuple.FilePath);
                }

                // ###### If out of order by multiple files per day or by only single, but unsorted, file
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
                            }
                            else if (dp.ValueText != null)
                            {
                                writer.Append(dp.StartTime.ToString(TimeFormat));
                                writer.Append(" ");
                                writer.Append(dp.ValueText);
                                writer.AppendNewline();
                            }
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

                    var msg = $"Sorted rewrite of file {filePath} {new FileInfo(filePath).Length} Bytes";
                    Console.WriteLine(msg);
                    DataLayer!.InsertLog(msg);

                }
            }
        }
        public static void UnMarkFileAsSorted(string currentPath)
        {
            string filenameMarkedUnsorted = SetSortMark(currentPath, false);
            filenameMarkedUnsorted = Path.Combine(Path.GetDirectoryName(filenameMarkedUnsorted)!, "unmarked_" + Path.GetFileName(filenameMarkedUnsorted));
            try
            {
                File.Move(currentPath, filenameMarkedUnsorted);
                Console.WriteLine($"unmarked {currentPath} to unsorted");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"failed renaming {currentPath} to unsorted. maybe already marked unsorted by parallel stream, or unsorted file {ex}");
            }
        }

        private IEnumerable<DataPoint> InternalStreamDataPoints(ReaderTuple sr, int fileyear, int filemonth, int fileday, bool checkUnsorted)
        {
            int lineIdx = 0;
            DataPoint? retPrev = null;
            string? dataline;

            while ((dataline = sr.StreamReader!.ReadLine()) != null)
            {
                lineIdx++;
                var ret = DatFileParser.ParseLine(fileyear, filemonth, fileday, dataline, sr.FilePath, lineIdx);
                //FORDEBUGONLY ret.Source = $"{sr.FilePath}, Line {lineIdx} {sr.MarkedAsSorted}";


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
                    DataLayer!.InsertError(err);
                    throw new UnexpectedlyUnsortedException(err, sr.FilePath);
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
            if (StreamReaderLookupForMerge != null)
            {
                StreamReaderLookupForMerge.AsParallel().ForAll(daySrs => daySrs.ToList().ForEach(sr => sr.StreamReader?.Dispose()));
            }
        }
    }
}