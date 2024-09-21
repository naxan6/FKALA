using FKala.Core.DataLayer.Infrastructure;
using FKala.Core.Interfaces;
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

        private SortedDictionary<DateTime, ReaderTuple> TimeSortedStreamReader;
        private DateTime StartTime;
        private DateTime EndTime;
        private bool IsActiveAutoSortRawFiles;
        private IDataLayer? DataLayer;

        private StorageAccess(string measurementPath, string measurementPathPart, DateTime startTime, DateTime endTime)
        {
            this.StartTime = startTime;
            this.EndTime = endTime;
            this.TimeSortedStreamReader = GetFilePaths(measurementPath, measurementPathPart, startTime, endTime);
        }

        public static StorageAccess Init(string measurementPath, string measurementPathPart, DateTime startTime, DateTime endTime)
        {
            var ret = new StorageAccess(measurementPath, measurementPathPart, startTime, endTime);
            return ret;
        }

        public StorageAccess OpenStreamReaders()
        {
            this.TimeSortedStreamReader.AsParallel().ForAll(t => t.Value.StreamReader = new StreamReader(t.Value.FilePath, Encoding.UTF8, false, fileStreamOptions));
            return this;
        }

        public SortedDictionary<DateTime, ReaderTuple> GetOpenStreamReaders()
        {
            return TimeSortedStreamReader;
        }

        private SortedDictionary<DateTime, ReaderTuple> GetFilePaths(string measurementPath, string measurementPathPart, DateTime startTime, DateTime endTime)
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



            SortedDictionary<DateTime, ReaderTuple> ret = new SortedDictionary<DateTime, ReaderTuple>();

            foreach (var candidate in fileCandidates)
            {
                string barename = Path.GetFileNameWithoutExtension(candidate);
                var datePart = barename.Substring(barename.Length - 11, 11);
                ReadOnlySpan<char> dateSpan = datePart.AsSpan();
                // DateOnly dt = new DateOnly(int.Parse(dateSpan.Slice(0, 4)), int.Parse(dateSpan.Slice(5, 2)), int.Parse(dateSpan.Slice(8, 2)));
                bool markedAsSorted = dateSpan[0] == '#';
                int fileyear = int.Parse(dateSpan.Slice(1, 4));
                int filemonth = int.Parse(dateSpan.Slice(6, 2));
                int fileday = int.Parse(dateSpan.Slice(9, 2));

                var fileDateTime = new DateTime(fileyear, filemonth, fileday, 0, 0, 0, DateTimeKind.Utc);

                if (startTime < fileDateTime.AddDays(1) && fileDateTime < endTime)
                {
                    try
                    {
                        ret.Add(fileDateTime, new ReaderTuple() { FileDate = fileDateTime, FilePath = candidate, MarkedAsSorted = markedAsSorted });
                    }
                    catch (ArgumentException)
                    {
                        Console.WriteLine($"BUG in raw data: multiple files for date {fileDateTime.ToString("s")} : {candidate}. Skipping file.");
                    }
                }
            }

            return ret;
        }

        private List<int> GetYearFolders(string measurementDir)
        {
            var entries = Directory.GetDirectories(measurementDir, "*", new EnumerationOptions() { ReturnSpecialDirectories = false, BufferSize = 131072, });
            return entries.Where(e => Path.GetFileName(e) != ".DS_Store").Select(y => int.Parse(Path.GetFileName(y))).ToList();
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
                    foreach (var dp in InternalStreamDataPoints(srTuple, fileyear, filemonth, fileday))
                    {
                        yield return dp;
                    }
                }
            }
        }

        private IEnumerable<DataPoint> InternalStreamDataPointsSort(ReaderTuple readerTuple, int fileyear, int filemonth, int fileday)
        {
            var dataPoints = InternalStreamDataPoints(readerTuple, fileyear, filemonth, fileday).ToList();
            bool persistenceIsSorted = IsSorted(dataPoints);
            // if not sorted, sort it
            if (!persistenceIsSorted)
            {
                dataPoints.Sort((a, b) => a.Time.CompareTo(b.Time));

                // persist sorted (if activated)
                // and only if it's at least older than 1-2 days (pathdate is start of day at midnight!)
                if (IsActiveAutoSortRawFiles && readerTuple.FileDate < DateTime.Now.AddDays(-2))
                {
                    WriteSortedFile(readerTuple.FilePath, dataPoints);
                    persistenceIsSorted = true;
                }
            }

            // mark as sorted (if it already was or is now) -
            // and only if it's at least older than 1-2 days (pathdate is start of day at midnight!)
            if (persistenceIsSorted && readerTuple.FileDate < DateTime.Now.AddDays(-2))
            {
                MarkFileAsSorted(readerTuple.FilePath);
            }

            foreach (var dp in dataPoints)
            {
                yield return dp;
            }
        }

        public StorageAccess ActiveAutoSortRawFiles(IDataLayer dataLayer)
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
                var writerSvc = DataLayer.WriterSvc;
                writerSvc.CreateWriteDispose(filePath + ".sorted", false, (writer) =>
                {
                    foreach (var dp in rs)
                    {
                        if (dp.Value != null)
                        {
                            writer.Append(dp.Time.ToString(TimeFormat));
                            writer.Append(" ");
                            writer.Append(dp.Value.Value.ToString(CultureInfo.InvariantCulture));
                            writer.AppendNewline();
                        };
                    }
                    writer.Flush();
                    writer.Close();
                    File.Move(filePath, filePath + ".bak");
                    File.Move(filePath + ".sorted", filePath);
                    //File.Delete(filePath + ".sorted", filePath);
                    Console.WriteLine($"Sorted rewrite of file {filePath}");
                });
            }
        }

        private void MarkFileAsSorted(string currentPath)
        {
            char[] newPath = currentPath.ToCharArray();// "measure$aasd_2024-11-02.dat"
            newPath[newPath.Length - 15] = '#';   // "<measure$aasd#2024-11-02.dat"
            string sortedMarkedPath = new string(newPath);
            File.Move(currentPath, sortedMarkedPath);
        }

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

        private IEnumerable<DataPoint> InternalStreamDataPoints(ReaderTuple sr, int fileyear, int filemonth, int fileday)
        {
            int lineIdx = 0;
            DataPoint? retPrev = null;
            string? dataline;
            while ((dataline = sr.StreamReader!.ReadLine()) != null)
            {
                lineIdx++;
                var ret = DatFileParser.ParseLine(fileyear, filemonth, fileday, dataline, sr.FilePath);
                ret.Source = $"{sr.FilePath}, Line {lineIdx} {sr.MarkedAsSorted}";


                if (retPrev == null) // initial pair or previous pair was fully consumed
                {
                    retPrev = ret;
                    continue;
                }

                if (retPrev.Time == ret.Time) // combine, if same time and consume both
                {
                    retPrev.Value = retPrev.Value ?? ret.Value;
                    retPrev.ValueText = retPrev.ValueText ?? ret.ValueText;
                    yield return retPrev;

                    retPrev = null;
                    continue;
                }

                if (retPrev.Time >= StartTime && retPrev.Time < EndTime) // send if DataPoint is in window
                {
                    yield return retPrev; //send retPrev
                }
                retPrev = ret; //consume retPrev
            }
            // send last DataPoint is in window
            if (retPrev != null && retPrev.Time >= StartTime && retPrev.Time < EndTime)
            {
                yield return retPrev;
            }

            sr.StreamReader.Close();
        }

        public void Dispose()
        {
            TimeSortedStreamReader!.AsParallel().ForAll(sr => sr.Value?.StreamReader?.Dispose());
        }
    }
}
