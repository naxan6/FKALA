using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static FKala.Core.DataLayer_Readable_Caching_V1;

namespace FKala.Core.DataLayers
{
    public class StorageHelper
    {
        FileStreamOptions fileStreamOptions = new FileStreamOptions()
        {
            Access = FileAccess.Read,
            BufferSize = 65536,
            Mode = FileMode.Open,
            Share = FileShare.ReadWrite | FileShare.Delete
        };

        EnumerationOptions optionFindFilesRecursive = new EnumerationOptions()
        {
            BufferSize = 65536,
            RecurseSubdirectories = true,
            ReturnSpecialDirectories = false,
            AttributesToSkip = FileAttributes.Hidden
        };

        private SortedDictionary<DateTime, ReaderTuple> tuples;

        public StorageHelper()
        {
        }

        public static StorageHelper Init(string measurementPath, string measurementPathPart, DateTime startTime, DateTime endTime)
        {
            var ret = new StorageHelper();
            ret.tuples = ret.GetFilePaths(measurementPath, measurementPathPart, startTime, endTime);
            return ret;
        }

        public StorageHelper OpenStreamReaders()
        {
            this.tuples.AsParallel().ForAll(t => t.Value.StreamReader = new StreamReader(t.Value.Path, Encoding.UTF8, false, fileStreamOptions));
            return this;
        }

        public SortedDictionary<DateTime, ReaderTuple> GetOpenStreamReaders()
        {
            return tuples;
        }
        
        private SortedDictionary<DateTime, ReaderTuple> GetFilePaths(string measurementPath, string measurementPathPart, DateTime startTime, DateTime endTime)
        {
            // years
            int startYear = startTime.Year;
            int endYear = endTime.Year;
            var years = GetYearFolders(measurementPath);
            var filteredYears = years.Where(y => y >= startYear && y <= endYear);

            // files
            string filter = $"{measurementPathPart}_*.dat";
            var fileCandidates = filteredYears.AsParallel().SelectMany(y => Directory.GetFileSystemEntries(Path.Combine(measurementPath, y.ToString()), filter, optionFindFilesRecursive)).ToList();
            SortedDictionary<DateTime, ReaderTuple> ret = new SortedDictionary<DateTime, ReaderTuple>();

            foreach (var candidate in fileCandidates)
            {
                string barename = Path.GetFileNameWithoutExtension(candidate);
                var datePart = barename.Substring(barename.Length - 10, 10);
                ReadOnlySpan<char> dateSpan = datePart.AsSpan();
                // DateOnly dt = new DateOnly(int.Parse(dateSpan.Slice(0, 4)), int.Parse(dateSpan.Slice(5, 2)), int.Parse(dateSpan.Slice(8, 2)));
                int fileyear = int.Parse(dateSpan.Slice(0, 4));
                int filemonth = int.Parse(dateSpan.Slice(5, 2));
                int fileday = int.Parse(dateSpan.Slice(8, 2));

                var fileDateTime = new DateTime(fileyear, filemonth, fileday, 0, 0, 0, DateTimeKind.Utc);

                if (startTime < fileDateTime.AddDays(1) && fileDateTime < endTime)
                {
                    try
                    {
                        ret.Add(fileDateTime, new ReaderTuple() { PathDate = fileDateTime, Path = candidate });
                    }
                    catch (ArgumentException ex)
                    {
                        Console.WriteLine($"BUG in raw data: multiple files for date {fileDateTime.ToString("s")} : {candidate}. Skipping file.");
                    }
                }
            }

            return ret;
        }

        private List<int> GetYearFolders(string measurementDir)
        {
            var entries = Directory.GetDirectories(measurementDir, "*", new EnumerationOptions() { ReturnSpecialDirectories = false, BufferSize = 16384, });
            return entries.Where(e => Path.GetFileName(e) != ".DS_Store").Select(y => int.Parse(Path.GetFileName(y))).ToList();
        }
    }
}
