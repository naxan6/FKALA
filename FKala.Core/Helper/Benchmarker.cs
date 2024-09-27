using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static FKala.Core.Helper.Benchmarker;

namespace FKala.Core.Helper
{
    public class Benchmarker
    {

        public class BenchmarkResult
        {
            public Dictionary<long, TimeSpan> Reading { get; set; }
            public Dictionary<long, TimeSpan> Writing { get; set; }
        }

        public static BenchmarkResult Bench(string baseDir)
        {
            var benchBase = Path.Combine(baseDir, "benches");
            var readDir = Path.Combine(benchBase, "read");
            var writeDir = Path.Combine(benchBase, "write");
            Directory.CreateDirectory(readDir);
            Directory.CreateDirectory(writeDir);
            var rResults = BenchmarkReads(readDir);
            var wResults = BenchmarkWrites(writeDir);
            Directory.Delete(benchBase, true);

            return new BenchmarkResult()
            {
                Reading = rResults,
                Writing = wResults
            };
        }

        static Dictionary<long, TimeSpan> BenchmarkReads(string dir)
        {
            Stopwatch sw = new Stopwatch();
            Random rand = new Random();  // seed a random number generator
            int numberOfBytes = 2 << 22; //8,192KB File
            byte nextByte;
            TimeSpan min = TimeSpan.MaxValue;
            string fastestString = "";
            string bestFactorString = "";
            long fastestBufferSize = 0;
            long cheapestBufferSize = 0;
            FileStreamOptions fileStreamOptions = new FileStreamOptions()
            {
                Access = FileAccess.Read,
                BufferSize = 131072,
                Mode = FileMode.Open,
                Share = FileShare.ReadWrite | FileShare.Delete
            };

            Dictionary<long, TimeSpan> results = new Dictionary<long, TimeSpan>();

            for (int i = 12; i <= 24; i++) //Limited loop to 28 to prevent out of memory
            {

                var buffersize = 2 << i;
                string testfile = Path.Combine(dir, $"TEST{i}.DAT");  // name of file
                using (FileStream fs = new FileStream(
                    testfile,  // name of file
                    FileMode.Create,    // create or overwrite existing file
                    FileAccess.Write,   // write-only access
                    FileShare.None,     // no sharing
                    2 << 20,             // block transfer of i=18 -> size = 256 KB
                    FileOptions.None))
                {
                    for (int j = 0; j < numberOfBytes; j++)
                    {
                        nextByte = (byte)(rand.Next() % 256); // generate a random byte
                        fs.WriteByte(nextByte);               // write it
                    }
                }

                fileStreamOptions.BufferSize = buffersize;
                sw.Start();
                for (int repeat = 0; repeat < 1; repeat++)
                {

                    var sr = new StreamReader(testfile, Encoding.UTF8, false, fileStreamOptions);
                    var readstring = sr.ReadToEnd();
                }
                sw.Stop();
                var elapsed = sw.Elapsed;
                var outs = $"READ Buffer is 2 << {i} - {buffersize} - Elapsed: {elapsed}";
                Console.WriteLine(outs);

                results.Add(buffersize, elapsed);

                if (min > sw.Elapsed)
                {
                    min = sw.Elapsed;
                    fastestString = outs;
                    fastestBufferSize = buffersize;
                }
                sw.Reset();

            }
            Console.WriteLine("FASTEST READ: " + fastestString);
            Console.WriteLine("CHEAPEST READ: " + bestFactorString);

            return results;
        }

        static Dictionary<long, TimeSpan> BenchmarkWrites(string dir)
        {
            Stopwatch sw = new Stopwatch();
            Random rand = new Random();  // seed a random number generator
            int numberOfBytes = 2 << 22; //8,192KB File
            byte nextByte;
            TimeSpan min = TimeSpan.MaxValue;
            string fastestString = "";
            string bestFactorString = "";
            long fastestBufferSize = 0;
            long cheapestBufferSize = 0;
            double bestPerFactor = 0;
            double perFactor = 0;
            Dictionary<long, TimeSpan> results = new Dictionary<long, TimeSpan>();
            for (int i = 12; i <= 24; i++) //Limited loop to 28 to prevent out of memory
            {

                var buffersize = 2 << i;
                sw.Start();
                string testfile = Path.Combine(dir, $"TEST{i}.DAT");
                for (int repeat = 0; repeat < 1; repeat++)
                {
                    using (FileStream fs = new FileStream(
                       testfile,  // name of file
                        FileMode.Create,    // create or overwrite existing file
                        FileAccess.Write,   // write-only access
                        FileShare.None,     // no sharing
                        buffersize,             // block transfer of i=18 -> size = 256 KB
                        FileOptions.None))
                    {
                        for (int j = 0; j < numberOfBytes; j++)
                        {
                            nextByte = (byte)(rand.Next() % 256); // generate a random byte
                            fs.WriteByte(nextByte);               // write it
                        }
                    }
                }
                sw.Stop();
                var elapsed = sw.Elapsed;
                perFactor = (1000000 - elapsed.TotalMilliseconds) / buffersize;
                var outs = $"WRITE Buffer is 2 << {i} - {buffersize} - Elapsed: {elapsed} - price/performance {perFactor}";
                Console.WriteLine(outs);
                results.Add(buffersize, elapsed);
                if (min > sw.Elapsed)
                {
                    min = sw.Elapsed;
                    fastestString = outs;
                    fastestBufferSize = buffersize;
                }
                if (bestPerFactor > perFactor)
                {
                    bestPerFactor = perFactor;
                    bestFactorString = outs;
                    cheapestBufferSize = buffersize;

                }
                sw.Reset();
            }

            Console.WriteLine("FASTEST WRITE: " + fastestString);
            Console.WriteLine("CHEAPEST WRITE: " + bestFactorString);
            return results;
        }

    }
}
