using FKala.Core.KalaQl;
using FKala.Core.Logic;
using FKala.Core;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FluentAssertions;
using FKala.Core.Interfaces;
using FKala.Core.Model;

namespace FKala.Unittests
{
    [TestClass]
    public class BugTests
    {
        


        [TestMethod]
        public void BugTest_EmptySensorDir_CausesSartAtDateTimeMinValue()
        {
            System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();

            sw.Start();
            using var dl = new DataLayer_Readable_Caching_V1(@".\Testdata\Empty\");

            var q = KalaQuery.Start()
                .FromQuery(@"Var $FROM ""2024-09-20T19:09:46.645Z""
Var $TO ""2024-09-21T19:09:46.645Z""
Var $CACHE Auto(3600000)_WAvg_RefreshIncremental
Var $AGG ""WAvg EmptyWindows""
Var $INTERVAL 3600000

Load rVar1: emptySensor $FROM $TO $CACHE
Load rVar2: emptySensor2 $FROM $TO $CACHE
Aggr aVar1: rVar1 $INTERVAL $AGG
Aggr aVar2: rVar2 $INTERVAL $AGG
Publ aVar1,aVar2 Table");

            var result = q.Execute(dl);

            sw.Stop();
            var ts = sw.Elapsed;
            string elapsedTime = String.Format("{0:00}:{1:00}:{2:00}.{3:000}", ts.Hours, ts.Minutes, ts.Seconds, ts.Milliseconds);
            Console.WriteLine("Verstrichene Zeit: " + elapsedTime);

            Console.WriteLine(KalaJson.Serialize(result.ResultTable));// JSON serialize
            Console.WriteLine(KalaJson.Serialize(result.Errors));// JSON serialize
            result.Errors.Should().BeEmpty();
            result.ResultTable.Should().HaveCount(24);
        }

        [TestMethod]
        public void BugExceptionTest_MarkedSortedsBut_DefectInDataFile_DoubleContent()
        {            
            var localPath = FileSystemHelper.ConvertToLocalPath(@".\Testdata\defectdata");
            var localCachePath = FileSystemHelper.ConvertToLocalPath(@".\Testdata\defectdata\cache");
            if (Directory.Exists(localCachePath))
            {
                Directory.Delete(localCachePath, true);
            }
            System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();

            sw.Start();
            using var dl = new DataLayer_Readable_Caching_V1(localPath);

            var q = KalaQuery.Start()
                .FromQuery(@"Var $FROM ""2024-09-03T19:48:51.348Z""
Var $TO ""2024-09-21T19:48:51.348Z""
Var $CACHE Auto(120000)_WAvg_RefreshIncremental
Var $AGG ""WAvg EmptyWindows""
Var $INTERVAL 120000

Load rVar1: doubledata $FROM $TO $CACHE
Aggr aVar1: rVar1 $INTERVAL $AGG
Publ aVar1 Table");

            var result = q.Execute(dl);

            sw.Stop();
            var ts = sw.Elapsed;
            string elapsedTime = String.Format("{0:00}:{1:00}:{2:00}.{3:000}", ts.Hours, ts.Minutes, ts.Seconds, ts.Milliseconds);
            Console.WriteLine("Verstrichene Zeit: " + elapsedTime);

            Console.WriteLine(KalaJson.Serialize(result.ResultTable));// JSON serialize
            Console.WriteLine(KalaJson.Serialize(result.Errors));// JSON serialize
            result.Errors.Should().HaveCount(2);
            result.Errors.Last().Should().ContainAll([
                "FKala.Core.DataLayers.UnexpectedlyUnsortedException: Marked sorted but unsorted at File "],
                ", Line 9904 True ## 09:23:11.9220000 15728\r\n", "invalid data should throw error message");

        }

        [TestMethod]
        public void BugTest_EmptyLatestFile_WatermarkFallsToMinValue_LateInsertCorruptsSortedDay()
        {
            var testStorage = Directory.CreateTempSubdirectory("fkala_watermark");
            try
            {
                var monthDir = Path.Combine(testStorage.FullName, "data", "wm", "2024", "03");
                Directory.CreateDirectory(monthDir);
                // sorted day 03-26, tail at 20:27
                File.WriteAllText(Path.Combine(monthDir, "wm#2024-03-26.dat"),
                    "09:00:00.0000000 1\n10:00:00.0000000 2\n20:27:00.0000000 3\n");
                // unsorted older day: must never seed the watermark
                File.WriteAllText(Path.Combine(monthDir, "wm_2024-03-05.dat"),
                    "08:00:00.0000000 7\n06:00:00.0000000 8\n");
                // newest file (empty) must not abort the newest-datapoint lookup
                File.WriteAllText(Path.Combine(monthDir, "wm#2024-03-28.dat"), "");

                using var dl = new DataLayer_Readable_Caching_V1(testStorage.FullName);

                // late point on day 26, below that day's tail: must be classified as delayed
                dl.Insert("wm 2024-03-26T02:01:14.0000000 6");
                dl.ProcessRemainingQueueItems();
                dl.BufferedWriterSvc.ForceFlushWriters();

                var points = dl.LoadData("wm", new DateTime(2024, 3, 1), new DateTime(2024, 4, 1),
                    CacheResolutionPredefined.NoCache, false, new KalaQlContext(new KalaQuery(), dl), false).ToList();

                points.Select(p => p.StartTime.ToString("MM-dd HH:mm:ss")).Should().BeEquivalentTo([
                    "03-05 06:00:00", "03-05 08:00:00",
                    "03-26 02:01:14", "03-26 09:00:00", "03-26 10:00:00", "03-26 20:27:00"],
                    "late point must land in time order via merge, not appended after a MinValue watermark");
                points.Last(p => p.StartTime.Day == 26).Value!.Value.Should().Be(3);
            }
            finally
            {
                testStorage.Delete(true);
            }
        }

        [TestMethod]
        public void BugTest_IncrementalRefreshWithEmptyLatestDatFile_DoesNotCrash()
        {
            var testStorage = Directory.CreateTempSubdirectory("fkala_incr_empty");
            try
            {
                var monthDir = Path.Combine(testStorage.FullName, "data", "wm", "2024", "03");
                Directory.CreateDirectory(monthDir);
                File.WriteAllText(Path.Combine(monthDir, "wm#2024-03-26.dat"),
                    "09:00:00.0000000 1\n10:00:00.0000000 2\n20:27:00.0000000 3\n");
                File.WriteAllText(Path.Combine(monthDir, "wm#2024-03-28.dat"), "");

                using var dl = new DataLayer_Readable_Caching_V1(testStorage.FullName);
                const string kala = @"
Load r: wm ""2024-03-01T00:00:00"" ""2024-03-29T00:00:00"" Minutely_WAvg_REFRESHINCREMENTAL
Publ ""r"" Table";

                var first = KalaQuery.Start().FromQuery(kala).Execute(dl);   // builds cache
                first.Errors.Should().BeEmpty();
                first.ResultTable!.Count().Should().BeGreaterThan(0);

                // second run takes the incremental branch with an empty newest dat file
                var second = KalaQuery.Start().FromQuery(kala).Execute(dl);
                second.Errors.Should().BeEmpty();
                second.Exceptions.Should().BeEmpty();
                second.ResultTable!.Should().NotBeEmpty();
            }
            finally
            {
                testStorage.Delete(true);
            }
        }

    }
}
