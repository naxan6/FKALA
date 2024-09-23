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
using System.Runtime.Intrinsics.X86;

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
            Directory.Delete(localCachePath, true);
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

    }
}
