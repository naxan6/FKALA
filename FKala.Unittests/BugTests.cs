using FKala.Core.KalaQl;
using FKala.Core.Logic;
using FKala.Core;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FluentAssertions;

namespace FKala.Unittests
{
    [TestClass]
    public class BugTests
    {
        string StoragePath = "\\\\naxds2\\docker\\fkala";


        [TestMethod]
        public void BugTest_Datenpunktüberseheneinzusortieren_v1()
        {
            System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();

            sw.Start();
            using var dl = new DataLayer_Readable_Caching_V1(StoragePath);

            var q = KalaQuery.Start()
                .FromQuery(@"
Load rSOC: Sofar/measure/batteryInput1/0x608_SOC_Bat1[%] ""2024-09-20T22:00:00.000Z"" ""2024-09-21T21:59:59.999Z"" Auto(300000)_WAvg_REFRESHINCREMENTAL
Load rSOH: Sofar/measure/batteryInput1/0x609_SOH_Bat1[%] ""2024-09-20T22:00:00.000Z"" ""2024-09-21T21:59:59.999Z"" Auto(300000)_WAvg_REFRESHINCREMENTAL
Aggr SOC: rSOC 300000 WAvg EmptyWindows
Aggr SOH: rSOH 300000 WAvg EmptyWindows
Publ ""SOC, SOH"" Table");

            var result = q.Execute(dl);

            sw.Stop();
            var ts = sw.Elapsed;
            string elapsedTime = String.Format("{0:00}:{1:00}:{2:00}.{3:000}", ts.Hours, ts.Minutes, ts.Seconds, ts.Milliseconds);
            Console.WriteLine("Verstrichene Zeit: " + elapsedTime);

            Console.WriteLine(KalaJson.Serialize(result.ResultTable));// JSON serialize
            Console.WriteLine(KalaJson.Serialize(result.Errors));// JSON serialize
            result.Errors.Should().BeEmpty();
            result.ResultTable.Should().HaveCount(288);
        }



    }
}
