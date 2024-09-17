using FKala.Core;
using FKala.Core.KalaQl;
using FKala.Core.KalaQl.Windowing;
using FKala.Core.Logic;
using FKala.Core.Model;
using FluentAssertions;
using System.Security.Principal;

namespace FKala.Unittests
{
    [TestClass]
    public class KalaQl
    {
        string StoragePath = "\\\\naxds2\\docker\\fkala";

        [TestMethod]
        public void KalaQl_2_Datasets()
        {
            System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();

            sw.Start();
            using var dl = new DataLayer_Readable_Caching_V1(StoragePath);

            var startTime = new DateTime(2024, 07, 20, 0, 0, 0);
            var endTime = new DateTime(2024, 08, 01, 0, 0, 0);

            var q = KalaQuery.Start()
                .Add(new Op_BaseQuery(null, "PV1", "Sofar/measure/PVInput1/0x586_Leistung_PV1[kW]", startTime, endTime, CacheResolutionPredefined.NoCache))
                .Add(new Op_BaseQuery(null, "PV2", "Sofar/measure/PVInput1/0x589_Leistung_PV2[kW]", startTime, endTime, CacheResolutionPredefined.NoCache))
                .Add(new Op_Publish(null, new List<string>() { "PV1", "PV2" }, PublishMode.MultipleResultsets));

            var result = q.Execute(dl);


            sw.Stop();
            var ts = sw.Elapsed;
            string elapsedTime = String.Format("{0:00}:{1:00}:{2:00}.{3:000}", ts.Hours, ts.Minutes, ts.Seconds, ts.Milliseconds);
            Console.WriteLine("Verstrichene Zeit: " + elapsedTime);

            Assert.AreEqual(2, result?.ResultSets.Count);
            Console.WriteLine(KalaJson.Serialize(result));// JSON serialize
        }
        [TestMethod]
        public void KalaQl_2_Datasets_Aggregated()
        {
            System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();

            sw.Start();
            using var dl = new DataLayer_Readable_Caching_V1(StoragePath);

            var startTime = new DateTime(2024, 05, 01, 0, 0, 0);
            var endTime = new DateTime(2024, 06, 01, 0, 0, 0);

            var q = KalaQuery.Start()
                .Add(new Op_BaseQuery(null, "PV1", "Sofar/measure/PVInput1/0x586_Leistung_PV1[kW]", startTime, endTime, CacheResolutionPredefined.NoCache))
                .Add(new Op_BaseQuery(null, "PV2", "Sofar/measure/PVInput1/0x589_Leistung_PV2[kW]", startTime, endTime, CacheResolutionPredefined.NoCache))
                .Add(new Op_Aggregate(null, "PV1_Windowed", "PV1", Window.Aligned_1Day, AggregateFunction.Avg, true))
                .Add(new Op_Aggregate(null, "PV2_Windowed", "PV2", Window.Aligned_1Day, AggregateFunction.Avg, true))
                .Add(new Op_Publish(null, new List<string>() { "PV1_Windowed", "PV2_Windowed" }, PublishMode.MultipleResultsets));

            var result = q.Execute(dl);


            sw.Stop();
            var ts = sw.Elapsed;
            string elapsedTime = String.Format("{0:00}:{1:00}:{2:00}.{3:000}", ts.Hours, ts.Minutes, ts.Seconds, ts.Milliseconds);
            Console.WriteLine("Verstrichene Zeit: " + elapsedTime);
            result.Should().NotBeNull();
            Assert.AreEqual(2, result!.ResultSets.Count);
            Console.WriteLine(KalaJson.Serialize(result!.ResultSets));// JSON serialize
        }

        [TestMethod]
        public void KalaQl_2_Datasets_Aggregated_1y()
        {
            System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();

            sw.Start();
            using var dl = new DataLayer_Readable_Caching_V1(StoragePath);

            var startTime = new DateTime(2023, 08, 01, 0, 0, 0);
            var endTime = new DateTime(2024, 08, 01, 0, 0, 0);

            var q = KalaQuery.Start()
                .Add(new Op_BaseQuery(null, "PV1", "Sofar/measure/PVInput1/0x586_Leistung_PV1[kW]", startTime, endTime, CacheResolutionPredefined.NoCache))
                .Add(new Op_Aggregate(null, "PV1_Windowed", "PV1", Window.Aligned_1Day, AggregateFunction.First, true))
                .Add(new Op_Publish(null, new List<string>() { "PV1_Windowed" }, PublishMode.MultipleResultsets));

            var result = q.Execute(dl);


            sw.Stop();
            var ts = sw.Elapsed;
            string elapsedTime = String.Format("{0:00}:{1:00}:{2:00}.{3:000}", ts.Hours, ts.Minutes, ts.Seconds, ts.Milliseconds);
            Console.WriteLine("Verstrichene Zeit: " + elapsedTime);

            result.Should().NotBeNull();
            Assert.AreEqual(1, result!.ResultSets.Count);
            Console.WriteLine(KalaJson.Serialize(result!.ResultSets));// JSON serialize
        }

        [TestMethod]
        public void KalaQl_Text_2_Datasets_Aggregated_Expresso()
        {
            System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();

            sw.Start();
            using var dl = new DataLayer_Readable_Caching_V1(StoragePath);

            var q = KalaQuery.Start()
                .FromQuery(@"
                    Load PV1: Sofar/measure/PVInput1/0x586_Leistung_PV1[kW] 2024-06-01T00:00:00 2024-08-01T00:00:00 NoCache
                    Load PV2: Sofar/measure/PVInput1/0x589_Leistung_PV2[kW] 2024-06-01T00:00:00 2024-08-01T00:00:00 NoCache
                    Aggr PV1_Windowed: PV1 Unaligned_1Month Avg
                    Aggr PV2_Windowed: PV2 Unaligned_1Month Avg
                    Expr PVSumInWatt: ""(PV1_Windowed.Value + PV2_Windowed.Value) * 1000""
                    Publ ""PVSumInWatt"" Default
                ");

            var result = q.Execute(dl);

            result.Errors.ForEach(e => Console.WriteLine(e));
            Assert.IsTrue(!result.Errors.Any());

            foreach (var rs in result.ResultSets)
            {
                rs.Resultset.ToList();
            }
            sw.Stop();
            var ts = sw.Elapsed;
            string elapsedTime = String.Format("{0:00}:{1:00}:{2:00}.{3:000}", ts.Hours, ts.Minutes, ts.Seconds, ts.Milliseconds);
            Console.WriteLine("Verstrichene Zeit: " + elapsedTime);

            Console.WriteLine(KalaJson.Serialize(result.ResultSets));// JSON serialize
        }

        [TestMethod]
        public void BUGTEST_KalaQl_2_Datasets_Aggregated_Expresso()
        {
            System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();

            sw.Start();
            using var dl = new DataLayer_Readable_Caching_V1(StoragePath);

            var startTime = new DateTime(2023, 08, 01, 0, 0, 0);
            var endTime = new DateTime(2024, 08, 01, 0, 0, 0);

            var q = KalaQuery.Start()
                .Add(new Op_BaseQuery(null, "PV1", "Sofar/measure/PVInput1/0x586_Leistung_PV1[kW]", startTime, endTime, CacheResolutionPredefined.NoCache))
                .Add(new Op_BaseQuery(null, "PV2", "Sofar/measure/PVInput1/0x589_Leistung_PV2[kW]", startTime, endTime, CacheResolutionPredefined.NoCache))
                .Add(new Op_Aggregate(null, "PV1_Windowed", "PV1", Window.Aligned_1Day, AggregateFunction.Avg, true))
                .Add(new Op_Aggregate(null, "PV2_Windowed", "PV2", Window.Aligned_1Day, AggregateFunction.Avg, true))
                .Add(new Op_Expresso(null, "PVSumInWatt", "(PV1_Windowed.Value + PV2_Windowed.Value) * 1000"))
                .Add(new Op_Publish(null, new List<string>() { "PVSumInWatt", "PV1_Windowed", "PV2_Windowed" }, PublishMode.CombinedResultset));

            var result = q.Execute(dl);


            sw.Stop();
            var ts = sw.Elapsed;
            string elapsedTime = String.Format("{0:00}:{1:00}:{2:00}.{3:000}", ts.Hours, ts.Minutes, ts.Seconds, ts.Milliseconds);
            Console.WriteLine("Verstrichene Zeit: " + elapsedTime);

            Console.WriteLine(KalaJson.Serialize(result.ResultTable));// JSON serialize
        }

        [TestMethod]
        public void BUGTEST_KalaQl_Text_2_Datasets_Aggregated_Expresso_Table()
        {
            System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();

            sw.Start();
            using var dl = new DataLayer_Readable_Caching_V1(StoragePath);

            var q = KalaQuery.Start()
                .FromQuery(@"
                    Load PV1: Sofar/measure/PVInput1/0x586_Leistung_PV1[kW] 2024-05-01T00:00:00 2024-06-01T00:00:00 NoCache
                    Load PV2: Sofar/measure/PVInput1/0x589_Leistung_PV2[kW] 2024-05-01T00:00:00 2024-06-01T00:00:00 NoCache
                    Aggr PV1_Windowed: PV1 Aligned_1Day Avg
                    Aggr PV2_Windowed: PV2 Aligned_1Day Avg
                    Expr PVSumInWatt: ""(PV1_Windowed.Value + PV2_Windowed.Value) * 1000""
                    Publ ""PVSumInWatt, PV1_Windowed, PV2_Windowed"" Table
                ");

            var result = q.Execute(dl);

            sw.Stop();
            var ts = sw.Elapsed;
            string elapsedTime = String.Format("{0:00}:{1:00}:{2:00}.{3:000}", ts.Hours, ts.Minutes, ts.Seconds, ts.Milliseconds);
            Console.WriteLine("Verstrichene Zeit: " + elapsedTime);

            Console.WriteLine(KalaJson.Serialize(result.ResultTable));// JSON serialize
            Console.WriteLine(KalaJson.Serialize(result.Errors));// JSON serialize
            Assert.AreEqual(0, result.Errors.Count());
        }

        [TestMethod]
        public void PerTest_PV_Query()
        {
            System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();

            sw.Start();
            using var dl = new DataLayer_Readable_Caching_V1(StoragePath);

            var q = KalaQuery.Start()
                .FromQuery(@"
                    Load rPV1: Sofar/measure/PVInput1/0x586_Leistung_PV1[kW] 2024-04-01T00:00:00Z 2024-09-01T00:00:00Z Hourly_WAvg
                    Load rPV2: Sofar/measure/PVInput1/0x589_Leistung_PV2[kW] 2024-04-01T00:00:00Z 2024-09-01T00:00:00Z Hourly_WAvg
                    Load rNetz: Sofar/measure/OnGridOutput/0x488_ActivePower_PCC_Total[kW] 2024-04-01T00:00:00Z 2024-09-01T00:00:00Z Hourly_WAvg
                    Load rAkku: Sofar/measure/batteryInput1/0x606_Power_Bat1[kW] 2024-04-01T00:00:00Z 2024-09-01T00:00:00Z Hourly_WAvg
                    Load rVerbrauch: Sofar/measure/OnGridOutput/0x4AF_ActivePower_Load_Sys[kW] 2024-04-01T00:00:00Z 2024-09-01T00:00:00Z Hourly_WAvg

                    Aggr aPV1: rPV1 Aligned_1Hour WAvg EmptyWindows
                    Aggr aPV2: rPV2 Aligned_1Hour WAvg EmptyWindows
                    Aggr aNetz: rNetz Aligned_1Hour WAvg EmptyWindows
                    Aggr aAkku: rAkku Aligned_1Hour WAvg EmptyWindows
                    Aggr aVerbrauch: rVerbrauch Aligned_1Hour WAvg EmptyWindows
                    Aggr aAkkuladung: rAkku Aligned_1Hour WAvg EmptyWindows


                    Expr PV: ""aPV1.Value + aPV2.Value""
                    Expr Netzbezug: ""aNetz.Value > 0 ? 0 : -aNetz.Value""
                    Expr Netzeinspeisung: ""aNetz.Value < 0 ? 0 : aNetz.Value""
                    Expr Akkuentladung: ""aAkku.Value > 0 ? 0 : -aAkku.Value""
                    Expr Akkuladung: ""aAkku.Value < 0 ? 0 : aAkku.Value""
                    Expr Verbrauch: ""aVerbrauch.Value""

                    Publ ""Netzbezug,PV,Netzeinspeisung,Akkuentladung,Akkuladung,Verbrauch"" Table
                ");

            var result = q.Execute(dl);

            sw.Stop();
            var ts = sw.Elapsed;
            string elapsedTime = String.Format("{0:00}:{1:00}:{2:00}.{3:000}", ts.Hours, ts.Minutes, ts.Seconds, ts.Milliseconds);
            Console.WriteLine("Verstrichene Zeit: " + elapsedTime);

            Console.WriteLine(KalaJson.Serialize(result.ResultTable));// JSON serialize
        }



    }
}