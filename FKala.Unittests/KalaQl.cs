using FKala.TestConsole;
using FKala.TestConsole.DataLayers;
using FKala.TestConsole.KalaQl;
using FKala.TestConsole.KalaQl.Windowing;
using FKala.TestConsole.Logic;
using System.Security.Principal;

namespace FKala.Unittests
{
    [TestClass]
    public class KalaQl
    {
        string testdata = "C:\\git\\FKALA\\FKala.TestConsole\\bin\\Debug\\net8.0\\data";

        [TestMethod]
        public void KalaQl_2_Datasets()
        {
            System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();

            sw.Start();
            using var dl = new DataLayer_Readable_V1(testdata);

            var startTime = new DateTime(2024, 07, 20, 0, 0, 0);
            var endTime = new DateTime(2024, 08, 01, 0, 0, 0);

            var q = KalaQuery.Start()
                .Add(new Op_BaseQuery("PV1", "Sofar/measure/PVInput1/0x586_Leistung_PV1[kW]", startTime, endTime))
                .Add(new Op_BaseQuery("PV2", "Sofar/measure/PVInput1/0x589_Leistung_PV2[kW]", startTime, endTime))
                .Add(new Op_Publish(new List<string>() { "PV1", "PV2" }, PublishMode.MultipleResultsets));

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
            using var dl = new DataLayer_Readable_V1(testdata);

            var startTime = new DateTime(2024, 05, 01, 0, 0, 0);
            var endTime = new DateTime(2024, 06, 01, 0, 0, 0);

            var q = KalaQuery.Start()
                .Add(new Op_BaseQuery("PV1", "Sofar/measure/PVInput1/0x586_Leistung_PV1[kW]", startTime, endTime))
                .Add(new Op_BaseQuery("PV2", "Sofar/measure/PVInput1/0x589_Leistung_PV2[kW]", startTime, endTime))
                .Add(new Op_Aggregate("PV1_Windowed", "PV1", Window.Aligned_1Day, AggregateFunction.Avg, true))
                .Add(new Op_Aggregate("PV2_Windowed", "PV2", Window.Aligned_1Day, AggregateFunction.Avg, true))
                .Add(new Op_Publish(new List<string>() { "PV1_Windowed", "PV2_Windowed" }, PublishMode.MultipleResultsets));

            var result = q.Execute(dl);


            sw.Stop();
            var ts = sw.Elapsed;
            string elapsedTime = String.Format("{0:00}:{1:00}:{2:00}.{3:000}", ts.Hours, ts.Minutes, ts.Seconds, ts.Milliseconds);
            Console.WriteLine("Verstrichene Zeit: " + elapsedTime);

            Assert.AreEqual(2, result?.ResultSets.Count);
            Console.WriteLine(KalaJson.Serialize(result.ResultSets));// JSON serialize
        }

        [TestMethod]
        public void KalaQl_2_Datasets_Aggregated_1y()
        {
            System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();

            sw.Start();
            using var dl = new DataLayer_Readable_V1(testdata);

            var startTime = new DateTime(2023, 08, 01, 0, 0, 0);
            var endTime = new DateTime(2024, 08, 01, 0, 0, 0);

            var q = KalaQuery.Start()
                .Add(new Op_BaseQuery("PV1", "Sofar/measure/PVInput1/0x586_Leistung_PV1[kW]", startTime, endTime))                
                .Add(new Op_Aggregate("PV1_Windowed", "PV1", Window.Aligned_1Day, AggregateFunction.First, true))
                .Add(new Op_Publish(new List<string>() { "PV1_Windowed" }, PublishMode.MultipleResultsets));

            var result = q.Execute(dl);


            sw.Stop();
            var ts = sw.Elapsed;
            string elapsedTime = String.Format("{0:00}:{1:00}:{2:00}.{3:000}", ts.Hours, ts.Minutes, ts.Seconds, ts.Milliseconds);
            Console.WriteLine("Verstrichene Zeit: " + elapsedTime);

            Assert.AreEqual(1, result?.ResultSets.Count);
            Console.WriteLine(KalaJson.Serialize(result.ResultSets));// JSON serialize
        }

        [TestMethod]
        public void KalaQl_Text_2_Datasets_Aggregated_Expresso()
        {
            System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();

            sw.Start();
            using var dl = new DataLayer_Readable_V1(testdata);

            var startTime = new DateTime(2024, 06, 01, 0, 0, 0);
            var endTime = new DateTime(2024, 08, 01, 0, 0, 0);

            var q = KalaQuery.Start()
                .FromQuery(@"
                    Load PV1: Sofar/measure/PVInput1/0x586_Leistung_PV1[kW] 2024-06-01T00:00:00 2024-08-01T00:00:00
                    Load PV2: Sofar/measure/PVInput1/0x589_Leistung_PV2[kW] 2024-06-01T00:00:00 2024-08-01T00:00:00
                    Aggr PV1_Windowed: PV1 Unaligned_1Month Avg
                    Aggr PV2_Windowed: PV2 Unaligned_1Month Avg
                    Expr PVSumInWatt: ""(PV1_Windowed.Value + PV2_Windowed.Value) * 1000""
                    Publ ""PVSumInWatt, PV1_Windowed"" Default
                ");                

            var result = q.Execute(dl);

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
            using var dl = new DataLayer_Readable_V1(testdata);

            var startTime = new DateTime(2023, 08, 01, 0, 0, 0);
            var endTime = new DateTime(2024, 08, 01, 0, 0, 0);

            var q = KalaQuery.Start()
                .Add(new Op_BaseQuery("PV1", "Sofar/measure/PVInput1/0x586_Leistung_PV1[kW]", startTime, endTime))
                .Add(new Op_BaseQuery("PV2", "Sofar/measure/PVInput1/0x589_Leistung_PV2[kW]", startTime, endTime))
                .Add(new Op_Aggregate("PV1_Windowed", "PV1", Window.Aligned_1Day, AggregateFunction.Avg, true))
                .Add(new Op_Aggregate("PV2_Windowed", "PV2", Window.Aligned_1Day, AggregateFunction.Avg, true))
                .Add(new Op_Expresso("PVSumInWatt", "(PV1_Windowed.Value + PV2_Windowed.Value) * 1000"))
                .Add(new Op_Publish(new List<string>() { "PVSumInWatt", "PV1_Windowed", "PV2_Windowed" }, PublishMode.CombinedResultset));

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
            using var dl = new DataLayer_Readable_V1(testdata);

            var q = KalaQuery.Start()
                .FromQuery(@"
                    Load PV1: Sofar/measure/PVInput1/0x586_Leistung_PV1[kW] 2024-05-01T00:00:00 2024-06-01T00:00:00
                    Load PV2: Sofar/measure/PVInput1/0x589_Leistung_PV2[kW] 2024-05-01T00:00:00 2024-06-01T00:00:00
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
        }
    }
}