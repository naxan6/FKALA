using FKala.TestConsole;
using FKala.TestConsole.DataLayers;

namespace FKala.Unittests
{
    [TestClass]
    public class MiniPerfTests
    {
        string testdata = "C:\\git\\FKALA\\FKala.TestConsole\\bin\\Debug\\net8.0\\data";
        [TestMethod]
        public void Aggregate_Count_Performance()
        {
            System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();

            sw.Start();
            using var dl = new DataLayer_Readable_V1(testdata);
            var ql = new KalaQlHardcoded(dl);
            //var result = ql.Query("Sofar$measure$PVInput1$0x586_Leistung_PV1[kW]", new DateTime(2023, 08, 01, 0, 0, 0), new DateTime(2024, 08, 01, 0, 0, 0));
            var result = ql.Aggregate("Sofar$measure$PVInput1$0x586_Leistung_PV1[kW]", new DateTime(2023, 08, 01, 0, 0, 0), new DateTime(2024, 08, 02, 0, 0, 0), new TimeSpan(24*367, 0, 0), "COUNT", true, 0);
            var list = result.ToList(); // Daten laden
            sw.Stop();
            var ts = sw.Elapsed;
            string elapsedTime = String.Format("{0:00}:{1:00}:{2:00}.{3:000}", ts.Hours, ts.Minutes, ts.Seconds, ts.Milliseconds);
            Console.WriteLine("Verstrichene Zeit: " + elapsedTime);
            Console.WriteLine("Treffer: " + list.Count());
            
            Console.WriteLine(ql.SerializeDatapoints(list));// JSON serialize
        }

        [TestMethod]
        public void Aggregate_First_Performance()
        {
            System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();

            sw.Start();
            using var dl = new DataLayer_Readable_V1(testdata);
            var ql = new KalaQlHardcoded(dl);
            //var result = ql.Query("Sofar$measure$PVInput1$0x586_Leistung_PV1[kW]", new DateTime(2023, 08, 01, 0, 0, 0), new DateTime(2024, 08, 01, 0, 0, 0));
            var result = ql.Aggregate("Sofar$measure$PVInput1$0x586_Leistung_PV1[kW]", new DateTime(2023, 08, 01, 0, 0, 0), new DateTime(2024, 08, 01, 0, 0, 0), new TimeSpan(24, 00, 0), "FIRST", true, 0);
            var list = result.ToList(); // Daten laden
            sw.Stop();
            var ts = sw.Elapsed;
            string elapsedTime = String.Format("{0:00}:{1:00}:{2:00}.{3:000}", ts.Hours, ts.Minutes, ts.Seconds, ts.Milliseconds);
            Console.WriteLine("Verstrichene Zeit: " + elapsedTime);
            Console.WriteLine("Treffer: " + list.Count());
            
            Console.WriteLine(ql.SerializeDatapoints(list));// JSON serialize
        }

        [TestMethod]
        public void Aggregate_Mean_Performance()
        {
            System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();

            sw.Start();
            using var dl = new DataLayer_Readable_V1(testdata);
            var ql = new KalaQlHardcoded(dl);
            //var result = ql.Query("Sofar$measure$PVInput1$0x586_Leistung_PV1[kW]", new DateTime(2023, 08, 01, 0, 0, 0), new DateTime(2024, 08, 01, 0, 0, 0));
            var result = ql.Aggregate("Sofar$measure$PVInput1$0x586_Leistung_PV1[kW]", new DateTime(2023, 08, 01, 0, 0, 0), new DateTime(2024, 08, 01, 0, 0, 0), new TimeSpan(24, 00, 0), "MEAN", true, 0);
            var list = result.ToList(); // Daten laden
            sw.Stop();
            var ts = sw.Elapsed;
            string elapsedTime = String.Format("{0:00}:{1:00}:{2:00}.{3:000}", ts.Hours, ts.Minutes, ts.Seconds, ts.Milliseconds);
            Console.WriteLine("Verstrichene Zeit: " + elapsedTime);
            Console.WriteLine("Treffer: " + list.Count());

            Console.WriteLine(ql.SerializeDatapoints(list));// JSON serialize
        }

        [TestMethod]
        public void Aggregate_Query_Performance()
        {
            System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();

            sw.Start();
            using var dl = new DataLayer_Readable_V1(testdata);
            var ql = new KalaQlHardcoded(dl);
            var result = ql.Query("Sofar$measure$PVInput1$0x586_Leistung_PV1[kW]", new DateTime(2023, 08, 01, 0, 0, 0), new DateTime(2024, 08, 01, 0, 0, 0));            
            var list = result.ToList(); // Daten laden
            sw.Stop();
            var ts = sw.Elapsed;
            string elapsedTime = String.Format("{0:00}:{1:00}:{2:00}.{3:000}", ts.Hours, ts.Minutes, ts.Seconds, ts.Milliseconds);
            Console.WriteLine("Verstrichene Zeit: " + elapsedTime);
            Console.WriteLine("Treffer: " + list.Count());
            //var jsonResult = ql.SerializeDatapoints(list); // JSON serialize
            //Console.WriteLine(jsonResult);
        }
    }
}