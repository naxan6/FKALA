using FKala.TestConsole.KalaQl;
using FKala.TestConsole.Logic;
using FKala.TestConsole.Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FKala.Unittests
{
    [TestClass]
    public class TempFolderTests_OutOfOrder
    {
        DataFaker DataFaker;

        [TestInitialize]
        public void Initialize()
        {
            DataFaker = new DataFaker();
            DataFaker.FakeMeasure_OutOfOrder("m1", new DateTime(2024, 01, 01), new DateTime(2024, 05, 01), new TimeSpan(0, 0, 1), new TimeSpan(0, 0, 15));
        }
        [TestCleanup]
        public void Cleanup()
        {
            DataFaker.Dispose();
        }

        [TestMethod]
        public void DataLayer_LoadData_CheckBorders()
        {
            // Act
            var resultset = DataFaker.TestDataLayer.LoadData("m1", new DateTime(2024, 03, 01), new DateTime(2024, 03, 15), CacheResolutionPredefined.NoCache, false);
            
            // Assert 1
            Console.WriteLine(KalaJson.Serialize(resultset));            
            Assert.AreEqual(new DateTime(2024, 03, 01, 0, 0, 10), resultset.First().Time);
            Assert.AreEqual(0.564860636165766m, resultset.First().Value);
            Assert.AreEqual(new DateTime(2024, 03, 14, 23, 59, 54), resultset.Last().Time);
            Assert.AreEqual(0.228472747946378m, resultset.Last().Value);

            // Assert 2
            var resultsetAll = DataFaker.TestDataLayer.LoadData("m1", new DateTime(0001, 01, 01), new DateTime(9999, 12, 31), CacheResolutionPredefined.NoCache, false);
            Assert.AreEqual(new DateTime(2024, 01, 01, 0, 0, 21), resultsetAll.First().Time);
            Assert.AreEqual(0.248668584157093m, resultsetAll.First().Value);
            Assert.AreEqual(new DateTime(2024, 04, 30, 23, 59, 58), resultsetAll.Last().Time);
            Assert.AreEqual(0.562627387960734m, resultsetAll.Last().Value);
        }

        [TestMethod]
        public void KalaQuery_NewestOnly()
        {
            // Prepare
            var kq = KalaQuery.Start().FromQuery(@"
                Load m1: m1 NewestOnly
                Publ ""m1"" Table
            ");
            // Act
            var result = kq.Execute(DataFaker.TestDataLayer);
            

            // Assert
            Console.WriteLine(KalaJson.Serialize(result));
            Assert.AreEqual(0, result.Errors.Count());            
            var resultset = result.ResultTable;
            Assert.AreEqual(1, resultset.Count());
            var firstEntry = ((dynamic)resultset.First());
            Assert.AreEqual(new DateTime(2024, 04, 30, 23, 59, 58), firstEntry.time);
            Assert.AreEqual(0.562627387960734m, firstEntry.m1);
        }

        [TestMethod]
        public void KalaQuery_CacheHourly_CheckBorders_First()
        {
            // Prepare
            var kq = KalaQuery.Start().FromQuery(@"
                Load m1: m1 2024-03-01T00:00:00Z 2024-03-15T00:00:00Z Hourly_First
                Publ ""m1"" Table
            ");
            // Act
            var result = kq.Execute(DataFaker.TestDataLayer);


            // Assert
            Console.WriteLine(KalaJson.Serialize(result));
            Assert.AreEqual(0, result.Errors.Count());
            var resultset = result.ResultTable;
            Assert.AreEqual(336, resultset.Count());
            var firstEntry = ((dynamic)resultset.First());
            Assert.AreEqual(new DateTime(2024, 03, 01, 00, 00, 00), firstEntry.time);
            Assert.AreEqual(0.564860636165766m, firstEntry.m1);
            var lastEntry = ((dynamic)resultset.Last());
            Assert.AreEqual(new DateTime(2024, 03, 14, 23, 00, 00), lastEntry.time);
            Assert.AreEqual(0.875275374797767m, lastEntry.m1);
        }
    }
}
