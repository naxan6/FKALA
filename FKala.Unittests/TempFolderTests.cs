using FKala.Core.KalaQl;
using FKala.Core.Logic;
using FKala.Core.Model;
using FluentAssertions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FKala.Unittests
{
    [TestClass]
    public class TempFolderTests
    {
#pragma warning disable CS8618 // Ein Non-Nullable-Feld muss beim Beenden des Konstruktors einen Wert ungleich NULL enthalten. Fügen Sie ggf. den „erforderlichen“ Modifizierer hinzu, oder deklarieren Sie den Modifizierer als NULL-Werte zulassend.
        static DataFaker DataFaker;
#pragma warning restore CS8618 // Ein Non-Nullable-Feld muss beim Beenden des Konstruktors einen Wert ungleich NULL enthalten. Fügen Sie ggf. den „erforderlichen“ Modifizierer hinzu, oder deklarieren Sie den Modifizierer als NULL-Werte zulassend.

        [ClassInitialize]
        public static void Initialize(TestContext context)
        {
            DataFaker = new DataFaker();
            DataFaker.FakeMeasure("m1", new DateTime(2024, 01, 01), new DateTime(2024, 05, 01), new TimeSpan(0, 0, 1), new TimeSpan(0, 0, 15));
        }
        [ClassCleanup]
        public static void Cleanup()
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
            Assert.AreEqual(new DateTime(2024, 03, 01, 0, 0, 2), resultset.First().Time);
            Assert.AreEqual(0.564860636165766m, resultset.First().Value);
            Assert.AreEqual(new DateTime(2024, 03, 14, 23, 59, 58), resultset.Last().Time);
            Assert.AreEqual(0.457086396616458m, resultset.Last().Value);

            // Assert 2
            var resultsetAll = DataFaker.TestDataLayer.LoadData("m1", new DateTime(0001, 01, 01), new DateTime(9999, 12, 31), CacheResolutionPredefined.NoCache, false);
            Assert.AreEqual(new DateTime(2024, 01, 01, 0, 0, 13), resultsetAll.First().Time);
            Assert.AreEqual(0.248668584157093m, resultsetAll.First().Value);
            Assert.AreEqual(new DateTime(2024, 04, 30, 23, 59, 59), resultsetAll.Last().Time);
            Assert.AreEqual(0.646428865681602m, resultsetAll.Last().Value);
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
            result.Errors.Should().BeEmpty();
            
            var resultset = result.ResultTable;
            resultset.Should().NotBeNull();
            resultset!.Count().Should().Be(1);
            
            var firstEntry = ((dynamic)resultset!.First());
            Assert.AreEqual(new DateTime(2024, 04, 30, 23, 59, 59), firstEntry.time);
            Assert.AreEqual(0.646428865681602m, firstEntry.m1);
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
            result.Errors.Should().BeEmpty();

            var resultset = result.ResultTable;
            resultset.Should().NotBeNull();
            resultset!.Count().Should().Be(336);
                        
            var firstEntry = ((dynamic)resultset!.First());
            Assert.AreEqual(new DateTime(2024, 03, 01, 00, 00, 00), firstEntry.time);
            Assert.AreEqual(0.564860636165766m, firstEntry.m1);
            var lastEntry = ((dynamic)resultset!.Last());
            Assert.AreEqual(new DateTime(2024, 03, 14, 23, 00, 00), lastEntry.time);
            Assert.AreEqual(0.875275374797767m, lastEntry.m1);
        }
    }
}
