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
            var resultset = DataFaker.TestDataLayer.LoadData("m1", new DateTime(2024, 03, 01), new DateTime(2024, 03, 15), CacheResolutionPredefined.NoCache, false, new KalaQlContext(DataFaker.TestDataLayer), false);

            resultset = resultset.ToList(); // persist result

            // Assert 1
            Console.WriteLine(KalaJson.Serialize(resultset));

            resultset.First().StartTime.Should().Be(new DateTime(2024, 03, 01, 0, 0, 2).AddTicks(5180212));

            Assert.AreEqual(0.564860636165766m, resultset.First().Value);
            resultset.Last().StartTime.Should().Be(new DateTime(2024, 03, 14, 23, 59, 58).AddTicks(2488239));

            Assert.AreEqual(0.457086396616458m, resultset.Last().Value);

            // Assert 2
            var resultsetAll = DataFaker.TestDataLayer.LoadData("m1", new DateTime(0001, 01, 01), new DateTime(9999, 12, 31), CacheResolutionPredefined.NoCache, false, new KalaQlContext(DataFaker.TestDataLayer), false);
            resultsetAll = resultsetAll.ToList();
            resultsetAll.First().StartTime.Should().Be(new DateTime(2024, 01, 01, 0, 0, 13).AddTicks(5443658));
            Assert.AreEqual(0.248668584157093m, resultsetAll.First().Value);
            resultsetAll.Last().StartTime.Should().Be(new DateTime(2024, 04, 30, 23, 59, 59).AddTicks(3647264));
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
            
            var firstEntry = resultset!.First();
            firstEntry["time"].Should().Be(new DateTime(2024, 04, 30, 23, 59, 59).AddTicks(3647264));
            Assert.AreEqual(0.646428865681602m, firstEntry["m1"]);
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
                        
            var firstEntry = resultset!.First();
            Assert.AreEqual(new DateTime(2024, 03, 01, 00, 00, 00), firstEntry["time"]);
            Assert.AreEqual(0.564860636165766m, firstEntry["m1"]);
            var lastEntry = resultset!.Last();
            Assert.AreEqual(new DateTime(2024, 03, 14, 23, 00, 00), lastEntry["time"]);
            Assert.AreEqual(0.875275374797767m, lastEntry["m1"]);
        }

        [TestMethod]
        public void KalaQuery_LAP_1()
        {
            // Prepare
            var kq = KalaQuery.Start().FromQuery(@"
                Load m1: m1 2024-03-01T00:00:00Z 2024-03-15T00:00:00Z Hourly_First
                Aggr a1: m1 08:00:00 Avg EmptyWindows
                Publ ""a1"" Table
            ");
            // Act
            var result = kq.Execute(DataFaker.TestDataLayer);


            // Assert
            Console.WriteLine(KalaJson.Serialize(result));
            result.Errors.Should().BeEmpty();

            var resultset = result.ResultTable;
            resultset.Should().NotBeNull();
            resultset!.Count().Should().Be(42);
        }

        [TestMethod]
        public void KalaQuery_LAAP_1()
        {
            // Prepare
            var kq = KalaQuery.Start().FromQuery(@"
                Load m1: m1 2024-03-01T00:00:00Z 2024-03-15T00:00:00Z Hourly_First
                Aggr a1: m1 08:00:00 Avg EmptyWindows                       
                Aggr a2: a1 Aligned_1Day Avg EmptyWindows
                Publ ""a2"" Table
            ");
            // Act
            var result = kq.Execute(DataFaker.TestDataLayer);


            // Assert
            Console.WriteLine(KalaJson.Serialize(result));
            result.Errors.Should().BeEmpty();

            var resultset = result.ResultTable;
            resultset.Should().NotBeNull();
            resultset!.Count().Should().Be(14);
        }

        [TestMethod]
        public void KalaQuery_LAAP_CutJoin()
        {
            // Prepare
            var kq = KalaQuery.Start().FromQuery(@"
                Load bm1: m1 2024-03-01T00:00:00Z 2024-03-15T00:00:00Z Hourly_First
                Aggr a1: bm1 08:00:00 Avg EmptyWindows
                Aggr a2: bm1 03:00:00 Avg EmptyWindows                
                Publ ""a1,a2"" Table
            ");
            // Act
            var result = kq.Execute(DataFaker.TestDataLayer);


            // Assert
            Console.WriteLine(KalaJson.Serialize(result));
            result.Errors.Should().BeEmpty();

            var resultset = result.ResultTable;
            resultset.Should().NotBeNull();
            resultset!.Count().Should().Be(140);
        }
    }
}
