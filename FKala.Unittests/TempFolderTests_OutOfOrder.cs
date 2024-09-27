using FKala.Core.KalaQl;
using FKala.Core.Logic;
using FKala.Core.Model;
using FluentAssertions;
using FluentAssertions.Execution;

namespace FKala.Unittests
{
    [TestClass]
    public class TempFolderTests_OutOfOrder
    {
#pragma warning disable CS8618 // Ein Non-Nullable-Feld muss beim Beenden des Konstruktors einen Wert ungleich NULL enthalten. Fügen Sie ggf. den „erforderlichen“ Modifizierer hinzu, oder deklarieren Sie den Modifizierer als NULL-Werte zulassend.
        static DataFaker DataFaker;
#pragma warning restore CS8618 // Ein Non-Nullable-Feld muss beim Beenden des Konstruktors einen Wert ungleich NULL enthalten. Fügen Sie ggf. den „erforderlichen“ Modifizierer hinzu, oder deklarieren Sie den Modifizierer als NULL-Werte zulassend.

        [ClassInitialize]
        public static void Initialize(TestContext context)
        { 

            DataFaker = new DataFaker();
            DataFaker.FakeMeasure_OutOfOrder("m1", new DateTime(2024, 01, 01), new DateTime(2024, 05, 01), new TimeSpan(0, 0, 5), new TimeSpan(0, 0, 15));

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
            var resultset = DataFaker.TestDataLayer.LoadData("m1", new DateTime(2024, 03, 01), new DateTime(2024, 03, 15), CacheResolutionPredefined.NoCache, false, false, new KalaQlContext(DataFaker.TestDataLayer));



            // Assert 1
            Console.WriteLine(KalaJson.Serialize(resultset));
            using (new AssertionScope())
            {
                resultset.First().Time.Should().Be(new DateTime(2024, 03, 01, 0, 0, 03).AddTicks(2370446));
                resultset.First().Value.Should().Be(0.429102592370055M);
                resultset.Last().Time.Should().Be(new DateTime(2024, 03, 14, 23, 59, 58).AddTicks(4310596));
                resultset.Last().Value.Should().Be(0.136571015760568M);
            }

            // Assert 2
            var resultsetAll = DataFaker.TestDataLayer.LoadData("m1", new DateTime(0001, 01, 01), new DateTime(9999, 12, 31), CacheResolutionPredefined.NoCache, false, false, new KalaQlContext(DataFaker.TestDataLayer));
            using (new AssertionScope())
            {
                resultsetAll.First().Time.Should().Be(new DateTime(2024, 01, 01, 0, 0, 10).AddTicks(0024500));
                resultsetAll.First().Value.Should().Be(0.248668584157093M);
                resultsetAll.Last().Time.Should().Be(new DateTime(2024, 04, 30, 23, 59, 58).AddTicks(2580834));
                resultsetAll.Last().Value.Should().Be(0.669364353022242M);
            }            
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
            result?.ResultTable?.Should().NotBeNull();
            var resultsetEnum = result!.ResultTable;
            var resultSet = resultsetEnum!.ToList();
            resultSet!.Count().Should().Be(1);
            
            var firstEntry = resultSet.First();
            using (new AssertionScope())
            {
                ((DateTime)firstEntry["time"]!).Should().Be(new DateTime(2024, 04, 30, 23, 59, 58).AddTicks(2580834));
                ((decimal)firstEntry["m1"]!).Should().Be(0.669364353022242M);
            }
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
                        
            

            using (new AssertionScope())
            {
                result.Errors.Should().BeEmpty();                
                result?.ResultTable?.Should().NotBeNull();
                var resultset = result!.ResultTable!;
                resultset!.Count().Should().Be(336);
                var firstEntry = resultset!.First();
                ((DateTime)firstEntry["time"]!).Should().Be(new DateTime(2024, 03, 01, 00, 00, 00));
                ((decimal)firstEntry["m1"]!).Should().Be(0.429102592370055M);
                var lastEntry = resultset!.Last();
                ((DateTime)lastEntry["time"]!).Should().Be(new DateTime(2024, 03, 14, 23, 00, 00));
                ((decimal)lastEntry["m1"]!).Should().Be(0.486422908718895M);
            }
        }
    }
}
