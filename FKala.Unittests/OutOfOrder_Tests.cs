using FKala.Core.KalaQl;
using FKala.Core.Logic;
using FluentAssertions;
using FluentAssertions.Execution;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FKala.Unittests
{
    [TestClass]
    public class OutOfOrder_Tests
    {
        [TestMethod]
        public void KalaQuery_OutOfOrder_LateInsert_CacheInvalide()
        {
            using var DataFaker = new DataFaker();
            DataFaker.FakeMeasure_OutOfOrder("m1", new DateTime(2024, 01, 01), new DateTime(2024, 3, 15), new TimeSpan(0, 0, 5), new TimeSpan(0, 0, 15));
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

            DataFaker.FakeMeasure_OutOfOrder("m1", new DateTime(2024, 03, 14, 23, 0, 0));
           
            {
                var kq = KalaQuery.Start().FromQuery(@"
                Load m1: m1 2024-03-01T00:00:00Z 2024-03-15T00:00:00Z Hourly_First
                Publ ""m1"" Table
            ");
                // Act
                var result = kq.Execute(DataFaker.TestDataLayer);

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
                    ((decimal)lastEntry["m1"]!).Should().Be(0.747528851846014M);
                }
            }
        }
    }
}
