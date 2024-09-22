using FluentAssertions;
using FKala.Core;
using FKala.Core.KalaQl;
using FKala.Core.KalaQl.Windowing;
using FKala.Core.Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FKala.Core.Logic;
using FluentAssertions.Execution;

namespace FKala.Unittests
{
    [TestClass]
    public class TzTests
    {

        //2024-03-29: 29.05     22:59:59.5055518
        //2024-03-30: 16.44     22:59:59.0673059
        //2024-03-31: 15.84     21:59:56.2395570

        [TestMethod]
        public void Test_CESTCET_Pre()
        {
            DataLayer_Readable_Caching_V1 dl = new DataLayer_Readable_Caching_V1(@".\Testdata\UTC_CET_CEST\");

            var query = KalaQuery.Start()
                .Add(new Op_BaseQuery(null, "overDst", "UTC_CET_CEST_Load",
                        new DateTime(2024, 03, 30, 0, 0, 0, DateTimeKind.Utc),
                        new DateTime(2024, 03, 31, 23, 0, 0, DateTimeKind.Utc),
                    new CacheResolution()
                    {
                        AggregateFunction = AggregateFunction.Last,
                        Resolution = Resolution.Hourly,
                        ForceRebuild = true
                    }
                ))
                .Add(new Op_Publish(null, new List<string>() { "overDst" }, PublishMode.CombinedResultset));


            var testResult = query.Execute(dl);
            testResult.Errors.ForEach(er => Console.WriteLine(er));
            testResult.Errors.Should().BeEmpty();
            var firstEntry = ((dynamic?)testResult.ResultTable);
            Console.WriteLine(KalaJson.Serialize(firstEntry));

        }

        [TestMethod]
        public void Test_OnlyCET()
        {
            DataLayer_Readable_Caching_V1 dl = new DataLayer_Readable_Caching_V1(@".\Testdata\UTC_CET_CEST\");

            var query = KalaQuery.Start()
                .Add(new Op_BaseQuery(null, "overDst", "UTC_CET_CEST_Load",
                        new DateTime(2024, 03, 28, 23, 0, 0, DateTimeKind.Utc),
                        new DateTime(2024, 03, 30, 23, 0, 0, DateTimeKind.Utc),
                    new CacheResolution()
                    {
                        AggregateFunction = AggregateFunction.Last,
                        Resolution = Resolution.Hourly,
                        ForceRebuild = true
                    }
                ))
                .Add(new Op_AlignTimezone(null, "Europe/Berlin"))
                .Add(new Op_Aggregate(null, "EndOfLocalDaily", "overDst", Window.Aligned_1Day, AggregateFunction.Last, true))
                //.Add(new Op_Aggregate(null, "SumUp", "EndOfLocalDaily", Window.Infinite, AggregateFunction.Sum, true))
                .Add(new Op_Publish(null, new List<string>() { "EndOfLocalDaily" }, PublishMode.CombinedResultset));


            var testResult = query.Execute(dl);
            testResult.Errors.ForEach(er => Console.WriteLine(er));
            testResult.Errors.Should().BeEmpty();
            var en = testResult.ResultTable!.GetEnumerator();
            en.MoveNext();
            var _1stEntry = en.Current;
            en.MoveNext();
            var _2ndEntry = en.Current;
            Console.WriteLine(KalaJson.Serialize(testResult.ResultTable));
            using (new AssertionScope())
            {
                ((DateTime)_1stEntry["time"]!).Should().Be(new DateTime(2024, 03, 28, 23, 0, 0));
                ((decimal?)_1stEntry["EndOfLocalDaily"]).Should().Be(29.05M);
                ((DateTime)_2ndEntry["time"]!).Should().Be(new DateTime(2024, 03, 29, 23, 0, 0));
                ((decimal?)_2ndEntry["EndOfLocalDaily"]).Should().Be(16.44M);


            }
        }

        [TestMethod]
        public void Test_CET2CEST()
        {
            DataLayer_Readable_Caching_V1 dl = new DataLayer_Readable_Caching_V1(@".\Testdata\UTC_CET_CEST\");

            var query = KalaQuery.Start()
                .Add(new Op_BaseQuery(null, "overDst", "UTC_CET_CEST_Load",
                        new DateTime(2024, 03, 29, 0, 0, 0, DateTimeKind.Utc),
                        new DateTime(2024, 03, 31, 22, 00, 00, DateTimeKind.Utc),
                    new CacheResolution()
                    {
                        AggregateFunction = AggregateFunction.Last,
                        Resolution = Resolution.Hourly,
                        ForceRebuild = true
                    }
                ))
                .Add(new Op_AlignTimezone(null, "Europe/Berlin"))
                .Add(new Op_Aggregate(null, "EndOfLocalDaily", "overDst", Window.Aligned_1Day, AggregateFunction.Last, true))
                .Add(new Op_Aggregate(null, "SumUp", "EndOfLocalDaily", Window.Infinite, AggregateFunction.Sum, true))
                .Add(new Op_Publish(null, new List<string>() { "EndOfLocalDaily" }, PublishMode.CombinedResultset));


            var testResult = query.Execute(dl);
            testResult.Errors.ForEach(er => Console.WriteLine(er));
            testResult.Errors.Should().BeEmpty();
            var en = testResult.ResultTable!.GetEnumerator();
            en.MoveNext();
            var _1stEntry = en.Current;
            en.MoveNext();
            var _2ndEntry = en.Current;
            en.MoveNext();
            var _3rdEntry = en.Current;
            Console.WriteLine(KalaJson.Serialize(testResult.ResultTable));
            using (new AssertionScope())
            {
                ((DateTime)_1stEntry["time"]!).Should().Be(new DateTime(2024, 03, 28, 23, 0, 0));
                ((decimal?)_1stEntry["EndOfLocalDaily"]!).Should().Be(29.05M);
                ((DateTime)_2ndEntry["time"]!).Should().Be(new DateTime(2024, 03, 29, 23, 0, 0));
                ((decimal?)_2ndEntry["EndOfLocalDaily"]!).Should().Be(16.44M);
                ((DateTime)_3rdEntry["time"]!).Should().Be(new DateTime(2024, 03, 30, 23, 0, 0));
                ((decimal?)_3rdEntry["EndOfLocalDaily"]!).Should().Be(15.84M);
                ((DateTime)_3rdEntry["time"]!).Should().Be(new DateTime(2024, 03, 30, 23, 0, 0));
                ((decimal?)_3rdEntry["EndOfLocalDaily"]!).Should().Be(15.84M);
            }
        }
    }
}
