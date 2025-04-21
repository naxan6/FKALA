using FKala.Core.KalaQl;
using FKala.Core.KalaQl.Windowing;
using FKala.Core.Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FKala.Unittests.KalaQl
{
    [TestClass]
    public class ParserTests
    {
        [TestMethod]
        public void TestLoadOperationParsing()
        {
            // Arrange
            var queryText = "Load test: testMeasurement 2023-01-01T00:00:00Z 2023-12-31T23:59:59Z NoCache";

            // Act
            var query = KalaQuery.Start().FromQuery(queryText);
            var parsedQueryText = query.ToQueryString();

            // Assert
            Assert.AreEqual(queryText, parsedQueryText);
        }

        [TestMethod]
        public void TestMgmtOperationParsing()
        {
            // Arrange
            var queryText = "Mgmt LoadMeasures";

            // Act
            var query = KalaQuery.Start().FromQuery(queryText);
            var parsedQueryText = query.ToQueryString();

            // Assert
            Assert.AreEqual(queryText, parsedQueryText);
        }

        [TestMethod]
        public void TestAggregateOperationParsing()
        {
            // Arrange
            var queryText = "Aggregate test: input Aligned_1Hour Avg";

            // Act
            var query = KalaQuery.Start().FromQuery(queryText);
            var parsedQueryText = query.ToQueryString();

            // Assert
            Assert.AreEqual(queryText, parsedQueryText);
        }

        [TestMethod]
        public void TestVarOperationParsing()
        {
            // Arrange
            var queryText = "Var test: value";

            // Act
            var query = KalaQuery.Start().FromQuery(queryText);
            var parsedQueryText = query.ToQueryString();

            // Assert
            Assert.AreEqual(queryText, parsedQueryText);
        }

        [TestMethod]
        public void TestPublishOperationParsing()
        {
            // Arrange
            var queryText = "Publish input1, input2 CombinedResultset";

            // Act
            var query = KalaQuery.Start().FromQuery(queryText);
            var parsedQueryText = query.ToQueryString();

            // Assert
            Assert.AreEqual(queryText, parsedQueryText);
        }

        [TestMethod]
        public void TestMultipleOperationsParsing()
        {
            // Arrange
            var queryText = "Load load: testMeasurement 2023-01-01T00:00:00Z 2023-12-31T23:59:59Z AUTO(20000) | Aggregate aggr: load 02:00:05 Avg | Publish aggr CombinedResultset";

            // Act
            var query = KalaQuery.Start().FromQuery(queryText);
            var parsedQueryText = query.ToQueryString();

            // Assert
            Assert.AreEqual(queryText, parsedQueryText);
        }
    }
}
