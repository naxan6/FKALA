using Microsoft.VisualStudio.TestTools.UnitTesting;
using FKala.Core.KalaQl.QueryParser;
using System.Collections.Generic;
using FKala.Core.Model;
using System;
using FKala.Core.KalaQl;

namespace FKala.Unittests
{
    [TestClass]
    public class AI_QueryParser_Tests_Additional
    {
        [TestMethod]
        public void Test_JsonQueryParser()
        {
            // Test JsonQueryParser functionality
            var parser = new JsonQueryParser();

            // Test CanParse method
            Assert.IsTrue(parser.CanParse("Loaj"));
            Assert.IsFalse(parser.CanParse("Other"));

            // Test parsing a json query command
            var fields = new List<string> { "Loaj", "testName:", "testMeasure", "$.data.value", "2020-01-01T00:00:00Z", "2021-01-01T00:00:00Z", "NoCache" };
            var result = parser.Parse("Loaj testName: testMeasure $.data.value 2020-01-01T00:00:00Z 2021-01-01T00:00:00Z NoCache", fields, new List<FKala.Core.Interfaces.IKalaQlOperation>());

            Assert.IsNotNull(result);
            Assert.IsInstanceOfType(result, typeof(Op_JsonQuery));
            var jsonQueryOp = (Op_JsonQuery)result;
            Assert.AreEqual("testName", jsonQueryOp.Name);
            Assert.AreEqual("testMeasure", jsonQueryOp.Measurement);
            Assert.AreEqual("$.data.value", jsonQueryOp.FieldPath);
        }

        [TestMethod]
        public void Test_AggregateParser()
        {
            // Test AggregateParser functionality
            var parser = new AggregateParser();

            // Test CanParse method
            Assert.IsTrue(parser.CanParse("Aggr"));
            Assert.IsTrue(parser.CanParse("Aggregate"));
            Assert.IsFalse(parser.CanParse("Other"));

            // Test parsing an aggregate command
            var fields = new List<string> { "Aggr", "testName:", "testInput", "Aligned_1Hour", "AVG", "EmptyWindows" };
            var result = parser.Parse("Aggr testName: testInput Aligned_1Hour AVG EmptyWindows", fields, new List<FKala.Core.Interfaces.IKalaQlOperation>());

            Assert.IsNotNull(result);
            Assert.IsInstanceOfType(result, typeof(Op_Aggregate));
            var aggregateOp = (Op_Aggregate)result;
            Assert.AreEqual("testName", aggregateOp.Name);
            Assert.AreEqual("testInput", aggregateOp.InputDataSetName);
            Assert.IsTrue(aggregateOp.EmptyWindows);
        }

        [TestMethod]
        public void Test_InterpolateParser()
        {
            // Test InterpolateParser functionality
            var parser = new InterpolateParser();

            // Test CanParse method
            Assert.IsTrue(parser.CanParse("Inpo"));
            Assert.IsFalse(parser.CanParse("Other"));

            // Test parsing an interpolate command
            var fields = new List<string> { "Inpo", "testName:", "testInput", "FORWARDS", "10.5" };
            var result = parser.Parse("Inpo testName: testInput FORWARDS 10.5", fields, new List<FKala.Core.Interfaces.IKalaQlOperation>());

            Assert.IsNotNull(result);
            Assert.IsInstanceOfType(result, typeof(Op_Interpolate));
            var interpolateOp = (Op_Interpolate)result;
            Assert.AreEqual("testName", interpolateOp.Name);
            Assert.AreEqual("testInput", interpolateOp.InputDataSetName);
            Assert.AreEqual(InterpolationMode.forwards, interpolateOp.Mode);
            Assert.AreEqual(10.5m, interpolateOp.ConstantValue);
        }

        [TestMethod]
        public void Test_MatViewParser()
        {
            // Test MatViewParser functionality
            var parser = new MatViewParser();

            // Test CanParse method
            Assert.IsTrue(parser.CanParse("MatView"));
            Assert.IsFalse(parser.CanParse("Other"));

            // Test parsing a matview command
            var fields = new List<string> { "MatView", "testName:", "testInput", "testMeasurement" };
            var result = parser.Parse("MatView testName: testInput testMeasurement", fields, new List<FKala.Core.Interfaces.IKalaQlOperation>());

            Assert.IsNotNull(result);
            Assert.IsInstanceOfType(result, typeof(Op_MatView));
            var matViewOp = (Op_MatView)result;
            Assert.AreEqual("testName", matViewOp.Name);
            Assert.AreEqual("testInput", matViewOp.InputDataSetName);
            Assert.AreEqual("testMeasurement", matViewOp.ViewName);
        }

        [TestMethod]
        public void Test_InsertParser()
        {
            // Test InsertParser functionality
            var parser = new InsertParser();

            // Test CanParse method
            Assert.IsTrue(parser.CanParse("Insert"));
            Assert.IsFalse(parser.CanParse("Other"));

            // Test parsing an insert command
            var fields = new List<string> { "Insert", "testName:", "testValue", "testMeasurement"};
            var result = parser.Parse("Insert testName: testValue testMeasurement", fields, new List<FKala.Core.Interfaces.IKalaQlOperation>());

            Assert.IsNotNull(result);
            Assert.IsInstanceOfType(result, typeof(Op_Insert));
            var insertOp = (Op_Insert)result;
            Assert.AreEqual("testName", insertOp.Name);
            Assert.AreEqual("testMeasurement", insertOp.TargetMeasure);
            Assert.AreEqual("testValue", insertOp.InputDataSetName);
        }

        [TestMethod]
        public void Test_ExpressoParser()
        {
            // Test ExpressoParser functionality
            var parser = new ExpressoParser();

            // Test CanParse method
            Assert.IsTrue(parser.CanParse("Expr"));
            Assert.IsFalse(parser.CanParse("Other"));

            // Test parsing an expresso command
            var fields = new List<string> { "Expr", "testName:", "testExpression" };
            var result = parser.Parse("Expr testName: testExpression", fields, new List<FKala.Core.Interfaces.IKalaQlOperation>());

            Assert.IsNotNull(result);
            Assert.IsInstanceOfType(result, typeof(Op_Expresso));
            var expressoOp = (Op_Expresso)result;
            Assert.AreEqual("testName", expressoOp.Name);
            Assert.AreEqual("testExpression", expressoOp.Expresso);
        }

        [TestMethod]
        public void Test_PublishParser()
        {
            // Test PublishParser functionality
            var parser = new PublishParser();

            // Test CanParse method
            Assert.IsTrue(parser.CanParse("Publ"));
            Assert.IsTrue(parser.CanParse("Publish"));
            Assert.IsFalse(parser.CanParse("Other"));

            // Test parsing a publish command
            var fields = new List<string> { "Publish", "input1, input2", "MultipleResultsets" };
            var result = parser.Parse("Publish input1, input2 MultipleResultsets", fields, new List<FKala.Core.Interfaces.IKalaQlOperation>());

            Assert.IsNotNull(result);
            Assert.IsInstanceOfType(result, typeof(Op_Publish));
            var publishOp = (Op_Publish)result;
            Assert.AreEqual(2, publishOp.NamesToPublish.Count);
            Assert.AreEqual("input1", publishOp.NamesToPublish[0]);
            Assert.AreEqual("input2", publishOp.NamesToPublish[1]);
            Assert.AreEqual(PublishMode.MultipleResultsets, publishOp.PublishMode);
        }
    }
}