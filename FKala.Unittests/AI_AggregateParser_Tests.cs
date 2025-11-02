using FKala.Core.KalaQl.QueryParser;
using FKala.Core.KalaQl.Windowing;
using FluentAssertions;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;

namespace Fkala.Unittests
{
    [TestClass]
    public class AI_AggregateParser_Tests
    {
        private AggregateParser _parser;

        [TestInitialize]
        public void Setup()
        {
            _parser = new AggregateParser();
        }

        [TestMethod]
        public void CanParse_WithAggrVerb_ReturnsTrue()
        {
            // Arrange
            string verb = "Aggr";

            // Act
            bool result = _parser.CanParse(verb);

            // Assert
            result.Should().BeTrue();
        }

        [TestMethod]
        public void CanParse_WithAggregateVerb_ReturnsTrue()
        {
            // Arrange
            string verb = "Aggregate";

            // Act
            bool result = _parser.CanParse(verb);

            // Assert
            result.Should().BeTrue();
        }

        [TestMethod]
        public void CanParse_WithDifferentVerb_ReturnsFalse()
        {
            // Arrange
            string verb = "Select";

            // Act
            bool result = _parser.CanParse(verb);

            // Assert
            result.Should().BeFalse();
        }

        [TestMethod]
        public void Parse_WithValidAggrLine_ReturnsOpAggregate()
        {
            // Arrange
            string line = "Aggr:TestAggr:InputData:Aligned_1Hour:Sum:false";
            var fields = new List<string> { "Aggr", "TestAggr", "InputData", "Aligned_1Hour", "Sum", "false" };

            // Act
            var result = _parser.Parse(line, fields);

            // Assert
            result.Should().NotBeNull();
            result.Should().BeOfType<FKala.Core.KalaQl.Op_Aggregate>();
            var opAggregate = result as FKala.Core.KalaQl.Op_Aggregate;
            opAggregate.Name.Should().Be("TestAggr");
            opAggregate.InputDataSetName.Should().Be("InputData");
            opAggregate.AggregateFunc.Should().Be(AggregateFunction.Sum);
            opAggregate.EmptyWindows.Should().BeFalse();
        }

        [TestMethod]
        public void Parse_WithValidAggregateLine_ReturnsOpAggregate()
        {
            // Arrange
            string line = "Aggregate:TestAggregate:InputData:Aligned_1Hour:Avg:EmptyWindows";
            var fields = new List<string> { "Aggregate", "TestAggregate", "InputData", "Aligned_1Hour", "Avg", "EmptyWindows" };

            // Act
            var result = _parser.Parse(line, fields);

            // Assert
            result.Should().NotBeNull();
            result.Should().BeOfType<FKala.Core.KalaQl.Op_Aggregate>();
            var opAggregate = result as FKala.Core.KalaQl.Op_Aggregate;
            opAggregate.Name.Should().Be("TestAggregate");
            opAggregate.InputDataSetName.Should().Be("InputData");
            opAggregate.AggregateFunc.Should().Be(AggregateFunction.Avg);
            opAggregate.EmptyWindows.Should().BeTrue(); // ParseEmptyWindows erkennt "EmptyWindows" im String
        }

        [TestMethod]
        public void Parse_WithEmptyFields_ThrowsException()
        {
            // Arrange
            string line = "Aggr:::";
            var fields = new List<string> { "Aggr", "", "", "" };

            // Act
            Action act = () => _parser.Parse(line, fields);

            // Assert
            act.Should().Throw<FormatException>();
        }

        [TestMethod]
        public void GenerateLine_WithValidParams_ReturnsCorrectLine()
        {
            // Arrange
            var window = Window.Aligned_1Hour;
            var aggregateFunction = AggregateFunction.Sum;
            bool emptyWindows = false;
            var parameters = new AggregateParams("TestAggr", "InputData", window, aggregateFunction, emptyWindows);

            // Act
            string result = AggregateParser.GenerateLine(parameters);

            // Assert
            result.Should().Be("Aggregate TestAggr: InputData Infinite SUM");
        }

        [TestMethod]
        public void GenerateLine_WithEmptyWindows_ReturnsCorrectLine()
        {
            // Arrange
            var window = Window.Aligned_1Hour;
            var aggregateFunction = AggregateFunction.Avg;
            bool emptyWindows = true;
            var parameters = new AggregateParams("TestAvg", "InputData", window, aggregateFunction, emptyWindows);

            // Act
            string result = AggregateParser.GenerateLine(parameters);

            // Assert
            result.Should().Be("Aggregate TestAvg: InputData Infinite AVG EmptyWindows");
        }

        [TestMethod]
        public void GetAggregateFunctionString_WithSum_ReturnsCorrectString()
        {
            // Arrange
            var aggregateFunction = AggregateFunction.Sum;

            // Act
            string result = AggregateParser.GetAggregateFunctionString(aggregateFunction);

            // Assert
            result.Should().Be("SUM");
        }

        [TestMethod]
        public void GetAggregateFunctionString_WithAvg_ReturnsCorrectString()
        {
            // Arrange
            var aggregateFunction = AggregateFunction.Avg;

            // Act
            string result = AggregateParser.GetAggregateFunctionString(aggregateFunction);

            // Assert
            result.Should().Be("AVG");
        }

        [TestMethod]
        public void GetAggregateFunctionString_WithCount_ReturnsCorrectString()
        {
            // Arrange
            var aggregateFunction = AggregateFunction.Count;

            // Act
            string result = AggregateParser.GetAggregateFunctionString(aggregateFunction);

            // Assert
            result.Should().Be("COUNT");
        }

        [TestMethod]
        public void GetAggregateFunctionString_WithMin_ReturnsCorrectString()
        {
            // Arrange
            var aggregateFunction = AggregateFunction.Min;

            // Act
            string result = AggregateParser.GetAggregateFunctionString(aggregateFunction);

            // Assert
            result.Should().Be("MIN");
        }

        [TestMethod]
        public void GetAggregateFunctionString_WithMax_ReturnsCorrectString()
        {
            // Arrange
            var aggregateFunction = AggregateFunction.Max;

            // Act
            string result = AggregateParser.GetAggregateFunctionString(aggregateFunction);

            // Assert
            result.Should().Be("MAX");
        }

        [TestMethod]
        public void GetAggregateFunctionString_WithFirst_ReturnsCorrectString()
        {
            // Arrange
            var aggregateFunction = AggregateFunction.First;

            // Act
            string result = AggregateParser.GetAggregateFunctionString(aggregateFunction);

            // Assert
            result.Should().Be("FIRST");
        }

        [TestMethod]
        public void GetAggregateFunctionString_WithLast_ReturnsCorrectString()
        {
            // Arrange
            var aggregateFunction = AggregateFunction.Last;

            // Act
            string result = AggregateParser.GetAggregateFunctionString(aggregateFunction);

            // Assert
            result.Should().Be("LAST");
        }

        [TestMethod]
        public void GetAggregateFunctionString_WithWAvg_ReturnsCorrectString()
        {
            // Arrange
            var aggregateFunction = AggregateFunction.WAvg;

            // Act
            string result = AggregateParser.GetAggregateFunctionString(aggregateFunction);

            // Assert
            result.Should().Be("WAVG");
        }

        [TestMethod]
        public void GetAggregateFunctionString_WithNone_ThrowsException()
        {
            // Arrange
            var aggregateFunction = AggregateFunction.None;

            // Act
            Action act = () => AggregateParser.GetAggregateFunctionString(aggregateFunction);

            // Assert
            act.Should().Throw<Exception>().WithMessage("Unbekannte AggregateFunction: None");
        }
    }
}