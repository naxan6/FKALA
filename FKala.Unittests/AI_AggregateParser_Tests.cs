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
        private AggregateParser _parser = null!;

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
            var result = _parser.Parse(line, fields, new List<FKala.Core.Interfaces.IKalaQlOperation>());

            // Assert
            result.Should().NotBeNull();
            result.Should().BeOfType<FKala.Core.KalaQl.Op_Aggregate>();
            var opAggregate = (FKala.Core.KalaQl.Op_Aggregate)result;
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
            var result = _parser.Parse(line, fields, new List<FKala.Core.Interfaces.IKalaQlOperation>());

            // Assert
            result.Should().NotBeNull();
            result.Should().BeOfType<FKala.Core.KalaQl.Op_Aggregate>();
            var opAggregate = (FKala.Core.KalaQl.Op_Aggregate)result;
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
            Action act = () => _parser.Parse(line, fields, new List<FKala.Core.Interfaces.IKalaQlOperation>());

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
    }
}