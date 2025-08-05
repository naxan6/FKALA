using Microsoft.VisualStudio.TestTools.UnitTesting;
using FluentAssertions;
using FKala.Core.KalaQl.QueryParser;
using FKala.Core.KalaQl.Windowing;
using System;

namespace FKala.Unittests
{
    [TestClass]
    public class AI_QueryParser_AggregateParams_Tests
    {
        [TestMethod]
        public void Constructor_ShouldInitializeAllPropertiesCorrectly()
        {
            // Arrange
            string name = "testAggregate";
            string input = "testInput";
            var window = Window.Aligned_1Hour;
            var aggregateFunction = AggregateFunction.Avg;
            bool emptyWindows = true;

            // Act
            var result = new AggregateParams(name, input, window, aggregateFunction, emptyWindows);

            // Assert
            result.Should().NotBeNull();
            result.Name.Should().Be(name);
            result.Input.Should().Be(input);
            result.Window.Should().Be(window);
            result.AggregateFunction.Should().Be(aggregateFunction);
            result.EmptyWindows.Should().Be(emptyWindows);
        }

        [TestMethod]
        public void Constructor_WithDefaultEmptyWindows_ShouldSetEmptyWindowsToFalse()
        {
            // Arrange
            string name = "testAggregate";
            string input = "testInput";
            var window = Window.Aligned_1Day;
            var aggregateFunction = AggregateFunction.Sum;

            // Act
            var result = new AggregateParams(name, input, window, aggregateFunction);

            // Assert
            result.Should().NotBeNull();
            result.EmptyWindows.Should().Be(false);
        }

        [TestMethod]
        public void Constructor_ShouldAcceptDifferentAggregateFunctions()
        {
            // Arrange
            var testCases = new[]
            {
                (AggregateFunction.Avg, "Average"),
                (AggregateFunction.Sum, "Sum"),
                (AggregateFunction.Min, "Minimum"),
                (AggregateFunction.Max, "Maximum"),
                (AggregateFunction.Count, "Count"),
                (AggregateFunction.First, "First"),
                (AggregateFunction.Last, "Last"),
                (AggregateFunction.WAvg, "WeightedAverage")
            };

            // Act & Assert
            foreach (var (function, description) in testCases)
            {
                var result = new AggregateParams("test", "input", Window.Aligned_1Hour, function);
                
                result.Should().NotBeNull($"{description} should create valid instance");
                result.AggregateFunction.Should().Be(function, $"{description} should set correct function");
            }
        }

        [TestMethod]
        public void Constructor_ShouldAcceptDifferentWindowTypes()
        {
            // Arrange
            var windows = new[]
            {
                Window.Aligned_1Minute,
                Window.Aligned_5Minutes,
                Window.Aligned_15Minutes,
                Window.Aligned_1Hour,
                Window.Aligned_1Day,
                Window.Aligned_1Week,
                Window.Aligned_1Month,
                Window.Infinite
            };

            // Act & Assert
            foreach (var window in windows)
            {
                var result = new AggregateParams("test", "input", window, AggregateFunction.Avg);
                
                result.Should().NotBeNull();
                result.Window.Should().Be(window);
            }
        }

        [TestMethod]
        public void Constructor_ShouldHandleEmptyStrings()
        {
            // Arrange
            string name = "";
            string input = "";

            // Act
            var result = new AggregateParams(name, input, Window.Aligned_1Hour, AggregateFunction.Count);

            // Assert
            result.Should().NotBeNull();
            result.Name.Should().Be("");
            result.Input.Should().Be("");
        }

        [TestMethod]
        public void Constructor_ShouldAcceptNullWindow()
        {
            // Arrange
            string name = "test";
            string input = "input";

            // Act
            var result = new AggregateParams(name, input, null!, AggregateFunction.Avg);

            // Assert
            result.Should().NotBeNull();
            result.Window.Should().BeNull();
        }
    }
}