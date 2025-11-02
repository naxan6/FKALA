using Microsoft.VisualStudio.TestTools.UnitTesting;
using FluentAssertions;
using FKala.Core.KalaQl.QueryParser;
using FKala.Core.KalaQl;
using FKala.Core.KalaQl.Windowing;
using FKala.Core.Model;
using System.Collections.Generic;

namespace FKala.Unittests
{
    [TestClass]
    public class AI_LoadParser_Tests
    {
        private LoadParser _parser = null!;

        [TestInitialize]
        public void Setup()
        {
            _parser = new LoadParser();
        }

        [TestMethod]
        public void CanParse_WithLoadVerb_ShouldReturnTrue()
        {
            // Arrange
            string verb = "Load";

            // Act
            var result = _parser.CanParse(verb);

            // Assert
            result.Should().BeTrue();
        }

        [TestMethod]
        public void CanParse_WithDifferentVerb_ShouldReturnFalse()
        {
            // Arrange
            string verb = "Aggr";

            // Act
            var result = _parser.CanParse(verb);

            // Assert
            result.Should().BeFalse();
        }

        [TestMethod]
        public void Parse_WithNewestOnly_ShouldReturnOpLoadWithNewestOnly()
        {
            // Arrange
            string line = "Load NAME: measurement NewestOnly";
            var fields = new List<string> { "Load", "NAME:", "measurement", "NewestOnly" };

            // Act
            var result = _parser.Parse(line, fields);

            // Assert
            result.Should().NotBeNull();
            result.Should().BeOfType<Op_Load>();
            var opLoad = (Op_Load)result;
            opLoad.Name.Should().Be("NAME");
            opLoad.Measurement.Should().Be("measurement");
            opLoad.StartTime.Should().Be(DateTime.MinValue);
            opLoad.EndTime.Should().Be(DateTime.MaxValue);
            opLoad.CacheResolution.Resolution.Should().Be(Resolution.Full);
            opLoad.NewestOnly.Should().BeTrue();
        }

        [TestMethod]
        public void Parse_WithFullParameters_ShouldReturnOpLoad()
        {
            // Arrange
            string line = "Load NAME: measurement 2024-01-01T00:00:00Z 2024-12-31T23:59:59Z HOURLY_SUM";
            var fields = new List<string> { "Load", "NAME:", "measurement", "2024-01-01T00:00:00Z", "2024-12-31T23:59:59Z", "HOURLY_SUM" };

            // Act
            var result = _parser.Parse(line, fields);

            // Assert
            result.Should().NotBeNull();
            result.Should().BeOfType<Op_Load>();
            var opLoad = (Op_Load)result;
            opLoad.Name.Should().Be("NAME");
            opLoad.Measurement.Should().Be("measurement");
            opLoad.StartTime.Should().Be(new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc));
            opLoad.EndTime.Should().Be(new DateTime(2024, 12, 31, 23, 59, 59, DateTimeKind.Utc));
            opLoad.CacheResolution.Resolution.Should().Be(Resolution.Hourly);
            opLoad.CacheResolution.AggregateFunction.Should().Be(AggregateFunction.Sum);
            opLoad.NewestOnly.Should().BeFalse();
        }

        [TestMethod]
        public void Parse_WithInvalidParameterCount_ShouldThrowException()
        {
            // Arrange
            string line = "Load NAME: measurement 2024-01-01T00:00:00Z";
            var fields = new List<string> { "Load", "NAME:", "measurement", "2024-01-01T00:00:00Z" };

            // Act & Assert
            var action = () => _parser.Parse(line, fields);
            action.Should().Throw<Exception>().WithMessage("6 Parameters needed. Example: Load NAME: measurename 0001-01-01T00:00:00 9999-12-31T00:00:00 NoCache. But got: " + line);
        }

        [TestMethod]
        public void GenerateLine_WithNewestOnly_ShouldReturnCorrectString()
        {
            // Arrange
            var parameters = new LoadParams("NAME", "measurement", DateTime.MinValue, DateTime.MaxValue, new CacheResolution { Resolution = Resolution.Full }, true);

            // Act
            var result = LoadParser.GenerateLine(parameters);

            // Assert
            result.Should().Be("Load NAME: measurement NewestOnly");
        }

        [TestMethod]
        public void GenerateLine_WithFullParameters_ShouldReturnCorrectString()
        {
            // Arrange
            var parameters = new LoadParams("NAME", "measurement", new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(2024, 12, 31, 23, 59, 59, DateTimeKind.Utc), new CacheResolution { Resolution = Resolution.Hourly, AggregateFunction = AggregateFunction.Sum });

            // Act
            var result = LoadParser.GenerateLine(parameters);

            // Assert
            result.Should().Be("Load NAME: measurement 2024-01-01T00:00:00Z 2024-12-31T23:59:59Z HOURLY_SUM");
        }

        [TestMethod]
        public void GenerateLine_WithForceRebuild_ShouldReturnCorrectString()
        {
            // Arrange
            var parameters = new LoadParams("NAME", "measurement", new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(2024, 12, 31, 23, 59, 59, DateTimeKind.Utc), new CacheResolution { Resolution = Resolution.Hourly, AggregateFunction = AggregateFunction.Sum, ForceRebuild = true });

            // Act
            var result = LoadParser.GenerateLine(parameters);

            // Assert
            result.Should().Be("Load NAME: measurement 2024-01-01T00:00:00Z 2024-12-31T23:59:59Z HOURLY_SUM_REBUILD");
        }

        [TestMethod]
        public void GenerateLine_WithIncrementalRefresh_ShouldReturnCorrectString()
        {
            // Arrange
            var parameters = new LoadParams("NAME", "measurement", new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(2024, 12, 31, 23, 59, 59, DateTimeKind.Utc), new CacheResolution { Resolution = Resolution.Hourly, AggregateFunction = AggregateFunction.Sum, IncrementalRefresh = true });

            // Act
            var result = LoadParser.GenerateLine(parameters);

            // Assert
            result.Should().Be("Load NAME: measurement 2024-01-01T00:00:00Z 2024-12-31T23:59:59Z HOURLY_SUM_REFRESHINCREMENTAL");
        }

        [TestMethod]
        public void GenerateLine_WithBothForceRebuildAndIncrementalRefresh_ShouldReturnCorrectString()
        {
            // Arrange
            var parameters = new LoadParams("NAME", "measurement", new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc), new DateTime(2024, 12, 31, 23, 59, 59, DateTimeKind.Utc), new CacheResolution { Resolution = Resolution.Hourly, AggregateFunction = AggregateFunction.Sum, ForceRebuild = true, IncrementalRefresh = true });

            // Act
            var result = LoadParser.GenerateLine(parameters);

            // Assert
            result.Should().Be("Load NAME: measurement 2024-01-01T00:00:00Z 2024-12-31T23:59:59Z HOURLY_SUM_REBUILD_REFRESHINCREMENTAL");
        }

        [TestMethod]
        public void GetCacheResolutionString_WithFullResolution_ShouldReturnNoCache()
        {
            // Arrange
            var cacheResolution = new CacheResolution { Resolution = Resolution.Full };

            // Act
            var result = LoadParser.GetCacheResolutionString(cacheResolution);

            // Assert
            result.Should().Be("NoCache");
        }

        [TestMethod]
        public void GetCacheResolutionString_WithMinutelyResolution_ShouldReturnCorrectString()
        {
            // Arrange
            var cacheResolution = new CacheResolution { Resolution = Resolution.Minutely, AggregateFunction = AggregateFunction.Avg };

            // Act
            var result = LoadParser.GetCacheResolutionString(cacheResolution);

            // Assert
            result.Should().Be("MINUTELY_AVG");
        }

        [TestMethod]
        public void GetCacheResolutionString_WithFiveMinutelyResolution_ShouldReturnCorrectString()
        {
            // Arrange
            var cacheResolution = new CacheResolution { Resolution = Resolution.FiveMinutely, AggregateFunction = AggregateFunction.Sum };

            // Act
            var result = LoadParser.GetCacheResolutionString(cacheResolution);

            // Assert
            result.Should().Be("FIVEMINUTELY_SUM");
        }

        [TestMethod]
        public void GetCacheResolutionString_WithHourlyResolutionAndForceRebuild_ShouldReturnCorrectString()
        {
            // Arrange
            var cacheResolution = new CacheResolution { Resolution = Resolution.Hourly, AggregateFunction = AggregateFunction.Max, ForceRebuild = true };

            // Act
            var result = LoadParser.GetCacheResolutionString(cacheResolution);

            // Assert
            result.Should().Be("HOURLY_MAX_REBUILD");
        }

        [TestMethod]
        public void GetAggregateFunctionString_WithAvg_ShouldReturnAVG()
        {
            // Arrange
            var aggregateFunction = AggregateFunction.Avg;

            // Act
            var result = LoadParser.GetAggregateFunctionString(aggregateFunction);

            // Assert
            result.Should().Be("AVG");
        }

        [TestMethod]
        public void GetAggregateFunctionString_WithWAvg_ShouldReturnWAVG()
        {
            // Arrange
            var aggregateFunction = AggregateFunction.WAvg;

            // Act
            var result = LoadParser.GetAggregateFunctionString(aggregateFunction);

            // Assert
            result.Should().Be("WAVG");
        }

        [TestMethod]
        public void GetAggregateFunctionString_WithFirst_ShouldReturnFIRST()
        {
            // Arrange
            var aggregateFunction = AggregateFunction.First;

            // Act
            var result = LoadParser.GetAggregateFunctionString(aggregateFunction);

            // Assert
            result.Should().Be("FIRST");
        }

        [TestMethod]
        public void GetAggregateFunctionString_WithLast_ShouldReturnLAST()
        {
            // Arrange
            var aggregateFunction = AggregateFunction.Last;

            // Act
            var result = LoadParser.GetAggregateFunctionString(aggregateFunction);

            // Assert
            result.Should().Be("LAST");
        }

        [TestMethod]
        public void GetAggregateFunctionString_WithMin_ShouldReturnMIN()
        {
            // Arrange
            var aggregateFunction = AggregateFunction.Min;

            // Act
            var result = LoadParser.GetAggregateFunctionString(aggregateFunction);

            // Assert
            result.Should().Be("MIN");
        }

        [TestMethod]
        public void GetAggregateFunctionString_WithMax_ShouldReturnMAX()
        {
            // Arrange
            var aggregateFunction = AggregateFunction.Max;

            // Act
            var result = LoadParser.GetAggregateFunctionString(aggregateFunction);

            // Assert
            result.Should().Be("MAX");
        }

        [TestMethod]
        public void GetAggregateFunctionString_WithCount_ShouldReturnCOUNT()
        {
            // Arrange
            var aggregateFunction = AggregateFunction.Count;

            // Act
            var result = LoadParser.GetAggregateFunctionString(aggregateFunction);

            // Assert
            result.Should().Be("COUNT");
        }

        [TestMethod]
        public void GetAggregateFunctionString_WithSum_ShouldReturnSUM()
        {
            // Arrange
            var aggregateFunction = AggregateFunction.Sum;

            // Act
            var result = LoadParser.GetAggregateFunctionString(aggregateFunction);

            // Assert
            result.Should().Be("SUM");
        }

        [TestMethod]
        public void GetAggregateFunctionString_WithUnknownFunction_ShouldThrowException()
        {
            // Arrange
            var aggregateFunction = (AggregateFunction)999; // Unknown function

            // Act & Assert
            var action = () => LoadParser.GetAggregateFunctionString(aggregateFunction);
            action.Should().Throw<Exception>().WithMessage("Unbekannte AggregateFunction: 999");
        }
    }
}