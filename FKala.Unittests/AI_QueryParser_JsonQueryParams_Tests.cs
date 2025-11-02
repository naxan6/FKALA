using Microsoft.VisualStudio.TestTools.UnitTesting;
using FluentAssertions;
using FKala.Core.KalaQl.QueryParser;
using FKala.Core.Model;
using FKala.Core.DataLayers;
using FKala.Core.KalaQl.Windowing;
using System;

namespace FKala.Unittests
{
    [TestClass]
    public class AI_QueryParser_JsonQueryParams_Tests
    {
        [TestMethod]
        public void Constructor_ShouldInitializeAllPropertiesCorrectly()
        {
            // Arrange
            string name = "testJsonQuery";
            string measurement = "sensor_data";
            string jsonPath = "temperature.value";
            DateTime from = new DateTime(2024, 1, 1);
            DateTime to = new DateTime(2024, 1, 31);
            CacheResolution cacheResolution = CacheResolutionPredefined.UseHourlyAvgCache;
            bool newestOnly = false;

            // Act
            var result = new JsonQueryParams(name, measurement, jsonPath, from, to, cacheResolution, newestOnly);

            // Assert
            result.Should().NotBeNull();
            result.Name.Should().Be(name);
            result.Measurement.Should().Be(measurement);
            result.JsonPath.Should().Be(jsonPath);
            result.From.Should().Be(from);
            result.To.Should().Be(to);
            result.CacheResolution.Should().Be(cacheResolution);
            result.NewestOnly.Should().Be(newestOnly);
        }

        [TestMethod]
        public void Constructor_ShouldHandleNewestOnlyTrue()
        {
            // Arrange
            string name = "testJsonQuery";
            string measurement = "sensor_data";
            string jsonPath = "humidity.value";
            DateTime from = new DateTime(2024, 1, 1);
            DateTime to = new DateTime(2024, 1, 31);
            CacheResolution cacheResolution = new CacheResolution { Resolution = Resolution.Hourly, AggregateFunction = AggregateFunction.Avg };
            bool newestOnly = true;

            // Act
            var result = new JsonQueryParams(name, measurement, jsonPath, from, to, cacheResolution, newestOnly);

            // Assert
            result.Should().NotBeNull();
            result.Name.Should().Be(name);
            result.Measurement.Should().Be(measurement);
            result.JsonPath.Should().Be(jsonPath);
            result.From.Should().Be(from);
            result.To.Should().Be(to);
            result.CacheResolution.Should().Be(cacheResolution);
            result.NewestOnly.Should().BeTrue();
        }

        [TestMethod]
        public void Constructor_ShouldHandleEmptyJsonPath()
        {
            // Arrange
            string name = "testJsonQuery";
            string measurement = "sensor_data";
            string jsonPath = "";
            DateTime from = new DateTime(2024, 1, 1);
            DateTime to = new DateTime(2024, 1, 31);
            CacheResolution cacheResolution = CacheResolutionPredefined.UseMinutelyAvgCache;
            bool newestOnly = false;

            // Act
            var result = new JsonQueryParams(name, measurement, jsonPath, from, to, cacheResolution, newestOnly);

            // Assert
            result.Should().NotBeNull();
            result.Name.Should().Be(name);
            result.Measurement.Should().Be(measurement);
            result.JsonPath.Should().Be("");
            result.From.Should().Be(from);
            result.To.Should().Be(to);
            result.CacheResolution.Should().Be(cacheResolution);
            result.NewestOnly.Should().BeFalse();
        }

        [TestMethod]
        public void Constructor_ShouldHandleComplexJsonPath()
        {
            // Arrange
            string name = "testJsonQuery";
            string measurement = "complex_data";
            string jsonPath = "data.sensors[0].readings.temperature.values[0]";
            DateTime from = new DateTime(2024, 1, 1);
            DateTime to = new DateTime(2024, 12, 31);
            CacheResolution cacheResolution = CacheResolutionPredefined.UseHourlyAvgCache;
            bool newestOnly = false;

            // Act
            var result = new JsonQueryParams(name, measurement, jsonPath, from, to, cacheResolution, newestOnly);

            // Assert
            result.Should().NotBeNull();
            result.Name.Should().Be(name);
            result.Measurement.Should().Be(measurement);
            result.JsonPath.Should().Be(jsonPath);
            result.From.Should().Be(from);
            result.To.Should().Be(to);
            result.CacheResolution.Should().Be(cacheResolution);
            result.NewestOnly.Should().BeFalse();
        }

        [TestMethod]
        public void Constructor_ShouldHandleDefaultNewestOnlyParameter()
        {
            // Arrange
            string name = "testJsonQuery";
            string measurement = "sensor_data";
            string jsonPath = "pressure.value";
            DateTime from = new DateTime(2024, 1, 1);
            DateTime to = new DateTime(2024, 1, 31);
            CacheResolution cacheResolution = new CacheResolution { Resolution = Resolution.Hourly, AggregateFunction = AggregateFunction.Avg };

            // Act
            var result = new JsonQueryParams(name, measurement, jsonPath, from, to, cacheResolution);

            // Assert
            result.Should().NotBeNull();
            result.Name.Should().Be(name);
            result.Measurement.Should().Be(measurement);
            result.JsonPath.Should().Be(jsonPath);
            result.From.Should().Be(from);
            result.To.Should().Be(to);
            result.CacheResolution.Should().Be(cacheResolution);
            result.NewestOnly.Should().BeFalse(); // Default value
        }
    }
}