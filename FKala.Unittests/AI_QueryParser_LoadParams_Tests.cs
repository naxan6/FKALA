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
    public class AI_QueryParser_LoadParams_Tests
    {
        [TestMethod]
        public void Constructor_ShouldInitializeAllPropertiesCorrectly()
        {
            // Arrange
            string name = "testLoad";
            string measurement = "sensor_data";
            DateTime from = new DateTime(2024, 1, 1);
            DateTime to = new DateTime(2024, 1, 31);
            CacheResolution cacheResolution = CacheResolutionPredefined.UseHourlyAvgCache;
            bool newestOnly = false;

            // Act
            var result = new LoadParams(name, measurement, from, to, cacheResolution, newestOnly);

            // Assert
            result.Should().NotBeNull();
            result.Name.Should().Be(name);
            result.Measurement.Should().Be(measurement);
            result.From.Should().Be(from);
            result.To.Should().Be(to);
            result.CacheResolution.Should().Be(cacheResolution);
            result.NewestOnly.Should().Be(newestOnly);
        }

        [TestMethod]
        public void Constructor_ShouldHandleNewestOnlyTrue()
        {
            // Arrange
            string name = "testLoad";
            string measurement = "sensor_data";
            DateTime from = new DateTime(2024, 1, 1);
            DateTime to = new DateTime(2024, 1, 31);
            CacheResolution cacheResolution = CacheResolutionPredefined.UseMinutelyAvgCache;
            bool newestOnly = true;

            // Act
            var result = new LoadParams(name, measurement, from, to, cacheResolution, newestOnly);

            // Assert
            result.Should().NotBeNull();
            result.Name.Should().Be(name);
            result.Measurement.Should().Be(measurement);
            result.From.Should().Be(from);
            result.To.Should().Be(to);
            result.CacheResolution.Should().Be(cacheResolution);
            result.NewestOnly.Should().BeTrue();
        }

        [TestMethod]
        public void Constructor_ShouldHandleDefaultNewestOnlyParameter()
        {
            // Arrange
            string name = "testLoad";
            string measurement = "sensor_data";
            DateTime from = new DateTime(2024, 1, 1);
            DateTime to = new DateTime(2024, 1, 31);
            CacheResolution cacheResolution = CacheResolutionPredefined.UseHourlyAvgCache;

            // Act
            var result = new LoadParams(name, measurement, from, to, cacheResolution);

            // Assert
            result.Should().NotBeNull();
            result.Name.Should().Be(name);
            result.Measurement.Should().Be(measurement);
            result.From.Should().Be(from);
            result.To.Should().Be(to);
            result.CacheResolution.Should().Be(cacheResolution);
            result.NewestOnly.Should().BeFalse(); // Default value
        }

        [TestMethod]
        public void Constructor_ShouldHandleCustomCacheResolution()
        {
            // Arrange
            string name = "testLoad";
            string measurement = "sensor_data";
            DateTime from = new DateTime(2024, 1, 1);
            DateTime to = new DateTime(2024, 12, 31);
            CacheResolution cacheResolution = new CacheResolution 
            { 
                Resolution = Resolution.Hourly, 
                AggregateFunction = AggregateFunction.Avg,
                ForceRebuild = true
            };
            bool newestOnly = false;

            // Act
            var result = new LoadParams(name, measurement, from, to, cacheResolution, newestOnly);

            // Assert
            result.Should().NotBeNull();
            result.Name.Should().Be(name);
            result.Measurement.Should().Be(measurement);
            result.From.Should().Be(from);
            result.To.Should().Be(to);
            result.CacheResolution.Should().Be(cacheResolution);
            result.NewestOnly.Should().BeFalse();
        }

        [TestMethod]
        public void Constructor_ShouldHandleEmptyNameAndMeasurement()
        {
            // Arrange
            string name = "";
            string measurement = "";
            DateTime from = new DateTime(2024, 1, 1);
            DateTime to = new DateTime(2024, 1, 31);
            CacheResolution cacheResolution = CacheResolutionPredefined.UseHourlyAvgCache;
            bool newestOnly = false;

            // Act
            var result = new LoadParams(name, measurement, from, to, cacheResolution, newestOnly);

            // Assert
            result.Should().NotBeNull();
            result.Name.Should().Be("");
            result.Measurement.Should().Be("");
            result.From.Should().Be(from);
            result.To.Should().Be(to);
            result.CacheResolution.Should().Be(cacheResolution);
            result.NewestOnly.Should().BeFalse();
        }
    }
}