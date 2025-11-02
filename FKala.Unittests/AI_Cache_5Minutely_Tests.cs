using Microsoft.VisualStudio.TestTools.UnitTesting;
using FluentAssertions;
using FKala.Core.DataLayer.Cache;
using FKala.Core.Model;
using FKala.Core.KalaQl.Windowing;
using FKala.Core.Interfaces;
using Moq;
using System;
using System.Collections.Generic;

namespace FKala.Unittests
{
    [TestClass]
    public class AI_Cache_5Minutely_Tests
    {
        private Mock<IDataLayer>? _dataLayerMock;
        private Cache_5Minutely? _cache5Minutely;

        [TestInitialize]
        public void Setup()
        {
            _dataLayerMock = new Mock<IDataLayer>();
            _cache5Minutely = new Cache_5Minutely(_dataLayerMock.Object);
        }

        [TestMethod]
        public void CacheSubdir_ShouldReturnCorrectDirectoryName()
        {
            // Act
            var result = _cache5Minutely!.CacheSubdir;

            // Assert
            result.Should().Be("5Minutely");
        }

        [TestMethod]
        public void GetTimeFormat_ShouldReturnCorrectFormat()
        {
            // Act
            var result = _cache5Minutely!.GetTimeFormat();

            // Assert
            result.Should().Be("MM-ddTHH:mm");
        }

        [TestMethod]
        public void Window_ShouldHaveCorrectWindowSize()
        {
            // Act
            var result = _cache5Minutely!.Window;

            // Assert
            result.Should().NotBeNull();
            result.Interval.Should().Be(TimeSpan.FromMinutes(5));
            result.Mode.Should().Be(WindowMode.Aligned5Minutes);
        }

        [TestMethod]
        public void ReadLine_ShouldParseDateTimeAndValueCorrectly()
        {
            // Arrange
            const int fileYear = 2024;
            const string line = "06-15T23:26 55.654105";

            // Act
            var result = _cache5Minutely!.ReadLine(fileYear, line);

            // Assert
            result.Should().NotBeNull();
            result.StartTime.Should().Be(new DateTime(2024, 6, 15, 23, 26, 0, DateTimeKind.Utc));
            result.EndTime.Should().Be(new DateTime(2024, 6, 15, 23, 31, 0, DateTimeKind.Utc)); // 5 minute window
            result.Value.Should().Be(55.654105m);
        }

        [TestMethod]
        public void ReadLine_ShouldHandleNullLine()
        {
            // Arrange
            const int fileYear = 2024;
            string? line = null;

            // Act
            Action act = () => _cache5Minutely!.ReadLine(fileYear, line);

            // Assert
            act.Should().Throw<ArgumentException>();
        }

        [TestMethod]
        public void ShouldUpdateFromWhere_ShouldReturnMaxValueWhenNoData()
        {
            // Arrange
            const int cacheYear = 2024;
            DataPoint? newestInCache = null;
            DataPoint? newestInRaw = null;

            // Act
            var result = _cache5Minutely!.ShouldUpdateFromWhere(cacheYear, newestInCache, newestInRaw);

            // Assert
            result.Should().Be(DateTime.MaxValue);
        }

        [TestMethod]
        public void ShouldUpdateFromWhere_ShouldReturnMaxValueWhenCacheIsRecent()
        {
            // Arrange
            const int cacheYear = 2024;
            var newestInCache = new DataPoint { StartTime = DateTime.UtcNow };
            var newestInRaw = new DataPoint { StartTime = DateTime.UtcNow.AddMinutes(-10) };

            // Act
            var result = _cache5Minutely!.ShouldUpdateFromWhere(cacheYear, newestInCache, newestInRaw);

            // Assert
            result.Should().Be(DateTime.MaxValue);
        }

        [TestMethod]
        public void ShouldUpdateFromWhere_ShouldReturnCalculatedDateWhenRefreshNeeded()
        {
            // Arrange
            const int cacheYear = 2024;
            var newestInCache = new DataPoint { StartTime = new DateTime(2024, 1, 1, 12, 0, 0, DateTimeKind.Utc) };
            var newestInRaw = new DataPoint { StartTime = new DateTime(2024, 1, 1, 11, 55, 0, DateTimeKind.Utc) };

            // Act
            var result = _cache5Minutely!.ShouldUpdateFromWhere(cacheYear, newestInCache, newestInRaw);

            // Assert
            result.Should().Be(DateTime.MaxValue);
        }
    }
}