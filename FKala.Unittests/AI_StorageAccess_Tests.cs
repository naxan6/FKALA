using Microsoft.VisualStudio.TestTools.UnitTesting;
using FluentAssertions;
using FKala.Core.DataLayers;
using FKala.Core.KalaQl;
using FKala.Core.Interfaces;
using FKala.Core;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Moq;

namespace FKala.Unittests
{
    [TestClass]
    public class AI_StorageAccess_Tests
    {
        private Mock<IDataLayer> _mockDataLayer = null!;
        private KalaQlContext _context = null!;
        private string _testDirectory = null!;

        [TestInitialize]
        public void Setup()
        {
            _mockDataLayer = new Mock<IDataLayer>();
            _mockDataLayer.Setup(x => x.ReadBuffer).Returns(131072);
            _mockDataLayer.Setup(x => x.WriteBuffer).Returns(131072);
            
            _context = new KalaQlContext(null!, _mockDataLayer.Object);

            // Create a temporary test directory
            _testDirectory = Path.Combine(Path.GetTempPath(), "StorageAccessTest");
            Directory.CreateDirectory(_testDirectory);
            
            // Create some test year directories
            Directory.CreateDirectory(Path.Combine(_testDirectory, "2024"));
            Directory.CreateDirectory(Path.Combine(_testDirectory, "2023"));
        }

        [TestCleanup]
        public void Cleanup()
        {
            if (Directory.Exists(_testDirectory))
            {
                Directory.Delete(_testDirectory, true);
            }
        }

        [TestMethod]
        public void Constructor_ShouldInitializeWithValidParameters()
        {
            // Arrange
            var dataLayer = new Mock<IDataLayer>().Object;
            var context = new KalaQlContext(null!, dataLayer);

            // Act
            var storageAccess = StorageAccess.ForRead(_testDirectory, "test_part", DateTime.Now, DateTime.Now.AddDays(1), context, false);

            // Assert
            storageAccess.Should().NotBeNull();
        }

        [TestMethod]
        public void ForRead_ShouldCreateStorageAccessInstance()
        {
            // Arrange
            var startTime = new DateTime(2024, 1, 1);
            var endTime = new DateTime(2024, 1, 2);

            // Act
            var result = StorageAccess.ForRead(_testDirectory, "test_part", startTime, endTime, _context, false);

            // Assert
            result.Should().NotBeNull();
        }

        [TestMethod]
        public void ForReadMultiFile_ShouldCreateStorageAccessInstance()
        {
            // Arrange
            var startTime = new DateTime(2024, 1, 1);
            var endTime = new DateTime(2024, 1, 2);

            // Act
            var result = StorageAccess.ForReadMultiFile(_testDirectory, "test_part", startTime, endTime, _context);

            // Assert
            result.Should().NotBeNull();
        }

        [TestMethod]
        public void ForSort_ShouldCreateStorageAccessInstance()
        {
            // Arrange
            var startTime = new DateTime(2024, 1, 1);
            var endTime = new DateTime(2024, 1, 2);

            // Act
            var result = StorageAccess.ForSort(_testDirectory, "test_part", startTime, endTime, _context);

            // Assert
            result.Should().NotBeNull();
        }

        [TestMethod]
        public void ForMerging_ShouldCreateStorageAccessInstance()
        {
            // Act
            var result = StorageAccess.ForMerging(_testDirectory, "test_part", _context);

            // Assert
            result.Should().NotBeNull();
        }

        [TestMethod]
        public void ForCleanup_ShouldCreateStorageAccessInstance()
        {
            // Act
            var result = StorageAccess.ForCleanup(_testDirectory, "test_part", _context);

            // Assert
            result.Should().NotBeNull();
        }

        [TestMethod]
        public void SetSortMark_WithSortedTrue_ShouldReturnPathWithHash()
        {
            // Arrange
            string filePath = "measure$test_2024-01-01.dat";

            // Act
            var result = StorageAccess.SetSortMark(filePath, true);

            // Assert
            result.Should().Be("measure$test#2024-01-01.dat");
        }

        [TestMethod]
        public void SetSortMark_WithSortedFalse_ShouldReturnPathWithUnderscore()
        {
            // Arrange
            string filePath = "measure$test#2024-01-01.dat";

            // Act
            var result = StorageAccess.SetSortMark(filePath, false);

            // Assert
            result.Should().Be("measure$test_2024-01-01.dat");
        }

        [TestMethod]
        public void IsSortMarkSet_WithHash_ShouldReturnTrue()
        {
            // Arrange
            string filePath = "measure$test#2024-01-01.dat";

            // Act
            var result = StorageAccess.IsSortMarkSet(filePath);

            // Assert
            result.Should().BeTrue();
        }

        [TestMethod]
        public void IsSortMarkSet_WithUnderscore_ShouldReturnFalse()
        {
            // Arrange
            string filePath = "measure$test_2024-01-01.dat";

            // Act
            var result = StorageAccess.IsSortMarkSet(filePath);

            // Assert
            result.Should().BeFalse();
        }

        [TestMethod]
        public void GetReaders_ShouldReturnEmptyListWhenNoReaders()
        {
            // Arrange
            var storageAccess = StorageAccess.ForCleanup(_testDirectory, "test_part", _context);

            // Act
            var readers = storageAccess.GetReaders();

            // Assert
            readers.Should().BeEmpty();
        }

        [TestMethod]
        public void TimeFormat_ShouldReturnCorrectFormat()
        {
            // Arrange
            var storageAccess = StorageAccess.ForRead(_testDirectory, "test_part", DateTime.Now, DateTime.Now.AddDays(1), _context, false);

            // Act
            var timeFormat = storageAccess.TimeFormat;

            // Assert
            timeFormat.Should().Be("HH:mm:ss.fffffff");
        }

        [TestMethod]
        public void Constructor_ShouldSetDefaultValues()
        {
            // Arrange
            var startTime = new DateTime(2024, 1, 1);
            var endTime = new DateTime(2024, 1, 2);

            // Act
            var storageAccess = StorageAccess.ForRead(_testDirectory, "test_part", startTime, endTime, _context, false);

            // Assert
            storageAccess.Context.Should().NotBeNull();
            storageAccess.ReadBuffer.Should().Be(131072);
            storageAccess.WriteBuffer.Should().Be(131072);
        }

        [TestMethod]
        public void GetReaders_ShouldHandleDifferentStorageAccessTypes()
        {
            // Test ForRead
            var readAccess = StorageAccess.ForRead(_testDirectory, "test_part", DateTime.Now, DateTime.Now.AddDays(1), _context, false);
            var readers1 = readAccess.GetReaders();
            readers1.Should().BeOfType<List<ReaderTuple>>();

            // Test ForMerging
            var mergeAccess = StorageAccess.ForMerging(_testDirectory, "test_part", _context);
            var readers2 = mergeAccess.GetReaders();
            readers2.Should().BeOfType<List<ReaderTuple>>();

            // Test ForCleanup
            var cleanupAccess = StorageAccess.ForCleanup(_testDirectory, "test_part", _context);
            var readers3 = cleanupAccess.GetReaders();
            readers3.Should().BeOfType<List<ReaderTuple>>();
        }
    }
}