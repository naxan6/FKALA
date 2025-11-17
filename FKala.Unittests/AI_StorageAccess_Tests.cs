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
        private const string TestDirectoryName = "StorageAccessTest";
        private Mock<IDataLayer> _mockDataLayer = null!;
        private KalaQlContext _context = null!;
        private string _testDirectory = null!;

        [TestInitialize]
        public void Setup()
        {
            try
            {
                // Initialize mock data layer with consistent buffer sizes
                _mockDataLayer = new Mock<IDataLayer>();
                _mockDataLayer.Setup(x => x.ReadBuffer).Returns(131072);
                _mockDataLayer.Setup(x => x.WriteBuffer).Returns(131072);
                
                // Initialize context with mocked data layer
                _context = new KalaQlContext(null!, _mockDataLayer.Object);

                // Create a temporary test directory
                _testDirectory = Path.Combine(Path.GetTempPath(), TestDirectoryName);
                Directory.CreateDirectory(_testDirectory);
                
                // Create some test year directories
                Directory.CreateDirectory(Path.Combine(_testDirectory, "2024"));
                Directory.CreateDirectory(Path.Combine(_testDirectory, "2023"));
            }
            catch (Exception ex)
            {
                // If setup fails, throw a more descriptive exception
                throw new InvalidOperationException("Test setup failed. Could not initialize test environment.", ex);
            }
        }

        [TestCleanup]
        public void Cleanup()
        {
            try
            {
                if (!string.IsNullOrEmpty(_testDirectory) && Directory.Exists(_testDirectory))
                {
                    Directory.Delete(_testDirectory, true);
                }
            }
            catch (Exception ex)
            {
                // Log the exception but don't fail the test cleanup
                Console.WriteLine($"Warning: Could not clean up test directory {_testDirectory}. Error: {ex.Message}");
            }
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
    }
}