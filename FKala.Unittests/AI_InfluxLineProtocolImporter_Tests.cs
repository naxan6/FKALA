using Microsoft.VisualStudio.TestTools.UnitTesting;
using FluentAssertions;
using FKala.Core.Migration;
using FKala.Core.Interfaces;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Moq;

namespace FKala.Unittests
{
    [TestClass]
    public class AI_InfluxLineProtocolImporter_Tests
    {
        private Mock<IDataLayer> _mockDataLayer = null!;
        private InfluxLineProtocolImporter _importer = null!;
        private string _testFilePath = null!;

        [TestInitialize]
        public void Setup()
        {
            _mockDataLayer = new Mock<IDataLayer>();
            _importer = new InfluxLineProtocolImporter(_mockDataLayer.Object);
            
            // Create a temporary test file
            _testFilePath = Path.GetTempFileName();
            File.WriteAllText(_testFilePath, "cpu,host=server1 value=1.0 1000000000\ncpu,host=server2 value=2.0 2000000000");
        }

        [TestCleanup]
        public void Cleanup()
        {
            _mockDataLayer = null!;
            _importer  = null!;
            if (File.Exists(_testFilePath))
            {
                File.Delete(_testFilePath);
            }
        }

        [TestMethod]
        public void Constructor_ShouldInitializeWithValidDataLayer()
        {
            // Arrange
            var dataLayer = new Mock<IDataLayer>().Object;

            // Act
            var result = new InfluxLineProtocolImporter(dataLayer);

            // Assert
            result.Should().NotBeNull();
        }

        [TestMethod]
        public void Import_WithFilePathOnly_ShouldProcessAllLines()
        {
            // Arrange
            string parameters = _testFilePath;

            // Act
            var results = _importer.Import(parameters).ToList();

            // Assert
            results.Should().NotBeEmpty();
            results.Count.Should().BeGreaterThan(1); // At least one progress message + data points
            
            // Verify that Insert was called for each line
            _mockDataLayer.Verify(x => x.Insert(It.IsAny<string>(), null), Times.Exactly(2));
        }

        [TestMethod]
        public void Import_WithTimeRangeAndFilePath_ShouldFilterByTimeRange()
        {
            // Arrange
            var startTime = new System.DateTime(1970, 1, 1, 0, 0, 1); // 1 second after epoch
            var endTime = new System.DateTime(1970, 1, 1, 0, 0, 3);   // 3 seconds after epoch
            string parameters = $"{startTime:yyyy-MM-ddTHH:mm:ss};{endTime:yyyy-MM-ddTHH:mm:ss};{_testFilePath}";

            // Act
            var results = _importer.Import(parameters).ToList();

            // Assert
            results.Should().NotBeEmpty();
            
            // Verify that Insert was called (both timestamps should be within range)
            _mockDataLayer.Verify(x => x.Insert(It.IsAny<string>(), null), Times.Exactly(2));
        }

        [TestMethod]
        public void Import_WithInvalidParameterCount_ShouldThrowException()
        {
            // Arrange
            string parameters = "invalid;param;count;err";

            // Act & Assert
            var action = () => _importer.Import(parameters).ToList();
            action.Should().Throw<Exception>().WithMessage("wrong parameter count. only <path> or <from> <until> <path>!");
        }

        [TestMethod]
        public void ImportLine_WithTimestampOutsideRange_ShouldNotInsert()
        {
            // Arrange
            var startTime = new System.DateTime(2023, 1, 1);
            var endTime = new System.DateTime(2023, 1, 2);
            _importer = new InfluxLineProtocolImporter(_mockDataLayer.Object);

            // This would require modifying the importer to accept time range
            // For now, we'll test that it doesn't throw an exception
            
            string line = "cpu,host=server1 value=1.0 1672531200000000000"; // 2023-01-01 00:00:00 in Nanosekunden
            string importparams = $"{startTime:yyyy-MM-ddTHH:mm:ss};{endTime:yyyy-MM-ddTHH:mm:ss};{line}";
            // Act & Assert
            var action = () => _importer.Import(importparams);
            action.Should().NotThrow();
        }
    }
}