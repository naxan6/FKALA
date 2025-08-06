using Microsoft.VisualStudio.TestTools.UnitTesting;
using FluentAssertions;
using FKala.Migrate.MariaDb;

namespace FKala.Unittests
{
    [TestClass]
    public class AI_EventRow_Tests
    {
        [TestMethod]
        public void Constructor_ShouldInitializeAllProperties()
        {
            // Arrange
            var eventRow = new EventRow
            {
                sensorName = "test_sensor",
                sensorPath = "/path/to/sensor",
                id = 1,
                timestamp = 1234567890,
                timeValue1 = 1000000000,
                timeValue2 = 2000000000,
                intValue1 = 42,
                intValue2 = 84,
                doubleValue1 = 3.14,
                doubleValue2 = 6.28,
                stringValue1 = "test_string_1",
                stringValue2 = "test_string_2"
            };

            // Assert
            eventRow.sensorName.Should().Be("test_sensor");
            eventRow.sensorPath.Should().Be("/path/to/sensor");
            eventRow.id.Should().Be(1);
            eventRow.timestamp.Should().Be(1234567890);
            eventRow.timeValue1.Should().Be(1000000000);
            eventRow.timeValue2.Should().Be(2000000000);
            eventRow.intValue1.Should().Be(42);
            eventRow.intValue2.Should().Be(84);
            eventRow.doubleValue1.Should().Be(3.14);
            eventRow.doubleValue2.Should().Be(6.28);
            eventRow.stringValue1.Should().Be("test_string_1");
            eventRow.stringValue2.Should().Be("test_string_2");
        }

        [TestMethod]
        public void Constructor_ShouldHandleNullableProperties()
        {
            // Arrange
            var eventRow = new EventRow
            {
                sensorName = "sensor",
                sensorPath = "/path",
                id = null,
                timestamp = 1234567890,
                timeValue1 = null,
                timeValue2 = null,
                intValue1 = null,
                intValue2 = null,
                doubleValue1 = null,
                doubleValue2 = null,
                stringValue1 = null,
                stringValue2 = null
            };

            // Assert
            eventRow.sensorName.Should().Be("sensor");
            eventRow.sensorPath.Should().Be("/path");
            eventRow.id.Should().BeNull();
            eventRow.timestamp.Should().Be(1234567890);
            eventRow.timeValue1.Should().BeNull();
            eventRow.timeValue2.Should().BeNull();
            eventRow.intValue1.Should().BeNull();
            eventRow.intValue2.Should().BeNull();
            eventRow.doubleValue1.Should().BeNull();
            eventRow.doubleValue2.Should().BeNull();
            eventRow.stringValue1.Should().BeNull();
            eventRow.stringValue2.Should().BeNull();
        }

        [TestMethod]
        public void Constructor_WithAllValuesSet_ShouldWorkCorrectly()
        {
            // Arrange
            long timestamp = 1640995200000; // 2022-01-01 00:00:00 UTC in milliseconds
            var eventRow = new EventRow
            {
                sensorName = "temperature_sensor",
                sensorPath = "/building/zone1/temperature",
                id = 12345,
                timestamp = timestamp,
                timeValue1 = timestamp + 1000,
                timeValue2 = timestamp + 2000,
                intValue1 = 25,
                intValue2 = 30,
                doubleValue1 = 23.5,
                doubleValue2 = 24.1,
                stringValue1 = "normal",
                stringValue2 = "optimal"
            };

            // Assert
            eventRow.sensorName.Should().Be("temperature_sensor");
            eventRow.sensorPath.Should().Be("/building/zone1/temperature");
            eventRow.id.Should().Be(12345);
            eventRow.timestamp.Should().Be(timestamp);
            eventRow.timeValue1.Should().Be(timestamp + 1000);
            eventRow.timeValue2.Should().Be(timestamp + 2000);
            eventRow.intValue1.Should().Be(25);
            eventRow.intValue2.Should().Be(30);
            eventRow.doubleValue1.Should().Be(23.5);
            eventRow.doubleValue2.Should().Be(24.1);
            eventRow.stringValue1.Should().Be("normal");
            eventRow.stringValue2.Should().Be("optimal");
        }

        [TestMethod]
        public void Constructor_WithEmptyStrings_ShouldWorkCorrectly()
        {
            // Arrange
            var eventRow = new EventRow
            {
                sensorName = "",
                sensorPath = "",
                id = null,
                timestamp = 0,
                timeValue1 = null,
                timeValue2 = null,
                intValue1 = null,
                intValue2 = null,
                doubleValue1 = null,
                doubleValue2 = null,
                stringValue1 = "",
                stringValue2 = ""
            };

            // Assert
            eventRow.sensorName.Should().Be("");
            eventRow.sensorPath.Should().Be("");
            eventRow.id.Should().BeNull();
            eventRow.timestamp.Should().Be(0);
            eventRow.timeValue1.Should().BeNull();
            eventRow.timeValue2.Should().BeNull();
            eventRow.intValue1.Should().BeNull();
            eventRow.intValue2.Should().BeNull();
            eventRow.doubleValue1.Should().BeNull();
            eventRow.doubleValue2.Should().BeNull();
            eventRow.stringValue1.Should().Be("");
            eventRow.stringValue2.Should().Be("");
        }

        [TestMethod]
        public void ToString_ShouldReturnCorrectFormat()
        {
            // Arrange
            var eventRow = new EventRow
            {
                sensorName = "test_sensor",
                sensorPath = "/path/to/sensor",
                id = 1,
                timestamp = 1234567890,
                timeValue1 = 1000000000,
                timeValue2 = 2000000000,
                intValue1 = 42,
                intValue2 = 84,
                doubleValue1 = 3.14,
                doubleValue2 = 6.28,
                stringValue1 = "test_string_1",
                stringValue2 = "test_string_2"
            };

            // Act
            var result = eventRow.ToString();

            // Assert
            result.Should().Be("test_sensor # /path/to/sensor # 1234567890 # 1000000000 # 2000000000 # 42 # 84 # 3.14 # 6.28 # test_string_1 # test_string_2");
        }

        [TestMethod]
        public void ToString_ShouldHandleNullValues()
        {
            // Arrange
            var eventRow = new EventRow
            {
                sensorName = "sensor",
                sensorPath = "/path",
                id = null,
                timestamp = 1234567890,
                timeValue1 = null,
                timeValue2 = null,
                intValue1 = null,
                intValue2 = null,
                doubleValue1 = null,
                doubleValue2 = null,
                stringValue1 = null,
                stringValue2 = null
            };

            // Act
            var result = eventRow.ToString();

            // Assert
            result.Should().Be("sensor # /path # 1234567890 #  #  #  #  #  #  #  # ");
        }

        [TestMethod]
        public void Constructor_WithSpecialCharacters_ShouldWorkCorrectly()
        {
            // Arrange
            var eventRow = new EventRow
            {
                sensorName = "sensor with spaces",
                sensorPath = "/path/with/special-chars",
                id = 999,
                timestamp = 1234567890,
                timeValue1 = 1111111111,
                timeValue2 = 2222222222,
                intValue1 = -1,
                intValue2 = 0,
                doubleValue1 = -3.14,
                doubleValue2 = 0.0,
                stringValue1 = "string with spaces",
                stringValue2 = "string-with-dashes"
            };

            // Assert
            eventRow.sensorName.Should().Be("sensor with spaces");
            eventRow.sensorPath.Should().Be("/path/with/special-chars");
            eventRow.id.Should().Be(999);
            eventRow.timestamp.Should().Be(1234567890);
            eventRow.timeValue1.Should().Be(1111111111);
            eventRow.timeValue2.Should().Be(2222222222);
            eventRow.intValue1.Should().Be(-1);
            eventRow.intValue2.Should().Be(0);
            eventRow.doubleValue1.Should().Be(-3.14);
            eventRow.doubleValue2.Should().Be(0.0);
            eventRow.stringValue1.Should().Be("string with spaces");
            eventRow.stringValue2.Should().Be("string-with-dashes");
        }
    }
}