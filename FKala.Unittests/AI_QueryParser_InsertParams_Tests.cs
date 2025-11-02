using Microsoft.VisualStudio.TestTools.UnitTesting;
using FluentAssertions;
using FKala.Core.KalaQl.QueryParser;
using System;

namespace FKala.Unittests
{
    [TestClass]
    public class AI_QueryParser_InsertParams_Tests
    {
        [TestMethod]
        public void Constructor_ShouldInitializeAllPropertiesCorrectly()
        {
            // Arrange
            string name = "testInsert";
            string measurement = "sensor_data";
            string value = "42.5";

            // Act
            var result = new InsertParams(name, measurement, value);

            // Assert
            result.Should().NotBeNull();
            result.Name.Should().Be(name);
            result.Measurement.Should().Be(measurement);
            result.Value.Should().Be(value);
        }

        [TestMethod]
        public void Constructor_ShouldHandleEmptyStrings()
        {
            // Arrange
            string name = "";
            string measurement = "";
            string value = "";

            // Act
            var result = new InsertParams(name, measurement, value);

            // Assert
            result.Should().NotBeNull();
            result.Name.Should().Be("");
            result.Measurement.Should().Be("");
            result.Value.Should().Be("");
        }

        [TestMethod]
        public void Constructor_ShouldHandleNullValues()
        {
            // Arrange
            string name = "test";
            string measurement = "sensor";
            string value = null!;

            // Act
            var result = new InsertParams(name, measurement, value);

            // Assert
            result.Should().NotBeNull();
            result.Name.Should().Be("test");
            result.Measurement.Should().Be("sensor");
            result.Value.Should().BeNull();
        }

        [TestMethod]
        public void Constructor_ShouldHandleComplexJsonValues()
        {
            // Arrange
            string name = "complexInsert";
            string measurement = "weather_data";
            string value = "{\"temperature\": 23.5, \"humidity\": 65, \"pressure\": 1013}";

            // Act
            var result = new InsertParams(name, measurement, value);

            // Assert
            result.Should().NotBeNull();
            result.Name.Should().Be("complexInsert");
            result.Measurement.Should().Be("weather_data");
            result.Value.Should().Be("{\"temperature\": 23.5, \"humidity\": 65, \"pressure\": 1013}");
        }

        [TestMethod]
        public void Constructor_ShouldHandleSpecialCharactersInValue()
        {
            // Arrange
            string name = "specialChars";
            string measurement = "test_sensor";
            string value = "value with spaces & special chars: áéíóú";

            // Act
            var result = new InsertParams(name, measurement, value);

            // Assert
            result.Should().NotBeNull();
            result.Name.Should().Be("specialChars");
            result.Measurement.Should().Be("test_sensor");
            result.Value.Should().Be("value with spaces & special chars: áéíóú");
        }

        [TestMethod]
        public void Constructor_ShouldHandleNumericValues()
        {
            // Arrange
            string name = "numericTest";
            string measurement = "temperature";
            string value = "123.456";

            // Act
            var result = new InsertParams(name, measurement, value);

            // Assert
            result.Should().NotBeNull();
            result.Name.Should().Be("numericTest");
            result.Measurement.Should().Be("temperature");
            result.Value.Should().Be("123.456");
        }
    }
}