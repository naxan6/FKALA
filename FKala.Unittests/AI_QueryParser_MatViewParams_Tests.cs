using Microsoft.VisualStudio.TestTools.UnitTesting;
using FluentAssertions;
using FKala.Core.KalaQl.QueryParser;
using System;

namespace FKala.Unittests
{
    [TestClass]
    public class AI_QueryParser_MatViewParams_Tests
    {
        [TestMethod]
        public void Constructor_ShouldInitializeAllPropertiesCorrectly()
        {
            // Arrange
            string name = "testMatView";
            string input = "source_data";
            string measurement = "materialized_view";

            // Act
            var result = new MatViewParams(name, input, measurement);

            // Assert
            result.Should().NotBeNull();
            result.Name.Should().Be(name);
            result.Input.Should().Be(input);
            result.Measurement.Should().Be(measurement);
        }

        [TestMethod]
        public void Constructor_ShouldHandleEmptyStrings()
        {
            // Arrange
            string name = "";
            string input = "";
            string measurement = "";

            // Act
            var result = new MatViewParams(name, input, measurement);

            // Assert
            result.Should().NotBeNull();
            result.Name.Should().Be("");
            result.Input.Should().Be("");
            result.Measurement.Should().Be("");
        }

        [TestMethod]
        public void Constructor_ShouldHandleSpecialCharactersInNames()
        {
            // Arrange
            string name = "test_mat_view_123";
            string input = "source_data_456";
            string measurement = "materialized_view_789";

            // Act
            var result = new MatViewParams(name, input, measurement);

            // Assert
            result.Should().NotBeNull();
            result.Name.Should().Be(name);
            result.Input.Should().Be(input);
            result.Measurement.Should().Be(measurement);
        }

        [TestMethod]
        public void Constructor_ShouldHandleComplexNames()
        {
            // Arrange
            string name = "complex_materialized_view_test";
            string input = "very_complex_source_data_input";
            string measurement = "extremely_long_materialized_view_name_for_testing";

            // Act
            var result = new MatViewParams(name, input, measurement);

            // Assert
            result.Should().NotBeNull();
            result.Name.Should().Be(name);
            result.Input.Should().Be(input);
            result.Measurement.Should().Be(measurement);
        }

        [TestMethod]
        public void Constructor_ShouldHandleSingleCharacterNames()
        {
            // Arrange
            string name = "a";
            string input = "b";
            string measurement = "c";

            // Act
            var result = new MatViewParams(name, input, measurement);

            // Assert
            result.Should().NotBeNull();
            result.Name.Should().Be("a");
            result.Input.Should().Be("b");
            result.Measurement.Should().Be("c");
        }
    }
}