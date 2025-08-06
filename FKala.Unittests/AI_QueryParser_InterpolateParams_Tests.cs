using Microsoft.VisualStudio.TestTools.UnitTesting;
using FluentAssertions;
using FKala.Core.KalaQl.QueryParser;
using FKala.Core.Model;
using System;

namespace FKala.Unittests
{
    [TestClass]
    public class AI_QueryParser_InterpolateParams_Tests
    {
        [TestMethod]
        public void Constructor_ShouldInitializeAllPropertiesCorrectly()
        {
            // Arrange
            string name = "testInterpolate";
            string input = "temperature";
            InterpolationMode interpolationMode = InterpolationMode.forwards;
            decimal? defaultValue = 20.5m;

            // Act
            var result = new InterpolateParams(name, input, interpolationMode, defaultValue);

            // Assert
            result.Should().NotBeNull();
            result.Name.Should().Be(name);
            result.Input.Should().Be(input);
            result.InterpolationMode.Should().Be(interpolationMode);
            result.DefaultValue.Should().Be(defaultValue);
        }

        [TestMethod]
        public void Constructor_ShouldHandleNullDefaultValue()
        {
            // Arrange
            string name = "testInterpolate";
            string input = "temperature";
            InterpolationMode interpolationMode = InterpolationMode.forwards;
            decimal? defaultValue = null;

            // Act
            var result = new InterpolateParams(name, input, interpolationMode, defaultValue);

            // Assert
            result.Should().NotBeNull();
            result.Name.Should().Be(name);
            result.Input.Should().Be(input);
            result.InterpolationMode.Should().Be(interpolationMode);
            result.DefaultValue.Should().BeNull();
        }

        [TestMethod]
        public void Constructor_ShouldHandleEmptyNameAndInput()
        {
            // Arrange
            string name = "";
            string input = "";
            InterpolationMode interpolationMode = InterpolationMode.constant;
            decimal? defaultValue = null;

            // Act
            var result = new InterpolateParams(name, input, interpolationMode, defaultValue);

            // Assert
            result.Should().NotBeNull();
            result.Name.Should().Be("");
            result.Input.Should().Be("");
            result.InterpolationMode.Should().Be(InterpolationMode.constant);
            result.DefaultValue.Should().BeNull();
        }

        [TestMethod]
        public void Constructor_ShouldHandleDifferentInterpolationModes()
        {
            // Arrange
            string name = "testInterpolate";
            string input = "pressure";
            InterpolationMode interpolationMode = InterpolationMode.backwards;
            decimal? defaultValue = 1013.25m;

            // Act
            var result = new InterpolateParams(name, input, interpolationMode, defaultValue);

            // Assert
            result.Should().NotBeNull();
            result.Name.Should().Be(name);
            result.Input.Should().Be(input);
            result.InterpolationMode.Should().Be(InterpolationMode.backwards);
            result.DefaultValue.Should().Be(defaultValue);
        }

        [TestMethod]
        public void Constructor_ShouldHandleZeroDefaultValue()
        {
            // Arrange
            string name = "testInterpolate";
            string input = "humidity";
            InterpolationMode interpolationMode = InterpolationMode.forwards;
            decimal? defaultValue = 0m;

            // Act
            var result = new InterpolateParams(name, input, interpolationMode, defaultValue);

            // Assert
            result.Should().NotBeNull();
            result.Name.Should().Be(name);
            result.Input.Should().Be(input);
            result.InterpolationMode.Should().Be(interpolationMode);
            result.DefaultValue.Should().Be(0m);
        }
    }
}