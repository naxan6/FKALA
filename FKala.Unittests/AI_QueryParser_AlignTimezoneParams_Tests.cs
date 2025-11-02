using Microsoft.VisualStudio.TestTools.UnitTesting;
using FluentAssertions;
using FKala.Core.KalaQl.QueryParser;
using System;

namespace FKala.Unittests
{
    [TestClass]
    public class AI_QueryParser_AlignTimezoneParams_Tests
    {
        [TestMethod]
        public void Constructor_ShouldInitializeTimezonePropertyCorrectly()
        {
            // Arrange
            string timezone = "Europe/Berlin";

            // Act
            var result = new AlignTimezoneParams(timezone);

            // Assert
            result.Should().NotBeNull();
            result.Timezone.Should().Be(timezone);
        }

        [TestMethod]
        public void Constructor_ShouldHandleEmptyTimezone()
        {
            // Arrange
            string timezone = "";

            // Act
            var result = new AlignTimezoneParams(timezone);

            // Assert
            result.Should().NotBeNull();
            result.Timezone.Should().Be("");
        }


        [TestMethod]
        public void Constructor_ShouldHandleDifferentTimezones()
        {
            // Arrange
            string timezone = "UTC";

            // Act
            var result = new AlignTimezoneParams(timezone);

            // Assert
            result.Should().NotBeNull();
            result.Timezone.Should().Be("UTC");
        }

        [TestMethod]
        public void Constructor_ShouldHandleComplexTimezoneName()
        {
            // Arrange
            string timezone = "America/New_York";

            // Act
            var result = new AlignTimezoneParams(timezone);

            // Assert
            result.Should().NotBeNull();
            result.Timezone.Should().Be("America/New_York");
        }
    }
}