using Microsoft.VisualStudio.TestTools.UnitTesting;
using FluentAssertions;
using FKala.Core.KalaQl;
using System;

namespace FKala.Unittests
{
    [TestClass]
    public class AI_DurationParser_Tests
    {
        [TestMethod]
        public void Parse_ShouldParsePositiveHours()
        {
            // Arrange
            string durationString = "+2h";

            // Act
            var result = DurationParser.Parse(durationString);

            // Assert
            result.Should().Be(TimeSpan.FromHours(2));
        }

        [TestMethod]
        public void Parse_ShouldParseNegativeMinutes()
        {
            // Arrange
            string durationString = "-30m";

            // Act
            var result = DurationParser.Parse(durationString);

            // Assert
            result.Should().Be(TimeSpan.FromMinutes(-30));
        }

        [TestMethod]
        public void Parse_ShouldParseDaysHoursMinutes()
        {
            // Arrange
            string durationString = "+1d2h30m";

            // Act
            var result = DurationParser.Parse(durationString);

            // Assert
            result.Should().Be(TimeSpan.FromDays(1) + TimeSpan.FromHours(2) + TimeSpan.FromMinutes(30));
        }

        [TestMethod]
        public void Parse_ShouldParseOnlyDays()
        {
            // Arrange
            string durationString = "+5d";

            // Act
            var result = DurationParser.Parse(durationString);

            // Assert
            result.Should().Be(TimeSpan.FromDays(5));
        }

        [TestMethod]
        public void Parse_ShouldParseOnlyHours()
        {
            // Arrange
            string durationString = "-12h";

            // Act
            var result = DurationParser.Parse(durationString);

            // Assert
            result.Should().Be(TimeSpan.FromHours(-12));
        }

        [TestMethod]
        public void Parse_ShouldParseDaysAndMinutes()
        {
            // Arrange
            string durationString = "+2d45m";

            // Act
            var result = DurationParser.Parse(durationString);

            // Assert
            result.Should().Be(TimeSpan.FromDays(2) + TimeSpan.FromMinutes(45));
        }

        [TestMethod]
        public void Parse_ShouldParseHoursAndMinutes()
        {
            // Arrange
            string durationString = "-3h15m";

            // Act
            var result = DurationParser.Parse(durationString);

            // Assert
            result.Should().Be(-(TimeSpan.FromHours(3) + TimeSpan.FromMinutes(15)));
        }

        [TestMethod]
        public void Parse_ShouldThrowException_WhenEmptyString()
        {
            // Arrange
            string durationString = "";

            // Act & Assert
            Assert.ThrowsException<FormatException>(() => DurationParser.Parse(durationString));
        }

        [TestMethod]
        public void Parse_ShouldThrowException_WhenInvalidFormat()
        {
            // Arrange
            string durationString = "invalid";

            // Act & Assert
            Assert.ThrowsException<FormatException>(() => DurationParser.Parse(durationString));
        }

        [TestMethod]
        public void Parse_ShouldThrowException_WhenNoComponents()
        {
            // Arrange
            string durationString = "+";

            // Act & Assert
            Assert.ThrowsException<FormatException>(() => DurationParser.Parse(durationString));
        }

        [TestMethod]
        public void TryParse_ShouldReturnTrue_ForValidFormat()
        {
            // Arrange
            string durationString = "+2h30m";

            // Act
            var success = DurationParser.TryParse(durationString, out var result);

            // Assert
            success.Should().BeTrue();
            result.Should().Be(TimeSpan.FromHours(2) + TimeSpan.FromMinutes(30));
        }

        [TestMethod]
        public void TryParse_ShouldReturnFalse_ForInvalidFormat()
        {
            // Arrange
            string durationString = "invalid";

            // Act
            var success = DurationParser.TryParse(durationString, out var result);

            // Assert
            success.Should().BeFalse();
        }

        [TestMethod]
        public void ToDurationString_ShouldConvertPositiveTimeSpan()
        {
            // Arrange
            var timeSpan = TimeSpan.FromDays(1) + TimeSpan.FromHours(2) + TimeSpan.FromMinutes(30);

            // Act
            var result = DurationParser.ToDurationString(timeSpan);

            // Assert
            result.Should().Be("+1d2h30m");
        }

        [TestMethod]
        public void ToDurationString_ShouldConvertNegativeTimeSpan()
        {
            // Arrange
            var timeSpan = -(TimeSpan.FromHours(3) + TimeSpan.FromMinutes(15));

            // Act
            var result = DurationParser.ToDurationString(timeSpan);

            // Assert
            result.Should().Be("-3h15m");
        }

        [TestMethod]
        public void ToDurationString_ShouldConvertOnlyHours()
        {
            // Arrange
            var timeSpan = TimeSpan.FromHours(5);

            // Act
            var result = DurationParser.ToDurationString(timeSpan);

            // Assert
            result.Should().Be("+5h");
        }

        [TestMethod]
        public void ToDurationString_ShouldConvertOnlyMinutes()
        {
            // Arrange
            var timeSpan = TimeSpan.FromMinutes(45);

            // Act
            var result = DurationParser.ToDurationString(timeSpan);

            // Assert
            result.Should().Be("+45m");
        }
    }
}
