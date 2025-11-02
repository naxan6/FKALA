using Microsoft.VisualStudio.TestTools.UnitTesting;
using FluentAssertions;
using FKala.Core.KalaQl.QueryParser;
using System;

namespace FKala.Unittests
{
    [TestClass]
    public class AI_QueryParser_ExpressoParams_Tests
    {
        [TestMethod]
        public void Constructor_ShouldInitializeAllPropertiesCorrectly()
        {
            // Arrange
            string name = "testExpresso";
            string expression = "temperature + 10";

            // Act
            var result = new ExpressoParams(name, expression);

            // Assert
            result.Should().NotBeNull();
            result.Name.Should().Be(name);
            result.Expression.Should().Be(expression);
        }

        [TestMethod]
        public void Constructor_ShouldHandleEmptyStrings()
        {
            // Arrange
            string name = "";
            string expression = "";

            // Act
            var result = new ExpressoParams(name, expression);

            // Assert
            result.Should().NotBeNull();
            result.Name.Should().Be("");
            result.Expression.Should().Be("");
        }

        [TestMethod]
        public void Constructor_ShouldHandleNullExpression()
        {
            // Arrange
            string name = "test";
            string expression = null!;

            // Act
            var result = new ExpressoParams(name, expression);

            // Assert
            result.Should().NotBeNull();
            result.Name.Should().Be("test");
            result.Expression.Should().BeNull();
        }

        [TestMethod]
        public void Constructor_ShouldHandleComplexExpressions()
        {
            // Arrange
            string name = "complexCalculation";
            string expression = "(temperature * 1.8) + 32";

            // Act
            var result = new ExpressoParams(name, expression);

            // Assert
            result.Should().NotBeNull();
            result.Name.Should().Be("complexCalculation");
            result.Expression.Should().Be("(temperature * 1.8) + 32");
        }

        [TestMethod]
        public void Constructor_ShouldHandleSpecialCharactersInExpression()
        {
            // Arrange
            string name = "specialChars";
            string expression = "value > 100 && value < 200";

            // Act
            var result = new ExpressoParams(name, expression);

            // Assert
            result.Should().NotBeNull();
            result.Name.Should().Be("specialChars");
            result.Expression.Should().Be("value > 100 && value < 200");
        }
    }
}