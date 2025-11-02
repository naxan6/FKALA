using Microsoft.VisualStudio.TestTools.UnitTesting;
using FluentAssertions;
using FKala.Core.KalaQl.QueryParser;

namespace FKala.Unittests
{
    [TestClass]
    public class AI_QueryParser_VarParams_Tests
    {
        [TestMethod]
        public void Constructor_WithValidNameAndValue_ShouldInitializeProperties()
        {
            // Arrange & Act
            var varParams = new VarParams("test_variable", "test_value");

            // Assert
            varParams.Name.Should().Be("test_variable");
            varParams.Value.Should().Be("test_value");
        }

        [TestMethod]
        public void Constructor_WithEmptyName_ShouldInitializeProperties()
        {
            // Arrange & Act
            var varParams = new VarParams("", "test_value");

            // Assert
            varParams.Name.Should().Be("");
            varParams.Value.Should().Be("test_value");
        }

        [TestMethod]
        public void Constructor_WithEmptyValue_ShouldInitializeProperties()
        {
            // Arrange & Act
            var varParams = new VarParams("test_variable", "");

            // Assert
            varParams.Name.Should().Be("test_variable");
            varParams.Value.Should().Be("");
        }

        [TestMethod]
        public void Constructor_WithBothEmptyStrings_ShouldInitializeProperties()
        {
            // Arrange & Act
            var varParams = new VarParams("", "");

            // Assert
            varParams.Name.Should().Be("");
            varParams.Value.Should().Be("");
        }

        [TestMethod]
        public void Constructor_WithSpecialCharacters_ShouldInitializeProperties()
        {
            // Arrange & Act
            var varParams = new VarParams("var_with_underscores", "value-with-dashes_and_special-chars_123!@#");

            // Assert
            varParams.Name.Should().Be("var_with_underscores");
            varParams.Value.Should().Be("value-with-dashes_and_special-chars_123!@#");
        }

        [TestMethod]
        public void Constructor_WithNullStrings_ShouldInitializeProperties()
        {
            // Arrange & Act
            var varParams = new VarParams(null!, null!);

            // Assert
            varParams.Name.Should().BeNull();
            varParams.Value.Should().BeNull();
        }

        [TestMethod]
        public void Constructor_WithMixedNullAndEmptyStrings_ShouldInitializeProperties()
        {
            // Arrange & Act
            var varParams1 = new VarParams("valid_name", null!);
            var varParams2 = new VarParams(null!, "valid_value");
            var varParams3 = new VarParams("", "valid_value");
            var varParams4 = new VarParams("valid_name", "");

            // Assert
            varParams1.Name.Should().Be("valid_name");
            varParams1.Value.Should().BeNull();

            varParams2.Name.Should().BeNull();
            varParams2.Value.Should().Be("valid_value");

            varParams3.Name.Should().Be("");
            varParams3.Value.Should().Be("valid_value");

            varParams4.Name.Should().Be("valid_name");
            varParams4.Value.Should().Be("");
        }

        [TestMethod]
        public void Constructor_WithWhitespaceStrings_ShouldInitializeProperties()
        {
            // Arrange & Act
            var varParams = new VarParams("  variable name  ", "  value with spaces  ");

            // Assert
            varParams.Name.Should().Be("  variable name  ");
            varParams.Value.Should().Be("  value with spaces  ");
        }
    }
}