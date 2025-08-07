using Microsoft.VisualStudio.TestTools.UnitTesting;
using FluentAssertions;
using FKala.Core.KalaQl.QueryParser;
using FKala.Core.Model;

namespace FKala.Unittests
{
    [TestClass]
    public class AI_QueryParser_MgmtParams_Tests
    {
        [TestMethod]
        public void Constructor_WithValidActionAndParameters_ShouldInitializeProperties()
        {
            // Arrange & Act
            var mgmtParams = new MgmtParams(MgmtAction.LoadMeasures, "test_params");

            // Assert
            mgmtParams.Action.Should().Be(MgmtAction.LoadMeasures);
            mgmtParams.Parameters.Should().Be("test_params");
        }

        [TestMethod]
        public void Constructor_WithDefaultParameters_ShouldInitializeProperties()
        {
            // Arrange & Act
            var mgmtParams = new MgmtParams(MgmtAction.SortAllRaw);

            // Assert
            mgmtParams.Action.Should().Be(MgmtAction.SortAllRaw);
            mgmtParams.Parameters.Should().Be("");
        }

        [TestMethod]
        public void Constructor_WithEmptyParameters_ShouldInitializeProperties()
        {
            // Arrange & Act
            var mgmtParams = new MgmtParams(MgmtAction.ImportInflux, "");

            // Assert
            mgmtParams.Action.Should().Be(MgmtAction.ImportInflux);
            mgmtParams.Parameters.Should().Be("");
        }

        [TestMethod]
        public void Constructor_WithNullParameters_ShouldInitializeProperties()
        {
            // Arrange & Act
            var mgmtParams = new MgmtParams(MgmtAction.ImportMariaDbTstsfe, null!);

            // Assert
            mgmtParams.Action.Should().Be(MgmtAction.ImportMariaDbTstsfe);
            mgmtParams.Parameters.Should().BeNull();
        }

        [TestMethod]
        public void Constructor_WithSpecialCharactersInParameters_ShouldInitializeProperties()
        {
            // Arrange & Act
            var mgmtParams = new MgmtParams(MgmtAction.BenchmarkIo, "params_with_special_chars_123!@#");

            // Assert
            mgmtParams.Action.Should().Be(MgmtAction.BenchmarkIo);
            mgmtParams.Parameters.Should().Be("params_with_special_chars_123!@#");
        }
    }
}