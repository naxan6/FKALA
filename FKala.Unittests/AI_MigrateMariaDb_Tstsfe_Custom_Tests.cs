using Microsoft.VisualStudio.TestTools.UnitTesting;
using FluentAssertions;
using Moq;
using FKala.Core.Interfaces;
using FKala.Migrate.MariaDb;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace FKala.Unittests
{
    [TestClass]
    public class AI_MigrateMariaDb_Tstsfe_Custom_Tests
    {
        [TestMethod]
        public void Constructor_ShouldInitializePropertiesCorrectly()
        {
            // Arrange
            string connectionString = "Server=test;Database=test;User=test;Password=test;";
            var mockDataLayer = new Mock<IDataLayer>();

            // Act
            var result = new MigrateMariaDb_Tstsfe_Custom(connectionString, mockDataLayer.Object);

            // Assert
            result.Should().NotBeNull();
            result.ConnectionString.Should().Be(connectionString);
            result.DataLayer.Should().Be(mockDataLayer.Object);
        }

        [TestMethod]
        public void Constructor_ShouldHandleEmptyConnectionString()
        {
            // Arrange
            string connectionString = "";
            var mockDataLayer = new Mock<IDataLayer>();

            // Act
            var result = new MigrateMariaDb_Tstsfe_Custom(connectionString, mockDataLayer.Object);

            // Assert
            result.Should().NotBeNull();
            result.ConnectionString.Should().Be("");
            result.DataLayer.Should().Be(mockDataLayer.Object);
        }

    }
}