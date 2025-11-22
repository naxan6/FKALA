using FKala.Core.KalaQl;
using FKala.Core.Interfaces;
using FKala.Core.Model;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using System;
using System.Collections.Generic;

namespace FKala.Unittests
{
    [TestClass]
    public class AI_Op_Load_Tests
    {
        [TestCategory("AI")]
        [TestMethod]
        public void Op_Load_Constructor_InitializesPropertiesCorrectly()
        {
            // Arrange
            string line = "test line";
            string name = "testName";
            string measurement = "testMeasurement";
            DateTime startTime = DateTime.Parse("2024-01-01T00:00:00Z");
            DateTime endTime = DateTime.Parse("2024-01-01T01:00:00Z");
            CacheResolution cacheResolution = new CacheResolution { Resolution = Resolution.Minutely };

            // Act
            var opLoad = new Op_Load(line, name, measurement, startTime, endTime, cacheResolution);

            // Assert
            Assert.AreEqual(line, opLoad.Line);
            Assert.AreEqual(name, opLoad.Name);
            Assert.AreEqual(measurement, opLoad.Measurement);
            Assert.AreEqual(startTime, opLoad.StartTime);
            Assert.AreEqual(endTime, opLoad.EndTime);
            Assert.AreEqual(cacheResolution, opLoad.CacheResolution);
        }

        [TestCategory("AI")]
        [TestMethod]
        public void Op_Load_CanExecute_ReturnsTrue()
        {
            // Arrange
            var opLoad = new Op_Load("", "", "", new DateTime(), new DateTime(), CacheResolutionPredefined.NoCache, false);

            // Act & Assert
            Assert.IsTrue(opLoad.CanExecute(null!));
        }

        [TestCategory("AI")]
        [TestMethod]
        public void Op_Load_Clone_ReturnsNewInstanceWithCopiedProperties()
        {
            // Arrange
            string line = "test line";
            string name = "testName";
            string measurement = "testMeasurement";
            DateTime startTime = DateTime.Parse("2024-01-01T00:00:00Z");
            DateTime endTime = DateTime.Parse("2024-01-01T01:00:00Z");
            CacheResolution cacheResolution = new CacheResolution { Resolution = Resolution.Minutely };
            var opLoad = new Op_Load(line, name, measurement, startTime, endTime, cacheResolution);

            // Act
            var clonedOp = (Op_Load)opLoad.Clone();

            // Assert
            Assert.AreNotSame(opLoad, clonedOp);
            Assert.AreEqual(name, clonedOp.Name);
            Assert.AreEqual(measurement, clonedOp.Measurement);
            Assert.AreEqual(startTime, clonedOp.StartTime);
            Assert.AreEqual(endTime, clonedOp.EndTime);
            Assert.AreEqual(cacheResolution.Resolution, clonedOp.CacheResolution.Resolution);
        }

        [TestCategory("AI")]
        [TestMethod]
        public void Op_Load_ToLine_ReturnsCorrectFormat()
        {
            // Arrange
            string name = "testName";
            string measurement = "testMeasurement";
            DateTime startTime = DateTime.Parse("2024-01-01T00:00:00Z");
            DateTime endTime = DateTime.Parse("2024-01-01T01:00:00Z");
            CacheResolution cacheResolution = new CacheResolution { Resolution = Resolution.Minutely };
            var opLoad = new Op_Load("line", name, measurement, startTime, endTime, cacheResolution);

            // Act
            string result = opLoad.ToLine();

            // Assert
            Assert.IsTrue(result.Contains(name));
            Assert.IsTrue(result.Contains(measurement));
            Assert.IsTrue(result.Contains(startTime.ToString("yyyy-MM-ddTHH:mm:ss")));
            Assert.IsTrue(result.Contains(endTime.ToString("yyyy-MM-ddTHH:mm:ss")));
        }

        [TestCategory("AI")]
        [TestMethod]
        public void Op_Load_ToLine_NewestOnly_ReturnsCorrectFormat()
        {
            // Arrange
            string name = "testName";
            var opLoad = new Op_Load("line", name, "testMeasurement", DateTime.MinValue, DateTime.MaxValue, CacheResolutionPredefined.NoCache, true);

            // Act
            string result = opLoad.ToLine();

            // Assert
            Assert.IsTrue(result.Contains(name));
            Assert.IsTrue(result.Contains("NewestOnly"));
        }
    }
}