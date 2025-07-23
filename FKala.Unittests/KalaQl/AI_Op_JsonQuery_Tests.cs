using FKala.Core.KalaQl;
using FKala.Core.Model;
using FKala.Core;
using FKala.Core.KalaQl.Windowing;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json.Linq;

namespace FKala.Unittests.KalaQl
{
    [TestClass]
    public class AI_Op_JsonQuery_Tests
    {
        [TestMethod]
        public void Op_JsonQuery_Constructor_InitializesCorrectly()
        {
            // Arrange
            string name = "testJsonQuery";
            string measurement = "testMeasurement";
            string fieldPath = "path/to/field";
            DateTime startTime = new DateTime(2023, 1, 1);
            DateTime endTime = new DateTime(2023, 1, 2);
            var cacheResolution = new CacheResolution
            {
                Resolution = Resolution.Full,
                AggregateFunction = AggregateFunction.None,
                ForceRebuild = false
            };
            bool newestOnly = false;

            // Act
            var opJsonQuery = new Op_JsonQuery(null, name, measurement, fieldPath, startTime, endTime, cacheResolution, newestOnly);

            // Assert
            Assert.AreEqual(name, opJsonQuery.Name);
            Assert.AreEqual(measurement, opJsonQuery.Measurement);
            Assert.AreEqual(fieldPath, opJsonQuery.FieldPath);
            Assert.AreEqual(startTime, opJsonQuery.StartTime);
            Assert.AreEqual(endTime, opJsonQuery.EndTime);
            Assert.AreEqual(cacheResolution, opJsonQuery.CacheResolution);
            Assert.AreEqual(newestOnly, opJsonQuery.NewestOnly);
        }

        [TestMethod]
        public void Op_JsonQuery_ToLine_ReturnsCorrectFormat()
        {
            // Arrange
            var cacheResolution = new CacheResolution
            {
                Resolution = Resolution.Full,
                AggregateFunction = AggregateFunction.None,
                ForceRebuild = false
            };
            var opJsonQuery = new Op_JsonQuery(null, "result", "measurement", "path/field", new DateTime(2023, 1, 1), new DateTime(2023, 1, 2), cacheResolution);

            // Act
            var line = opJsonQuery.ToLine();

            // Assert
            Assert.AreEqual("Loaj result: measurement 2023-01-01T00:00:00 2023-01-02T00:00:00 Full_None", line);
        }

        [TestMethod]
        public void Op_JsonQuery_Clone_ReturnsNewInstance()
        {
            // Arrange
            var cacheResolution = new CacheResolution
            {
                Resolution = Resolution.Full,
                AggregateFunction = AggregateFunction.None,
                ForceRebuild = false
            };
            var opJsonQuery = new Op_JsonQuery(null, "test", "measurement", "path/field", new DateTime(2023, 1, 1), new DateTime(2023, 1, 2), cacheResolution);

            // Act
            var cloned = opJsonQuery.Clone() as Op_JsonQuery;

            // Assert
            Assert.IsNotNull(cloned);
            Assert.AreEqual(opJsonQuery.Name, cloned.Name);
            Assert.AreEqual(opJsonQuery.Measurement, cloned.Measurement);
            Assert.AreEqual(opJsonQuery.FieldPath, cloned.FieldPath);
            Assert.AreEqual(opJsonQuery.StartTime, cloned.StartTime);
            Assert.AreEqual(opJsonQuery.EndTime, cloned.EndTime);
            Assert.AreEqual(opJsonQuery.CacheResolution, cloned.CacheResolution);
            Assert.AreEqual(opJsonQuery.NewestOnly, cloned.NewestOnly);
            Assert.AreNotSame(opJsonQuery, cloned);
        }

        [TestMethod]
        public void Op_JsonQuery_GetInputNames_ReturnsEmptyList()
        {
            // Arrange
            var cacheResolution = new CacheResolution
            {
                Resolution = Resolution.Full,
                AggregateFunction = AggregateFunction.None,
                ForceRebuild = false
            };
            var opJsonQuery = new Op_JsonQuery(null, "test", "measurement", "path/field", new DateTime(2023, 1, 1), new DateTime(2023, 1, 2), cacheResolution);

            // Act
            var inputNames = opJsonQuery.GetInputNames();

            // Assert
            Assert.IsNotNull(inputNames);
            Assert.AreEqual(0, inputNames.Count);
        }

        [TestMethod]
        public void Op_JsonQuery_CanExecute_AlwaysReturnsTrue()
        {
            // Arrange
            var cacheResolution = new CacheResolution
            {
                Resolution = Resolution.Full,
                AggregateFunction = AggregateFunction.None,
                ForceRebuild = false
            };
            var opJsonQuery = new Op_JsonQuery(null, "test", "measurement", "path/field", new DateTime(2023, 1, 1), new DateTime(2023, 1, 2), cacheResolution);
            var kalaQuery = new KalaQuery();
            var tempDir = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "FKalaTestData");
            var dataLayer = new DataLayer_Readable_Caching_V1(tempDir);
            var context = new KalaQlContext(kalaQuery, dataLayer);

            // Act
            var canExecute = opJsonQuery.CanExecute(context);

            // Assert
            Assert.IsTrue(canExecute);
        }

        [TestMethod]
        public void Op_JsonQuery_ReadJson_ExtractsValueFromJsonPath()
        {
            // Arrange
            var cacheResolution = new CacheResolution
            {
                Resolution = Resolution.Full,
                AggregateFunction = AggregateFunction.None,
                ForceRebuild = false
            };
            var opJsonQuery = new Op_JsonQuery(null, "test", "measurement", "path/field", new DateTime(2023, 1, 1), new DateTime(2023, 1, 2), cacheResolution);
            var dataPoints = new List<DataPoint>
            {
                new DataPoint
                {
                    ValueText = "{\"path\": {\"field\": 42}}"
                }
            };

            // Act
            var results = opJsonQuery.ReadJson(dataPoints, new[] {"path", "field"}).ToList();

            // Assert
            Assert.AreEqual(1, results.Count);
            Assert.AreEqual(42, results[0].Value);
        }

        [TestMethod]
        public void Op_JsonQuery_ReadJson_ExtractsArrayElement()
        {
            // Arrange
            var cacheResolution = new CacheResolution
            {
                Resolution = Resolution.Full,
                AggregateFunction = AggregateFunction.None,
                ForceRebuild = false
            };
            var opJsonQuery = new Op_JsonQuery(null, "test", "measurement", "path/field[0]", new DateTime(2023, 1, 1), new DateTime(2023, 1, 2), cacheResolution);
            var dataPoints = new List<DataPoint>
            {
                new DataPoint
                {
                    ValueText = "{\"path\": {\"field\": [1, 2, 3]}}"
                }
            };

            // Act
            var results = opJsonQuery.ReadJson(dataPoints, new[] {"path", "field[0]"}).ToList();

            // Assert
            Assert.AreEqual(1, results.Count);
            Assert.AreEqual("1", results[0].ValueText);
        }

        [TestMethod]
        public void Op_JsonQuery_ReadJson_HandlesNestedObjects()
        {
            // Arrange
            var cacheResolution = new CacheResolution
            {
                Resolution = Resolution.Full,
                AggregateFunction = AggregateFunction.None,
                ForceRebuild = false
            };
            var opJsonQuery = new Op_JsonQuery(null, "test", "measurement", "path/field/subfield", new DateTime(2023, 1, 1), new DateTime(2023, 1, 2), cacheResolution);
            var dataPoints = new List<DataPoint>
            {
                new DataPoint
                {
                    ValueText = "{\"path\": {\"field\": {\"subfield\": 99}}}"
                }
            };

            // Act
            var results = opJsonQuery.ReadJson(dataPoints, new[] {"path", "field", "subfield"}).ToList();

            // Assert
            Assert.AreEqual(1, results.Count);
            Assert.AreEqual(99, results[0].Value);
        }

        [TestMethod]
        public void Op_JsonQuery_ReadJson_HandlesStringValues()
        {
            // Arrange
            var cacheResolution = new CacheResolution
            {
                Resolution = Resolution.Full,
                AggregateFunction = AggregateFunction.None,
                ForceRebuild = false
            };
            var opJsonQuery = new Op_JsonQuery(null, "test", "measurement", "path/field", new DateTime(2023, 1, 1), new DateTime(2023, 1, 2), cacheResolution);
            var dataPoints = new List<DataPoint>
            {
                new DataPoint
                {
                    ValueText = "{\"path\": {\"field\": \"hello\"}}"
                }
            };

            // Act
            var results = opJsonQuery.ReadJson(dataPoints, new[] {"path", "field"}).ToList();

            // Assert
            Assert.AreEqual(1, results.Count);
            Assert.AreEqual("hello", results[0].ValueText);
        }
    }
}