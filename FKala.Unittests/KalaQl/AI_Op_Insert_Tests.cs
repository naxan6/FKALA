using FKala.Core.KalaQl;
using FKala.Core.Model;
using FKala.Core;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;

namespace FKala.Unittests.KalaQl
{
    [TestClass]
    public class AI_Op_Insert_Tests
    {
        [TestMethod]
        public void Op_Insert_Constructor_InitializesCorrectly()
        {
            // Arrange
            string name = "testInsert";
            string inputDataSet = "input1";
            string targetMeasure = "targetMeasure";

            // Act
            var opInsert = new Op_Insert(null, name, inputDataSet, targetMeasure);

            // Assert
            Assert.AreEqual(name, opInsert.Name);
            Assert.AreEqual(inputDataSet, opInsert.InputDataSetName);
            Assert.AreEqual(targetMeasure, opInsert.TargetMeasure);
        }

        [TestMethod]
        public void Op_Insert_ToLine_ReturnsCorrectFormat()
        {
            // Arrange
            var opInsert = new Op_Insert(null, "result", "input1", "targetMeasure");

            // Act
            var line = opInsert.ToLine();

            // Assert
            Assert.AreEqual("Insert result: input1 targetMeasure", line);
        }

        [TestMethod]
        public void Op_Insert_Clone_ReturnsNewInstance()
        {
            // Arrange
            var opInsert = new Op_Insert(null, "test", "input1", "targetMeasure");

            // Act
            var cloned = opInsert.Clone() as Op_Insert;

            // Assert
            Assert.IsNotNull(cloned);
            Assert.AreEqual(opInsert.Name, cloned.Name);
            Assert.AreEqual(opInsert.InputDataSetName, cloned.InputDataSetName);
            Assert.AreEqual(opInsert.TargetMeasure, cloned.TargetMeasure);
            Assert.AreNotSame(opInsert, cloned);
        }

        [TestMethod]
        public void Op_Insert_GetInputNames_ReturnsCorrectInputName()
        {
            // Arrange
            var opInsert = new Op_Insert(null, "test", "input1", "targetMeasure");

            // Act
            var inputNames = opInsert.GetInputNames();

            // Assert
            Assert.AreEqual(1, inputNames.Count);
            Assert.AreEqual("input1", inputNames[0]);
        }

        [TestMethod]
        public void Op_Insert_CanExecute_ShouldReturnTrue_WhenInputAvailable()
        {
            // Arrange
            var kalaQuery = new KalaQuery();
            var tempDir = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "FKalaTestData");
            var dataLayer = new DataLayer_Readable_Caching_V1(tempDir);
            var context = new KalaQlContext(kalaQuery, dataLayer);
            var inputDataSource = new ResultPromise
            {
                Name = "input1",
                ResultsetFactory = () => new List<DataPoint>
                {
                    new DataPoint { Value = 10 }
                }
            };
            context.IntermediateDatasources.Add(inputDataSource);

            var opInsert = new Op_Insert(null, "result", "input1", "targetMeasure");

            // Act
            var canExecute = opInsert.CanExecute(context);

            // Assert
            Assert.IsTrue(canExecute);
        }

        [TestMethod]
        public void Op_Insert_CanExecute_ShouldReturnFalse_WhenInputMissing()
        {
            // Arrange
            var kalaQuery = new KalaQuery();
            var tempDir = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "FKalaTestData");
            var dataLayer = new DataLayer_Readable_Caching_V1(tempDir);
            var context = new KalaQlContext(kalaQuery, dataLayer);
            var opInsert = new Op_Insert(null, "result", "missingInput", "targetMeasure");

            // Act
            var canExecute = opInsert.CanExecute(context);

            // Assert
            Assert.IsFalse(canExecute);
        }

        [TestMethod]
        public void Op_Insert_Execute_WithValidContext_ShouldExecuteSuccessfully()
        {
            // Arrange
            var kalaQuery = new KalaQuery();
            var tempDir = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "FKalaTestData");
            var dataLayer = new DataLayer_Readable_Caching_V1(tempDir);
            var context = new KalaQlContext(kalaQuery, dataLayer);
            var inputDataSource = new ResultPromise
            {
                Name = "input1",
                ResultsetFactory = () => new List<DataPoint>
                {
                    new DataPoint { Value = 10 },
                    new DataPoint { Value = 20 }
                }
            };
            context.IntermediateDatasources.Add(inputDataSource);

            var opInsert = new Op_Insert(null, "result", "input1", "targetMeasure");

            // Act
            opInsert.Execute(context);

            // Assert
            Assert.IsTrue(opInsert.hasExecuted);
            Assert.IsTrue(context.IntermediateDatasources.Any(ds => ds.Name == "result"));
        }

        [TestMethod]
        public void Op_Insert_Execute_ShouldInsertDataPoints()
        {
            // Arrange
            var kalaQuery = new KalaQuery();
            var tempDir = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "FKalaTestData");
            var dataLayer = new DataLayer_Readable_Caching_V1(tempDir);
            var context = new KalaQlContext(kalaQuery, dataLayer);
            var inputDataSource = new ResultPromise
            {
                Name = "input1",
                ResultsetFactory = () => new List<DataPoint>
                {
                    new DataPoint { Value = 10 },
                    new DataPoint { Value = 20 }
                }
            };
            context.IntermediateDatasources.Add(inputDataSource);

            var opInsert = new Op_Insert(null, "result", "input1", "targetMeasure");

            // Act
            opInsert.Execute(context);
            var resultDataSource = context.IntermediateDatasources.First(ds => ds.Name == "result");
            var results = resultDataSource.ResultsetFactory().ToList();

            // Assert
            Assert.AreEqual(1, results.Count);
            Assert.AreEqual($"Inserted 2 datapoints into {opInsert.TargetMeasure}", results[0].ValueText);
        }
    }
}