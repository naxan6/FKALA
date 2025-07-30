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
    public class AI_Op_Interpolate_Tests
    {
        [TestMethod]
        public void Op_Interpolate_Constructor_InitializesCorrectly()
        {
            // Arrange
            string name = "testInterpolate";
            string inputDataSet = "input1";
            InterpolationMode mode = InterpolationMode.forwards;
            decimal? constantValue = 100;

            // Act
            var opInterpolate = new Op_Interpolate("line", name, inputDataSet, mode, constantValue);

            // Assert
            Assert.AreEqual(name, opInterpolate.Name);
            Assert.AreEqual(inputDataSet, opInterpolate.InputDataSetName);
            Assert.AreEqual(mode, opInterpolate.Mode);
            Assert.AreEqual(constantValue, opInterpolate.ConstantValue);
        }

        [TestMethod]
        public void Op_Interpolate_ToLine_ReturnsCorrectFormat()
        {
            // Arrange
            var opInterpolate = new Op_Interpolate("line", "result", "input1", InterpolationMode.forwards, 100);

            // Act
            var line = opInterpolate.ToLine();

            // Assert
            Assert.AreEqual("InPo result: input1 forwards 100", line);
        }

        [TestMethod]
        public void Op_Interpolate_Clone_ReturnsNewInstance()
        {
            // Arrange
            var opInterpolate = new Op_Interpolate("line", "test", "input1", InterpolationMode.backwards, 50);

            // Act
            var cloned = opInterpolate.Clone() as Op_Interpolate;

            // Assert
            Assert.IsNotNull(cloned);
            Assert.AreEqual(opInterpolate.Name, cloned.Name);
            Assert.AreEqual(opInterpolate.InputDataSetName, cloned.InputDataSetName);
            Assert.AreEqual(opInterpolate.Mode, cloned.Mode);
            Assert.AreEqual(opInterpolate.ConstantValue, cloned.ConstantValue);
            Assert.AreNotSame(opInterpolate, cloned);
        }

        [TestMethod]
        public void Op_Interpolate_GetInputNames_ReturnsCorrectInputName()
        {
            // Arrange
            var opInterpolate = new Op_Interpolate("line", "test", "input1", InterpolationMode.constant, 0);

            // Act
            var inputNames = opInterpolate.GetInputNames();

            // Assert
            Assert.AreEqual(1, inputNames.Count);
            Assert.AreEqual("input1", inputNames[0]);
        }

        [TestMethod]
        public void Op_Interpolate_CanExecute_ShouldReturnTrue_WhenInputAvailable()
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

            var opInterpolate = new Op_Interpolate("line", "result", "input1", InterpolationMode.forwards, 0);

            // Act
            var canExecute = opInterpolate.CanExecute(context);

            // Assert
            Assert.IsTrue(canExecute);
        }

        [TestMethod]
        public void Op_Interpolate_CanExecute_ShouldReturnFalse_WhenInputMissing()
        {
            // Arrange
            var kalaQuery = new KalaQuery();
            var tempDir = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "FKalaTestData");
            var dataLayer = new DataLayer_Readable_Caching_V1(tempDir);
            var context = new KalaQlContext(kalaQuery, dataLayer);
            var opInterpolate = new Op_Interpolate("line", "result", "missingInput", InterpolationMode.forwards, 0);

            // Act
            var canExecute = opInterpolate.CanExecute(context);

            // Assert
            Assert.IsFalse(canExecute);
        }

        [TestMethod]
        public void Op_Interpolate_Execute_WithValidContext_ShouldExecuteSuccessfully()
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
                    new DataPoint { Value = null },
                    new DataPoint { Value = 20 }
                }
            };
            context.IntermediateDatasources.Add(inputDataSource);

            var opInterpolate = new Op_Interpolate("line", "result", "input1", InterpolationMode.forwards, 0);

            // Act
            opInterpolate.Execute(context);

            // Assert
            Assert.IsTrue(opInterpolate.hasExecuted);
            Assert.IsTrue(context.IntermediateDatasources.Any(ds => ds.Name == "result"));
        }

        [TestMethod]
        public void Op_Interpolate_Execute_ForwardsMode_ShouldInterpolateNullValues()
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
                    new DataPoint { Value = null },
                    new DataPoint { Value = 20 }
                }
            };
            context.IntermediateDatasources.Add(inputDataSource);

            var opInterpolate = new Op_Interpolate("line", "result", "input1", InterpolationMode.forwards, 0);

            // Act
            opInterpolate.Execute(context);
            var resultDataSource = context.IntermediateDatasources.First(ds => ds.Name == "result");
            var results = resultDataSource.ResultsetFactory().ToList();

            // Assert
            Assert.AreEqual(3, results.Count);
            Assert.AreEqual(10, results[0].Value);
            Assert.AreEqual(10, results[1].Value); // Should be interpolated
            Assert.AreEqual(20, results[2].Value);
        }

        [TestMethod]
        public void Op_Interpolate_Execute_BackwardsMode_ShouldInterpolateNullValues()
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
                    new DataPoint { Value = null },
                    new DataPoint { Value = 20 }
                }
            };
            context.IntermediateDatasources.Add(inputDataSource);

            var opInterpolate = new Op_Interpolate("line", "result", "input1", InterpolationMode.backwards, 0);

            // Act
            opInterpolate.Execute(context);
            var resultDataSource = context.IntermediateDatasources.First(ds => ds.Name == "result");
            var results = resultDataSource.ResultsetFactory().ToList();

            // Assert
            Assert.AreEqual(3, results.Count);
            Assert.AreEqual(10, results[0].Value);
            Assert.AreEqual(20, results[1].Value); // Should be interpolated
            Assert.AreEqual(20, results[2].Value);
        }

        [TestMethod]
        public void Op_Interpolate_Execute_ConstantMode_ShouldFillNullValues()
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
                    new DataPoint { Value = null },
                    new DataPoint { Value = 20 }
                }
            };
            context.IntermediateDatasources.Add(inputDataSource);

            var opInterpolate = new Op_Interpolate("line", "result", "input1", InterpolationMode.constant, 50);

            // Act
            opInterpolate.Execute(context);
            var resultDataSource = context.IntermediateDatasources.First(ds => ds.Name == "result");
            var results = resultDataSource.ResultsetFactory().ToList();

            // Assert
            Assert.AreEqual(3, results.Count);
            Assert.AreEqual(10, results[0].Value);
            Assert.AreEqual(50, results[1].Value); // Should be constant value
            Assert.AreEqual(20, results[2].Value);
        }
    }
}