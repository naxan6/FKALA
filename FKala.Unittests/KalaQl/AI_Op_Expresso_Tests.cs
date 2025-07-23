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
    public class AI_Op_Expresso_Tests
    {
        [TestMethod]
        public void Op_Expresso_Constructor_InitializesCorrectly()
        {
            // Arrange
            string name = "testExpression";
            string expresso = "input1.Value + input2.Value";

            // Act
            var opExpresso = new Op_Expresso(null, name, expresso);

            // Assert
            Assert.AreEqual(name, opExpresso.Name);
            Assert.AreEqual(expresso, opExpresso.Expresso);
            Assert.IsNotNull(opExpresso.UnknownIdInfo);
        }

        [TestMethod]
        public void Op_Expresso_ToLine_ReturnsCorrectFormat()
        {
            // Arrange
            var opExpresso = new Op_Expresso(null, "result", "input1.Value * 2");

            // Act
            var line = opExpresso.ToLine();

            // Assert
            Assert.AreEqual("Expr result: \"input1.Value * 2\"", line);
        }

        [TestMethod]
        public void Op_Expresso_Clone_ReturnsNewInstance()
        {
            // Arrange
            var opExpresso = new Op_Expresso(null, "test", "input.Value");

            // Act
            var cloned = opExpresso.Clone() as Op_Expresso;

            // Assert
            Assert.IsNotNull(cloned);
            Assert.AreEqual(opExpresso.Name, cloned.Name);
            Assert.AreEqual(opExpresso.Expresso, cloned.Expresso);
            Assert.AreNotSame(opExpresso, cloned);
        }

        [TestMethod]
        public void Op_Expresso_NameProperty_IsCorrectlySet()
        {
            // Arrange & Act
            var opExpresso = new Op_Expresso(null, "testName", "input.Value");

            // Assert
            Assert.AreEqual("testName", opExpresso.Name);
        }

        [TestMethod]
        public void Op_Expresso_ExpressoProperty_IsCorrectlySet()
        {
            // Arrange & Act
            string expression = "input.Value + 100";
            var opExpresso = new Op_Expresso(null, "test", expression);

            // Assert
            Assert.AreEqual(expression, opExpresso.Expresso);
        }

        [TestMethod]
        public void Op_Expresso_Execute_WithValidContext_ShouldExecuteSuccessfully()
        {
            // Arrange
            var kalaQuery = new KalaQuery();
            var tempDir = Path.Combine(Path.GetTempPath(), "FKalaTestData");
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

            var opExpresso = new Op_Expresso(null, "result", "input1.Value * 2");

            // Act
            opExpresso.Execute(context);

            // Assert
            Assert.IsTrue(opExpresso.hasExecuted);
            Assert.IsTrue(context.IntermediateDatasources.Any(ds => ds.Name == "result"));
        }

        [TestMethod]
        public void Op_Expresso_ExecuteInternal_ShouldReturnCorrectResults()
        {
            // Arrange
            var kalaQuery = new KalaQuery();
            var tempDir = Path.Combine(Path.GetTempPath(), "FKalaTestData");
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

            var opExpresso = new Op_Expresso(null, "result", "input1.Value * 2");
            var dataSources = new List<ResultPromise> { inputDataSource };

            // Act
            var results = opExpresso.ExecuteInternal(context, dataSources).ToList();

            // Assert
            Assert.AreEqual(1, results.Count);
            Assert.AreEqual(20, results[0].Value);
        }

        [TestMethod]
        public void Op_Expresso_ExecuteInternal_WithSkip_ShouldNotReturnResults()
        {
            // Arrange
            var kalaQuery = new KalaQuery();
            var tempDir = Path.Combine(Path.GetTempPath(), "FKalaTestData");
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

            var opExpresso = new Op_Expresso(null, "result", "skip");

            // Act
            var results = opExpresso.ExecuteInternal(context, new List<ResultPromise> { inputDataSource }).ToList();

            // Assert
            Assert.AreEqual(0, results.Count);
        }

        [TestMethod]
        public void Op_Expresso_CanExecute_ShouldReturnTrue_WhenAllInputsAvailable()
        {
            // Arrange
            var kalaQuery = new KalaQuery();
            var tempDir = Path.Combine(Path.GetTempPath(), "FKalaTestData");
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

            var opExpresso = new Op_Expresso(null, "result", "input1.Value * 2");

            // Act
            var canExecute = opExpresso.CanExecute(context);

            // Assert
            Assert.IsTrue(canExecute);
        }

        [TestMethod]
        public void Op_Expresso_CanExecute_ShouldReturnFalse_WhenInputsMissing()
        {
            // Arrange
            var kalaQuery = new KalaQuery();
            var tempDir = Path.Combine(Path.GetTempPath(), "FKalaTestData");
            var dataLayer = new DataLayer_Readable_Caching_V1(tempDir);
            var context = new KalaQlContext(kalaQuery, dataLayer);
            var opExpresso = new Op_Expresso(null, "result", "input1.Value * 2");

            // Act
            var canExecute = opExpresso.CanExecute(context);

            // Assert
            Assert.IsFalse(canExecute);
        }
    }
}