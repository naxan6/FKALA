using FKala.Core.KalaQl;
using FKala.Core.Interfaces;
using FKala.Core.Model;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using System;
using System.Collections.Generic;
using System.Linq;

namespace FKala.Unittests
{
    [TestClass]
    public class AI_Op_Insert_Tests
    {
        [TestCategory("AI")]
        [TestMethod]
        public void Op_Insert_Constructor_InitializesPropertiesCorrectly()
        {
            // Arrange
            string line = "test line";
            string name = "testName";
            string inputDataSet = "testInput";
            string targetMeasure = "testMeasure";

            // Act
            var opInsert = new Op_Insert(line, name, inputDataSet, targetMeasure);

            // Assert
            Assert.AreEqual(name, opInsert.Name);
            Assert.AreEqual(inputDataSet, opInsert.InputDataSetName);
            Assert.AreEqual(targetMeasure, opInsert.TargetMeasure);
        }

        [TestCategory("AI")]
        [TestMethod]
        public void Op_Insert_CanExecute_ReturnsTrue_WhenInputExists()
        {
            // Arrange
            var mockDataLayer = new Mock<IDataLayer>();
            var kalaQuery = new KalaQuery();
            var context = new KalaQlContext(kalaQuery, mockDataLayer.Object);
            
            var inputData = new ResultPromise
            {
                Name = "existingInput",
                ResultsetFactory = () => new List<DataPoint>()
            };
            context.IntermediateDatasources.Add(inputData);
            
            var opInsert = new Op_Insert(null, "test", "existingInput", "testMeasure");

            // Act
            bool canExecute = opInsert.CanExecute(context);

            // Assert
            Assert.IsTrue(canExecute);
        }

        [TestCategory("AI")]
        [TestMethod]
        public void Op_Insert_CanExecute_ReturnsFalse_WhenInputDoesNotExist()
        {
            // Arrange
            var mockDataLayer = new Mock<IDataLayer>();
            var kalaQuery = new KalaQuery();
            var context = new KalaQlContext(kalaQuery, mockDataLayer.Object);
            var opInsert = new Op_Insert(null, "test", "nonExistingInput", "testMeasure");

            // Act
            bool canExecute = opInsert.CanExecute(context);

            // Assert
            Assert.IsFalse(canExecute);
        }

        [TestCategory("AI")]
        [TestMethod]
        public void Op_Insert_GetInputNames_ReturnsCorrectInput()
        {
            // Arrange
            var opInsert = new Op_Insert(null, "test", "testInput", "testMeasure");

            // Act
            var inputNames = opInsert.GetInputNames();

            // Assert
            CollectionAssert.AreEqual(new List<string> { "testInput" }, inputNames);
        }

        [TestCategory("AI")]
        [TestMethod]
        public void Op_Insert_Clone_ReturnsNewInstanceWithCopiedProperties()
        {
            // Arrange
            var original = new Op_Insert("test line", "testName", "testInput", "testMeasure");

            // Act
            var cloned = original.Clone() as Op_Insert;

            // Assert
            Assert.AreNotSame(original, cloned);
            Assert.AreEqual(original.Name, cloned.Name);
            Assert.AreEqual(original.InputDataSetName, cloned.InputDataSetName);
            Assert.AreEqual(original.TargetMeasure, cloned.TargetMeasure);
        }

        [TestCategory("AI")]
        [TestMethod]
        public void Op_Insert_ToLine_ReturnsCorrectFormat()
        {
            // Arrange
            var opInsert = new Op_Insert(null, "testName", "testInput", "testMeasure");

            // Act
            string result = opInsert.ToLine();

            // Assert
            Assert.AreEqual("Insert testName: testInput testMeasure", result);
        }
    }
}