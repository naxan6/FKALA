using FKala.Core.DataLayer.Infrastructure;
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
    public class AI_Op_MatView_Tests
    {
        [TestMethod]
        public void Op_MatView_Constructor_InitializesCorrectly()
        {
            // Arrange
            string name = "testMatView";
            string inputDataSet = "inputDataSet";
            string viewName = "viewName";

            // Act
            var opMatView = new Op_MatView("line", name, inputDataSet, viewName);

            // Assert
            Assert.AreEqual(name, opMatView.Name);
            Assert.AreEqual(inputDataSet, opMatView.InputDataSetName);
            Assert.AreEqual(viewName, opMatView.ViewName);
        }

        [TestMethod]
        public void Op_MatView_ToLine_ReturnsCorrectFormat()
        {
            // Arrange
            var opMatView = new Op_MatView("line", "result", "inputDataSet", "viewName");

            // Act
            var line = opMatView.ToLine();

            // Assert
            Assert.AreEqual("MatView result: inputDataSet viewName", line);
        }

        [TestMethod]
        public void Op_MatView_Clone_ReturnsNewInstance()
        {
            // Arrange
            var opMatView = new Op_MatView("line", "test", "inputDataSet", "viewName");

            // Act
            var cloned = opMatView.Clone() as Op_MatView;

            // Assert
            Assert.IsNotNull(cloned);
            Assert.AreEqual(opMatView.Name, cloned.Name);
            Assert.AreEqual(opMatView.InputDataSetName, cloned.InputDataSetName);
            Assert.AreEqual(opMatView.ViewName, cloned.ViewName);
            Assert.AreNotSame(opMatView, cloned);
        }

        [TestMethod]
        public void Op_MatView_CanExecute_ShouldReturnTrue_WhenInputExists()
        {
            // Arrange
            var kalaQuery = new KalaQuery();
            var tempDir = Path.Combine(Path.GetTempPath(), "FKalaTestData");
            var dataLayer = new DataLayer_Readable_Caching_V1(tempDir);
            var context = new KalaQlContext(kalaQuery, dataLayer);
            var inputDataSource = new ResultPromise
            {
                Name = "inputDataSet",
                ResultsetFactory = () => new List<DataPoint>
                {
                    new DataPoint { Value = 10 }
                }
            };
            context.IntermediateDatasources.Add(inputDataSource);

            var opMatView = new Op_MatView("line", "test", "inputDataSet", "viewName");

            // Act
            var canExecute = opMatView.CanExecute(context);

            // Assert
            Assert.IsTrue(canExecute);
        }

        [TestMethod]
        public void Op_MatView_CanExecute_ShouldReturnFalse_WhenInputMissing()
        {
            // Arrange
            var kalaQuery = new KalaQuery();
            var tempDir = Path.Combine(Path.GetTempPath(), "FKalaTestData");
            var dataLayer = new DataLayer_Readable_Caching_V1(tempDir);
            var context = new KalaQlContext(kalaQuery, dataLayer);
            var opMatView = new Op_MatView("line", "test", "inputDataSet", "viewName");

            // Act
            var canExecute = opMatView.CanExecute(context);

            // Assert
            Assert.IsFalse(canExecute);
        }

        [TestMethod]
        public void Op_MatView_GetInputNames_ShouldReturnInputDataSetName()
        {
            // Arrange
            var opMatView = new Op_MatView("line", "test", "inputDataSet", "viewName");

            // Act
            var inputNames = opMatView.GetInputNames();

            // Assert
            Assert.AreEqual(1, inputNames.Count);
            Assert.AreEqual("inputDataSet", inputNames[0]);
        }

        [TestMethod]
        public void Op_MatView_NameProperty_IsCorrectlySet()
        {
            // Arrange & Act
            var opMatView = new Op_MatView("line", "testName", "input", "view");

            // Assert
            Assert.AreEqual("testName", opMatView.Name);
        }

        [TestMethod]
        public void Op_MatView_InputDataSetNameProperty_IsCorrectlySet()
        {
            // Arrange & Act
            var opMatView = new Op_MatView("line", "name", "inputDataSet", "view");

            // Assert
            Assert.AreEqual("inputDataSet", opMatView.InputDataSetName);
        }

        [TestMethod]
        public void Op_MatView_ViewNameProperty_IsCorrectlySet()
        {
            // Arrange & Act
            var opMatView = new Op_MatView("line", "name", "input", "viewName");

            // Assert
            Assert.AreEqual("viewName", opMatView.ViewName);
        }
		
		[TestMethod]
        public void Op_MatView_Execute_ShouldAddResultPromiseToContext()
        {
            // Arrange
            var kalaQuery = new KalaQuery();
            var tempDir = Path.Combine(Path.GetTempPath(), "FKalaTestData");
            var dataLayer = new DataLayer_Readable_Caching_V1(tempDir);
            var context = new KalaQlContext(kalaQuery, dataLayer);
            
            // Create input datasource
            var inputDataSource = new ResultPromise
            {
                Name = "inputDataSet",
                ResultsetFactory = () => new List<DataPoint>
                {
                    new DataPoint { Value = 10 }
                }
            };
            context.IntermediateDatasources.Add(inputDataSource);

            var opMatView = new Op_MatView("line", "resultName", "inputDataSet", "viewName");
			
			// Act
            opMatView.Execute(context);
			
			// Assert - Check that the result was added to IntermediateDatasources
			Assert.IsTrue(context.IntermediateDatasources.Any(ds => ds.Name == "resultName"));
        }
    }
}
