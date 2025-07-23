using FKala.Core.KalaQl;
using FKala.Core.Model;
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
    }
}