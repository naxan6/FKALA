using System.Collections.Generic;
using FKala.Core.KalaQl;
using FKala.Core.KalaQl.QueryParser;
using FKala.Core.Model;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace FKala.Unittests
{
    [TestClass]
    public class AI_QueryParser_InterpolateParser_Tests
    {
        [TestMethod]
        public void InterpolateParser_Parse_Returns_Op_Interpolate_With_Correct_Properties()
        {
            // Arrange
            var parser = new InterpolateParser();
            var line = "InPo MyOp: InputDS forwards 5.5";
            var fields = new List<string>
            {
                "InPo",
                "MyOp",
                "InputDS",
                "forwards",
                "5.5"
            };

            // Act
            var op = parser.Parse(line, fields);

            // Assert
            Assert.IsInstanceOfType(op, typeof(Op_Interpolate));
            var interpolate = (Op_Interpolate)op;
            Assert.AreEqual("MyOp", interpolate.Name);
            Assert.AreEqual("InputDS", interpolate.InputDataSetName);
            Assert.AreEqual(InterpolationMode.forwards, interpolate.Mode);
            Assert.AreEqual(5.5m, interpolate.ConstantValue);
        }

        [TestMethod]
        public void InterpolateParser_Parse_With_Null_ConstantValue_Returns_Op_Interpolate_With_Null()
        {
            // Arrange
            var parser = new InterpolateParser();
            var line = "InPo MyOp: InputDS forwards";
            var fields = new List<string>
            {
                "InPo",
                "MyOp",
                "InputDS",
                "forwards"
            };

            // Act
            var op = parser.Parse(line, fields);

            // Assert
            Assert.IsInstanceOfType(op, typeof(Op_Interpolate));
            var interpolate = (Op_Interpolate)op;
            Assert.AreEqual("MyOp", interpolate.Name);
            Assert.AreEqual("InputDS", interpolate.InputDataSetName);
            Assert.AreEqual(InterpolationMode.forwards, interpolate.Mode);
            Assert.IsNull(interpolate.ConstantValue);
        }
    }
}