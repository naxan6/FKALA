using Microsoft.VisualStudio.TestTools.UnitTesting;
using FKala.Core.KalaQl;
using System.Collections.Generic;

namespace FKala.Unittests.KalaQl
{
    [TestClass]
    public class Op_AlignTimezone_Tests
    {
        [TestMethod]
        public void ToString_Should_Return_Correct_String()
        {
            // Arrange
            var op = new Op_AlignTimezone("testLine", "Europe/Berlin");
            
            // Act
            string result = op.ToString();
            
            // Assert
            Assert.AreEqual("Op_AlignTimezone: Europe/Berlin", result);
        }
        
        [TestMethod]
        public void GetInputNames_Should_Return_Empty_List()
        {
            // Arrange
            var op = new Op_AlignTimezone("testLine", "Europe/Berlin");
            
            // Act
            List<string> result = op.GetInputNames();
            
            // Assert
            Assert.IsNotNull(result);
            Assert.AreEqual(0, result.Count);
        }
        
        [TestMethod]
        public void Clone_Should_Create_New_Instance_With_Same_Properties()
        {
            // Arrange
            var originalOp = new Op_AlignTimezone("testLine", "Europe/Berlin");
            
            // Act
            var clonedOp = originalOp.Clone() as Op_AlignTimezone;
            
            // Assert
            Assert.IsNotNull(clonedOp);
            Assert.AreEqual("testLine", clonedOp.Line);
            Assert.AreEqual("Europe/Berlin", clonedOp.TzId);
            Assert.AreNotSame(originalOp, clonedOp);
        }
        
        [TestMethod]
        public void ToLine_Should_Return_Correct_Line_Format()
        {
            // Arrange
            var op = new Op_AlignTimezone("testLine", "Europe/Berlin");
            
            // Act
            string result = op.ToLine();
            
            // Assert
            Assert.AreEqual("AlTz Europe/Berlin", result);
        }
    }
}
