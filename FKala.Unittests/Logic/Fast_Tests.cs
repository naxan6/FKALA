using System;
using FKala.Core.Logic;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace FKala.Unittests.Logic
{
    [TestClass]
    public class Fast_Tests
    {
        [TestMethod]
        public void IntParse_Should_Parse_Valid_Integer_String()
        {
            // Arrange
            var input = "123";
            ReadOnlySpan<char> span = input.AsSpan();
            
            // Act
            bool result = Fast.IntParse(span, out int parsedValue);
            
            // Assert
            Assert.IsTrue(result);
            Assert.AreEqual(123, parsedValue);
        }
        
        [TestMethod]
        public void IntParse_Should_Return_False_For_Negative_Number()
        {
            // Arrange
            var input = "-123";
            ReadOnlySpan<char> span = input.AsSpan();
            
            // Act
            bool result = Fast.IntParse(span, out int parsedValue);
            
            // Assert
            Assert.IsFalse(result);
            Assert.AreEqual(-1, parsedValue);
        }
        
        [TestMethod]
        public void IntParse_Should_Return_False_For_Non_Digit_Characters()
        {
            // Arrange
            var input = "12a";
            ReadOnlySpan<char> span = input.AsSpan();
            
            // Act
            bool result = Fast.IntParse(span, out int parsedValue);
            
            // Assert
            Assert.IsFalse(result);
            Assert.AreEqual(-1, parsedValue);
        }
        
        [TestMethod]
        public void IntParse_Should_Parse_Empty_String()
        {
            // Arrange
            var input = "";
            ReadOnlySpan<char> span = input.AsSpan();
            
            // Act
            bool result = Fast.IntParse(span, out int parsedValue);
            
            // Assert
            Assert.IsTrue(result);
            Assert.AreEqual(0, parsedValue);
        }
    }
}
