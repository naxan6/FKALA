using System;
using System.Collections.Generic;
using FKala.Core.Helper;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace FKala.Unittests.Helper
{
    [TestClass]
    public class Msg_Tests
    {
        [TestMethod]
        public void Get_Should_Return_Dictionary_With_Key_And_Value()
        {
            // Arrange
            var key = "testKey";
            var value = "testValue";
            
            // Act
            var result = Msg.Get(key, value);
            
            // Assert
            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsKey(key));
            Assert.AreEqual(value, result[key]);
        }
        
        [TestMethod]
        public void Get_Should_Return_Dictionary_With_String_Key_Value_Pair()
        {
            // Arrange
            var key = "name";
            var value = "testName";
            
            // Act
            var result = Msg.Get(key, value);
            
            // Assert
            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsKey(key));
            Assert.AreEqual(value, result[key]);
        }
    }
}
