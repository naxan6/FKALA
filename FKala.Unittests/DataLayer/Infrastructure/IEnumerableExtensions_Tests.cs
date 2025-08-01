using System.Collections.Generic;
using System.Threading.Tasks;
using FKala.Core.DataLayer.Infrastructure;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace FKala.Unittests.DataLayer.Infrastructure
{
    [TestClass]
    public class IEnumerableExtensions_Tests
    {
        [TestMethod]
        public async Task AsAsyncEnumerable_Should_Return_Original_Values()
        {
            // Arrange
            var input = new List<int> { 1, 2, 3, 4, 5 };
            
            // Act
            var result = new List<int>();
            await foreach (var item in input.AsAsyncEnumerable())
            {
                result.Add(item);
            }
            
            // Assert
            CollectionAssert.AreEqual(input.ToArray(), result.ToArray());
        }
        
        [TestMethod]
        public async Task AsAsyncEnumerable_Should_Handle_Empty_Enumerable()
        {
            // Arrange
            var input = new List<int>();
            
            // Act
            var result = new List<int>();
            await foreach (var item in input.AsAsyncEnumerable())
            {
                result.Add(item);
            }
            
            // Assert
            Assert.AreEqual(0, result.Count);
        }
    }
}
