using System;
using FKala.Core.Helper;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace FKala.Unittests.Helper
{
    [TestClass]
    public class Benchmarker_Tests
    {
        [TestMethod]
        public void BenchmarkResult_Should_Have_Reading_And_Writing_Dictionaries()
        {
            // Arrange & Act
            var benchmarkResult = new Benchmarker.BenchmarkResult()
            {
                Reading = new System.Collections.Generic.Dictionary<long, System.TimeSpan>(),
                Writing = new System.Collections.Generic.Dictionary<long, System.TimeSpan>()
            };
            
            // Assert
            Assert.IsNotNull(benchmarkResult.Reading);
            Assert.IsNotNull(benchmarkResult.Writing);
        }
    }
}
