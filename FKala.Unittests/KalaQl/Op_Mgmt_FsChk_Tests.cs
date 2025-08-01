using FKala.Core.KalaQl;
using FKala.Core.Model;
using FKala.Core.Interfaces;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using System.Collections.Generic;
using System.Linq;

namespace FKala.Unittests.KalaQl
{
    [TestClass]
    public class Op_Mgmt_FsChk_Tests
    {
        [TestMethod]
        public void FsChk_Execute_Should_Set_HasExecuted()
        {
            // Arrange
            var mockDataLayer = new Mock<IDataLayer>();
            var testMeasurements = new List<string> { "measurement1" };
            
            // Setup data layer to return our test measurements
            mockDataLayer.Setup(d => d.LoadMeasurementList()).Returns(testMeasurements);
            
            // Create context with the mocked data layer
            var context = new KalaQlContext(new KalaQuery(), mockDataLayer.Object);
            
            // Create Op_Mgmt instance for FsChk operation
            var opMgmt = new Op_Mgmt("test", MgmtAction.FsChk, "");
            
            // Act
            opMgmt.Execute(context);
            
            // Assert - Verify that hasExecuted flag is set to true
            Assert.IsTrue(opMgmt.hasExecuted);
        }

        [TestMethod]
        public void FsChk_Execute_Should_Set_Result_StreamResult()
        {
            // Arrange
            var mockDataLayer = new Mock<IDataLayer>();
            var testMeasurements = new List<string> { "measurement1" };
            
            // Setup data layer to return our test measurements
            mockDataLayer.Setup(d => d.LoadMeasurementList()).Returns(testMeasurements);
            
            // Create context with the mocked data layer
            var context = new KalaQlContext(new KalaQuery(), mockDataLayer.Object);
            
            // Create Op_Mgmt instance for FsChk operation
            var opMgmt = new Op_Mgmt("test", MgmtAction.FsChk, "");
            
            // Act
            opMgmt.Execute(context);
            
            // Assert - Verify that result and stream result are not null
            Assert.IsNotNull(context.Result);
            Assert.IsNotNull(context.Result.StreamResult);
        }
        
        [TestMethod]
        public void FsChk_Should_Return_Success_Results_When_No_Errors()
        {
            // Arrange
            var mockDataLayer = new Mock<IDataLayer>();
            var testMeasurements = new List<string> { "measurement1", "measurement2" };
            
            // Setup data layer to return our test measurements
            mockDataLayer.Setup(d => d.LoadMeasurementList()).Returns(testMeasurements);
            
            // Create context with the mocked data layer
            var context = new KalaQlContext(new KalaQuery(), mockDataLayer.Object);
            
            // Create Op_Mgmt instance for FsChk operation
            var opMgmt = new Op_Mgmt("test", MgmtAction.FsChk, "");
            
            // Act
            opMgmt.Execute(context);
            
            // Assert - Verify that results contain expected success entries
            Assert.IsNotNull(context.Result);
            Assert.IsNotNull(context.Result.StreamResult);
            Assert.IsTrue(context.Result.StreamResult.Any());
        }

        [TestMethod]
        public void FsChk_Should_Handle_Error_Cases()
        {
            // Arrange
            var mockDataLayer = new Mock<IDataLayer>();
            var testMeasurements = new List<string> { "errorMeasurement" };
            
            // Setup data layer to return our test measurements
            mockDataLayer.Setup(d => d.LoadMeasurementList()).Returns(testMeasurements);
            
            // Create context with the mocked data layer
            var context = new KalaQlContext(new KalaQuery(), mockDataLayer.Object);
            
            // Create Op_Mgmt instance for FsChk operation
            var opMgmt = new Op_Mgmt("test", MgmtAction.FsChk, "");
            
            // Act
            opMgmt.Execute(context);
            
            // Assert - Verify that results are returned properly even with errors
            Assert.IsNotNull(context.Result);
            Assert.IsNotNull(context.Result.StreamResult);
        }
    }
}
