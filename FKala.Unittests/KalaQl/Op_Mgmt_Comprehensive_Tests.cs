using FKala.Core.KalaQl;
using FKala.Core.Model;
using FKala.Core;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.IO;
using FKala.Unittests.Helper;

namespace FKala.Unittests.KalaQl
{
    [TestClass]
    public class Op_Mgmt_Comprehensive_Tests
    {
        [TestMethod]
        public void Constructor_InitializesCorrectly()
        {
            // Arrange
            string line = "test line";
            MgmtAction action = MgmtAction.LoadMeasures;
            string parameters = "test params";

            // Act
            var opMgmt = new Op_Mgmt(line, action, parameters);

            // Assert
            Assert.AreEqual(action, opMgmt.MgmtAction);
            Assert.AreEqual(parameters, opMgmt.Params);
            Assert.AreEqual("_NONE_MGMT", opMgmt.Name);
        }

        [TestMethod]
        public void Execute_LoadMeasures_ReturnsExpectedResult()
        {
            // Arrange - Using same pattern as working tests
            var tempDir = Path.Combine(Path.GetTempPath(), "FKalaTestData");
            var dataLayer = new DataLayer_Readable_Caching_V1(tempDir);
            var context = new KalaQlContext(new KalaQuery(), dataLayer);
            var opMgmt = new Op_Mgmt("test", MgmtAction.LoadMeasures, "");

            // Act
            opMgmt.Execute(context);

            // Assert
            Assert.IsTrue(opMgmt.hasExecuted);
            Assert.IsNotNull(context.Result);
            Assert.IsNotNull(context.Result.MeasureList);
        }

        [TestMethod]
        public void Execute_Blacklist_ReturnsExpectedResult()
        {
            // Arrange
            var tempDir = Path.Combine(Path.GetTempPath(), "FKalaTestData");
            var dataLayer = new DataLayer_Readable_Caching_V1(tempDir);
            var context = new KalaQlContext(new KalaQuery(), dataLayer);
            var opMgmt = new Op_Mgmt("test", MgmtAction.Blacklist, "\"measurement\"");

            // Act
            opMgmt.Execute(context);

            // Assert
            Assert.IsTrue(opMgmt.hasExecuted);
            Assert.IsNotNull(context.Result);
            Assert.IsNotNull(context.Result.StreamResult);
            CheckStreamResultsHelper.AssertNoNullItemsInStream(context.Result.StreamResult);
        }

        [TestMethod]
        public void Execute_UnBlacklist_ReturnsExpectedResult()
        {
            // Arrange
            var tempDir = Path.Combine(Path.GetTempPath(), "FKalaTestData");
            var dataLayer = new DataLayer_Readable_Caching_V1(tempDir);
            var context = new KalaQlContext(new KalaQuery(), dataLayer);
            var opMgmt = new Op_Mgmt("test", MgmtAction.UnBlacklist, "\"measurement\"");

            // Act
            opMgmt.Execute(context);

            // Assert
            Assert.IsTrue(opMgmt.hasExecuted);
            Assert.IsNotNull(context.Result);
            Assert.IsNotNull(context.Result.StreamResult);
            CheckStreamResultsHelper.AssertNoNullItemsInStream(context.Result.StreamResult);
        }


        [TestMethod]
        public void Execute_BenchmarkIo_ReturnsExpectedResult()
        {
            // Arrange
            var tempDir = Path.Combine(Path.GetTempPath(), "FKalaTestData");
            var dataLayer = new DataLayer_Readable_Caching_V1(tempDir);
            var context = new KalaQlContext(new KalaQuery(), dataLayer);
            var opMgmt = new Op_Mgmt("test", MgmtAction.BenchmarkIo, "");

            // Act
            opMgmt.Execute(context);

            // Assert
            Assert.IsTrue(opMgmt.hasExecuted);
            Assert.IsNotNull(context.Result);
            Assert.IsNotNull(context.Result.StreamResult);
            CheckStreamResultsHelper.AssertNoNullItemsInStream(context.Result.StreamResult);
        }

        [TestMethod]
        public void ToString_ReturnsCorrectFormat()
        {
            // Arrange
            var opMgmt = new Op_Mgmt("test", MgmtAction.LoadMeasures, "params");

            // Act
            var result = opMgmt.ToString();

            // Assert
            Assert.AreEqual("Op_Mgmt: LoadMeasures", result);
        }
    }
}
