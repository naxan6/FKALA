using FKala.Core.KalaQl;
using FKala.Core.Model;
using FKala.Core;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace FKala.Unittests.KalaQl
{
    [TestClass]
    public class AI_Op_Mgmt_Tests
    {
        [TestMethod]
        public void Op_Mgmt_Constructor_InitializesCorrectly()
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
        public void Op_Mgmt_CanExecute_ReturnsTrue()
        {
            // Arrange
            var opMgmt = new Op_Mgmt("test", MgmtAction.LoadMeasures, "params");

            // Act
            var result = opMgmt.CanExecute(new KalaQlContext(new KalaQuery(), new DataLayer_Readable_Caching_V1(Path.Combine(Path.GetTempPath(), "FKalaTestData"))));

            // Assert
            Assert.IsTrue(result);
        }

        [TestMethod]
        public async Task Op_Mgmt_Execute_LoadMeasures_ReturnsExpectedResult()
        {
            // Arrange
            var kalaQuery = new KalaQuery();
            var tempDir = Path.Combine(Path.GetTempPath(), "FKalaTestData");
            var dataLayer = new DataLayer_Readable_Caching_V1(tempDir);
            var context = new KalaQlContext(kalaQuery, dataLayer);
            var opMgmt = new Op_Mgmt("test", MgmtAction.LoadMeasures, "");

            // Act
            opMgmt.Execute(context);

            // Assert
            Assert.IsTrue(opMgmt.hasExecuted);
            Assert.IsNotNull(context.Result);
            Assert.IsNotNull(context.Result.MeasureList);
        }

        [TestMethod]
        public async Task Op_Mgmt_Execute_SortAllRaw_ReturnsExpectedResult()
        {
            // Arrange
            var kalaQuery = new KalaQuery();
            var tempDir = Path.Combine(Path.GetTempPath(), "FKalaTestData");
            var dataLayer = new DataLayer_Readable_Caching_V1(tempDir);
            var context = new KalaQlContext(kalaQuery, dataLayer);
            var opMgmt = new Op_Mgmt("test", MgmtAction.SortAllRaw, "");

            // Act
            opMgmt.Execute(context);

            // Assert
            Assert.IsTrue(opMgmt.hasExecuted);
            Assert.IsNotNull(context.Result);
            Assert.IsNotNull(context.Result.StreamResult);
        }

        [TestMethod]
        public async Task Op_Mgmt_Execute_FsChk_ReturnsExpectedResult()
        {
            // Arrange
            var kalaQuery = new KalaQuery();
            var tempDir = Path.Combine(Path.GetTempPath(), "FKalaTestData");
            var dataLayer = new DataLayer_Readable_Caching_V1(tempDir);
            var context = new KalaQlContext(kalaQuery, dataLayer);
            var opMgmt = new Op_Mgmt("test", MgmtAction.FsChk, "");

            // Act
            opMgmt.Execute(context);

            // Assert
            Assert.IsTrue(opMgmt.hasExecuted);
            Assert.IsNotNull(context.Result);
            Assert.IsNotNull(context.Result.StreamResult);
        }

        [TestMethod]
        public async Task Op_Mgmt_Execute_Copy_ReturnsExpectedResult()
        {
            // Arrange
            var kalaQuery = new KalaQuery();
            var tempDir = Path.Combine(Path.GetTempPath(), "FKalaTestData");
            var dataLayer = new DataLayer_Readable_Caching_V1(tempDir);
            var context = new KalaQlContext(kalaQuery, dataLayer);
            var opMgmt = new Op_Mgmt("test", MgmtAction.Copy, "\"source target\"");

            // Act
            opMgmt.Execute(context);

            // Assert
            Assert.IsTrue(opMgmt.hasExecuted);
            Assert.IsNotNull(context.Result);
            Assert.IsNotNull(context.Result.StreamResult);
        }

        [TestMethod]
        public async Task Op_Mgmt_Execute_Rename_ReturnsExpectedResult()
        {
            // Arrange
            var kalaQuery = new KalaQuery();
            var tempDir = Path.Combine(Path.GetTempPath(), "FKalaTestData");
            var dataLayer = new DataLayer_Readable_Caching_V1(tempDir);
            var context = new KalaQlContext(kalaQuery, dataLayer);
            var opMgmt = new Op_Mgmt("test", MgmtAction.Rename, "\"source target\"");

            // Act
            opMgmt.Execute(context);

            // Assert
            Assert.IsTrue(opMgmt.hasExecuted);
            Assert.IsNotNull(context.Result);
            Assert.IsNotNull(context.Result.StreamResult);
        }

        [TestMethod]
        public async Task Op_Mgmt_Execute_Blacklist_ReturnsExpectedResult()
        {
            // Arrange
            var kalaQuery = new KalaQuery();
            var tempDir = Path.Combine(Path.GetTempPath(), "FKalaTestData");
            var dataLayer = new DataLayer_Readable_Caching_V1(tempDir);
            var context = new KalaQlContext(kalaQuery, dataLayer);
            var opMgmt = new Op_Mgmt("test", MgmtAction.Blacklist, "\"measurement\"");

            // Act
            opMgmt.Execute(context);

            // Assert
            Assert.IsTrue(opMgmt.hasExecuted);
            Assert.IsNotNull(context.Result);
            Assert.IsNotNull(context.Result.StreamResult);
        }

        [TestMethod]
        public async Task Op_Mgmt_Execute_UnBlacklist_ReturnsExpectedResult()
        {
            // Arrange
            var kalaQuery = new KalaQuery();
            var tempDir = Path.Combine(Path.GetTempPath(), "FKalaTestData");
            var dataLayer = new DataLayer_Readable_Caching_V1(tempDir);
            var context = new KalaQlContext(kalaQuery, dataLayer);
            var opMgmt = new Op_Mgmt("test", MgmtAction.UnBlacklist, "\"measurement\"");

            // Act
            opMgmt.Execute(context);

            // Assert
            Assert.IsTrue(opMgmt.hasExecuted);
            Assert.IsNotNull(context.Result);
            Assert.IsNotNull(context.Result.StreamResult);
        }

        [TestMethod]
        public async Task Op_Mgmt_Execute_ImportInflux_ReturnsExpectedResult()
        {
            // Arrange
            var kalaQuery = new KalaQuery();
            var tempDir = Path.Combine(Path.GetTempPath(), "FKalaTestData");
            var dataLayer = new DataLayer_Readable_Caching_V1(tempDir);
            var context = new KalaQlContext(kalaQuery, dataLayer);
            var opMgmt = new Op_Mgmt("test", MgmtAction.ImportInflux, "test params");

            // Act
            opMgmt.Execute(context);

            // Assert
            Assert.IsTrue(opMgmt.hasExecuted);
            Assert.IsNotNull(context.Result);
            Assert.IsNotNull(context.Result.StreamResult);
        }

        [TestMethod]
        public async Task Op_Mgmt_Execute_ImportMariaDbTstsfe_ReturnsExpectedResult()
        {
            // Arrange
            var kalaQuery = new KalaQuery();
            var tempDir = Path.Combine(Path.GetTempPath(), "FKalaTestData");
            var dataLayer = new DataLayer_Readable_Caching_V1(tempDir);
            var context = new KalaQlContext(kalaQuery, dataLayer);
            var opMgmt = new Op_Mgmt("test", MgmtAction.ImportMariaDbTstsfe, "test params");

            // Act
            opMgmt.Execute(context);

            // Assert
            Assert.IsTrue(opMgmt.hasExecuted);
            Assert.IsNotNull(context.Result);
            Assert.IsNotNull(context.Result.StreamResult);
        }

        [TestMethod]
        public async Task Op_Mgmt_Execute_BenchmarkIo_ReturnsExpectedResult()
        {
            // Arrange
            var kalaQuery = new KalaQuery();
            var tempDir = Path.Combine(Path.GetTempPath(), "FKalaTestData");
            var dataLayer = new DataLayer_Readable_Caching_V1(tempDir);
            var context = new KalaQlContext(kalaQuery, dataLayer);
            var opMgmt = new Op_Mgmt("test", MgmtAction.BenchmarkIo, "");

            // Act
            opMgmt.Execute(context);

            // Assert
            Assert.IsTrue(opMgmt.hasExecuted);
            Assert.IsNotNull(context.Result);
            Assert.IsNotNull(context.Result.StreamResult);
        }

        [TestMethod]
        public void Op_Mgmt_ToString_ReturnsCorrectFormat()
        {
            // Arrange
            var opMgmt = new Op_Mgmt("test", MgmtAction.LoadMeasures, "params");

            // Act
            var result = opMgmt.ToString();

            // Assert
            Assert.AreEqual("Op_Mgmt: LoadMeasures", result);
        }

        [TestMethod]
        public void Op_Mgmt_GetInputNames_ReturnsEmptyList()
        {
            // Arrange
            var opMgmt = new Op_Mgmt("test", MgmtAction.LoadMeasures, "params");

            // Act
            var result = opMgmt.GetInputNames();

            // Assert
            Assert.IsNotNull(result);
            Assert.AreEqual(0, result.Count);
        }
    }
}