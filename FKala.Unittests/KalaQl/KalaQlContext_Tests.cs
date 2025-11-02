using FKala.Core.KalaQl;
using FKala.Core.DataLayer;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.IO;
using FKala.Core;

namespace FKala.Unittests.KalaQl
{
    [TestClass]
    public class KalaQlContext_Tests
    {
        [TestMethod]
        public void AddError_AddsMessageToErrorsAndWritesToConsole()
        {
            var tempDir = Path.Combine(Path.GetTempPath(), "FKalaTestData");
            Directory.CreateDirectory(tempDir);
            var context = new KalaQlContext(new KalaQuery(), new DataLayer_Readable_Caching_V1(tempDir));

            var sw = new StringWriter();
            var originalOut = Console.Out;
            Console.SetOut(sw);

            context.AddError("Test error");

            Console.SetOut(originalOut);

            Assert.AreEqual(1, context.Errors.Count);
            Assert.IsTrue(context.Errors[0].Contains("Test error"));
            Assert.IsTrue(sw.ToString().Contains("Test error"));
        }

        [TestMethod]
        public void AddWarning_AddsMessageToWarningsAndWritesToConsole()
        {
            var tempDir = Path.Combine(Path.GetTempPath(), "FKalaTestData");
            Directory.CreateDirectory(tempDir);
            var context = new KalaQlContext(new KalaQuery(), new DataLayer_Readable_Caching_V1(tempDir));

            var sw = new StringWriter();
            var originalOut = Console.Out;
            Console.SetOut(sw);

            context.AddWarning("Test warning");

            Console.SetOut(originalOut);

            Assert.AreEqual(1, context.Warnings.Count);
            Assert.IsTrue(context.Warnings[0].Contains("Test warning"));
            Assert.IsTrue(sw.ToString().Contains("Test warning"));
        }
    }
}