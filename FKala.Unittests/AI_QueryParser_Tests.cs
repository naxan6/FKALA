using Microsoft.VisualStudio.TestTools.UnitTesting;
using FKala.Core.KalaQl.QueryParser;
using FKala.Core.KalaQl;
using System.Collections.Generic;
using FKala.Core.Model;
using System;
using FKala.Core.Interfaces;

namespace FKala.Unittests
{
    [TestClass]
    public class AI_QueryParser_Tests
    {
        [TestMethod]
        public void Test_KalaQlParserRegistry()
        {
            // Test parser registration and routing
            var registry = new KalaQlParserRegistry();

            // Register a test parser
            var testParser = new TestParser();
            registry.RegisterParser(testParser);

            // Test parsing with registered parser
            var fields = new List<string> { "TestVerb", "param1", "param2" };
            var result = registry.Parse("TestVerb param1 param2", fields);

            Assert.IsNotNull(result);
            Assert.IsInstanceOfType(result, typeof(Op_Test));
        }

        [TestMethod]
        public void Test_MgmtParser()
        {
            // Test MgmtParser functionality
            var parser = new MgmtParser();

            // Test CanParse method
            Assert.IsTrue(parser.CanParse("Mgmt"));
            Assert.IsFalse(parser.CanParse("Other"));

            // Test parsing a management command
            var fields = new List<string> { "Mgmt", "LOADMEASURES", "param1" };
            var result = parser.Parse("Mgmt LOADMEASURES param1", fields);

            Assert.IsNotNull(result);
            Assert.IsInstanceOfType(result, typeof(Op_Mgmt));
            var mgmtOp = (Op_Mgmt)result;
            Assert.AreEqual(MgmtAction.LoadMeasures, mgmtOp.MgmtAction);
            Assert.AreEqual("param1", mgmtOp.Params);
        }

        [TestMethod]
        public void Test_AlignTimezoneParser()
        {
            // Test AlignTimezoneParser functionality
            var parser = new AlignTimezoneParser();

            // Test CanParse method
            Assert.IsTrue(parser.CanParse("AlTz"));
            Assert.IsFalse(parser.CanParse("Other"));

            // Test parsing an align timezone command
            var fields = new List<string> { "AlTz", "Europe/Berlin" };
            var result = parser.Parse("AlTz Europe/Berlin", fields);

            Assert.IsNotNull(result);
            Assert.IsInstanceOfType(result, typeof(Op_AlignTimezone));
            var alignOp = (Op_AlignTimezone)result;
            Assert.AreEqual("Europe/Berlin", alignOp.TzId);
        }

        [TestMethod]
        public void Test_VarParser()
        {
            // Test VarParser functionality
            var parser = new VarParser();

            // Test CanParse method
            Assert.IsTrue(parser.CanParse("Var"));
            Assert.IsFalse(parser.CanParse("Other"));

            // Test parsing a variable assignment
            var fields = new List<string> { "Var", "testVar:", "testValue" };
            var result = parser.Parse("Var testVar: testValue", fields);

            Assert.IsNotNull(result);
            Assert.IsInstanceOfType(result, typeof(Op_Var));
            var varOp = (Op_Var)result;
            Assert.AreEqual("testVar", varOp.Name);
            Assert.AreEqual("testValue", varOp.VarValue);
        }

        [TestMethod]
        public void Test_LoadParser()
        {
            // Test LoadParser functionality
            var parser = new LoadParser();

            // Test CanParse method
            Assert.IsTrue(parser.CanParse("Load"));
            Assert.IsFalse(parser.CanParse("Other"));

            // Test parsing a load command
            var fields = new List<string> { "Load", "testName:", "testMeasure", "2020-01-01T00:00:00Z", "2021-01-01T00:00:00Z", "NoCache" };
            var result = parser.Parse("Load testName: testMeasure 2020-01-01T00:00:00Z 2021-01-01T00:00:00Z NoCache", fields);

            Assert.IsNotNull(result);
            Assert.IsInstanceOfType(result, typeof(Op_Load));
            var loadOp = (Op_Load)result;
            Assert.AreEqual("testName", loadOp.Name);
            Assert.AreEqual("testMeasure", loadOp.Measurement);
        }

        // Add more parser tests as needed...

        // Test helper class for testing registry
        private class TestParser : KalaQlParserBase
        {
            public override bool CanParse(string verb) => verb == "TestVerb";

            public override Op_Base Parse(string line, List<string> fields)
            {
                return new Op_Test(line);
            }

            public override string GenerateLine(object parameters)
            {
                return "TestVerb";
            }
        }

        // Test operation class
        private class Op_Test : Op_Base
        {
            public Op_Test(string line) : base(line) { }

            public override bool CanExecute(KalaQlContext context)
            {
                return true;
            }

            public override void Execute(KalaQlContext context)
            {
                // Test implementation
            }

            public override List<string> GetInputNames()
            {
                return new List<string>();
            }

            public override IKalaQlOperation Clone()
            {
                return new Op_Test(this.Line!);
            }

            public override string ToLine()
            {
                return this.Line!;
            }
        }
    }
}
