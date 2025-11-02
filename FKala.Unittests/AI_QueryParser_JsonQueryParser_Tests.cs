using System;
using System.Collections.Generic;
using FKala.Core.KalaQl;
using FKala.Core.KalaQl.QueryParser;
using FKala.Core.Model;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace FKala.Unittests
{
    [TestClass]
    public class AI_QueryParser_JsonQueryParser_Tests
    {
        [TestMethod]
        public void JsonQueryParser_Parse_Returns_Op_JsonQuery_With_Correct_Properties()
        {
            // Arrange
            var parser = new JsonQueryParser();
            var line = "Loaj MyOp: Measurement 2025-01-01T00:00:00 2025-12-31T23:59:59 NoCache";
            var fields = new List<string>
            {
                "Loaj",
                "MyOp",
                "Measurement",
                "2025-01-01T00:00:00",
                "2025-12-31T23:59:59",
                "NoCache"
            };

            // Act
            var op = parser.Parse(line, fields);

            // Assert
            Assert.IsInstanceOfType(op, typeof(Op_JsonQuery));
            var jsonOp = (Op_JsonQuery)op;
            Assert.AreEqual("MyOp", jsonOp.Name);
            Assert.AreEqual("Measurement", jsonOp.Measurement);
            Assert.AreEqual(DateTime.SpecifyKind(new DateTime(2025, 1, 1, 0, 0, 0), DateTimeKind.Utc), jsonOp.StartTime);
            Assert.AreEqual(DateTime.SpecifyKind(new DateTime(2025, 12, 31, 23, 59, 59), DateTimeKind.Utc), jsonOp.EndTime);
            // Überprüfe den String-Wert, da es sich um unterschiedliche Typen handelt
            Assert.AreEqual("Full_None", jsonOp.CacheResolution.ToString());
            Assert.IsFalse(jsonOp.NewestOnly);
        }

        [TestMethod]
        public void JsonQueryParser_Parse_With_NewestOnly_Returns_Op_JsonQuery_With_NewestOnly()
        {
            // Arrange
            var parser = new JsonQueryParser();
            var line = "Loaj MyOp: Measurement 2025-01-01T00:00:00 2025-12-31T23:59:59 NoCache NewestOnly";
            var fields = new List<string>
            {
                "Loaj",
                "MyOp",
                "Measurement",
                "2025-01-01T00:00:00",
                "2025-12-31T23:59:59",
                "NoCache",
                "NewestOnly"
            };

            // Act
            var op = parser.Parse(line, fields);

            // Assert
            Assert.IsInstanceOfType(op, typeof(Op_JsonQuery));
            var jsonOp = (Op_JsonQuery)op;
            Assert.IsTrue(jsonOp.NewestOnly);
        }
    }
}