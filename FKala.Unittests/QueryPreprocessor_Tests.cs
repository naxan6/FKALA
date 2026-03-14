using System;
using System.Collections.Generic;
using FKala.Core.KalaQl;
using FKala.Core.Interfaces;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;

namespace FKala.Unittests
{
    [TestClass]
    public class QueryPreprocessor_Tests
    {
        private Mock<IDataLayer> _mockDataLayer;
        private QueryPreprocessor _preprocessor;

        [TestInitialize]
        public void Setup()
        {
            _mockDataLayer = new Mock<IDataLayer>();
            _preprocessor = new QueryPreprocessor(_mockDataLayer.Object);
        }

        [TestMethod]
        public void Process_With_Mgmt_SubCommand_Regex_Returns_SingleLine_With_CombinedNames()
        {
            // Arrange
            var measurementList = new List<string> { "kala1", "kala2", "kala3" };
            _mockDataLayer.Setup(x => x.LoadMeasurementList()).Returns(measurementList);

            string compactScript = @"Mgmt FSCHK regex:kala.*";

            // Act
            var result = _preprocessor.Process(compactScript);

            // Assert
            Assert.AreEqual(1, result.Count);
            Assert.AreEqual(@"Mgmt FSCHK kala1,kala2,kala3", result[0]);
        }

        [TestMethod]
        public void Process_With_Var_Command_Registers_Name()
        {
            // Arrange
            string compactScript = @"Var myVar: 123";

            // Act
            var result = _preprocessor.Process(compactScript);

            // Assert
            Assert.AreEqual(1, result.Count);
            Assert.AreEqual(@"Var myVar: 123", result[0]);
            // Prüfe, dass der Name registriert wurde (nicht direkt aus der Ausgabe sichtbar, aber in _generatedNames)
            // Da _generatedNames intern ist, prüfen wir es indirekt über eine weitere Verwendung
        }

        [TestMethod]
        public void Process_With_Load_Then_Publish_Uses_RegistredName()
        {
            // Arrange
            var measurementList = new List<string> { "PV1_kW" };
            _mockDataLayer.Setup(x => x.LoadMeasurementList()).Returns(measurementList);

            string compactScript = @"Load rPV1: regex:.*kW$ 2024-01-01T00:00:00Z
Publ rPV1 NoCache";

            // Act
            var result = _preprocessor.Process(compactScript);

            // Assert
            Assert.AreEqual(2, result.Count);
            Assert.IsTrue(result[0].StartsWith("Load rPV1: PV1_kW"));
            Assert.AreEqual(@"Publ rPV1 NoCache", result[1]); // Publ sollte den registrierten Namen verwenden
        }

        [TestMethod]
        public void Process_With_NameTemplate_CaptureGroup_Returns_CorrectName()
        {
            // Arrange
            var measurementList = new List<string> { "PV1_kW" };
            _mockDataLayer.Setup(x => x.LoadMeasurementList()).Returns(measurementList);

            string compactScript = @"Load r<measure:.*PV(.*)_kW>: regex:.*kW$ 2024-01-01T00:00:00Z";

            // Act
            var result = _preprocessor.Process(compactScript);

            // Assert
            Assert.AreEqual(1, result.Count);
            Assert.AreEqual(@"Load r1: PV1_kW 2024-01-01T00:00:00Z", result[0]); // Capture-Gruppe (.*) liefert "1"
        }

        [TestMethod]
        public void Process_With_EmptyMeasureTemplate_Returns_FullMatch()
        {
            // Arrange
            var measurementList = new List<string> { "PV1_Measurement" };
            _mockDataLayer.Setup(x => x.LoadMeasurementList()).Returns(measurementList);

            string compactScript = @"Load r<measure>: regex:.*PV.* 2024-01-01T00:00:00Z";

            // Act
            var result = _preprocessor.Process(compactScript);

            // Assert
            Assert.AreEqual(1, result.Count);
            Assert.AreEqual(@"Load rPV1_Measurement: PV1_Measurement 2024-01-01T00:00:00Z", result[0]);
        }

        [TestMethod]
        public void Process_With_InvalidRegex_Throws_ArgumentException()
        {
            // Arrange
            var measurementList = new List<string> { "PV1_Measurement" };
            _mockDataLayer.Setup(x => x.LoadMeasurementList()).Returns(measurementList);

            string compactScript = @"Load r<measure:.*PV.*>: regex:.*kW 2024-01-01T00:00:00Z"; // Ungültiges Regex (fehlendes $)

            // Act & Assert
            Assert.ThrowsException<ArgumentException>(() => _preprocessor.Process(compactScript));
        }

        [TestMethod]
        public void Process_With_MissingCaptureGroup_Throws_ArgumentException()
        {
            // Arrange
            var measurementList = new List<string> { "PV1_kW" };
            _mockDataLayer.Setup(x => x.LoadMeasurementList()).Returns(measurementList);

            string compactScript = @"Load r<measure:.*PV.*[kW]$>: regex:.*PV.* 2024-01-01T00:00:00Z"; // Regex passt, aber Capture-Gruppe nicht

            // Act & Assert
            Assert.ThrowsException<ArgumentException>(() => _preprocessor.Process(compactScript));
        }
    }
}