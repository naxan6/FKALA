using Microsoft.VisualStudio.TestTools.UnitTesting;
using FluentAssertions;
using FKala.Core.KalaQl.QueryParser;
using FKala.Core.KalaQl;
using FKala.Core.Model;
using System.Collections.Generic;

namespace FKala.Unittests
{
    [TestClass]
    public class AI_MgmtParser_Tests
    {
        private MgmtParser _parser;

        [TestInitialize]
        public void Setup()
        {
            _parser = new MgmtParser();
        }

        [TestMethod]
        public void CanParse_WithMgmtVerb_ShouldReturnTrue()
        {
            // Arrange
            string verb = "Mgmt";

            // Act
            var result = _parser.CanParse(verb);

            // Assert
            result.Should().BeTrue();
        }

        [TestMethod]
        public void CanParse_WithDifferentVerb_ShouldReturnFalse()
        {
            // Arrange
            string verb = "Load";

            // Act
            var result = _parser.CanParse(verb);

            // Assert
            result.Should().BeFalse();
        }

        [TestMethod]
        public void Parse_WithValidLine_ShouldReturnOpMgmt()
        {
            // Arrange
            string line = "Mgmt LOADMEASURES";
            var fields = new List<string> { "Mgmt", "LOADMEASURES" };

            // Act
            var result = _parser.Parse(line, fields);

            // Assert
            result.Should().NotBeNull();
            result.Should().BeOfType<Op_Mgmt>();
            var opMgmt = (Op_Mgmt)result;
            opMgmt.MgmtAction.Should().Be(MgmtAction.LoadMeasures);
            opMgmt.Params.Should().BeEmpty();
        }

        [TestMethod]
        public void Parse_WithParameters_ShouldReturnOpMgmtWithParameters()
        {
            // Arrange
            string line = "Mgmt COPY source destination";
            var fields = new List<string> { "Mgmt", "COPY", "source", "destination" };

            // Act
            var result = _parser.Parse(line, fields);

            // Assert
            result.Should().NotBeNull();
            result.Should().BeOfType<Op_Mgmt>();
            var opMgmt = (Op_Mgmt)result;
            opMgmt.MgmtAction.Should().Be(MgmtAction.Copy);
            opMgmt.Params.Should().Be("source destination");
        }

        [TestMethod]
        public void GenerateLine_WithLoadMeasuresAction_ShouldReturnCorrectString()
        {
            // Arrange
            var parameters = new MgmtParams(MgmtAction.LoadMeasures, "");

            // Act
            var result = _parser.GenerateLine(parameters);

            // Assert
            result.Should().Be("Mgmt LOADMEASURES");
        }

        [TestMethod]
        public void GenerateLine_WithSortAllRawAction_ShouldReturnCorrectString()
        {
            // Arrange
            var parameters = new MgmtParams(MgmtAction.SortAllRaw, "");

            // Act
            var result = _parser.GenerateLine(parameters);

            // Assert
            result.Should().Be("Mgmt SORTRAWFILES");
        }

        [TestMethod]
        public void GenerateLine_WithImportInfluxAction_ShouldReturnCorrectString()
        {
            // Arrange
            var parameters = new MgmtParams(MgmtAction.ImportInflux, "");

            // Act
            var result = _parser.GenerateLine(parameters);

            // Assert
            result.Should().Be("Mgmt IMPORTINFLUX");
        }

        [TestMethod]
        public void GenerateLine_WithImportMariaDbTstsfeAction_ShouldReturnCorrectString()
        {
            // Arrange
            var parameters = new MgmtParams(MgmtAction.ImportMariaDbTstsfe, "");

            // Act
            var result = _parser.GenerateLine(parameters);

            // Assert
            result.Should().Be("Mgmt IMPORTTSTSFE");
        }

        [TestMethod]
        public void GenerateLine_WithBenchmarkIoAction_ShouldReturnCorrectString()
        {
            // Arrange
            var parameters = new MgmtParams(MgmtAction.BenchmarkIo, "");

            // Act
            var result = _parser.GenerateLine(parameters);

            // Assert
            result.Should().Be("Mgmt BENCHIO");
        }

        [TestMethod]
        public void GenerateLine_WithFsChkAction_ShouldReturnCorrectString()
        {
            // Arrange
            var parameters = new MgmtParams(MgmtAction.FsChk, "");

            // Act
            var result = _parser.GenerateLine(parameters);

            // Assert
            result.Should().Be("Mgmt FSCHK");
        }

        [TestMethod]
        public void GenerateLine_WithCopyActionAndParameters_ShouldReturnCorrectString()
        {
            // Arrange
            var parameters = new MgmtParams(MgmtAction.Copy, "source destination");

            // Act
            var result = _parser.GenerateLine(parameters);

            // Assert
            result.Should().Be("Mgmt COPY source destination");
        }

        [TestMethod]
        public void GenerateLine_WithRenameActionAndParameters_ShouldReturnCorrectString()
        {
            // Arrange
            var parameters = new MgmtParams(MgmtAction.Rename, "oldname newname");

            // Act
            var result = _parser.GenerateLine(parameters);

            // Assert
            result.Should().Be("Mgmt RENAME oldname newname");
        }

        [TestMethod]
        public void GenerateLine_WithSortActionAndParameters_ShouldReturnCorrectString()
        {
            // Arrange
            var parameters = new MgmtParams(MgmtAction.Sort, "measurement");

            // Act
            var result = _parser.GenerateLine(parameters);

            // Assert
            result.Should().Be("Mgmt SORT measurement");
        }

        [TestMethod]
        public void GenerateLine_WithCleanActionAndParameters_ShouldReturnCorrectString()
        {
            // Arrange
            var parameters = new MgmtParams(MgmtAction.Clean, "measurement");

            // Act
            var result = _parser.GenerateLine(parameters);

            // Assert
            result.Should().Be("Mgmt CLEAN measurement");
        }

        [TestMethod]
        public void GenerateLine_WithBlacklistActionAndParameters_ShouldReturnCorrectString()
        {
            // Arrange
            var parameters = new MgmtParams(MgmtAction.Blacklist, "measurement");

            // Act
            var result = _parser.GenerateLine(parameters);

            // Assert
            result.Should().Be("Mgmt BLACKLIST measurement");
        }

        [TestMethod]
        public void GenerateLine_WithUnBlacklistActionAndParameters_ShouldReturnCorrectString()
        {
            // Arrange
            var parameters = new MgmtParams(MgmtAction.UnBlacklist, "measurement");

            // Act
            var result = _parser.GenerateLine(parameters);

            // Assert
            result.Should().Be("Mgmt UNBLACKLIST measurement");
        }

    }
}