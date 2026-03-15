using FKala.Core.KalaQl;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;

namespace FKala.Unittests
{
    [TestClass]
    public class AI_ShiftTime_Integration_Tests
    {
        [TestCategory("AI")]
        [TestMethod]
        public void KalaQuery_ParseShift_ShouldCreateOp_ShiftTime()
        {
            // Arrange
            var query = KalaQuery.Start();

            // Act
            query.FromQuery("Shift shifted: loaded +2h");

            // Assert
            Assert.AreEqual(1, query.ops.Count);
            var op = query.ops[0] as Op_ShiftTime;
            Assert.IsNotNull(op);
            Assert.AreEqual("shifted", op.Name);
            Assert.AreEqual("loaded", op.InputName);
            Assert.AreEqual(TimeSpan.FromHours(2), op.Offset);
        }

        [TestCategory("AI")]
        [TestMethod]
        public void KalaQuery_ParseShift_WithNegativeOffset()
        {
            // Arrange
            var query = KalaQuery.Start();

            // Act
            query.FromQuery("Shift shifted: loaded -30m");

            // Assert
            var op = query.ops[0] as Op_ShiftTime;
            Assert.IsNotNull(op);
            Assert.AreEqual(TimeSpan.FromMinutes(-30), op.Offset);
        }

        [TestCategory("AI")]
        [TestMethod]
        public void KalaQuery_ParseShift_WithDaysHoursMinutes()
        {
            // Arrange
            var query = KalaQuery.Start();

            // Act
            query.FromQuery("Shift shifted: loaded +1d2h30m");

            // Assert
            var op = query.ops[0] as Op_ShiftTime;
            Assert.IsNotNull(op);
            Assert.AreEqual(TimeSpan.FromDays(1) + TimeSpan.FromHours(2) + TimeSpan.FromMinutes(30), op.Offset);
        }

        [TestCategory("AI")]
        [TestMethod]
        public void KalaQuery_ParseShift_WithPipeSyntax()
        {
            // Arrange
            var query = KalaQuery.Start();

            // Act
            query.FromQuery("Load loaded: measurement 2024-01-01T00:00:00Z 2024-01-01T02:00:00Z NoCache | Shift shifted: loaded +2h");

            // Assert
            Assert.AreEqual(2, query.ops.Count);
            var loadOp = query.ops[0] as Op_Load;
            Assert.IsNotNull(loadOp);
            Assert.AreEqual("loaded", loadOp.Name);
            
            var shiftOp = query.ops[1] as Op_ShiftTime;
            Assert.IsNotNull(shiftOp);
            Assert.AreEqual("shifted", shiftOp.Name);
            Assert.AreEqual("loaded", shiftOp.InputName);
        }

        [TestCategory("AI")]
        [TestMethod]
        public void KalaQuery_ToLine_ShouldReturnCorrectFormat()
        {
            // Arrange
            var query = KalaQuery.Start();
            query.FromQuery("Shift shifted: loaded +2h");

            // Act
            var lines = query.AsLines();

            // Assert
            Assert.AreEqual(1, lines.Count);
            Assert.AreEqual("Shift shifted: loaded +2h", lines[0]);
        }

        [TestCategory("AI")]
        [TestMethod]
        public void KalaQuery_ToQueryString_ShouldReturnCorrectFormat()
        {
            // Arrange
            var query = KalaQuery.Start();
            query.FromQuery("Shift shifted: loaded +2h");

            // Act
            var queryString = query.ToQueryString();

            // Assert
            Assert.AreEqual("Shift shifted: loaded +2h", queryString);
        }

        [TestCategory("AI")]
        [TestMethod]
        public void KalaQuery_ParseShift_ThrowsException_WhenInvalidFormat()
        {
            // Arrange
            var query = KalaQuery.Start();

            // Act & Assert
            Assert.ThrowsException<System.Exception>(() => query.FromQuery("Shift shifted: loaded"));
        }

        [TestCategory("AI")]
        [TestMethod]
        public void KalaQuery_ParseShift_ThrowsException_WhenInvalidDuration()
        {
            // Arrange
            var query = KalaQuery.Start();

            // Act & Assert
            Assert.ThrowsException<System.Exception>(() => query.FromQuery("Shift shifted: loaded invalid"));
        }
    }
}
