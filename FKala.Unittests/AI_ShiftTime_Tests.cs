using FKala.Core.KalaQl;
using FKala.Core.Interfaces;
using FKala.Core.Model;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using System;
using System.Collections.Generic;
using System.Linq;

namespace FKala.Unittests
{
    [TestClass]
    public class AI_ShiftTime_Tests
    {
        [TestCategory("AI")]
        [TestMethod]
        public void Op_ShiftTime_Constructor_InitializesPropertiesCorrectly()
        {
            // Arrange
            string line = "Shift shifted: loaded +2h";
            string name = "shifted";
            string inputName = "loaded";
            TimeSpan offset = TimeSpan.FromHours(2);
            string offsetString = "+2h";

            // Act
            var opShiftTime = new Op_ShiftTime(line, name, inputName, offset, offsetString);

            // Assert
            Assert.AreEqual(line, opShiftTime.Line);
            Assert.AreEqual(name, opShiftTime.Name);
            Assert.AreEqual(inputName, opShiftTime.InputName);
            Assert.AreEqual(offset, opShiftTime.Offset);
            Assert.AreEqual(offsetString, opShiftTime.OffsetString);
        }

        [TestCategory("AI")]
        [TestMethod]
        public void Op_ShiftTime_CanExecute_ReturnsTrue_WhenInputExists()
        {
            // Arrange
            var opShiftTime = new Op_ShiftTime("", "shifted", "loaded", TimeSpan.FromHours(2), "+2h");
            var context = new KalaQlContext(new KalaQuery(), null!);
            context.IntermediateDatasources.Add(new ResultPromise()
            {
                Name = "loaded",
                ResultsetFactory = () => new List<DataPoint>()
            });

            // Act & Assert
            Assert.IsTrue(opShiftTime.CanExecute(context));
        }

        [TestCategory("AI")]
        [TestMethod]
        public void Op_ShiftTime_CanExecute_ReturnsFalse_WhenInputMissing()
        {
            // Arrange
            var opShiftTime = new Op_ShiftTime("", "shifted", "loaded", TimeSpan.FromHours(2), "+2h");
            var context = new KalaQlContext(new KalaQuery(), null!);
            context.IntermediateDatasources.Add(new ResultPromise()
            {
                Name = "other",
                ResultsetFactory = () => new List<DataPoint>()
            });

            // Act & Assert
            Assert.IsFalse(opShiftTime.CanExecute(context));
        }

        [TestCategory("AI")]
        [TestMethod]
        public void Op_ShiftTime_Execute_ShiftsTimestamps()
        {
            // Arrange
            var opShiftTime = new Op_ShiftTime("", "shifted", "loaded", TimeSpan.FromHours(2), "+2h");
            var context = new KalaQlContext(new KalaQuery(), null!);
            
            var startTime = DateTime.Parse("2024-01-01T00:00:00Z");
            var endTime = DateTime.Parse("2024-01-01T01:00:00Z");
            
            var testDataPoints = new List<DataPoint>
            {
                new DataPoint() { StartTime = startTime, EndTime = endTime, Value = 10 }
            };
            
            context.IntermediateDatasources.Add(new ResultPromise()
            {
                Name = "loaded",
                Query_StartTime = startTime,
                Query_EndTime = endTime,
                ResultsetFactory = () => testDataPoints
            });

            // Act
            opShiftTime.Execute(context);

            // Assert
            var resultSource = context.IntermediateDatasources.FirstOrDefault(ds => ds.Name == "shifted");
            Assert.IsNotNull(resultSource);
            
            var resultDataPoints = resultSource.ResultsetFactory().ToList();
            Assert.AreEqual(1, resultDataPoints.Count);
            
            var shiftedPoint = resultDataPoints[0];
            Assert.AreEqual(DateTime.Parse("2024-01-01T02:00:00Z"), shiftedPoint.StartTime);
            Assert.AreEqual(DateTime.Parse("2024-01-01T03:00:00Z"), shiftedPoint.EndTime);
            Assert.AreEqual(10, shiftedPoint.Value); // Value should remain unchanged
        }

        [TestCategory("AI")]
        [TestMethod]
        public void Op_ShiftTime_Execute_ShiftsTimestamps_Negative()
        {
            // Arrange
            var opShiftTime = new Op_ShiftTime("", "shifted", "loaded", TimeSpan.FromMinutes(-30), "-30m");
            var context = new KalaQlContext(new KalaQuery(), null!);
            
            var startTime = DateTime.Parse("2024-01-01T01:00:00Z");
            var endTime = DateTime.Parse("2024-01-01T02:00:00Z");
            
            var testDataPoints = new List<DataPoint>
            {
                new DataPoint() { StartTime = startTime, EndTime = endTime, Value = 10 }
            };
            
            context.IntermediateDatasources.Add(new ResultPromise()
            {
                Name = "loaded",
                Query_StartTime = startTime,
                Query_EndTime = endTime,
                ResultsetFactory = () => testDataPoints
            });

            // Act
            opShiftTime.Execute(context);

            // Assert
            var resultSource = context.IntermediateDatasources.FirstOrDefault(ds => ds.Name == "shifted");
            Assert.IsNotNull(resultSource);
            
            var resultDataPoints = resultSource.ResultsetFactory().ToList();
            Assert.AreEqual(1, resultDataPoints.Count);
            
            var shiftedPoint = resultDataPoints[0];
            Assert.AreEqual(DateTime.Parse("2024-01-01T00:30:00Z"), shiftedPoint.StartTime);
            Assert.AreEqual(DateTime.Parse("2024-01-01T01:30:00Z"), shiftedPoint.EndTime);
        }

        [TestCategory("AI")]
        [TestMethod]
        public void Op_ShiftTime_Clone_ReturnsNewInstanceWithCopiedProperties()
        {
            // Arrange
            string line = "Shift shifted: loaded +2h";
            string name = "shifted";
            string inputName = "loaded";
            TimeSpan offset = TimeSpan.FromHours(2);
            string offsetString = "+2h";
            var opShiftTime = new Op_ShiftTime(line, name, inputName, offset, offsetString);

            // Act
            var clonedOp = (Op_ShiftTime)opShiftTime.Clone();

            // Assert
            Assert.AreNotSame(opShiftTime, clonedOp);
            Assert.AreEqual(name, clonedOp.Name);
            Assert.AreEqual(inputName, clonedOp.InputName);
            Assert.AreEqual(offset, clonedOp.Offset);
            Assert.AreEqual(offsetString, clonedOp.OffsetString);
        }

        [TestCategory("AI")]
        [TestMethod]
        public void Op_ShiftTime_ToLine_ReturnsCorrectFormat()
        {
            // Arrange
            var opShiftTime = new Op_ShiftTime("Shift shifted: loaded +2h", "shifted", "loaded", TimeSpan.FromHours(2), "+2h");

            // Act
            string result = opShiftTime.ToLine();

            // Assert
            Assert.AreEqual("Shift shifted: loaded +2h", result);
        }

        [TestCategory("AI")]
        [TestMethod]
        public void Op_ShiftTime_Verb_ReturnsShift()
        {
            // Arrange
            var opShiftTime = new Op_ShiftTime("", "shifted", "loaded", TimeSpan.FromHours(2), "+2h");

            // Act
            string result = opShiftTime.Verb();

            // Assert
            Assert.AreEqual("Shift", result);
        }

        [TestCategory("AI")]
        [TestMethod]
        public void Op_ShiftTime_GetInputNames_ReturnsInputName()
        {
            // Arrange
            var opShiftTime = new Op_ShiftTime("", "shifted", "loaded", TimeSpan.FromHours(2), "+2h");

            // Act
            var result = opShiftTime.GetInputNames();

            // Assert
            Assert.AreEqual(1, result.Count);
            Assert.AreEqual("loaded", result[0]);
        }

        [TestCategory("AI")]
        [TestMethod]
        public void Op_ShiftTime_Execute_PreservesValueText()
        {
            // Arrange
            var opShiftTime = new Op_ShiftTime("", "shifted", "loaded", TimeSpan.FromHours(2), "+2h");
            var context = new KalaQlContext(new KalaQuery(), null!);
            
            var startTime = DateTime.Parse("2024-01-01T00:00:00Z");
            var endTime = DateTime.Parse("2024-01-01T01:00:00Z");
            
            var testDataPoints = new List<DataPoint>
            {
                new DataPoint()
                {
                    StartTime = startTime,
                    EndTime = endTime,
                    Value = 10,
                    ValueText = "test value"
                }
            };
            
            context.IntermediateDatasources.Add(new ResultPromise()
            {
                Name = "loaded",
                Query_StartTime = startTime,
                Query_EndTime = endTime,
                ResultsetFactory = () => testDataPoints
            });

            // Act
            opShiftTime.Execute(context);

            // Assert
            var resultSource = context.IntermediateDatasources.FirstOrDefault(ds => ds.Name == "shifted");
            var resultDataPoints = resultSource.ResultsetFactory().ToList();
            
            var shiftedPoint = resultDataPoints[0];
            Assert.AreEqual("test value", shiftedPoint.ValueText);
        }

        [TestCategory("AI")]
        [TestMethod]
        public void Op_ShiftTime_Execute_PreservesSource()
        {
            // Arrange
            var opShiftTime = new Op_ShiftTime("", "shifted", "loaded", TimeSpan.FromHours(2), "+2h");
            var context = new KalaQlContext(new KalaQuery(), null!);
            
            var startTime = DateTime.Parse("2024-01-01T00:00:00Z");
            var endTime = DateTime.Parse("2024-01-01T01:00:00Z");
            
            var testDataPoints = new List<DataPoint>
            {
                new DataPoint()
                {
                    StartTime = startTime,
                    EndTime = endTime,
                    Value = 10,
                    Source = "test_source"
                }
            };
            
            context.IntermediateDatasources.Add(new ResultPromise()
            {
                Name = "loaded",
                Query_StartTime = startTime,
                Query_EndTime = endTime,
                ResultsetFactory = () => testDataPoints
            });

            // Act
            opShiftTime.Execute(context);

            // Assert
            var resultSource = context.IntermediateDatasources.FirstOrDefault(ds => ds.Name == "shifted");
            var resultDataPoints = resultSource.ResultsetFactory().ToList();
            
            var shiftedPoint = resultDataPoints[0];
            Assert.AreEqual("test_source", shiftedPoint.Source);
        }

        [TestCategory("AI")]
        [TestMethod]
        public void ShiftTimeParams_Constructor_InitializesPropertiesCorrectly()
        {
            // Arrange
            string name = "shifted";
            string input = "loaded";
            TimeSpan offset = TimeSpan.FromHours(2);

            // Act
            var result = new FKala.Core.KalaQl.QueryParser.ShiftTimeParams(name, input, offset);

            // Assert
            Assert.IsNotNull(result);
            Assert.AreEqual(name, result.Name);
            Assert.AreEqual(input, result.Input);
            Assert.AreEqual(offset, result.Offset);
        }
    }
}
