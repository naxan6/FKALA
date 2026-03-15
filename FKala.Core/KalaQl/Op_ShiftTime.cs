using FKala.Core.Interfaces;
using FKala.Core.Model;
using System;
using System.Collections.Generic;
using System.Linq;

namespace FKala.Core.KalaQl
{
    /// <summary>
    /// Operation that shifts timestamps of data points by a specified duration
    /// </summary>
    public class Op_ShiftTime : Op_Base, IKalaQlOperation
    {
        private string _name = string.Empty;
        public override string Name => _name;
        public string InputName { get; private set; }
        public TimeSpan Offset { get; private set; }
        public string OffsetString { get; private set; }

        public Op_ShiftTime(string line, string name, string inputName, TimeSpan offset, string offsetString) : base(line)
        {
            _name = name;
            InputName = inputName;
            Offset = offset;
            OffsetString = offsetString;
        }

        public override bool CanExecute(KalaQlContext context)
        {
            return context.IntermediateDatasources.Any(ds => ds.Name == InputName);
        }

        public override void Execute(KalaQlContext context)
        {
            var inputSource = context.IntermediateDatasources
                .First(ds => ds.Name == InputName);

            context.IntermediateDatasources.Add(
                new ResultPromise()
                {
                    Name = this.Name,
                    Creator = this,
                    Query_StartTime = inputSource.Query_StartTime + Offset,
                    Query_EndTime = inputSource.Query_EndTime + Offset,
                    ResultsetFactory = () => ShiftDataPoints(inputSource.ResultsetFactory())
                }
            );
            this.hasExecuted = true;
        }

        private IEnumerable<DataPoint> ShiftDataPoints(IEnumerable<DataPoint> input)
        {
            foreach (var dp in input)
            {
                var shifted = dp.Clone();
                shifted.StartTime = shifted.StartTime + Offset;
                shifted.EndTime = shifted.EndTime + Offset;
                yield return shifted;
            }
        }

        public override List<string> GetInputNames()
        {
            return new List<string> { InputName };
        }

        public override IKalaQlOperation Clone()
        {
            return new Op_ShiftTime(base.Line, this.Name, this.InputName, this.Offset, this.OffsetString);
        }

        public override string ToLine()
        {
            return $"Shift {this.Name}: {this.InputName} {this.OffsetString}";
        }

        public override string Verb()
        {
            return "Shift";
        }
    }
}
