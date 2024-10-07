using FKala.Core.DataLayer.Infrastructure;
using FKala.Core.Interfaces;
using FKala.Core.KalaQl.Windowing;
using FKala.Core.Logic;
using FKala.Core.Model;

namespace FKala.Core.KalaQl
{
    public class Op_Interpolate : Op_Base, IKalaQlOperation
    {
        public override string Name { get; }
        public string InputDataSetName { get; }
        public InterpolationMode Mode { get; }
        public decimal? ConstantValue { get; }

        public Op_Interpolate(string? line, string name, string inputDataSet, InterpolationMode mode, decimal? value) : base(line)
        {
            Name = name;
            InputDataSetName = inputDataSet;
            Mode = mode;
            ConstantValue = value;
        }

        public override bool CanExecute(KalaQlContext context)
        {
            return context.IntermediateDatasources.Any(x => x.Name == InputDataSetName);
        }

        public override void Execute(KalaQlContext context)
        {
            //var input = context.IntermediateResults.First(x => x.Name == InputDataSetName);
            // Dieses ToList ist wichtig, da bei nachfolgendem Expresso mit Vermarmelung mehrerer Serien
            // und gleichzeitiger Ausgabe aller dieser Serien im Publish
            // sich die Zugriffe auf den Enumerable überschneiden und das ganze dann buggt
            // (noch nicht final geklärt, z.B. siehe BUGTEST_KalaQl_2_Datasets_Aggregated_Expresso). 
            var input = context.IntermediateDatasources.First(x => x.Name == InputDataSetName);

            var outgoingResult =
                new ResultPromise()
                {
                    Name = this.Name,
                    Creator = this,
                    Query_StartTime = input.Query_StartTime,
                    Query_EndTime = input.Query_EndTime,
                    ResultsetFactory = () =>
                    {
                        var resultset = InternalExecute(context, input);
                        return resultset;
                    }
                };

            context.IntermediateDatasources.Add(outgoingResult);
            this.hasExecuted = true;
        }

        private IEnumerable<DataPoint> InternalExecute(KalaQlContext context, ResultPromise input)
        {
            var enumerable = input.ResultsetFactory();
            var dataPointsEnumerator = enumerable.GetEnumerator();

            if (Mode == InterpolationMode.forwards)
            {
                decimal? lastValue = ConstantValue;
                foreach (var w in enumerable)
                {
                    lastValue = w.Value ?? lastValue;
                    if (w.Value == null)
                    {
                        w.Value = lastValue;
                    }
                    yield return w;
                }
            }
            else if (Mode == InterpolationMode.backwards)
            {
                enumerable = enumerable.ToList();
                decimal? lastValue = ConstantValue;
                foreach (var w in enumerable.Reverse())
                {
                    lastValue = w.Value ?? lastValue;
                    if (w.Value == null)
                    {
                        w.Value = lastValue;
                    }
                }
                foreach (var w in enumerable)
                {
                    yield return w;
                }
            }
            else if (Mode == InterpolationMode.constant)
            {
                foreach (var w in enumerable)
                {
                    if (w.Value == null)
                    {
                        w.Value = ConstantValue;
                    }
                    yield return w;
                }
            }
        }

        public override List<string> GetInputNames()
        {
            return new List<string> { InputDataSetName };
        }

        public override IKalaQlOperation Clone()
        {
            return new Op_Interpolate(null, Name, InputDataSetName, Mode, ConstantValue);
        }

        public override string ToLine()
        {
            return $"InPo {Name}: {InputDataSetName} {Mode} {ConstantValue}";
        }
    }
}