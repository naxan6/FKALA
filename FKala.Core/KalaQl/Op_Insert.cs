using FKala.Core.DataLayer.Infrastructure;
using FKala.Core.Helper;
using FKala.Core.Interfaces;
using FKala.Core.KalaQl.Windowing;
using FKala.Core.Logic;
using FKala.Core.Model;
using System.Runtime.Intrinsics.Arm;

namespace FKala.Core.KalaQl
{
    public class Op_Insert : Op_Base, IKalaQlOperation
    {
        public override string Name { get; }
        public string InputDataSetName { get; }
        public string TargetMeasure { get; }


        public Op_Insert(string? line, string name, string inputDataSet, string targetMeasure) : base(line)
        {
            Name = name;
            InputDataSetName = inputDataSet;
            TargetMeasure = targetMeasure;
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

            int count = 0;
            foreach (var dp in enumerable)
            {
                context.DataLayer.Insert(TargetMeasure, dp, $"Op_Insert <{Name}>");
                count++;
            }
            yield return new DataPoint()
            {
                StartTime = input.Query_StartTime,
                EndTime = input.Query_EndTime,
                Source = Name,
                ValueText = $"Inserted {count} datapoints into {TargetMeasure}"
            };
        }

        public override List<string> GetInputNames()
        {
            return new List<string>() { InputDataSetName };
        }

        public override IKalaQlOperation Clone()
        {
            return new Op_Insert(Line, Name, InputDataSetName, TargetMeasure);
        }

        public override string ToLine()
        {
            return $"Insert {Name}: {InputDataSetName} {TargetMeasure}";
        }
    }
}