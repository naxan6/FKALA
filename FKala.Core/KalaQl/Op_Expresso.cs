using DynamicExpresso;
using FKala.Core.Interfaces;
using FKala.Core.KalaQl;
using FKala.Core.KalaQl.Windowing;
using FKala.Core.Logic;
using FKala.Core.Model;
using Newtonsoft.Json.Linq;

namespace FKala.Core.KalaQl
{
    public class Op_Expresso : Op_Base, IKalaQlOperation
    {
        Interpreter Interpreter;
        Lambda Lambda;
        public string Name { get; }
        public string Expresso { get; }


        public Op_Expresso(string? line, string name, string expresso) : base(line)
        {
            Name = name;
            Expresso = expresso;

            Interpreter = new Interpreter();
            Interpreter.SetDefaultNumberType(DefaultNumberType.Decimal);

            var unknownIdInfo = Interpreter.DetectIdentifiers(Expresso);
            var parameters = unknownIdInfo.UnknownIdentifiers
                .Order()
                .Select(identifier => new Parameter(identifier, typeof(DataPoint)));

            Lambda = Interpreter.Parse(Expresso, parameters.ToArray());
        }

        public override bool CanExecute(KalaQlContext context)
        {
            var IdInfo = Interpreter.DetectIdentifiers(Expresso);
            return IdInfo.UnknownIdentifiers.All(id => context.IntermediateResults.Any(res => res.Name == id));
        }

        public override void Execute(KalaQlContext context)
        {
            var unknownIdInfo = Interpreter.DetectIdentifiers(Expresso);
            var datenquellen = context.IntermediateResults.Where(im => unknownIdInfo.UnknownIdentifiers.Contains(im.Name));
            var firstStartTime = datenquellen.Min(d => d.StartTime);
            var lastEndTime = datenquellen.Max(d => d.EndTime);

            context.IntermediateResults.Add(
                new Result()
                {
                    Name = this.Name,
                    StartTime = firstStartTime,
                    EndTime = lastEndTime,
                    Creator = this,
                    ResultsetFactory = () =>
                    {
                        var result = ExecuteInternal(context, datenquellen);
                        return result;
                    }
                }
            );
            this.hasExecuted = true;

        }

        public IEnumerable<DataPoint> ExecuteInternal(KalaQlContext context, IEnumerable<Result> datenquellen)
        {
            var combined = new List<(DateTime Timestamp, int ListIndex, Result Item)>();

            foreach (var synchronizedItems in DatasetsCombiner2.CombineSynchronizedResults(datenquellen.ToList()))
            {
                var paramValues = synchronizedItems.Select(si => new Parameter(si.Result.Name, si.DataPoint));

                decimal? expressoResultValue = (decimal?)Lambda.Invoke(paramValues);
                var currentDataPoint = new DataPoint()
                {
                    Time = synchronizedItems.First().DataPoint.Time,
                    Value = expressoResultValue
                };
                yield return currentDataPoint;
            }
        }
    }
}