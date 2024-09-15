using DynamicExpresso;
using FKala.TestConsole.Interfaces;
using FKala.TestConsole.KalaQl;
using FKala.TestConsole.KalaQl.Windowing;
using FKala.TestConsole.Logic;
using FKala.TestConsole.Model;
using Newtonsoft.Json.Linq;

namespace FKala.TestConsole.KalaQl
{
    public class Op_Expresso : Op_Base, IKalaQlOperation
    {
        Interpreter Interpreter;
        Lambda Lambda;
        public string Name { get; }
        public string Fieldname { get; }
        public string Expresso { get; }


        public Op_Expresso(string name, string expresso)
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
            var result = ExecuteInternal(context, datenquellen);
            context.IntermediateResults.Add(new Result() { Name = this.Name, Resultset = result, StartTime = firstStartTime, EndTime = lastEndTime, Creator = this });
            this.hasExecuted = true;

        }

        public IEnumerable<DataPoint> ExecuteInternal(KalaQlContext context, IEnumerable<Result> datenquellen)
        {
            var combined = new List<(DateTime Timestamp, int ListIndex, Result Item)>();            

            foreach (var synchronizedItems in DatasetsCombiner.CombineSynchronizedResults(datenquellen.ToList()))
            {
                //var paramValues = synchronizedItems.OrderBy(si => si.ResultName).Select(dp => dp.DataPoint).ToArray();
                var paramValues = synchronizedItems.Select(si => new Parameter(si.ResultName, si.DataPoint));

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