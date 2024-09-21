using DynamicExpresso;
using FKala.Core.DataLayer.Infrastructure;
using FKala.Core.Interfaces;
using FKala.Core.KalaQl;
using FKala.Core.KalaQl.Windowing;
using FKala.Core.Logic;
using FKala.Core.Model;
using Newtonsoft.Json.Linq;
using System;

namespace FKala.Core.KalaQl
{
    public class Op_Expresso : Op_Base, IKalaQlOperation
    {
        Interpreter Interpreter;
        Lambda Lambda;
        public string Name { get; }
        public string Expresso { get; }
        public IdentifiersInfo UnknownIdInfo { get; private set; }

        public Op_Expresso(string? line, string name, string expresso) : base(line)
        {
            Name = name;
            Expresso = expresso;

            Interpreter = new Interpreter();
            Interpreter.SetDefaultNumberType(DefaultNumberType.Decimal);

            UnknownIdInfo = Interpreter.DetectIdentifiers(Expresso);
            var parameters = UnknownIdInfo.UnknownIdentifiers
                .Order()
                .Select(identifier => new Parameter(identifier, typeof(DataPoint)));

            Lambda = Interpreter.Parse(Expresso, parameters.ToArray());
        }

        public override bool CanExecute(KalaQlContext context)
        {
            var IdInfo = Interpreter.DetectIdentifiers(Expresso);
            return IdInfo.UnknownIdentifiers.All(id => context.IntermediateDatasources.Any(res => res.Name == id));
        }

        public override void Execute(KalaQlContext context)
        {
            var datenquellen = context.IntermediateDatasources.Where(im => UnknownIdInfo.UnknownIdentifiers.Contains(im.Name));

            context.IntermediateDatasources.Add(
                new ResultPromise()
                {
                    Name = this.Name,
                    Creator = this,
                    Query_StartTime = datenquellen.Min(d => d.Query_StartTime),
                    Query_EndTime = datenquellen.Min(d => d.Query_EndTime),
                    ResultsetFactory = () =>
                    {
                        var result = ExecuteInternal(context, datenquellen);
                        return result;
                    }
                }
            );
            this.hasExecuted = true;

        }
        DateTime firstStartTime;
        public IEnumerable<DataPoint> ExecuteInternal(KalaQlContext context, IEnumerable<ResultPromise> datenquellen)
        {
            var combined = new List<(DateTime Timestamp, int ListIndex, ResultPromise Item)>();
            bool isFirstStartTime = true;
            foreach (var timeSynchronizedItems in DatasetsCombiner2.CombineSynchronizedResults(datenquellen.ToList()))
            {
                if (isFirstStartTime)
                {
                    firstStartTime = timeSynchronizedItems.First().DataPoint.Time;
                }

                var missingIdentifiers = UnknownIdInfo.UnknownIdentifiers.ToList();
                var paramValues = new List<Parameter>();
                foreach (var si in timeSynchronizedItems)
                {
                    paramValues.Add(new Parameter(si.Result.Name, si.DataPoint));
                    missingIdentifiers.Remove(si.Result.Name);
                }
                var localDps = new List<DataPoint>();
                foreach (string mi in missingIdentifiers)
                {
                    var ldp = Pools.DataPoint.Get();
                    localDps.Add(ldp);
                    paramValues.Add(new Parameter(mi, ldp));
                }

                decimal? expressoResultValue = (decimal?)Lambda.Invoke(paramValues);

                var currentDataPoint = Pools.DataPoint.Get();
                currentDataPoint.Time = timeSynchronizedItems.First().DataPoint.Time;
                currentDataPoint.Value = expressoResultValue;
                foreach (var item in timeSynchronizedItems)
                {
                    Pools.DataPoint.Return(item.DataPoint);
                }
                yield return currentDataPoint;
            }
        }
    }
}