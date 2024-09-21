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
            return IdInfo.UnknownIdentifiers.All(id => context.IntermediateDatasources.Any(res => res.Name == id));
        }

        public override void Execute(KalaQlContext context)
        {
            

            context.IntermediateDatasources.Add(
                new Result()
                {
                    Name = this.Name,
                    Creator = this,
                    ResultsetFactory = () =>
                    {
                        var unknownIdInfo = Interpreter.DetectIdentifiers(Expresso);
                        var datenquellen = context.IntermediateDatasources.Where(im => unknownIdInfo.UnknownIdentifiers.Contains(im.Name));
                        var result = ExecuteInternal(context, datenquellen);
                        return result;
                    }
                }
            );
            this.hasExecuted = true;

        }
        DateTime firstStartTime;
        public IEnumerable<DataPoint> ExecuteInternal(KalaQlContext context, IEnumerable<Result> datenquellen)
        {
            var combined = new List<(DateTime Timestamp, int ListIndex, Result Item)>();
            bool isFirstStartTime = true;
            foreach (var timeSynchronizedItems in DatasetsCombiner2.CombineSynchronizedResults(datenquellen.ToList()))
            {
                if (isFirstStartTime)
                {
                    firstStartTime = timeSynchronizedItems.First().DataPoint.Time;
                }
                var paramValues = timeSynchronizedItems.Select(si => new Parameter(si.Result.Name, si.DataPoint));

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