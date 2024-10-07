using DynamicExpresso;
using FKala.Core.DataLayer.Infrastructure;
using FKala.Core.Interfaces;
using FKala.Core.KalaQl;
using FKala.Core.KalaQl.Windowing;
using FKala.Core.Logic;
using FKala.Core.Model;
using Newtonsoft.Json.Linq;
using System;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics.Arm;

namespace FKala.Core.KalaQl
{
    public class Op_Expresso : Op_Base, IKalaQlOperation
    {
        Interpreter Interpreter;
        Lambda Lambda;
        public override string Name { get; }
        public string Expresso { get; }
        public IdentifiersInfo UnknownIdInfo { get; private set; }

        public Op_Expresso(string? line, string name, string expresso) : base(line)
        {
            Name = name;
            Expresso = expresso;

            Interpreter = new Interpreter();
            Interpreter.SetDefaultNumberType(DefaultNumberType.Decimal);
            Interpreter.SetVariable("skip", new Skip());
            UnknownIdInfo = Interpreter.DetectIdentifiers(Expresso);
            var parameters = UnknownIdInfo.UnknownIdentifiers
                .Order()
                .Select(identifier =>
                {
                    if (identifier == "previousInput")
                    {
                        return new Parameter(identifier, typeof(Dictionary<string, DataPoint>));
                    }
                    else
                    {
                        return new Parameter(identifier, typeof(DataPoint));
                    }

                });

            Lambda = Interpreter.Parse(Expresso, parameters.ToArray());
        }

        public class Skip { }

        public override bool CanExecute(KalaQlContext context)
        {
            List<string> missingIdentifiers = GetNeededInputnames();
            return missingIdentifiers.All(id => context.IntermediateDatasources.Any(res => res.Name == id));
        }

        private List<string> GetNeededInputnames()
        {
            var missingIdentifiers = UnknownIdInfo.UnknownIdentifiers.ToList();
            missingIdentifiers.Remove("previousInput");
            missingIdentifiers.Remove("previousOutput");
            return missingIdentifiers;
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

        public IEnumerable<DataPoint> ExecuteInternal(KalaQlContext context, IEnumerable<ResultPromise> datenquellen)
        {
            Dictionary<string, DataPoint> previousInput = new Dictionary<string, DataPoint>();
            foreach (string id in UnknownIdInfo.UnknownIdentifiers)
            {
                if (id == "previousInput") continue;
                previousInput[id] = Pools.DataPoint.Get();
                previousInput[id].Value = 0;
            }


            var pInitial = Pools.DataPoint.Get();
            pInitial.Value = 0;
            DataPoint? previousOutput = pInitial;
            var combined = new List<(DateTime Timestamp, int ListIndex, ResultPromise Item)>();
            foreach (var timeSynchronizedItems in DatasetsCombiner2.CombineSynchronizedResults(datenquellen.ToList()))
            {

                var missingIdentifiers = UnknownIdInfo.UnknownIdentifiers.ToList();
                var paramValues = new List<Parameter>
                {
                    new Parameter("previousInput", previousInput),
                    new Parameter("previousOutput", previousOutput)
                };
                missingIdentifiers.Remove("previousInput");
                missingIdentifiers.Remove("previousOutput");

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
                var nextPreviousInput = new Dictionary<string, DataPoint>();
                foreach (var pv in paramValues)
                {
                    if (pv.Name == "previousInput") continue;
                    nextPreviousInput[pv.Name] = ((DataPoint)pv.Value).Clone();
                }
                object? result = Lambda.Invoke(paramValues);
                if (result is Skip)
                {
                    continue;
                }
                else
                {
                    decimal? expressoResultValue = (decimal?)result;
                    var currentDataPoint = Pools.DataPoint.Get();
                    currentDataPoint.StartTime = timeSynchronizedItems.Key.Item1;
                    currentDataPoint.EndTime = timeSynchronizedItems.Key.Item2;
                    currentDataPoint.Value = expressoResultValue;
                    previousOutput.Value = expressoResultValue;
                    yield return currentDataPoint;
                }

                //foreach (var item in previousInput.Values)
                //{
                //    Pools.DataPoint.Return(item);
                //}
                previousInput = nextPreviousInput;
            }
        }
        public override List<string> GetInputNames()
        {
            return GetNeededInputnames();
        }

        public override IKalaQlOperation Clone()
        {
            return new Op_Expresso(null, Name, Expresso);
        }

        public override string ToLine()
        {
            return $"Expr {this.Name}: \"{this.Expresso}\"";
        }
    }
}