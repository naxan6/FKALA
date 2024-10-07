using FKala.Core.DataLayer.Infrastructure;
using FKala.Core.Helper;
using FKala.Core.Interfaces;
using FKala.Core.KalaQl.Windowing;
using FKala.Core.Logic;
using FKala.Core.Model;
using System;
using System.Reflection;
using System.Runtime.Intrinsics.Arm;

namespace FKala.Core.KalaQl
{
    public class Op_MatView : Op_Base, IKalaQlOperation
    {
        public override string Name { get; }
        public string InputDataSetName { get; }
        public string ViewName { get; }


        public Op_MatView(string? line, string name, string inputDataSet, string viewName) : base(line)
        {
            Name = name;
            InputDataSetName = inputDataSet;
            ViewName = viewName;
        }

        public override bool CanExecute(KalaQlContext context)
        {
            return context.IntermediateDatasources.Any(x => x.Name == InputDataSetName);
        }

        private List<IKalaQlOperation> GetAllIntermediateDatasourcesTransitive(KalaQlContext context)
        {
            List<IKalaQlOperation> result = new List<IKalaQlOperation>();
            Queue<IKalaQlOperation> worklist = new Queue<IKalaQlOperation>();
            worklist.Enqueue(this);

            while (worklist.Count > 0)
            {
                var operation = worklist.Dequeue();
                var inputs = operation.GetInputNames();
                result.Add(operation);
                foreach (var input in inputs)
                {
                    worklist.Enqueue(context.KalaQuery.ops.First(op => op.Name == input));
                }
            }
            result.Reverse();
            return result;
        }


        public override void Execute(KalaQlContext context)
        {
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
            if (!MaterializationIsAvailable(context))
            {
                Console.WriteLine("Materializing");
                MaterializeFull(context, input);
            }
            else
            {
                Console.WriteLine("MaterializeAvail");
            }
            var transInputs = GetAllIntermediateDatasourcesTransitive(context);
            Op_Load timeFilter = transInputs.First(ti => ti is Op_Load) as Op_Load;
            return ReadFromMaterialization(context, timeFilter.StartTime, timeFilter.EndTime); // - but timefiltered!! hack: use timefilter from first found Op_Load
        }

        private IEnumerable<DataPoint> ReadFromMaterialization(KalaQlContext context, DateTime startTime, DateTime endTime)
        {
            KalaQuery matQ = KalaQuery.Start()
                .Add(new Op_Load("noline", "matq", ViewName, startTime, endTime, CacheResolutionPredefined.NoCache, false))
                .Add(new Op_Publish("noline", new List<string>() { "matq" }, PublishMode.MultipleResultsets));
            KalaResult matRes = matQ.Execute(context.DataLayer);
            return matRes.ResultSets.First().Resultset;
        }

        private bool MaterializationIsAvailable(KalaQlContext context)
        {
            return context.DataLayer.DoesMeasurementExist(ViewName);
        }

        private void MaterializeFull(KalaQlContext context, ResultPromise input)
        {
            var transInputs = GetAllIntermediateDatasourcesTransitive(context);
            var q = KalaQuery.Start();
            foreach (var trans in transInputs)
            {
                var myTrans = trans.Clone();
                if (myTrans is Op_Load)
                {
                    var load = (myTrans as Op_Load);
                    load.StartTime = DateTime.MinValue;
                    load.EndTime = DateTime.MaxValue;
                }
                if (trans != this)
                {
                    q.Add(myTrans);
                }
            }
            q.Add(new Op_Publish("noline", new List<string>() { InputDataSetName }, PublishMode.MultipleResultsets));

            

            KalaResult matRes = q.Execute(context.DataLayer);
            var enumerable = matRes.ResultSets.First().Resultset;

            //var enumerable = input.ResultsetFactory();
            var dataPointsEnumerator = enumerable.GetEnumerator();

            int count = 0;
            foreach (var dp in enumerable)
            {
                context.DataLayer.Insert(ViewName, dp, $"Op_MatView <{ViewName}>");
                count++;
            }

            List<string> lines = q.AsLines();
            context.DataLayer.WriteMatViewFile(ViewName, lines);
        }

        public override List<string> GetInputNames()
        {
            return new List<string> { this.InputDataSetName };
        }

        public override IKalaQlOperation Clone()
        {
            return new Op_MatView(null, Name, InputDataSetName, ViewName); 
        }

        public override string ToLine()
        {
            return $"MatView {Name}: {InputDataSetName} {ViewName}";
        }
    }
}