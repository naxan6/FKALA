using FKala.Core.Model;
using FKala.Core.Interfaces;

namespace FKala.Core.KalaQl
{
    public class Op_Mgmt : Op_Base, IKalaQlOperation
    {
        public MgmtAction MgmtAction { get; }

        public Op_Mgmt(string line, MgmtAction mgmtAction) : base(line)
        {

            this.MgmtAction = mgmtAction;
        }

        public override bool CanExecute(KalaQlContext context)
        {
            return true;
        }

        public override void Execute(KalaQlContext context)
        {
            if (MgmtAction == MgmtAction.LoadMeasures)
            {
                var result = context.DataLayer.LoadMeasurementList();
                context.Result = new KalaResult();
                context.Result.MeasureList = result;
                this.hasExecuted = true;
            }
            else if (MgmtAction == MgmtAction.SortRawFiles)
            {
                var result = context.DataLayer.LoadMeasurementList();
                foreach (var measurement in result)
                {
                    
                    var q = new KalaQuery()
                        .Add(new Op_BaseQuery("SortRawFiles", "toSort", measurement, DateTime.MinValue, DateTime.MaxValue, CacheResolutionPredefined.NoCache, false, true))
                        .Add(new Op_Publish("SortRawFiles", new List<string>() { "toSort" }, PublishMode.MultipleResultsets));
                    var localresult = q.Execute(context.DataLayer).ResultSets!.First().Resultset;

                    foreach ( var r in localresult) // iterate to load everything
                    {
                        var t = r.Time;
                    }
                    Console.WriteLine($"Sorted measurement {measurement}.");
                }
                context.Result = new KalaResult();
                context.Result.MeasureList = result;
                this.hasExecuted = true;
            }
        }

        public override string ToString()
        {
            return $"Op_Mgmt: {MgmtAction}";
        }
    }
}
