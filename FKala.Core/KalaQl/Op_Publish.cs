using FKala.Core.DataLayer.Infrastructure;
using FKala.Core.Interfaces;
using FKala.Core.Logic;
using System.Dynamic;

namespace FKala.Core.KalaQl
{
    public class Op_Publish : Op_Base, IKalaQlOperation
    {
        public List<string> NamesToPublish { get; private set; }
        public PublishMode PublishMode { get; }

        public Op_Publish(string? line, List<string> namesToPublish, PublishMode mode) : base(line)
        {
            this.NamesToPublish = namesToPublish;
            this.PublishMode = mode;
        }

        public override bool CanExecute(KalaQlContext context)
        {
            return NamesToPublish.All(name => context.IntermediateDatasources.Any(x => x.Name == name));
        }

        public override void Execute(KalaQlContext context)
        {
            var resultsets = context.IntermediateDatasources
                        .Where(x => NamesToPublish.Any(ntp => ntp == x.Name))
                        .OrderBy(n => NamesToPublish.IndexOf(n.Name)) // Ausgabereihenfolge so sortieren wie vorgegeben
                        .ToList();

            if (PublishMode == PublishMode.MultipleResultsets)
            {
                context.Result = new KalaResult()
                {
                    ResultSets = resultsets.Select(r => r.ToResult_Materialized()).ToList()
                };
                hasExecuted = true;
            }
            else if (PublishMode == PublishMode.CombinedResultset)
            {
                var synced = DatasetsCombiner2.CombineSynchronizedResults(resultsets);

                List<ExpandoObject> resultRows = new List<ExpandoObject>();
                int count = 0;
                foreach (var syncedResult in synced)
                {
                    if (count++ >= 50000) // limit result to 50000 datapoints
                    {
                        break;
                    };
                    dynamic row = new ExpandoObject();
                    var expandoDict = (IDictionary<string, object?>)row;
                    
                    foreach (var item in syncedResult)
                    {
                        expandoDict["time"] = item.DataPoint.Time;
                        if (item.DataPoint.Value.HasValue)
                        {
                            expandoDict[item.Result.Name] = item.DataPoint.Value;
                        }
                        else
                        {
                            expandoDict[item.Result.Name] = item.DataPoint.ValueText;
                        }
                        Pools.DataPoint.Return(item.DataPoint);
                    }
                    resultRows.Add(row);
                    
                }
                context.Result = new KalaResult()
                {                    
                    ResultTable = resultRows
                };
                hasExecuted = true;
            }
            
        }

        public override string ToString()
        {
            return $"Op_Publish: {string.Join(",", NamesToPublish)}";
        }
    }
}
