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
            return NamesToPublish.All(name => context.IntermediateResults.Any(x => x.Name == name));
        }

        public override void Execute(KalaQlContext context)
        {
            var resultsets = context.IntermediateResults
                        .Where(x => NamesToPublish.Any(ntp => ntp == x.Name))
                        .OrderBy(n => NamesToPublish.IndexOf(n.Name)) // Ausgabereihenfolge so sortieren wie vorgegeben
                        .ToList();

            if (PublishMode == PublishMode.MultipleResultsets)
            {
                context.Result = new KalaResult()
                {                    
                    ResultSets = resultsets
                };
                hasExecuted = true;
            }
            else if (PublishMode == PublishMode.CombinedResultset)
            {
                var synced = DatasetsCombiner.CombineSynchronizedResults(resultsets);

                List<ExpandoObject> resultRows = new List<ExpandoObject>();
                foreach (var syncedResult in synced)
                {
                    dynamic row = new ExpandoObject();
                    var expandoDict = (IDictionary<string, object?>)row;
                    
                    foreach (var item in syncedResult)
                    {
                        expandoDict["time"] = item.DataPoint.Time;
                        expandoDict[item.ResultName] = item.DataPoint.Value;
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
