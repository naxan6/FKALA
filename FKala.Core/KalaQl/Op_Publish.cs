using FKala.Core.DataLayer.Infrastructure;
using FKala.Core.Interfaces;
using FKala.Core.Logic;

namespace FKala.Core.KalaQl
{
    public class Op_Publish : Op_Base, IKalaQlOperation
    {
        public List<string> NamesToPublish { get; private set; }
        public PublishMode PublishMode { get; }
        public int Limit { get; private set; }

        public Op_Publish(string? line, List<string> namesToPublish, PublishMode mode) : base(line)
        {
            this.NamesToPublish = namesToPublish;
            this.PublishMode = mode;
            this.Limit = 250000;
        }

        public override bool CanExecute(KalaQlContext context)
        {
            return NamesToPublish.All(name => context.IntermediateDatasources.Any(x => x.Name == name));
        }

        public async IAsyncEnumerable<Dictionary<string, object?>> ExecuteStreaming(KalaQlContext context)
        {
            var resultsets = context.IntermediateDatasources
            .Where(x => NamesToPublish.Any(ntp => ntp == x.Name))
            .OrderBy(n => NamesToPublish.IndexOf(n.Name)) // Ausgabereihenfolge so sortieren wie vorgegeben
            .ToList();

            var synced = DatasetsCombiner2.CombineSynchronizedResults(resultsets);
            Dictionary<string, bool> receivedDatapoint = new Dictionary<string, bool>();
            foreach (var key in NamesToPublish)
            {
                receivedDatapoint.Add(key, false);
            }            
            int count = 0;
            await foreach (var syncedResult in synced.AsAsyncEnumerable())
            {
                if (count++ >= Limit) // limit result to 50000 datapoints
                {
                    break;
                };
                Dictionary<string, object?> row = new Dictionary<string, object?>();

                foreach (var item in syncedResult)
                {
                    row["time"] = syncedResult.Key.Item1;
                    if (item.DataPoint.Value.HasValue)
                    {
                        row[item.Result.Name] = item.DataPoint.Value;
                    }
                    else
                    {
                        row[item.Result.Name] = item.DataPoint.ValueText;
                    }
                    if (row[item.Result.Name] != null)
                    {
                        receivedDatapoint[item.Result.Name] = true;
                    }
                }
                foreach (var key in NamesToPublish)
                {
                    if (!receivedDatapoint[key])
                    {
                        row[key] = null;
                        receivedDatapoint[key] = false;
                    }

                }
                yield return row;
            }
            hasExecuted = true;
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
            else if (PublishMode == PublishMode.CombinedResultset && context.Streaming)
            {
                context.Result = new KalaResult()
                {
                    StreamResult = ExecuteStreaming(context)
                };
                hasExecuted = true;
                
            }
                
            else if (PublishMode == PublishMode.CombinedResultset)
            {
                var synced = DatasetsCombiner2.CombineSynchronizedResults(resultsets);
                Dictionary<string, bool> receivedDatapoint = new Dictionary<string, bool>();
                foreach (var key in NamesToPublish)
                {
                    receivedDatapoint.Add(key, false);
                }
                List<Dictionary<string, object?>> resultRows = new List<Dictionary<string, object?>>();
                int count = 0;
                foreach (var syncedResult in synced)
                {
                    if (count++ >= Limit) // limit result to 50000 datapoints
                    {
                        break;
                    };
                    Dictionary<string, object?> row = new Dictionary<string, object?>();
                    
                    foreach (var item in syncedResult)
                    {
                        row["time"] = syncedResult.Key.Item1;
                        if (item.DataPoint.Value.HasValue)
                        {
                            row[item.Result.Name] = item.DataPoint.Value;
                        }
                        else
                        {
                            row[item.Result.Name] = item.DataPoint.ValueText;
                        }
                        if (row[item.Result.Name] != null)
                        {
                            receivedDatapoint[item.Result.Name] = true;
                        }                        
                    }
                    foreach (var key in NamesToPublish)
                    {
                        if (!receivedDatapoint[key])
                        {
                            row[key] = null;
                            receivedDatapoint[key] = false;
                        }
                        
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
