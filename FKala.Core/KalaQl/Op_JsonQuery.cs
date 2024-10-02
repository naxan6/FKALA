using FKala.Core.DataLayer.Infrastructure;
using FKala.Core.Interfaces;
using FKala.Core.KalaQl.Windowing;
using FKala.Core.Model;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FKala.Core.KalaQl
{
    public class Op_JsonQuery : Op_Base, IKalaQlOperation
    {
        public string Name { get; }
        public string Measurement { get; }
        public DateTime StartTime { get; }
        public DateTime EndTime { get; }
        public CacheResolution CacheResolution { get; }
        public bool NewestOnly { get; }
        public string FieldPath { get; }
        public bool DontInvalidateCache_ForUseWhileCacheRebuild { get; set; } = false;

        public Op_JsonQuery(string? line, string name, string measurement, string fieldPath, DateTime startTime, DateTime endTime, CacheResolution cacheResolution, bool newestOnly = false) : base(line)
        {
            this.Name = name;
            this.Measurement = measurement;
            this.StartTime = startTime;
            this.EndTime = endTime;
            this.CacheResolution = cacheResolution;
            this.NewestOnly = newestOnly;
            this.FieldPath = fieldPath;
        }

        public override bool CanExecute(KalaQlContext context)
        {
            return true;
        }

        public override void Execute(KalaQlContext context)
        {
            //var result = context.DataLayer.LoadData(this.Measurement, this.StartTime, this.EndTime, CacheResolution, NewestOnly, DoSortRawFiles);

            //TODODODODODODDODODO REBUILD IST SOMIT DEFEKT            
            // bei ForceRebuild auch ohne Ausgabe etc. den Rebuild durchführen, ..was erst geschieht beim Materialisieren                        
            //if (CacheResolution.ForceRebuild) result = result.ToList();

            var pathParts = FieldPath.Split("/");

            context.IntermediateDatasources.Add(
                new ResultPromise()
                {
                    Name = this.Name,
                    Query_StartTime = StartTime,
                    Query_EndTime = EndTime,
                    Creator = this,
                    ResultsetFactory = () =>
                    {
                        var result = context.DataLayer.LoadData(this.Measurement, this.StartTime, this.EndTime, CacheResolution, NewestOnly, context, DontInvalidateCache_ForUseWhileCacheRebuild);
                        return ReadJson(result, pathParts);
                    }
                });
            this.hasExecuted = true;
        }

        public IEnumerable<DataPoint> ReadJson(IEnumerable<DataPoint> jsonEnum, string[]? pathParts)
        {
            int index = -1;
            if (pathParts[pathParts.Length - 1].EndsWith("]"))
            {
                var ps = pathParts[pathParts.Length - 1].Split('[');
                pathParts[pathParts.Length - 1] = ps[0];
                index = int.Parse(ps[1].TrimEnd(']'));
            }
            foreach (var item in jsonEnum)
            {
                var jsonDict = JsonConvert.DeserializeObject<Dictionary<string, object>>(item.ValueText);
                foreach (var pathPart in pathParts)
                {
                    if (jsonDict.ContainsKey(pathPart))
                    {
                        object jsonDictEntry = jsonDict[pathPart];
                        if (jsonDictEntry is JToken)
                        {
                            if ((jsonDictEntry as JToken).Type == JTokenType.Object)
                            {
                                jsonDict = (jsonDictEntry as JObject).ToObject<Dictionary<string, object>>();
                            }
                            else if ((jsonDictEntry as JToken).Type == JTokenType.Array)
                            {
                                var jarray = (jsonDictEntry as JArray);
                                var dp = Pools.DataPoint.Get();
                                dp.StartTime = item.StartTime;
                                dp.EndTime = item.EndTime;
                                dp.Source = item.Source;
                                dp.ValueText = jarray.ToList()[index].ToString();
                                yield return dp;
                            }
                        }
                        else
                        {
                            if (decimal.TryParse(jsonDictEntry.ToString(), out decimal decimalValue))
                            {
                                var dp = Pools.DataPoint.Get();
                                dp.StartTime = item.StartTime;
                                dp.EndTime = item.EndTime;
                                dp.Source = item.Source;
                                dp.Value = decimalValue;
                                yield return dp;
                            }
                            else
                            {
                                var dp = Pools.DataPoint.Get();
                                dp.StartTime = item.StartTime;
                                dp.EndTime = item.EndTime;
                                dp.Source = item.Source;
                                dp.ValueText = jsonDictEntry.ToString();
                                yield return dp;
                            }
                        }
                    }
                }
            }
        }
    }
}
