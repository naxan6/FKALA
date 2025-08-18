using FKala.Core.Interfaces;
using FKala.Core.KalaQl.Windowing;
using FKala.Core.Model;
using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace FKala.Core.KalaQl
{
    public class Op_Load : Op_Base, IKalaQlOperation
    {
        private string _name = string.Empty;
        public override string Name => _name;
        public string Measurement { get; private set; }
        public DateTime StartTime { get; set; }
        public DateTime EndTime { get; set; }
        public CacheResolution CacheResolution { get; private set; }
        public string? RawCacheResolution { get; set; }
        public bool NewestOnly { get; private set; }
        public bool DoSortRawFiles { get; private set; }
        public bool DontInvalidateCache_ForUseWhileCacheRebuild { get; set; } = false;
        
        public Op_Load(string line, string name, string measurement, DateTime startTime, DateTime endTime, CacheResolution cacheResolution, bool newestOnly = false) : base(line)
        {
            this._name = name;
            this.Measurement = measurement;
            this.StartTime = startTime;
            this.EndTime = endTime;
            this.CacheResolution = cacheResolution;
            this.NewestOnly = newestOnly;            
        }

        public override bool CanExecute(KalaQlContext context)
        {
            return true;
        }

        public override void Execute(KalaQlContext context)
        {
            //in-mem copy probably mem-leak
            //var result = context.DataLayer.LoadData(this.Measurement, this.StartTime, this.EndTime, CacheResolution, NewestOnly, context, DontInvalidateCache_ForUseWhileCacheRebuild).ToList();

            context.IntermediateDatasources.Add(
                new ResultPromise()
                {
                    Name = this.Name,
                    Query_StartTime = StartTime,
                    Query_EndTime = EndTime,
                    Creator = this,
                    ResultsetFactory = () =>
                    {
                        // source file streaming
                        var result = context.DataLayer.LoadData(this.Measurement, this.StartTime, this.EndTime, CacheResolution, NewestOnly, context, DontInvalidateCache_ForUseWhileCacheRebuild);
                        return result;

                        //in-mem copy probably mem-leak
                        //return Clone(result);
                    }
                });
            this.hasExecuted = true;
        }


        private static IEnumerable<DataPoint> Clone(IEnumerable<DataPoint> input)
        {
            foreach(var dp in input)
            {
                yield return dp.Clone();
            }
        }

        public override List<string> GetInputNames()
        {
            return new List<string>();
        }

        public override IKalaQlOperation Clone()
        {
            return new Op_Load(base.Line, this.Name, this.Measurement, this.StartTime, this.EndTime, this.CacheResolution, this.NewestOnly);
        }

        public override string ToLine()
        {
            if (NewestOnly)
            {
                return $"Load {Name}: {Measurement} NewestOnly";
            }
            else
            {
                return $"Load {Name}: {Measurement} {StartTime:yyyy-MM-ddTHH:mm:ssZ} {EndTime:yyyy-MM-ddTHH:mm:ssZ} {this.RawCacheResolution}";
            }
        }

        public override string Verb()
        {
            return "Load";
        }

        public override Op_Base FromLine(string line, List<string> fields)
        {
            this.Line = line;
            this._name = fields[1].Trim(':');
            this.Measurement = fields[2];
            this.StartTime = ParseDateTime(fields[3]);
            this.EndTime = ParseDateTime(fields[4]);
            this.CacheResolution = ParseCacheResolution(fields[5]);

            if (fields[3] == "NewestOnly")
            {
                return new Op_Load(line, fields[1].Trim(':'), fields[2], DateTime.MinValue, DateTime.MaxValue, CacheResolutionPredefined.NoCache, true);
            }
            if (fields.Count < 6) throw new Exception($"6 Parameters needed. Example: Load NAME: mqtt/temperatureOutside 0001-01-01T00:00:00 9999-12-31T00:00:00 FiveMinutely_WAvg_RefreshIncremental. But got: {line}");
            return new Op_Load(line, fields[1].Trim(':'), fields[2], ParseDateTime(fields[3]), ParseDateTime(fields[4]), ParseCacheResolution(fields[5]));
        }

        private CacheResolution ParseCacheResolution(string v)
        {
            this.RawCacheResolution = v;
            v = v.Trim();

            var parts = v.Split('_');

            Resolution? resolution = ParseResolution(parts[0]);
            if (resolution != null && resolution != Resolution.Full)
            {
                var aggregate = ParseAggregate(parts[1]);
                var forceRebuild = parts.Length > 2 && parts[2].ToUpper().Contains("REBUILD");
                var refreshIncremental = parts.Length > 2 && parts[2].ToUpper().Contains("REFRESHINCREMENTAL");
                return new CacheResolution() { Resolution = resolution.Value, AggregateFunction = aggregate, ForceRebuild = forceRebuild, IncrementalRefresh = refreshIncremental };
            }
            else
            {
                return CacheResolutionPredefined.NoCache;
            }
        }

        private static Resolution? ParseResolution(string v)
        {
            if (v.ToUpper() == "MINUTELY")
            {
                return Resolution.Minutely;
            }
            else if (v.ToUpper() == "FIVEMINUTELY")
            {
                return Resolution.FiveMinutely;
            }
            else if (v.ToUpper() == "FIFTEENMINUTELY")
            {
                return Resolution.FifteenMinutely;
            }
            else if (v.ToUpper() == "HOURLY")
            {
                return Resolution.Hourly;
            }
            else if (v.ToUpper().StartsWith("AUTO("))
            {
                var parts = v.Split(['(', ')']);
                var queriedwindowsize = long.Parse(parts[1]);

                Resolution autoresolution = Resolution.Hourly;

                if (queriedwindowsize < 1 * 60 * 1000)
                {
                    autoresolution = Resolution.Full;
                }
                else if (queriedwindowsize < 5 * 60 * 1000)
                {
                    autoresolution = Resolution.Full;           // no azto-minutely cache, beacuse mostly raw data is faster
                }
                else if (queriedwindowsize < 15 * 60 * 1000)
                {
                    autoresolution = Resolution.FiveMinutely;
                }
                else if (queriedwindowsize < 60 * 60 * 1000)
                {
                    autoresolution = Resolution.FifteenMinutely;
                }
                else
                {
                    autoresolution = Resolution.Hourly;
                }

                Console.WriteLine($"autoselect cache {autoresolution} for {queriedwindowsize}");
                return autoresolution;
            }
            return null;
        }
    }
}
