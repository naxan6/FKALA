using FKala.Core.DataLayers;
using FKala.Core.KalaQl.Windowing;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FKala.Core.Model
{
    public class CacheResolution
    {
        public Resolution Resolution { get; set; }
        public AggregateFunction AggregateFunction { get; set; }

        public bool ForceRebuild;
        public bool IncrementalRefresh;
        
        // Speichert die ursprüngliche Fenstergröße, wenn die Resolution aus einem "AUTO(...)"-String erstellt wurde
        public long? OriginalAutoWindowSize { get; set; }

        public override string ToString()
        {
            return $"{Resolution}_{AggregateFunction}{(ForceRebuild ? "_ForceRebuild" : "")}{(IncrementalRefresh ? "_IncrementalRefresh" : "")}";
        }

    }

    public static class CacheResolutionPredefined
    {
        public static CacheResolution NoCache { get { return new CacheResolution() { Resolution = Resolution.Full, AggregateFunction = AggregateFunction.None, ForceRebuild = false }; } }
        public static CacheResolution UseHourlyAvgCache { get { return new CacheResolution() { Resolution = Resolution.Hourly, AggregateFunction = AggregateFunction.Avg, ForceRebuild = false }; } }
        public static CacheResolution UseMinutelyAvgCache { get { return new CacheResolution() { Resolution = Resolution.Hourly, AggregateFunction = AggregateFunction.Avg, ForceRebuild = false }; } }
        public static CacheResolution RebuildHourlyAvgCache { get { return new CacheResolution() { Resolution = Resolution.Hourly, AggregateFunction = AggregateFunction.Avg, ForceRebuild = true }; } }
        public static CacheResolution RebuildMinutelyAvgCache { get { return new CacheResolution() { Resolution = Resolution.Hourly, AggregateFunction = AggregateFunction.Avg, ForceRebuild = true }; } }

    }
}
