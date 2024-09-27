using FKala.Core.KalaQl.Windowing;
using FKala.Core.Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FKala.Core.Interfaces
{
    public interface ICache
    {
        string CacheSubdir { get; }
        IEnumerable<DataPoint> GetAggregateForCaching(string measurement, DateTime start, DateTime end, AggregateFunction aggrFunc);
        DataPoint ReadLine(int fileyear, string? line);

        IEnumerable<DataPoint> LoadCache(DateTime startTime, DateTime endTime, int year, string yearFilePath, int readBuffer);
        void GenerateWholeYearCache(string measurement, int year, string yearFilePath, AggregateFunction aggrFunc, bool forceRebuild);
        string EnsureDirectory(string cacheDirectory);
        DateTime ShouldUpdateFromWhere(int cacheYear, DataPoint? newestInCache, DataPoint? newestInRaw);
        IEnumerable<DataPoint?> LoadNewestDatapoint(string newestFile);
        void UpdateData(string measurement, DateTime rebuildFromDateTime, AggregateFunction aggrFunc, string newestCacheFile);
    }
}
