using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FKala.Core.DataLayer.Infrastructure;
using FKala.Core.KalaQl;
using FKala.Core.KalaQl.Windowing;
using FKala.Core.Model;
using static FKala.Core.DataLayer_Readable_Caching_V1;

namespace FKala.Core.Interfaces
{
    public interface IDataLayer
    {
        string DataDirectory { get; }
        IEnumerable<DataPoint> LoadData(string measurement, DateTime startTime, DateTime endTime, CacheResolution cacheResolution, bool NewestOnly, bool doSortRawFiles, KalaQl.KalaQlContext context);
        void Insert(string kalaLinedata);
        void Insert(string kalaLinedata, string? source);
        List<int> LoadAvailableYears(string measurement);
        List<string> LoadMeasurementList();
        IEnumerable<DataPoint?> LoadNewestDatapoint(string measurement, KalaQl.KalaQlContext context);
        BufferedWriterService WriterSvc { get; }

        IAsyncEnumerable<Dictionary<string, object>> MergeRawFilesFromMeasurementToMeasurement(string measurement, string targetmeasurement, KalaQlContext context);
        IAsyncEnumerable<Dictionary<string, object>> MoveMeasurement(string measurementOld, string measurementNew, KalaQlContext context);
        IAsyncEnumerable<Dictionary<string, object>> Cleanup(string measurement, KalaQlContext context, bool cleanSorted);
        void Flush();
        void Flush(string filePath);

        int ReadBuffer { get; }
        int WriteBuffer { get; }

        IAsyncEnumerable<Dictionary<string, object?>> Blacklist(string measurement);
        IAsyncEnumerable<Dictionary<string, object?>> UnBlacklist(string measurement);
        bool IsBlacklisted(string filePath, bool checkOnDisk);
    }
}
