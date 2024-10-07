using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FKala.Core.DataLayer.Cache;
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
        CachingLayer CachingLayer { get; }
        IEnumerable<DataPoint> LoadData(string measurement, DateTime startTime, DateTime endTime, CacheResolution cacheResolution, bool NewestOnly, KalaQl.KalaQlContext context, bool dontInvalidateCache_ForUseWhileCacheRebuild);        
        void Insert(string measurement, DataPoint dataPoint, string? source = null);
        void Insert(string kalaLinedata, string? source = null);
        List<int> LoadAvailableYears(string measurement);
        List<string> LoadMeasurementList();
        IEnumerable<DataPoint?> LoadNewestDatapoint(string measurement, KalaQl.KalaQlContext context);
        BufferedWriterService BufferedWriterSvc { get; }

        IAsyncEnumerable<Dictionary<string, object>> CopyFilesFromMeasurementToMeasurement(string measurement, string targetmeasurement, KalaQlContext context);
        IAsyncEnumerable<Dictionary<string, object>> MoveMeasurement(string measurementOld, string measurementNew, KalaQlContext context);
        IAsyncEnumerable<Dictionary<string, object>> Cleanup(string measurement, KalaQlContext context);
        void Flush();
        void Flush(string filePath);

        int ReadBuffer { get; }
        int WriteBuffer { get; }

        IAsyncEnumerable<Dictionary<string, object?>> Blacklist(string measurement);
        IAsyncEnumerable<Dictionary<string, object?>> UnBlacklist(string measurement);
        bool IsBlacklisted(string filePath, bool checkOnDisk);
        void InsertError(string err);
        IAsyncEnumerable<Dictionary<string, object?>> SortRawFiles(string measurement, KalaQlContext context);
        string GetInsertTargetFilepath(string measurement, ReadOnlySpan<char> yyyy_MM_dd);
        bool DoesMeasurementExist(string name);
        void WriteMatViewFile(string viewName, List<string> lines);
    }
}
