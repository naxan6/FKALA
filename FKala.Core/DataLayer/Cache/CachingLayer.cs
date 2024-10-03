using FKala.Core.DataLayer.Infrastructure;
using FKala.Core.Interfaces;
using FKala.Core.KalaQl;
using FKala.Core.KalaQl.Windowing;
using FKala.Core.Logic;
using FKala.Core.Model;
using System.Collections.Concurrent;

namespace FKala.Core.DataLayer.Cache
{
    public class CachingLayer
    {
        public IDataLayer DataLayer { get; }
        public string CacheDirectory { get; }
        private readonly LockManager _lockManager = new LockManager();

        public CachingLayer(IDataLayer dataLayer, string storagePath)
        {
            DataLayer = dataLayer;
            CacheDirectory = Path.Combine(storagePath, "cache");
            Directory.CreateDirectory(CacheDirectory);
        }



        //Resolution.Hourly Window.Aligned_1Hour PrepareHourlyData ReadHourlyLine "MM-ddTHH"
        //Resolution.Minutely Window.Aligned_1Minute PrepareMinutelyData ReadMinutelyLine "MM-ddTHH:mm"


        public IEnumerable<DataPoint> LoadDataFromCache(string measurement, DateTime startTime, DateTime endTime, CacheResolution cacheResolution, KalaQl.KalaQlContext context)
        {
            var measurementPath = PathSanitizer.SanitizePath(measurement);
            ICache cache = GetCacheImplementation(cacheResolution);

            var measurementCachePath = cache.EnsureDirectory(CacheDirectory);

            var years = Enumerable.Range(startTime.Year, endTime.Year - startTime.Year + 1);
            years = FilterYearsForExistingRawData(measurement, years);
            foreach (int year in years)
            {
                if (DataLayer.CachingLayer.IsMarked2Invalidate(measurement, new DateOnly(year, 1, 1)))
                {
                    DataLayer.CachingLayer.Invalidate(measurement, new DateOnly(year, 1, 1));
                }

                string cachefile = $"{measurementPath}_{year}_{cacheResolution.AggregateFunction}.dat";
                var cacheFilePath = Path.Combine(measurementCachePath, cachefile);

                // synchronize Cache-Updates

                if (!File.Exists(cacheFilePath) || cacheResolution.ForceRebuild)
                {

                    bool cacheAlreadyInWork = _lockManager.IsLocked(cacheFilePath); // dont refresh if cache is already in work
                    {
                        using (var lockHandle = _lockManager.AcquireLock(cacheFilePath))
                        {
                            if (!cacheAlreadyInWork)
                            {
                                Console.WriteLine($"Building Cache: {cache.CacheSubdir}/{Path.GetFileName(cacheFilePath)} {cacheResolution}");
                                cache.GenerateWholeYearCache(measurement, year, cacheFilePath, cacheResolution.AggregateFunction, cacheResolution.ForceRebuild);
                            }
                        }
                    }
                }
                else
                {
                    if (cacheResolution.IncrementalRefresh && year == years.Max())
                    {
                        bool cacheAlreadyInWork = _lockManager.IsLocked(cacheFilePath); // dont refresh if cache is already in work
                        {
                            using (var lockHandle = _lockManager.AcquireLock(cacheFilePath))
                            {
                                if (!cacheAlreadyInWork)
                                {
                                    Console.WriteLine($"Incremental Update requested: {cache.CacheSubdir}/{Path.GetFileName(cacheFilePath)}");
                                    IncrementalUpdateCache(measurement, cacheResolution, cacheFilePath, context);
                                }
                            }
                        }
                    }
                }

                Console.WriteLine($"Loading from Cache: {cache.CacheSubdir}/{cachefile}");
                var yearEnumerable = cache.LoadCache(startTime, endTime, year, cacheFilePath, DataLayer.ReadBuffer);
                foreach (var item in yearEnumerable)
                {
                    yield return item;
                }

            }
        }

        

        private ICache GetCacheImplementation(CacheResolution cacheResolution)
        {
            ICache cache;
            if (cacheResolution.Resolution == Resolution.Hourly)
            {
                cache = new Cache_Hourly(DataLayer);
            }
            else if (cacheResolution.Resolution == Resolution.FiveMinutely)
            {
                cache = new Cache_5Minutely(DataLayer);
            }
            else if (cacheResolution.Resolution == Resolution.FifteenMinutely)
            {
                cache = new Cache_15Minutely(DataLayer);
            }
            else if (cacheResolution.Resolution == Resolution.Minutely)
            {
                cache = new Cache_Minutely(DataLayer);
            }
            else
            {
                throw new Exception("Bug: unsupported cacheResolution");
            }

            return cache;
        }


        public void IncrementalUpdateCache(string measurement, CacheResolution cacheResolution, string cacheFilePath, KalaQlContext context)
        {
            ICache cache = GetCacheImplementation(cacheResolution);
            var sanitizedMeasurement = PathSanitizer.SanitizePath(measurement);
            var rebuildFromDateTime = ShouldUpdateFromWhere(sanitizedMeasurement, cacheResolution.Resolution, cacheResolution.AggregateFunction, cache, context);
            if (rebuildFromDateTime != DateTime.MaxValue)
            {
                string newestCacheFile = GetNewestCacheFilepath(sanitizedMeasurement, cache, cacheResolution.AggregateFunction);
                Console.WriteLine($"Doing Incremental Update: {Path.GetFileName(newestCacheFile)}");
                var parts = newestCacheFile.Split('_');
                var fileYear = int.Parse(parts[parts.Length - 2]);
                //var validCacheEntries = cache.LoadCache(DateTime.MinValue, rebuildFromDateTime, fileYear, newestCacheFile);
                cache.UpdateData(measurement, rebuildFromDateTime, cacheResolution.AggregateFunction, newestCacheFile);
            }
            else
            {
                Console.WriteLine($"Incremental Update Not Necessary: {Path.GetFileName(cacheFilePath)}");
            }
        }

        public DateTime ShouldUpdateFromWhere(string sanitizedMeasurement, Resolution resolution, AggregateFunction aggregateFunction, ICache cache, KalaQlContext context)
        {
            string newest = GetNewestCacheFilepath(sanitizedMeasurement, cache, aggregateFunction);
            var parts = newest.Split('_');
            var cacheYear = int.Parse(parts[parts.Length - 2]);
            var newestInRaw = DataLayer.LoadNewestDatapoint(sanitizedMeasurement, context);
            var newestInCache = cache.LoadNewestDatapoint(newest);
            return cache.ShouldUpdateFromWhere(cacheYear, newestInCache.First(), newestInRaw.First());
        }



        private string GetNewestCacheFilepath(string sanitizedMeasurement, ICache cache, AggregateFunction aggregateFunction)
        {
            var measurementCachePath = Path.Combine(CacheDirectory, cache.CacheSubdir);
            var yearFileNameFilter = $"{sanitizedMeasurement}_*_{aggregateFunction}.dat";
            var newest = Directory.GetFiles(measurementCachePath, yearFileNameFilter).OrderDescending().First();
            return newest;
        }
        private IEnumerable<int> FilterYearsForExistingRawData(string measurement, IEnumerable<int> years)
        {
            var rawYears = DataLayer.LoadAvailableYears(measurement);
            years = years.Where(year => rawYears.Contains(year));
            return years;
        }

        internal void Mark2Invalidate(string measurement, DateOnly year)
        {
            var flagfile = GetInvalidateCacheFlagFile(measurement, year);
            using (_lockManager.AcquireLock(flagfile))
            {
                if (!invalidateFlagFiles.ContainsKey(flagfile))
                {
                    using (var fs = File.Create(flagfile)) { };
                    invalidateFlagFiles[flagfile] = true;
                }
            }
        }

        ConcurrentDictionary<string, bool> invalidateFlagFiles = new ConcurrentDictionary<string, bool>();

        private string GetInvalidateCacheFlagFile(string measurement, DateOnly day)
        {
            return Path.Combine(CacheDirectory, $"INVALIDATE_{measurement}_{day.Year}.flag");
        }

        internal bool IsMarked2Invalidate(string measurement, DateOnly year)
        {
            var flagfile = GetInvalidateCacheFlagFile(measurement, year);
            if (invalidateFlagFiles.ContainsKey(flagfile)) {
                return true;
            }
            else
            {
                var exists = File.Exists(GetInvalidateCacheFlagFile(measurement, year));
                if (exists)
                {
                    invalidateFlagFiles[flagfile] = true;
                }
                return exists;
            }
            
        }

        internal void Invalidate(string measurement, DateOnly year)
        {
            var flagfile = GetInvalidateCacheFlagFile(measurement, year);
            using (_lockManager.AcquireLock(flagfile))
            {
                if (File.Exists(flagfile))
                {
                    new Cache_Hourly(DataLayer).Invalidate(measurement, year);
                    new Cache_15Minutely(DataLayer).Invalidate(measurement, year);
                    new Cache_5Minutely(DataLayer).Invalidate(measurement, year);
                    new Cache_Minutely(DataLayer).Invalidate(measurement, year);
                    File.Delete(flagfile);
                    invalidateFlagFiles.Remove(flagfile, out bool trash);
                }
            }
        }
    }
}
