using FKala.TestConsole.KalaQl.Windowing;
using FKala.TestConsole.KalaQl;
using FKala.TestConsole.Logic;
using FKala.TestConsole.Model;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FKala.TestConsole.Interfaces;
using System.Runtime.ExceptionServices;
using System.Diagnostics.Metrics;
using FKala.Core.Interfaces;
using FKala.Core.DataLayers;

namespace FKala.TestConsole.DataLayers
{
    public class CachingLayer
    {
        public IDataLayer DataLayer { get; }
        public string CacheDirectory { get; }

        public CachingLayer(IDataLayer dataLayer, string storagePath)
        {
            DataLayer = dataLayer;
            this.CacheDirectory = Path.Combine(storagePath, "cache");
            Directory.CreateDirectory(CacheDirectory);
        }

        

        //Resolution.Hourly Window.Aligned_1Hour PrepareHourlyData ReadHourlyLine "MM-ddTHH"
        //Resolution.Minutely Window.Aligned_1Minute PrepareMinutelyData ReadMinutelyLine "MM-ddTHH:mm"


        public IEnumerable<DataPoint> LoadDataFromCache(string measurement, DateTime startTime, DateTime endTime, CacheResolution cacheResolution)
        {
            var measurementPath = PathSanitizer.SanitizePath(measurement);
            ICache cache = GetCacheImplementation(cacheResolution);

            var measurementCachePath = cache.EnsureDirectory(CacheDirectory);


            var years = Enumerable.Range(startTime.Year, endTime.Year - startTime.Year + 1);
            years = FilterYearsForExistingRawData(measurement, years);
            foreach (int year in years)
            {
                var cacheFilePath = Path.Combine(measurementCachePath, $"{measurementPath}_{year}_{cacheResolution.AggregateFunction}.dat");                
                if (!File.Exists(cacheFilePath) || cacheResolution.ForceRebuild)
                {
                    Console.WriteLine($"Building Cache: {Path.GetFileName(cacheFilePath)}");
                    cache.GenerateWholeYearCache(measurement, year, cacheFilePath, cacheResolution.AggregateFunction, cacheResolution.ForceRebuild);
                } 
                else
                {
                    if (cacheResolution.IncrementalRefresh && year == years.Max())
                    {
                        Console.WriteLine($"Incremental Update requested: {Path.GetFileName(cacheFilePath)}");
                        IncrementalUpdateCache(measurement, cacheResolution, cacheFilePath);
                    }
                }

                var yearEnumerable = cache.LoadCache(startTime, endTime, year, cacheFilePath);
                foreach (var item in yearEnumerable)
                {
                    yield return item;
                }
            }
        }

        private ICache GetCacheImplementation(CacheResolution cacheResolution)
        {
            ICache cache = null;
            if (cacheResolution.Resolution == Resolution.Hourly)
            {
                cache = new Cache_Hourly(this.DataLayer);
            }
            else if (cacheResolution.Resolution == Resolution.Minutely)
            {
                cache = new Cache_Minutely(this.DataLayer);
            }

            return cache;
        }


        public void IncrementalUpdateCache(string measurement, CacheResolution cacheResolution, string cacheFilePath)
        {
            ICache cache = GetCacheImplementation(cacheResolution);
            var sanitizedMeasurement = PathSanitizer.SanitizePath(measurement);
            var rebuildFromDateTime = ShouldUpdateFromWhere(sanitizedMeasurement, cacheResolution.Resolution, cacheResolution.AggregateFunction, cache);
            if (rebuildFromDateTime != DateTime.MaxValue)
            {                
                string newestCacheFile = GetNewestCacheFilepath(sanitizedMeasurement, cache, cacheResolution.AggregateFunction);
                Console.WriteLine($"Doing Incremental Update: {Path.GetFileName(newestCacheFile)}");
                var parts = newestCacheFile.Split('_');
                var fileYear = int.Parse(parts[parts.Length - 2]);
                //var validCacheEntries = cache.LoadCache(DateTime.MinValue, rebuildFromDateTime, fileYear, newestCacheFile);
                cache.UpdateData(measurement, rebuildFromDateTime, cacheResolution.AggregateFunction, newestCacheFile);
            } else
            {
                Console.WriteLine($"Incremental Update Not Necessary: {Path.GetFileName(cacheFilePath)}");
            }
        }

        public DateTime ShouldUpdateFromWhere(string sanitizedMeasurement, Resolution resolution, AggregateFunction aggregateFunction, ICache cache)
        {
            string newest = GetNewestCacheFilepath(sanitizedMeasurement, cache, aggregateFunction);
            var newestInRaw = DataLayer.LoadNewestDatapoint(sanitizedMeasurement);
            var newestInCache = cache.LoadNewestDatapoint(newest);
            return cache.ShouldUpdateFromWhere(newestInCache.First(), newestInRaw.First());
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
    }
}
