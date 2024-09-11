using FKala.TestConsole.Interfaces;
using FKala.TestConsole.Model;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FKala.TestConsole.DataLayers
{
    public class KalaQlHardcoded
    {
        public IDataLayer DataLayer { get; }

        public KalaQlHardcoded(IDataLayer dataLayer) {
            this.DataLayer = dataLayer;
        }


        public IEnumerable<DataPoint> Query(string measurement, DateTime startTime, DateTime endTime)
        {
            var results = this.DataLayer.ReadData(measurement, startTime, endTime);          

            return results;
        }

        public string SerializeDatapoints(IEnumerable<DataPoint> results)
        {
            var settings = new JsonSerializerSettings
            {
                NullValueHandling = NullValueHandling.Ignore
            };
            return JsonConvert.SerializeObject(results, settings);
        }

        public IEnumerable<string> SerializeDatapointsStreamed(IEnumerable<DataPoint> results)
        {
            var settings = new JsonSerializerSettings
            {
                NullValueHandling = NullValueHandling.Ignore
            };
            foreach (var result in results)
            {
                yield return JsonConvert.SerializeObject(result, settings);
            }
        }

        public List<DataPoint> Aggregate(string measurement, DateTime startTime, DateTime endTime, TimeSpan windowSize, string aggregationFunction, bool includeEmptyIntervals = false, decimal? emptyIntervalValue = null)
        {
            if (windowSize <= TimeSpan.Zero)
            {
                throw new ArgumentException("Das Intervall muss größer als null sein.");
            }

            if (endTime - startTime < windowSize)
            {
                throw new ArgumentException("Das Intervall ist größer als der angegebene Zeitbereich.");
            }

            var dataPoints = this.DataLayer.ReadData(measurement, startTime, endTime)
                .OrderBy(dp => dp.Time)
                .ToList();

            var results = new List<DataPoint>();
            var currentIntervalStart = startTime;
            var currentIntervalEnd = startTime.Add(windowSize);

            while (currentIntervalStart < endTime)
            {
                var intervalDataPoints = dataPoints
                    .Where(dp => dp.Time >= currentIntervalStart && dp.Time < currentIntervalEnd)
                    .ToList();

                if (intervalDataPoints.Any())
                {
                    decimal? aggregatedValue;

                    switch (aggregationFunction.ToUpper())
                    {
                        case "AVG":
                            aggregatedValue = intervalDataPoints.Average(dp => dp.Value);
                            break;
                        case "FIRST":
                            aggregatedValue = intervalDataPoints.First().Value;
                            break;
                        case "LAST":
                            aggregatedValue = intervalDataPoints.Last().Value;
                            break;
                        case "MIN":
                            aggregatedValue = intervalDataPoints.Min(dp => dp.Value);
                            break;
                        case "MAX":
                            aggregatedValue = intervalDataPoints.Max(dp => dp.Value);
                            break;
                        case "COUNT":
                            aggregatedValue = intervalDataPoints.Count();
                            break;
                        default:
                            throw new ArgumentException("Ungültige Aggregationsfunktion");
                    }

                    results.Add(new DataPoint()
                    {
                        Time = currentIntervalStart,
                        Value = aggregatedValue
                    });
                }
                else if (includeEmptyIntervals)
                {
                    results.Add(new DataPoint()
                    {
                        Time = currentIntervalStart,
                        Value = emptyIntervalValue
                    });
                }

                currentIntervalStart = currentIntervalEnd;
                currentIntervalEnd = currentIntervalStart.Add(windowSize);
            }

            return results;
        }


    }
}
