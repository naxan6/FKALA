using FKala.TestConsole.Interfaces;
using FKala.TestConsole.Logic;
using FKala.TestConsole.Model;
using Newtonsoft.Json;

namespace FKala.TestConsole.DataLayers
{
    public class KalaQlHardcoded
    {
        public IDataLayer DataLayer { get; }

        public KalaQlHardcoded(IDataLayer dataLayer)
        {
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

        public IEnumerable<DataPoint> Aggregate(string measurement, DateTime startTime, DateTime endTime, TimeSpan windowSize, string aggregationFunction, bool includeEmptyIntervals = false, decimal? emptyIntervalValue = null)
        {
            if (windowSize <= TimeSpan.Zero)
            {
                throw new ArgumentException("Das Intervall muss größer als null sein.");
            }

            if (endTime - startTime < windowSize)
            {
                throw new ArgumentException("Das Intervall ist größer als der angegebene Zeitbereich.");
            }

            var dataPointsEnumerator = this.DataLayer.ReadData(measurement, startTime, endTime)
                .OrderBy(dp => dp.Time).GetEnumerator();
            ;
            var currentDataPoint = new DataPoint() { Time = startTime };
            var currentAggregator = new StreamingAggregator(aggregationFunction);

            var results = new List<DataPoint>();
            var currentIntervalStart = startTime;
            var currentIntervalEnd = startTime.Add(windowSize);

            while (dataPointsEnumerator.MoveNext())
            {
                var c = dataPointsEnumerator.Current;
                if (c.Time >= currentIntervalStart && c.Time < currentIntervalEnd)
                {
                    currentAggregator.AddValue(c.Value.Value);
                }
                else if (c.Time < currentIntervalStart)
                {
                    throw new Exception("Bug 1, Datenpunkt übersehen einzusortieren");
                }
                else if (c.Time >= currentIntervalEnd)
                {
                    while (c.Time >= currentIntervalEnd)
                    {
                        currentDataPoint.Value = currentAggregator.GetAggregatedValue();
                        yield return currentDataPoint;
                        
                        currentIntervalStart = currentIntervalEnd;
                        currentIntervalEnd = currentIntervalStart.Add(windowSize);
                        
                        currentDataPoint = new DataPoint() { Time = currentIntervalStart };
                        currentAggregator = new StreamingAggregator(aggregationFunction);
                    }
                }

                // Abbruchbedingung: Interval durchlaufen
                if (currentIntervalStart >= endTime)
                {
                    if (dataPointsEnumerator.MoveNext() == true)
                    {
                        if (dataPointsEnumerator.Current.Time > startTime && dataPointsEnumerator.Current.Time < endTime)
                        {
                            throw new Exception("Bug 2, Datenpunkt übersehen einzusortieren");
                        }
                    }
                    break;
                }
            }
            // finales Interval hinzufügen
            currentDataPoint.Value = currentAggregator.GetAggregatedValue();
            yield return currentDataPoint;

            while (currentIntervalStart < endTime)
            {
                currentIntervalStart = currentIntervalEnd;
                currentIntervalEnd = currentIntervalStart.Add(windowSize);

                currentDataPoint = new DataPoint() { Time = currentIntervalStart };
                currentAggregator = new StreamingAggregator(aggregationFunction);
                currentDataPoint.Value = currentAggregator.GetAggregatedValue();
                yield return currentDataPoint;
            }
        }

       

        public List<DataPoint> AggregateSlow(string measurement, DateTime startTime, DateTime endTime, TimeSpan windowSize, string aggregationFunction, bool includeEmptyIntervals = false, decimal? emptyIntervalValue = null)
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
