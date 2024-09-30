using FKala.Core.DataLayer.Infrastructure;
using FKala.Core.Interfaces;
using FKala.Core.KalaQl;
using FKala.Core.Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace FKala.Core.Logic
{
    public static class DatasetsCombiner2
    {
        private class ResultEnumerator
        {
            public required ResultPromise Result;
            public required IEnumerator<DataPoint> Enumerator;
        }

        public static IEnumerable<IGrouping<(DateTime, DateTime), (ResultPromise Result, DataPoint DataPoint)>> CombineSynchronizedResults(List<ResultPromise> results)
        {

            // Start Iterators
            var enumerators = results.Select(r =>
            {
                return new ResultEnumerator()
                {
                    Result = r,
                    Enumerator = r.ResultsetFactory().GetEnumerator()
                };
            }).ToList();

            // Consume in timley manner
            List<ResultEnumerator> consumedEnumerators = enumerators.ToList();
            DateTime previousSyncResultEnd = DateTime.MinValue;
            while (true)
            {
                bool someLeft = false;
                foreach (var result in consumedEnumerators)
                {
                    if (result.Enumerator.Current != null)
                    {
                        Pools.DataPoint.Return(result.Enumerator.Current);
                    }
                    bool success = result.Enumerator.MoveNext();
                    someLeft = someLeft | success;
                    if (!success) enumerators.Remove(result);
                }
                if (!enumerators.Any())
                {
                    yield break;
                }
                consumedEnumerators.Clear();

                var minStartTime = enumerators.Min(e => e.Enumerator.Current.StartTime);
                minStartTime = minStartTime < previousSyncResultEnd ? previousSyncResultEnd : minStartTime;                
                var minEndTime = enumerators.Min(e => e.Enumerator.Current.StartTime > minStartTime ? e.Enumerator.Current.StartTime : e.Enumerator.Current.EndTime);
                previousSyncResultEnd = minEndTime;
                List<(ResultPromise result, DataPoint datapoint)> timeMatches = new List<(ResultPromise result, DataPoint datapoint)>();
                foreach (var enumerator in enumerators)
                {
                    if (minStartTime < enumerator.Enumerator.Current.EndTime && minEndTime >= enumerator.Enumerator.Current.StartTime)
                    {
                        timeMatches.Add((enumerator.Result, enumerator.Enumerator.Current));                        
                    }
                    if (minStartTime == minEndTime &&
                        minStartTime == enumerator.Enumerator.Current.StartTime &&
                        minStartTime == enumerator.Enumerator.Current.EndTime)
                    {
                        timeMatches.Add((enumerator.Result, enumerator.Enumerator.Current));
                    }
                    if (minStartTime >= enumerator.Enumerator.Current.EndTime || minEndTime == enumerator.Enumerator.Current.EndTime)
                    {
                        consumedEnumerators.Add(enumerator);
                    }
                }

                yield return new Grouping<(DateTime, DateTime), (ResultPromise result, DataPoint DataPoint)>((minStartTime, minEndTime), timeMatches);
            }
        }

        class Grouping<TKey, TElement> : IGrouping<TKey, TElement>
        {
            private readonly IEnumerable<TElement> _elements;

            public Grouping(TKey key, IEnumerable<TElement> elements)
            {
                Key = key;
                _elements = elements;
            }

            public TKey Key { get; }

            public IEnumerator<TElement> GetEnumerator()
            {
                return _elements.GetEnumerator();
            }

            System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }
        }
    }
}
