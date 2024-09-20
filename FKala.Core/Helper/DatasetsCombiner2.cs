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
            public required Result Result;
            public required IEnumerator<DataPoint> Enumerator;
        }

        public static IEnumerable<IGrouping<DateTime, (Result Result, DataPoint DataPoint)>> CombineSynchronizedResults(List<Result> results)
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
            while (true)
            {
                bool someLeft = false;
                foreach (var result in consumedEnumerators)
                {
                    bool success = result.Enumerator.MoveNext();
                    someLeft = someLeft | success;
                    if (!success) enumerators.Remove(result);
                }
                if (!someLeft)
                {
                    yield break;
                }
                consumedEnumerators.Clear();

                var minTime = enumerators.Min(e => e.Enumerator.Current.Time);
                List<(Result result, DataPoint datapoint)> timeMatches = new List<(Result result, DataPoint datapoint)>();
                foreach (var enumerator in enumerators)
                {
                    if (minTime == enumerator.Enumerator.Current.Time)
                    {
                        timeMatches.Add((enumerator.Result, enumerator.Enumerator.Current));
                        consumedEnumerators.Add(enumerator);
                    }
                }

                yield return new Grouping<DateTime, (Result result, DataPoint DataPoint)>(minTime, timeMatches);
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
