using FKala.TestConsole.Interfaces;
using FKala.TestConsole.KalaQl;
using FKala.TestConsole.Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FKala.TestConsole.Logic
{
    public static class DatasetsSynchronizer
    {
        public static IEnumerable<IGrouping<DateTime, (string ResultName, DataPoint DataPoint)>> SynchronizeResults(List<Result> results)
        {
            var enumerators = results.Select(result => result.Resultset.GetEnumerator()).ToList();
            var currentItems = new SortedList<DateTime, List<(string ResultName, DataPoint DataPoint)>>();

            for (int i = 0; i < enumerators.Count; i++)
            {
                if (enumerators[i].MoveNext())
                {
                    var current = enumerators[i].Current;
                    if (!currentItems.ContainsKey(current.Time))
                    {
                        currentItems[current.Time] = new List<(string, DataPoint)>();
                    }
                    currentItems[current.Time].Add((results[i].Name, current));
                }
            }

            while (currentItems.Count > 0)
            {
                var minTimestamp = currentItems.Keys.First();
                var itemsToProcess = currentItems[minTimestamp];

                yield return new Grouping<DateTime, (string ResultName, DataPoint DataPoint)>(minTimestamp, itemsToProcess);

                foreach (var item in itemsToProcess)
                {
                    if (enumerators[results.FindIndex(r => r.Name == item.ResultName)].MoveNext())
                    {
                        var current = enumerators[results.FindIndex(r => r.Name == item.ResultName)].Current;
                        if (!currentItems.ContainsKey(current.Time))
                        {
                            currentItems[current.Time] = new List<(string, DataPoint)>();
                        }
                        currentItems[current.Time].Add((item.ResultName, current));
                    }
                }

                currentItems.Remove(minTimestamp);
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
