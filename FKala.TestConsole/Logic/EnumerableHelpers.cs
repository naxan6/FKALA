using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FKala.TestConsole.Logic
{
    public class EnumerableHelpers
    {
        public static IEnumerable<T> SkipLast<T>(IEnumerable<T> source)
        {
            if (source == null)
                throw new ArgumentNullException(nameof(source));

            T prevItem = default;
            bool isFirst = true;

            foreach (T item in source)
            {
                if (!isFirst)
                    yield return prevItem;

                prevItem = item;
                isFirst = false;
            }
        }
    }
}
