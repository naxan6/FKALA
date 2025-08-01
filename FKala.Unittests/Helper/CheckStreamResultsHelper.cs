using System;
using System.Collections.Generic;
using System.Linq;

namespace FKala.Unittests.Helper
{
    public class CheckStreamResultsHelper
    {
        public static void AssertNoNullItemsInStream(IEnumerable<Dictionary<string, object>>? streamResult)
        {
            Assert.IsNotNull(streamResult);
            foreach (var w in streamResult)
            {
                Assert.IsNotNull(w);
            }
        }
    }
}