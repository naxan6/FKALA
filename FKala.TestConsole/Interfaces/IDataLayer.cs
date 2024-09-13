using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FKala.TestConsole.KalaQl.Windowing;
using FKala.TestConsole.Model;

namespace FKala.TestConsole.Interfaces
{
    public interface IDataLayer
    {
        IEnumerable<DataPoint> ReadData(string measurement, DateTime startTime, DateTime endTime, CacheResolution cacheResolution);
        void Insert(string rawData, bool locking = true);
    }
}
