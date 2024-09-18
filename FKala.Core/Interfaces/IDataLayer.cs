using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FKala.Core.DataLayer.Infrastructure;
using FKala.Core.KalaQl.Windowing;
using FKala.Core.Model;

namespace FKala.Core.Interfaces
{
    public interface IDataLayer
    {
        IEnumerable<DataPoint> LoadData(string measurement, DateTime startTime, DateTime endTime, CacheResolution cacheResolution, bool NewestOnly);
        void Insert(string kalaLinedata, bool locking = true);
        List<int> LoadAvailableYears(string measurement);
        List<string> LoadMeasurementList();
        IEnumerable<DataPoint?> LoadNewestDatapoint(string measurement);
        BufferedWriterService WriterSvc { get; }
    }
}
