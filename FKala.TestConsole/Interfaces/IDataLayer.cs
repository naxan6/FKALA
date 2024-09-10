using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FKala.TestConsole.Model;

namespace FKala.TestConsole.Interfaces
{
    public interface IDataLayer
    {
        List<DataPoint> ReadData(string measurement, DateTime startTime, DateTime endTime);
        void Insert(string rawData, bool locking = true);
    }
}
