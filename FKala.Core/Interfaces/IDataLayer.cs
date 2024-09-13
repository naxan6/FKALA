﻿using System;
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
        IEnumerable<DataPoint> LoadData(string measurement, DateTime startTime, DateTime endTime, CacheResolution cacheResolution);
        void Insert(string kalaLinedata, bool locking = true);
        List<int> LoadAvailableYears(string measurement);
        List<string> LoadMeasurementList(string measurement);
        DataPoint LoadNewestDatapoint(string measurement);
    }
}