using FKala.Core;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;

namespace FKala.Unittests
{
    public class DataFaker : IDisposable
    {
        public DirectoryInfo TestStorage { get; }
        public DataLayer_Readable_Caching_V1 TestDataLayer { get; }

        public DataFaker()
        {
            TestStorage = Directory.CreateTempSubdirectory("fkalaunittest");
            TestDataLayer = new DataLayer_Readable_Caching_V1(TestStorage.FullName);
        }

        public DataFaker FakeMeasure(string measure, DateTime start, DateTime end, TimeSpan distMin, TimeSpan distMax)
        {
            DateTime currentFakeTime = start;
            Random randomTime = new Random(0);
            Random randomValue = new Random(1);
            var range = distMax.Ticks - distMin.Ticks;
            long variation = randomTime.NextInt64(range);
            decimal currentFakeValue;
            currentFakeTime = currentFakeTime.Add(distMin).AddTicks(variation);
            while (currentFakeTime < end)
            {
                currentFakeValue = new decimal(randomValue.NextDouble());
                TestDataLayer.Insert($"{measure} {currentFakeTime:yyyy-MM-ddTHH:mm:ss.fffffff} {currentFakeValue.ToString(CultureInfo.InvariantCulture)}");

                variation = randomTime.NextInt64(range);
                currentFakeTime = currentFakeTime.Add(distMin).AddTicks(variation);
            }
            TestDataLayer.BufferedWriterSvc.ForceFlushWriters();
            return this;
        }
        internal void FakeMeasure_OutOfOrder(string measure, DateTime from, DateTime to, TimeSpan fromDistMinus, TimeSpan toDistPlus)
        {
            FakeMeasure(measure, from, to, fromDistMinus.Negate(), toDistPlus);
        }

        public void Dispose()
        {
            TestDataLayer.Dispose();
            TestStorage.Delete(true);
        }

        internal DataFaker FakeMeasure_OutOfOrder(string measure, DateTime dateTime)
        {
            Random randomValue = new Random((int)(dateTime.Ticks % int.MaxValue));
            var currentFakeValue = new decimal(randomValue.NextDouble());
            TestDataLayer.Insert($"{measure} {dateTime:yyyy-MM-ddTHH:mm:ss.fffffff} {currentFakeValue.ToString(CultureInfo.InvariantCulture)}");
            TestDataLayer.BufferedWriterSvc.ForceFlushWriters();
            return this;
        }
    }
}
