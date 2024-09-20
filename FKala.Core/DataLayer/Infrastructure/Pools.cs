using FKala.Core.Model;
using Microsoft.Extensions.ObjectPool;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FKala.Core.DataLayer.Infrastructure
{
    public static class Pools
    {
        public static DefaultObjectPool<DataPoint> DataPoint;
        static Pools()
        {
            DataPoint = new DefaultObjectPool<DataPoint>(new DpPolicy(), 100000);
        }
    }

    class DpPolicy : IPooledObjectPolicy<DataPoint>
    {
        public DataPoint Create()
        {
            return new DataPoint();
        }

        public bool Return(DataPoint obj)
        {
            obj.Source = null;
            obj.ValueText = null;
            obj.Value = null;
            obj.ValueText = null;
            obj.Time = DateTime.MinValue;
            return true;
        }
    }
}
