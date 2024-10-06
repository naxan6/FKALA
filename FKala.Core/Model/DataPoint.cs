using FKala.Core.DataLayer.Infrastructure;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Diagnostics.Metrics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FKala.Core.Model
{
    public class DataPoint : IComparable<DataPoint>, IEqualityComparer<DataPoint>, IDisposable
    {
        public DateTime StartTime;
        public DateTime EndTime;
        public decimal? Value;
        public string? ValueText;
        public string? Source;

        public int CompareTo(DataPoint? other)
        {
            if (other == null) return 1;
            return StartTime.CompareTo(other.StartTime);
        }

        public void Dispose()
        {
            Pools.DataPoint.Return(this);
        }

        public override string ToString()
        {
            return $"{StartTime.ToString("s")} # {Value} # {ValueText} # # {Source}";
        }

        public bool Equals(DataPoint? x, DataPoint? y)
        {
            if (x != null && y != null)
            {
                return x.StartTime == y.StartTime && x.EndTime == y.EndTime && x.Value == y.Value && x.ValueText == y.ValueText;
            }
            if (x == null && y == null)
            {
                return true;
            }
            return false;
        }

        public int GetHashCode([DisallowNull] DataPoint obj)
        {
            return $"{obj.StartTime.Ticks} # {obj.Value} # {obj.ValueText} #".GetHashCode();
        }

        public string AsLineData(string measurement)
        {
            return $"{measurement} {StartTime.ToString("yyyy-MM-ddTHH:mm:ss.fffffff")} {Value?.ToString() ?? ValueText}";
        }

        public DataPoint Clone()
        {
            var ret = Pools.DataPoint.Get();
            ret.StartTime = new DateTime(StartTime.Ticks);
            ret.EndTime = new DateTime(EndTime.Ticks);
            ret.Value = Value;
            ret.ValueText = ValueText;
            ret.Source = Source;
            return ret;
        }
    }
}