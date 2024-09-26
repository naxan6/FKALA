using FKala.Core.DataLayer.Infrastructure;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FKala.Core.Model
{
    public class DataPoint : IComparable<DataPoint>, IEqualityComparer<DataPoint>, IDisposable
    {
        public DateTime Time;
        public decimal? Value;
        public string? ValueText;
        public string? Source;

        public int CompareTo(DataPoint? other)
        {
            if (other == null) return 1;
            return Time.CompareTo(other.Time);
        }

        public void Dispose()
        {
            Pools.DataPoint.Return(this);
        }

        public override string ToString()
        {
            return $"{Time.ToString("s")} # {Value} # {ValueText} # # {Source}";
        }

        public bool Equals(DataPoint? x, DataPoint? y)
        {
            if (x != null && y != null)
            {
                return x.Time == y.Time && x.Value == y.Value && x.ValueText == y.ValueText;
            }
            if (x == null && y == null)
            {
                return true;
            }
            return false;
        }

        public int GetHashCode([DisallowNull] DataPoint obj)
        {
            return $"{obj.Time.Ticks} # {obj.Value} # {obj.ValueText} #".GetHashCode();
        }
    }
}