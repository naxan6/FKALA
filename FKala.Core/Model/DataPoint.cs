using FKala.Core.DataLayer.Infrastructure;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FKala.Core.Model
{
    public class DataPoint : IComparable<DataPoint>, IDisposable
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
    }
}