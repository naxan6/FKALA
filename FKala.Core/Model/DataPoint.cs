using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FKala.Core.Model
{
    public class DataPoint : IComparable<DataPoint>
    {
        public DateTime Time;
        public decimal? Value;
        //public string? ValueText { get; set; }

        public int CompareTo(DataPoint? other)
        {
            if (other == null) return 1;
            return Time.CompareTo(other.Time);
        }
    }
}