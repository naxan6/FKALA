using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FKala.Core.Model
{
    public class DataPoint
    {
        public DateTime Time { get; set; }
        public decimal? Value { get; set; }
        //public string? ValueText { get; set; }
    }

}
