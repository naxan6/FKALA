using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FKala.TestConsole.Model
{
    public struct DataPoint
    {
        public DateTime Time { get; set; }
        public decimal? Value { get; set; }
        public string Text { get; set; }
    }

}
