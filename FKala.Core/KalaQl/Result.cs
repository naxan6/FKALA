using FKala.Core.Interfaces;
using FKala.Core.Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FKala.Core.KalaQl
{
    public class Result : ITimeRange
    {
        public required string Name { get; set; }
        public required IEnumerable<DataPoint> Resultset { get; set; }
        public DateTime StartTime { get; set; }
        public DateTime EndTime { get; set; }
        public IKalaQlOperation? Creator { get; set; }
    }
}
