using FKala.Core.Interfaces;
using FKala.Core.Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection.Metadata.Ecma335;
using System.Text;
using System.Threading.Tasks;

namespace FKala.Core.KalaQl
{
    public class ResultPromise
    {
        public required string Name { get; set; }
        //public required IEnumerable<DataPoint> Resultset { get; set; }
        public required Func<IEnumerable<DataPoint>> ResultsetFactory { get; set; }
        public IKalaQlOperation? Creator { get; set; }
        public DateTime Query_StartTime { get; internal set; }
        public DateTime Query_EndTime { get; internal set; }

        public Result_Materialized ToResult_Materialized()
        {
            return new Result_Materialized()
            {
                Name = Name,
                Resultset = ResultsetFactory(),
                Query_StartTime = Query_StartTime,
                Query_EndTime = Query_EndTime
            };
        }
    }

    public class Result_Materialized
    {
        public required string Name { get; set; }
        public required IEnumerable<DataPoint> Resultset { get; set; }
        public DateTime Query_StartTime { get; internal set; }
        public DateTime Query_EndTime { get; internal set; }
    }
}
