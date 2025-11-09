using FKala.Core.DataLayer.Infrastructure;
using FKala.Core.Model;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FKala.Core.KalaQl
{
    public class KalaResult
    {
        public List<Result_Materialized>? ResultSets { get; set; }
        public IEnumerable<Dictionary<string, object>>? ResultTable { get; set; }
        public List<string>? MeasureList { get; internal set; }
        public List<string> Errors { get; internal set; } = new List<string>();
        public List<Exception> Exceptions { get; internal set; } = new List<Exception>();
        public IEnumerable<Dictionary<string, object>>? StreamResult { get; internal set; }

        /// <summary>
        /// läuft alle Punkte durch, teils für Cache oder MatView nötig
        /// </summary>
        public void ConsumeResultSetsNoOutput()
        {
            foreach (var rs in this.ResultSets) // iterate to load everything

                foreach (var r in rs.Resultset) // iterate to load everything
                {
                    var t = r.StartTime;
                    Pools.DataPoint.Return(r);
                }
        }
    }
}
