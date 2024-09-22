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
        public IEnumerable<Dictionary<string, object?>>? ResultTable { get; set; }
        public List<string>? MeasureList { get; internal set; }
        public List<string> Errors { get; internal set; } = new List<string>();
        public IAsyncEnumerable<Dictionary<string, object?>>? StreamResult { get; internal set; }
    }
}
