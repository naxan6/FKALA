using FKala.TestConsole.Model;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FKala.TestConsole.KalaQl
{
    public class KalaResult
    {
        public List<Result> ResultSets { get; set; }
        public List<ExpandoObject> ResultTable { get; set; }
        public List<string> MeasureList { get; internal set; }
    }
}
