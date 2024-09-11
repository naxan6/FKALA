using FKala.TestConsole.Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FKala.TestConsole.KalaQl
{
    public class KalaResult
    {
        public required KalaQlContext Context {  get; set; }
        public required List<Result> ResultSets { get; set; }
    }
}
