using FKala.TestConsole.Interfaces;
using NodaTime;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FKala.TestConsole.KalaQl
{
    public class KalaQlContext
    {
        public IDataLayer DataLayer { get; private set; }
        public List<Result> IntermediateResults { get; set; } = new List<Result>();
        public KalaResult Result { get; set; }
        public string? AlignTzTimeZoneId { get; set; }

        public KalaQlContext(IDataLayer DataLayer)
        {
            this.DataLayer = DataLayer;
        }
    }
}
