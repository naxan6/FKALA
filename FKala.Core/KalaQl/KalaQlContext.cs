using FKala.Core.Interfaces;
using NodaTime;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FKala.Core.KalaQl
{
    public class KalaQlContext
    {
        public IDataLayer DataLayer { get; private set; }
        public List<ResultPromise> IntermediateDatasources { get; set; } = new List<ResultPromise>();
        public KalaResult Result { get; set; }
        public string? AlignTzTimeZoneId { get; set; }

        public KalaQlContext(IDataLayer DataLayer)
        {
            this.DataLayer = DataLayer;
            this.Result = new KalaResult();
        }
    }
}
