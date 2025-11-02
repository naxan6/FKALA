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
        public KalaQuery KalaQuery { get; }
        public IDataLayer DataLayer { get; private set; }
        public List<ResultPromise> IntermediateDatasources { get; set; } = new List<ResultPromise>();
        public KalaResult Result { get; set; }
        public string? AlignTzTimeZoneId { get; set; }
        public bool Streaming { get; internal set; }

        public List<string> Warnings { get; private set; } = new List<string>();
        public List<string> Errors { get; private set; } = new List<string>();
        public KalaQlContext(KalaQuery kalaQuery, IDataLayer DataLayer)
        {
            this.KalaQuery = kalaQuery;
            this.DataLayer = DataLayer;
            this.Result = new KalaResult();
        }

        public void AddError(string msg)
        {
            Errors.Add($"ERROR: {msg}");
            Console.WriteLine(msg);
        }
        public void AddWarning(string msg)
        {
            Warnings.Add($"WARNING: {msg}");
            Console.WriteLine(msg);
        }
    }
}
