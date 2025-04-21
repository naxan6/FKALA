using FKala.Core.Interfaces;
using FKala.Core.KalaQl.Windowing;
using FKala.Core.Model;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FKala.Core.KalaQl
{
    public abstract class Op_Base : IKalaQlOperation
    {
        public bool hasExecuted { get; protected set; }
        public string? Line { get; protected set; }
        public virtual string Name { get; } = string.Empty;

        public Op_Base() { }
        public Op_Base(string? line)
        {
            this.Line = line;
        }

        public abstract bool CanExecute(KalaQlContext context);

        public abstract void Execute(KalaQlContext context);

        public bool HasExecuted(KalaQlContext context)
        {
            return hasExecuted;
        }

        public abstract List<string> GetInputNames();
        public abstract IKalaQlOperation Clone();

        public abstract string ToLine();
        
        public virtual string Verb()
        {
            throw new NotImplementedException();
        }
        
        public virtual Op_Base FromLine(string line, List<string> fields)
        {
            throw new NotImplementedException();
        }

        protected DateTime ParseDateTime(string v)
        {
            string[] dateFormats = {
                "yyyy-MM-ddTHH:mm:ss.ffffffZ",
                "yyyy-MM-ddTHH:mm:ss.ffffff",
                "yyyy-MM-ddTHH:mm:ss.fffZ",
                "yyyy-MM-ddTHH:mm:ss.fff",
                "yyyy-MM-ddTHH:mm:ssZ",
                "yyyy-MM-ddTHH:mm:ss",
                "yyyy-MM-ddZ",
                "yyyy-MM-dd"
            };
            var ci = CultureInfo.InvariantCulture;

            DateTime parsedDate;
            foreach (var format in dateFormats)
            {
                if (DateTime.TryParseExact(v, format, CultureInfo.InvariantCulture, DateTimeStyles.None, out parsedDate))
                {
                    return parsedDate.ToUniversalTime();
                }
            }
            throw new Exception($"Zeitangabe {v} ist ungültig");
        }

        protected AggregateFunction ParseAggregate(string v)
        {
            v = v.Trim().ToUpper();
            switch (v)
            {
                case "AVG":
                case "MEAN":
                    return AggregateFunction.Avg;
                case "WAVG":
                    return AggregateFunction.WAvg;
                case "FIRST":
                    return AggregateFunction.First;
                case "LAST":
                    return AggregateFunction.Last;
                case "MIN":
                    return AggregateFunction.Min;
                case "MAX":
                    return AggregateFunction.Max;
                case "COUNT":
                    return AggregateFunction.Count;
                case "SUM":
                    return AggregateFunction.Sum;
                default:
                    throw new Exception($"Unkown Aggregate <{v}>");
            }
        }
    }
}
