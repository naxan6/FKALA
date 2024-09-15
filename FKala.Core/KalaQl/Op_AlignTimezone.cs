using FKala.Core.Model;
using FKala.TestConsole.Interfaces;
using FKala.TestConsole.Logic;
using FKala.TestConsole.Model;
using NodaTime;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.XPath;
using static System.Runtime.InteropServices.JavaScript.JSType;

namespace FKala.TestConsole.KalaQl
{
    public class Op_AlignTimezone : Op_Base, IKalaQlOperation
    {
        // see https://nodatime.org/TimeZones
        public string TzId { get; }        

        public Op_AlignTimezone(string line, string timezone) : base(line)
        {

            this.TzId = timezone;            
        }
        

        public override bool CanExecute(KalaQlContext context)
        {
            context.AlignTzTimeZoneId= TzId;
            return true;
        }

        public override void Execute(KalaQlContext context)
        {
            this.hasExecuted = true;
        }

        public override string ToString()
        {
            return $"Op_AlignTimezone: {this.TzId}";
        }
    }
}
