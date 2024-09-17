using FKala.Core.Interfaces;

namespace FKala.Core.KalaQl
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
