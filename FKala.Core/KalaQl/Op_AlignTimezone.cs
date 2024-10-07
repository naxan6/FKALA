using FKala.Core.Interfaces;

namespace FKala.Core.KalaQl
{
    public class Op_AlignTimezone : Op_Base, IKalaQlOperation
    {
        // see https://nodatime.org/TimeZones
        public string TzId { get; }

        public override string Name => throw new NotImplementedException();

        public Op_AlignTimezone(string? line, string timezone) : base(line)
        {

            this.TzId = timezone;            
        }
        

        public override bool CanExecute(KalaQlContext context)
        {            
            return true;
        }

        public override void Execute(KalaQlContext context)
        {
            context.AlignTzTimeZoneId = TzId;
            this.hasExecuted = true;
        }

        public override string ToString()
        {
            return $"Op_AlignTimezone: {this.TzId}";
        }

        public override List<string> GetInputNames()
        {
            throw new NotImplementedException();
        }

        public override IKalaQlOperation Clone()
        {
            throw new NotImplementedException();
        }

        public override string ToLine()
        {
            throw new NotImplementedException();
        }
    }
}
