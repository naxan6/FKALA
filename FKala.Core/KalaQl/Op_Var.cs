using FKala.Core.Interfaces;

namespace FKala.Core.KalaQl
{
    public class Op_Var : Op_Base, IKalaQlOperation
    {        
        public string VarName { get; }
        public string VarValue { get; }

        public override string Name => throw new NotImplementedException();

        public Op_Var(string line, string VarName, string VarValue) : base(line)
        {

            this.VarName = VarName;
            this.VarValue = VarValue;
            this.hasExecuted = true;
        }

        public override bool CanExecute(KalaQlContext context)
        {
            return true;
        }

        public override void Execute(KalaQlContext context)
        {
            this.hasExecuted = true;
        }

        public override string ToString()
        {
            return $"Op_Var: {VarName} -> {VarValue}";
        }

        internal string Replace(string line)
        {
            return line.Replace(this.VarName, this.VarValue);
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
