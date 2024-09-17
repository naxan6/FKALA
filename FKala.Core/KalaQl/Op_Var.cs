using FKala.TestConsole.Interfaces;

namespace FKala.TestConsole.KalaQl
{
    public class Op_Var : Op_Base, IKalaQlOperation
    {        
        public string VarName { get; }
        public string VarValue { get; }

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
    }
}
