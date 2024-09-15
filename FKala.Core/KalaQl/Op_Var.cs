using FKala.Core.Model;
using FKala.TestConsole.Interfaces;
using FKala.TestConsole.Logic;
using FKala.TestConsole.Model;
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
    public class Op_Var : Op_Base, IKalaQlOperation
    {        
        public string VarName { get; }
        public string VarValue { get; }

        public Op_Var(string VarName, string VarValue)
        {

            this.VarName = VarName;
            this.VarValue = VarValue;
            this.hasExecuted = true;
        }

        public override bool CanExecute(KalaQlContext context)
        {
            return false;
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
