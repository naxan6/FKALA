using FKala.TestConsole.Interfaces;
using FKala.TestConsole.Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FKala.TestConsole.KalaQl
{
    public abstract class Op_Base : IKalaQlOperation
    {
        public bool hasExecuted { get; protected set; }

        public abstract bool CanExecute(KalaQlContext context);

        public abstract void Execute(KalaQlContext context);

        public bool HasExecuted(KalaQlContext context)
        {
            return hasExecuted;
        }
    }
}
