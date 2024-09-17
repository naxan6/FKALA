using FKala.Core.Interfaces;
using FKala.Core.Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FKala.Core.KalaQl
{
    public abstract class Op_Base : IKalaQlOperation
    {
        public bool hasExecuted { get; protected set; }
        public string? Line { get; }

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
    }
}
