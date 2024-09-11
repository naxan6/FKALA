using FKala.TestConsole.KalaQl;
using FKala.TestConsole.Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FKala.TestConsole.Interfaces
{
    public interface IKalaQlOperation
    {
        bool CanExecute(KalaQlContext context);
        bool HasExecuted(KalaQlContext context);
        void Execute(KalaQlContext context);
    }
}
