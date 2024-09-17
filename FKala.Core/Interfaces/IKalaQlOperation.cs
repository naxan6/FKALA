using FKala.Core.KalaQl;
using FKala.Core.Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FKala.Core.Interfaces
{
    public interface IKalaQlOperation
    {
        bool CanExecute(KalaQlContext context);
        bool HasExecuted(KalaQlContext context);
        void Execute(KalaQlContext context);
    }
}
