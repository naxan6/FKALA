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
        public string Name { get; }
        public string? Line { get; }
        bool CanExecute(KalaQlContext context);
        List<string> GetInputNames();
        bool HasExecuted(KalaQlContext context);
        void Execute(KalaQlContext context);
        IKalaQlOperation Clone();
        string ToLine();
    }
}
