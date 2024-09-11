using FKala.TestConsole.Interfaces;
using FKala.TestConsole.Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FKala.TestConsole.KalaQl
{
    public class Op_Publish : Op_Base, IKalaQlOperation
    {
        public List<string> NamesToPublish { get; private set; }

        public Op_Publish(List<string> namesToPublish)
        {
            this.NamesToPublish = namesToPublish;
        }

        public override bool CanExecute(KalaQlContext context)
        {
            return NamesToPublish.All(name => context.IntermediateResults.Any(x => x.Name == name));
        }

        public override void Execute(KalaQlContext context)
        {
            context.Result = new KalaResult()
            {
                Context = context,
                ResultSets = context.IntermediateResults.Where(x => NamesToPublish.Any(ntp => ntp == x.Name)).ToList()
            };
            hasExecuted = true;
        }

        public override string ToString()
        {
            return $"Op_Publish: {string.Join(",", NamesToPublish)}";
        }
    }
}
