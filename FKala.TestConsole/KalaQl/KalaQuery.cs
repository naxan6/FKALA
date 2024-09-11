using FKala.TestConsole.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FKala.TestConsole.KalaQl
{
    public class KalaQuery
    {
        List<IKalaQlOperation> ops = new List<IKalaQlOperation>();

        public static KalaQuery Start()
        {
            return new KalaQuery();
        }
        public KalaQuery Add(IKalaQlOperation operation)
        {
            this.ops.Add(operation);
            return this;
        }

        public KalaResult? Execute(IDataLayer dataLayer)
        {
            var context = new KalaQlContext(dataLayer);
            while (true)
            {
                var nextop = ops.FirstOrDefault(op => op.CanExecute(context) && !op.HasExecuted(context));
                if (nextop != null)
                {
                    nextop.Execute(context);
                }
                else
                {
                    var notExecuted = ops.Where(op => !op.HasExecuted(context) && !op.CanExecute(context));
                    if (notExecuted.Any())
                    {
                        throw new Exception("KalaQuery could not execute : " + string.Join(", ", notExecuted.Select(x => x.ToString())));
                    } else
                    {
                        break;
                    }
                }
            }
            var result = context.Result;
            context.Result = null;
            return result;
        }
    }
}
