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
    public class Op_Mgmt : Op_Base, IKalaQlOperation
    {
        public MgmtAction MgmtAction { get; }

        public Op_Mgmt(MgmtAction mgmtAction)
        {
            
            this.MgmtAction = mgmtAction;
        }

        public override bool CanExecute(KalaQlContext context)
        {
            return true;
        }

        public override void Execute(KalaQlContext context)
        {
           if (MgmtAction == MgmtAction.LoadMeasures)
            {
                var result = context.DataLayer.LoadMeasurementList();
                context.Result = new KalaResult();
                context.Result.MeasureList = result;
                this.hasExecuted = true;
            }            
        }

        public override string ToString()
        {
            return $"Op_Mgmt: {MgmtAction}";
        }
    }
}
