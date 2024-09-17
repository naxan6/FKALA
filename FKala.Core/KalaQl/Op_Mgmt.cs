using FKala.Core.Model;
using FKala.Core.Interfaces;

namespace FKala.Core.KalaQl
{
    public class Op_Mgmt : Op_Base, IKalaQlOperation
    {
        public MgmtAction MgmtAction { get; }

        public Op_Mgmt(string line, MgmtAction mgmtAction) : base(line)
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
