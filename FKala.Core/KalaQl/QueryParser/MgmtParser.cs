using FKala.Core.Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FKala.Core.KalaQl.QueryParser
{
    /// <summary>
    /// Parser für Mgmt-Operationen
    /// </summary>
    public class MgmtParser : KalaQlParserBase
    {
        /// <summary>
        /// Prüft, ob dieser Parser das angegebene Verb parsen kann
        /// </summary>
        /// <param name="verb">Das zu prüfende Verb</param>
        /// <returns>True, wenn dieser Parser das Verb parsen kann, sonst False</returns>
        public override bool CanParse(string verb) => verb == "Mgmt";

        /// <summary>
        /// Parst eine Zeile und erstellt eine Operation
        /// </summary>
        /// <param name="line">Die zu parsende Zeile</param>
        /// <param name="fields">Die bereits aufgeteilten Felder der Zeile</param>
        /// <returns>Die erstellte Operation</returns>
        public override Op_Base Parse(string line, List<string> fields)
        {
            var action = ParseMgmtAction(fields[1]);
            var parameters = fields.Count > 2 ? String.Join(" ", fields.Skip(2)) : "";
            return new Op_Mgmt(line, action, parameters);
        }

        /// <summary>
        /// Generiert eine Zeile aus den angegebenen Parametern
        /// </summary>
        /// <param name="parameters">Die Parameter für die Zeile</param>
        /// <returns>Die generierte Zeile</returns>
        public override string GenerateLine(object parameters)
        {
            return GenerateLine((MgmtParams)parameters);
        }

        /// <summary>
        /// Generiert eine Zeile aus den angegebenen Parametern
        /// </summary>
        /// <param name="parameters">Die Parameter für die Zeile</param>
        /// <returns>Die generierte Zeile</returns>
        public string GenerateLine(MgmtParams parameters)
        {
            return $"Mgmt {GetMgmtActionString(parameters.Action)} {parameters.Parameters}".Trim();
        }

        /// <summary>
        /// Konvertiert eine MgmtAction in einen String
        /// </summary>
        /// <param name="action">Die zu konvertierende MgmtAction</param>
        /// <returns>Der konvertierte String</returns>
        private string GetMgmtActionString(MgmtAction action)
        {
            switch (action)
            {
                case MgmtAction.LoadMeasures:
                    return "LOADMEASURES";
                case MgmtAction.SortAllRaw:
                    return "SORTRAWFILES";
                case MgmtAction.ImportInflux:
                    return "IMPORTINFLUX";
                case MgmtAction.ImportMariaDbTstsfe:
                    return "IMPORTTSTSFE";
                case MgmtAction.BenchmarkIo:
                    return "BENCHIO";
                case MgmtAction.FsChk:
                    return "FSCHK";
                case MgmtAction.Copy:
                    return "COPY";
                case MgmtAction.Rename:
                    return "RENAME";
                case MgmtAction.Sort:
                    return "SORT";
                case MgmtAction.Clean:
                    return "CLEAN";
                case MgmtAction.Blacklist:
                    return "BLACKLIST";
                case MgmtAction.UnBlacklist:
                    return "UNBLACKLIST";
                default:
                    throw new Exception($"Unbekannte MgmtAction: {action}");
            }
        }
    }
}
