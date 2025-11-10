using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FKala.Core.Interfaces;

namespace FKala.Core.KalaQl.QueryParser
{
    /// <summary>
    /// Schnittstelle für Parser, die KalaQl-Befehle parsen können
    /// </summary>
    public interface IKalaQlParser
    {
        /// <summary>
        /// Prüft, ob dieser Parser das angegebene Verb parsen kann
        /// </summary>
        /// <param name="verb">Das zu prüfende Verb</param>
        /// <returns>True, wenn dieser Parser das Verb parsen kann, sonst False</returns>
        bool CanParse(string verb);

        /// <summary>
        /// Parst eine Zeile und erstellt eine Operation
        /// </summary>
        /// <param name="line">Die zu parsende Zeile</param>
        /// <param name="fields">Die bereits aufgeteilten Felder der Zeile</param>
        /// <returns>Die erstellte Operation</returns>
        Op_Base Parse(string line, List<string> fields, List<IKalaQlOperation> previousOps);

        /// <summary>
        /// Generiert eine Zeile aus den angegebenen Parametern
        /// </summary>
        /// <param name="parameters">Die Parameter für die Zeile</param>
        /// <returns>Die generierte Zeile</returns>
        string GenerateLine(object parameters);
    }
}
