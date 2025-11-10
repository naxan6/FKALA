using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FKala.Core.Interfaces;

namespace FKala.Core.KalaQl.QueryParser
{
    /// <summary>
    /// Registry für KalaQl-Parser
    /// </summary>
    public class KalaQlParserRegistry
    {
        private readonly List<IKalaQlParser> _parsers = new List<IKalaQlParser>();

        /// <summary>
        /// Registriert einen Parser
        /// </summary>
        /// <param name="parser">Der zu registrierende Parser</param>
        public void RegisterParser(IKalaQlParser parser)
        {
            _parsers.Add(parser);
        }

        /// <summary>
        /// Parst eine Zeile und erstellt eine Operation
        /// </summary>
        /// <param name="line">Die zu parsende Zeile</param>
        /// <param name="fields">Die bereits aufgeteilten Felder der Zeile</param>
        /// <returns>Die erstellte Operation</returns>
        public Op_Base Parse(string line, List<string> fields, List<IKalaQlOperation> previousOps)
        {
            var verb = fields[0];
            var parser = _parsers.FirstOrDefault(p => p.CanParse(verb));
            if (parser == null)
                throw new Exception($"Unkown Verb <{verb}>");

            return parser.Parse(line, fields, previousOps);
        }

        /// <summary>
        /// Generiert eine Zeile aus den angegebenen Parametern
        /// </summary>
        /// <param name="verb">Das Verb</param>
        /// <param name="parameters">Die Parameter für die Zeile</param>
        /// <returns>Die generierte Zeile</returns>
        public string GenerateLine(string verb, object parameters)
        {
            var parser = _parsers.FirstOrDefault(p => p.CanParse(verb));
            if (parser == null)
                throw new Exception($"Unkown Verb <{verb}>");

            return parser.GenerateLine(parameters);
        }
    }
}
