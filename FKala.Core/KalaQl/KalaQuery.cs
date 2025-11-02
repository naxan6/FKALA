using FKala.Core.Interfaces;
using FKala.Core.KalaQl.QueryParser;
using FKala.Core.KalaQl.Windowing;
using FKala.Core.Model;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;

namespace FKala.Core.KalaQl
{
    public class KalaQuery
    {
        private readonly KalaQlParserRegistry _parserRegistry;

        public List<IKalaQlOperation> ops = new List<IKalaQlOperation>();

        List<Op_Var> opvars = new List<Op_Var>();

        public bool Streaming { get; private set; }

        public KalaQuery()
        {
            _parserRegistry = new KalaQlParserRegistry();
            RegisterParsers();
        }

        private void RegisterParsers()
        {
            _parserRegistry.RegisterParser(new AlignTimezoneParser());
            _parserRegistry.RegisterParser(new VarParser());
            _parserRegistry.RegisterParser(new LoadParser());
            _parserRegistry.RegisterParser(new JsonQueryParser());
            _parserRegistry.RegisterParser(new AggregateParser());
            _parserRegistry.RegisterParser(new InterpolateParser());
            _parserRegistry.RegisterParser(new MatViewParser());
            _parserRegistry.RegisterParser(new InsertParser());
            _parserRegistry.RegisterParser(new ExpressoParser());
            _parserRegistry.RegisterParser(new PublishParser());
            _parserRegistry.RegisterParser(new MgmtParser());
        }

        public static KalaQuery Start(bool streaming = false)
        {
            var ret = new KalaQuery();
            ret.Streaming = streaming;
            return ret;
        }
        public KalaQuery Add(IKalaQlOperation operation)
        {
            this.ops.Add(operation);
            return this;
        }

        public KalaResult Execute(IDataLayer dataLayer)
        {
            var context = new KalaQlContext(this, dataLayer);
            context.Streaming = Streaming;
            while (true)
            {
                var nextop = ops.FirstOrDefault(op => op.CanExecute(context) && !op.HasExecuted(context));
                if (nextop != null)
                {
                    try
                    {
                        nextop.Execute(context);
                    }
                    catch (Exception ex)
                    {
                        context.Result = new KalaResult();
                        context.Result.Errors.Add($"Error while processing {nextop.Line}");
                        context.Result.Errors.Add(ex.ToString());
                        return context.Result;
                    }
                }
                else
                {
                    var notExecuted = ops.Where(op => !op.HasExecuted(context) && !op.CanExecute(context));
                    if (notExecuted.Any())
                    {
                        context.Result = new KalaResult();
                        context.Result.Errors.Add("KalaQuery missing inputs for: " + string.Join(", ", notExecuted.Select(x => x.ToString())));
                        return context.Result;
                    }
                    else
                    {
                        break;
                    }
                }
            }
            return context.Result;
        }

        public KalaQuery FromQuery(string queryText)
        {
            queryText = Regex.Unescape(queryText);
            
            // Überprüfen, ob die Abfrage mehrere Operationen enthält
            if (queryText.Contains(" | "))
            {
                string[] operations = queryText.Split(" | ", StringSplitOptions.RemoveEmptyEntries);
                foreach (var operation in operations)
                {
                    var op = ParseQueryText(operation);
                    if (op != null)
                    {
                        this.Add(op);
                    }
                }
            }
            else
            {
                string[] lines = queryText.Split("\n", StringSplitOptions.RemoveEmptyEntries);
                //string[] lines = queryText.Split(new[] { '\n', '\r' }, StringSplitOptions.RemoveEmptyEntries);

                foreach (var line in lines)
                {
                    var op = ParseQueryText(line);
                    if (op != null)
                    {
                        this.Add(op);
                    }
                }
            }

            return this;
        }

        private Op_Base? ParseQueryText(string line)
        {
            foreach (var opVar in opvars)
            {
                line = opVar.Replace(line);
            }

            line = line.Trim();
            if (line.Length == 0) return null;
            if (line.StartsWith("//") || line.StartsWith("#")) return null;

            string pattern = @"(?<element>[^\s""]+)|\""(?<quotedElement>[^""]*)\""";
            var matches = Regex.Matches(line, pattern);

            List<string> fields = new List<string>();
            foreach (Match match in matches)
            {
                string field;
                if (match.Groups["quotedElement"].Success)
                {
                    field = match.Groups["quotedElement"].Value;
                }
                else if (match.Groups["element"].Success)
                {
                    field = match.Groups["element"].Value;
                }
                else
                {
                    throw new Exception("KalaTQL konnte nicht gelesen werden");
                }
                fields.Add(field);
            }

            var verb = fields[0];
            if (verb == "Var")
            {
                var opvar = new Op_Var(line, fields[1].Trim(':'), fields[2]);
                opvars.RemoveAll(e => e.VarName == opvar.VarName);
                opvars.Add(opvar);
                return opvar;
            }
            else
            {
                try
                {
                    return _parserRegistry.Parse(line, fields);
                }
                catch (Exception ex)
                {
                    throw new Exception($"Fehler beim Parsen von {line}: {ex.Message}");
                }
            }
        }

        public List<string> AsLines()
        {
            return ops.Select(op => op.ToLine()).ToList();
        }

        public string ToQueryString()
        {
            return string.Join(" | ", AsLines());
        }
    }
}
