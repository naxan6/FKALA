using FKala.Core.Interfaces;
using FKala.Core.KalaQl.Windowing;
using FKala.Core.Model;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;

namespace FKala.Core.KalaQl
{
    public class KalaQuery
    {

        List<Op_Base> ops = new List<Op_Base>();

        List<Op_Var> opvars = new List<Op_Var>();

        public bool Streaming { get; private set; }

        public static KalaQuery Start(bool streaming = false)
        {
            var ret = new KalaQuery();
            ret.Streaming = streaming;
            return ret;
        }
        public KalaQuery Add(Op_Base operation)
        {
            this.ops.Add(operation);
            return this;
        }

        public KalaResult Execute(IDataLayer dataLayer)
        {
            var context = new KalaQlContext(dataLayer);
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
                        context.Result.Errors.Add("KalaQuery could not execute : " + string.Join(", ", notExecuted.Select(x => x.ToString())));
                        return context.Result;
                    }
                    else
                    {
                        break;
                    }
                }
            }
            context.Result.Errors.InsertRange(0, context.Warnings);
            context.Result.Errors.InsertRange(0, context.Errors);
            return context.Result;
        }

        public KalaQuery FromQuery(string queryText)
        {
            queryText = Regex.Unescape(queryText);
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
            switch (verb)
            {
                case "AlTz":
                    return new Op_AlignTimezone(line, fields[1]);
                case "Var":
                    var opvar = new Op_Var(line, fields[1].Trim(':'), fields[2]);
                    opvars.RemoveAll(e => e.VarName == opvar.VarName);
                    opvars.Add(opvar);
                    return opvar;
                case "Load":
                    if (fields[3] == "NewestOnly")
                    {
                        return new Op_BaseQuery(line, fields[1].Trim(':'), fields[2], DateTime.MinValue, DateTime.MaxValue, CacheResolutionPredefined.NoCache, true);
                    }
                    if (fields.Count < 6) throw new Exception($"6 Parameters needed. Example: Load NAME: mesaurename 0001-01-01T00:00:00 9999-12-31T00:00:00 NoCache. But got: {line}");
                    return new Op_BaseQuery(line, fields[1].Trim(':'), fields[2], ParseDateTime(fields[3]), ParseDateTime(fields[4]), ParseCacheResolution(fields[5]));
                case "Aggr":
                    return new Op_Aggregate(line, fields[1].Trim(':'), fields[2], ParseWindow(fields[3]), ParseAggregate(fields[4]), ParseEmptyWindows(fields.Count > 5 ? fields[5] : ""));
                case "Inpo":
                    return new Op_Interpolate(line, fields[1].Trim(':'), fields[2], ParseInterpolationMode(fields[3]), ParseDecimalNullable(fields[4]));
                case "Expr":
                    return new Op_Expresso(line, fields[1].Trim(':'), fields[2]);
                case "Publ":
                    return new Op_Publish(line, fields[1].Split(",", StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries).ToList(), ParsePublishMode(fields[2]));
                case "Mgmt":
                    return new Op_Mgmt(line, ParseMgmtAction(fields[1]), fields.Count > 2 ? String.Join(" ", fields.Skip(2)) : "");
                default:
                    throw new Exception($"Unkown Verb <{verb}>");
            }
        }

        private decimal? ParseDecimalNullable(string v)
        {
            if (v.ToUpper() == "NULL")
            {
                return null;
            }
            else
            {
                return decimal.Parse(v);
            }

        }

        private InterpolationMode ParseInterpolationMode(string v)
        {
            if (v.ToUpper() == "FORWARDS")
            {
                return InterpolationMode.forward;
            }
            else if (v.ToUpper() == "BACKWARDS")
            {
                return InterpolationMode.backwards;
            }
            else if (v.ToUpper() == "CONSTANT")
            {
                return InterpolationMode.constant;
            }
            throw new Exception($"InterpolationMode {v} is invalid");
        }

        private MgmtAction ParseMgmtAction(string v)
        {
            if (v.ToUpper() == "LOADMEASURES")
            {
                return MgmtAction.LoadMeasures;
            }
            else if (v.ToUpper() == "SORTRAWFILES")
            {
                return MgmtAction.SortAllRaw;
            }
            else if (v.ToUpper() == "IMPORTINFLUX")
            {
                return MgmtAction.ImportInflux;
            }
            else if (v.ToUpper() == "IMPORTTSTSFE")
            {
                return MgmtAction.ImportMariaDbTstsfe;
            }
            else if (v.ToUpper() == "BENCHIO")
            {
                return MgmtAction.BenchmarkIo;
            }
            else if (v.ToUpper() == "FSCHK")
            {
                return MgmtAction.FsChk;
            }
            else if (v.ToUpper() == "COPY")
            {
                return MgmtAction.Copy;
            }
            else if (v.ToUpper() == "RENAME")
            {
                return MgmtAction.Rename;
            }
            else if (v.ToUpper() == "SORT")
            {
                return MgmtAction.Sort;
            }
            else if (v.ToUpper() == "CLEAN")
            {
                return MgmtAction.Clean;
            }
            else if (v.ToUpper() == "MERGE")
            {
                return MgmtAction.Merge;
            }
            else if (v.ToUpper() == "MERGEALL")
            {
                return MgmtAction.MergeAll;
            }
            else if (v.ToUpper() == "BLACKLIST")
            {
                return MgmtAction.Blacklist;
            }
            else if (v.ToUpper() == "UNBLACKLIST")
            {
                return MgmtAction.UnBlacklist;
            }

            throw new Exception($"MgmtAction {v} is invalid");
        }

        private CacheResolution ParseCacheResolution(string v)
        {
            v = v.Trim();

            var parts = v.Split('_');

            Resolution? resolution = ParseResolution(parts[0]);
            if (resolution != null && resolution != Resolution.Full)
            {
                var aggregate = ParseAggregate(parts[1]);
                var forceRebuild = parts.Length > 2 && parts[2].ToUpper().Contains("REBUILD");
                var refreshIncremental = parts.Length > 2 && parts[2].ToUpper().Contains("REFRESHINCREMENTAL");
                return new CacheResolution() { Resolution = resolution.Value, AggregateFunction = aggregate, ForceRebuild = forceRebuild, IncrementalRefresh = refreshIncremental };
            }
            else
            {
                return CacheResolutionPredefined.NoCache;
            }
        }

        private Resolution? ParseResolution(string v)
        {
            if (v.ToUpper() == "MINUTELY")
            {
                return Resolution.Minutely;
            }
            else if (v.ToUpper() == "FIVEMINUTELY")
            {
                return Resolution.FiveMinutely;
            }
            else if (v.ToUpper() == "FIFTEENMINUTELY")
            {
                return Resolution.FifteenMinutely;
            }
            else if (v.ToUpper() == "HOURLY")
            {
                return Resolution.Hourly;
            }
            else if (v.ToUpper().StartsWith("AUTO("))
            {
                var parts = v.Split(['(', ')']);
                var queriedwindowsize = long.Parse(parts[1]);

                Resolution autoresolution = Resolution.Hourly;

                if (queriedwindowsize < 1 * 60 * 1000)
                {
                    autoresolution = Resolution.Full;
                }
                else if (queriedwindowsize < 5 * 60 * 1000)
                {
                    autoresolution = Resolution.Full;           // no azto-minutely cache, beacuse mostly raw data is faster
                }
                else if (queriedwindowsize < 15 * 60 * 1000)
                {
                    autoresolution = Resolution.FiveMinutely;
                }
                else if (queriedwindowsize < 60 * 60 * 1000)
                {
                    autoresolution = Resolution.FifteenMinutely;
                }
                else
                {
                    autoresolution = Resolution.Hourly;
                }

                Console.WriteLine($"autoselect cache {autoresolution} for {queriedwindowsize}");
                return autoresolution;
            }
            return null;
        }

        private bool ParseEmptyWindows(string v)
        {
            if (v.Contains("EmptyWindows"))
            {
                return true;
            }
            return false;
        }

        private PublishMode ParsePublishMode(string v)
        {
            v = v.Trim();
            switch (v)
            {
                case "CombinedResultset":
                case "Table":
                    return PublishMode.CombinedResultset;
                default:
                    return PublishMode.MultipleResultsets;
            }
        }

        private DateTime ParseDateTime(string v)
        {
            string[] dateFormats = {
                "yyyy-MM-ddTHH:mm:ss.ffffffZ",
                "yyyy-MM-ddTHH:mm:ss.ffffff",
                "yyyy-MM-ddTHH:mm:ss.fffZ",
                "yyyy-MM-ddTHH:mm:ss.fff",
                "yyyy-MM-ddTHH:mm:ssZ",
                "yyyy-MM-ddTHH:mm:ss",
                "yyyy-MM-ddZ",
                "yyyy-MM-dd"
            };
            var ci = CultureInfo.InvariantCulture;

            DateTime parsedDate;
            foreach (var format in dateFormats)
            {
                if (DateTime.TryParseExact(v, format, CultureInfo.InvariantCulture, DateTimeStyles.None, out parsedDate))
                {
                    return parsedDate.ToUniversalTime();
                }
            }
            throw new Exception($"Zeitangabe {v} ist ungültig");
        }

        private Window ParseWindow(string v)

        {
            v = v.Trim();
            switch (v)
            {
                case "Aligned_5Minutes":
                    return Window.Aligned_5Minutes;
                case "Aligned_15Minutes":
                    return Window.Aligned_15Minutes;
                case "Aligned_1Hour":
                    return Window.Aligned_1Hour;
                case "Aligned_1Day":
                    return Window.Aligned_1Day;
                case "Aligned_1Week":
                    return Window.Aligned_1Week;
                case "Aligned_1Month":
                    return Window.Aligned_1Month;
                case "Aligned_1YearStartAtHalf":
                    return Window.Aligned_1YearStartAtHalf;
                case "Aligned_1Year":
                    return Window.Aligned_1Year;
                case "Unaligned_1Month":
                    return Window.Unaligned_1Month;
                case "Unaligned_1Year":
                    return Window.Unaligned_1Year;
                case "Scalarize":
                case "Infinite":
                    return Window.Infinite;
                default:
                    TimeSpan timespan;
                    if (int.TryParse(v, out int vint))
                    {
                        timespan = TimeSpan.FromMilliseconds(vint);
                    }
                    else
                    {
                        timespan = TimeSpan.Parse(v);
                    }

                    return new Window(WindowMode.FixedIntervall, timespan);

            }
        }

        private AggregateFunction ParseAggregate(string v)
        {
            v = v.Trim().ToUpper();
            switch (v)
            {
                case "AVG":
                case "MEAN":
                    return AggregateFunction.Avg;
                case "WAVG":
                    return AggregateFunction.WAvg;
                case "FIRST":
                    return AggregateFunction.First;
                case "LAST":
                    return AggregateFunction.Last;
                case "MIN":
                    return AggregateFunction.Min;
                case "MAX":
                    return AggregateFunction.Max;
                case "COUNT":
                    return AggregateFunction.Count;
                case "SUM":
                    return AggregateFunction.Sum;
                default:
                    throw new Exception($"Unkown Aggregate <{v}>");
            }
        }
    }
}