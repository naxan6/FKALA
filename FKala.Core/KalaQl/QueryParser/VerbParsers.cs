using FKala.Core.Interfaces;
using FKala.Core.KalaQl.Windowing;
using FKala.Core.Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FKala.Core.KalaQl.QueryParser
{

public static class ParserHelper
    {
        /// <summary>
        /// Konvertiert eine AggregateFunction in einen String
        /// </summary>
        /// <param name="aggregateFunction">Die zu konvertierende AggregateFunction</param>
        /// <returns>Der konvertierte String</returns>
        public static string GetAggregateFunctionString(AggregateFunction aggregateFunction)
        {
            switch (aggregateFunction)
            {
                case AggregateFunction.Avg:
                    return "AVG";
                case AggregateFunction.WAvg:
                    return "WAVG";
                case AggregateFunction.First:
                    return "FIRST";
                case AggregateFunction.Last:
                    return "LAST";
                case AggregateFunction.Min:
                    return "MIN";
                case AggregateFunction.Max:
                    return "MAX";
                case AggregateFunction.Count:
                    return "COUNT";
                case AggregateFunction.Sum:
                    return "SUM";
                default:
                    throw new Exception($"Unbekannte AggregateFunction: {aggregateFunction}");
            }
        }
    }

    /// <summary>
    /// Parser für AlignTimezone-Operationen
    /// </summary>
    public class AlignTimezoneParser : KalaQlParserBase
    {
        /// <summary>
        /// Prüft, ob dieser Parser das angegebene Verb parsen kann
        /// </summary>
        /// <param name="verb">Das zu prüfende Verb</param>
        /// <returns>True, wenn dieser Parser das Verb parsen kann, sonst False</returns>
        public override bool CanParse(string verb) => verb == "AlTz";

        /// <summary>
        /// Parst eine Zeile und erstellt eine Operation
        /// </summary>
        /// <param name="line">Die zu parsende Zeile</param>
        /// <param name="fields">Die bereits aufgeteilten Felder der Zeile</param>
        /// <returns>Die erstellte Operation</returns>
        public override Op_Base Parse(string line, List<string> fields, List<IKalaQlOperation> previousOps)
        {
            return new Op_AlignTimezone(line, fields[1]);
        }

        /// <summary>
        /// Generiert eine Zeile aus den angegebenen Parametern
        /// </summary>
        /// <param name="parameters">Die Parameter für die Zeile</param>
        /// <returns>Die generierte Zeile</returns>
        public override string GenerateLine(object parameters)
        {
            return GenerateLine((AlignTimezoneParams)parameters);
        }

        /// <summary>
        /// Generiert eine Zeile aus den angegebenen Parametern
        /// </summary>
        /// <param name="parameters">Die Parameter für die Zeile</param>
        /// <returns>Die generierte Zeile</returns>
        public static string GenerateLine(AlignTimezoneParams parameters)
        {
            return $"AlTz {parameters.Timezone}";
        }
    }

    /// <summary>
    /// Parser für Var-Operationen
    /// </summary>
    public class VarParser : KalaQlParserBase
    {
        /// <summary>
        /// Prüft, ob dieser Parser das angegebene Verb parsen kann
        /// </summary>
        /// <param name="verb">Das zu prüfende Verb</param>
        /// <returns>True, wenn dieser Parser das Verb parsen kann, sonst False</returns>
        public override bool CanParse(string verb) => verb == "Var";

        /// <summary>
        /// Parst eine Zeile und erstellt eine Operation
        /// </summary>
        /// <param name="line">Die zu parsende Zeile</param>
        /// <param name="fields">Die bereits aufgeteilten Felder der Zeile</param>
        /// <returns>Die erstellte Operation</returns>
        public override Op_Base Parse(string line, List<string> fields, List<IKalaQlOperation> previousOps)
        {
            return new Op_Var(line, fields[1].Trim(':'), fields[2]);
        }

        /// <summary>
        /// Generiert eine Zeile aus den angegebenen Parametern
        /// </summary>
        /// <param name="parameters">Die Parameter für die Zeile</param>
        /// <returns>Die generierte Zeile</returns>
        public override string GenerateLine(object parameters)
        {
            return GenerateLine((VarParams)parameters);
        }

        /// <summary>
        /// Generiert eine Zeile aus den angegebenen Parametern
        /// </summary>
        /// <param name="parameters">Die Parameter für die Zeile</param>
        /// <returns>Die generierte Zeile</returns>
        public static string GenerateLine(VarParams parameters)
        {
            return $"Var {parameters.Name}: {parameters.Value}";
        }
    }

    /// <summary>
    /// Parser für Load-Operationen
    /// </summary>
    public class LoadParser : KalaQlParserBase
    {
        /// <summary>
        /// Prüft, ob dieser Parser das angegebene Verb parsen kann
        /// </summary>
        /// <param name="verb">Das zu prüfende Verb</param>
        /// <returns>True, wenn dieser Parser das Verb parsen kann, sonst False</returns>
        public override bool CanParse(string verb) => verb == "Load";

        /// <summary>
        /// Parst eine Zeile und erstellt eine Operation
        /// </summary>
        /// <param name="line">Die zu parsende Zeile</param>
        /// <param name="fields">Die bereits aufgeteilten Felder der Zeile</param>
        /// <returns>Die erstellte Operation</returns>
        public override Op_Base Parse(string line, List<string> fields, List<IKalaQlOperation> previousOps)
        {
            if (fields[3] == "NewestOnly")
            {
                return new Op_Load(line, fields[1].Trim(':'), fields[2], DateTime.MinValue, DateTime.MaxValue, CacheResolutionPredefined.NoCache, true);
            }
            if (fields.Count < 6) throw new Exception($"6 Parameters needed. Example: Load NAME: measurename 0001-01-01T00:00:00 9999-12-31T00:00:00 NoCache. But got: {line}");
            
            var loadOp = new Op_Load(line, fields[1].Trim(':'), fields[2], ParseDateTime(fields[3], false), ParseDateTime(fields[4], true), ParseCacheResolution(fields[5]));
            
            // Setze die RawCacheResolution-Eigenschaft
            loadOp.RawCacheResolution = fields[5];
            
            return loadOp;
        }

        /// <summary>
        /// Generiert eine Zeile aus den angegebenen Parametern
        /// </summary>
        /// <param name="parameters">Die Parameter für die Zeile</param>
        /// <returns>Die generierte Zeile</returns>
        public override string GenerateLine(object parameters)
        {
            return GenerateLine((LoadParams)parameters);
        }

        /// <summary>
        /// Generiert eine Zeile aus den angegebenen Parametern
        /// </summary>
        /// <param name="parameters">Die Parameter für die Zeile</param>
        /// <returns>Die generierte Zeile</returns>
        public static string GenerateLine(LoadParams parameters)
        {
            if (parameters.NewestOnly)
            {
                return $"Load {parameters.Name}: {parameters.Measurement} NewestOnly";
            }
            else
            {
                return $"Load {parameters.Name}: {parameters.Measurement} {parameters.From:yyyy-MM-ddTHH:mm:ssZ} {parameters.To:yyyy-MM-ddTHH:mm:ssZ} {GetCacheResolutionString(parameters.CacheResolution)}";
            }
        }

        /// <summary>
        /// Konvertiert eine CacheResolution in einen String
        /// </summary>
        /// <param name="cacheResolution">Die zu konvertierende CacheResolution</param>
        /// <returns>Der konvertierte String</returns>
        public static string GetCacheResolutionString(CacheResolution cacheResolution)
        {
            // Wenn die OriginalAutoWindowSize-Eigenschaft gesetzt ist, verwenden wir das AUTO-Format
            if (cacheResolution.OriginalAutoWindowSize.HasValue)
            {
                return $"AUTO({cacheResolution.OriginalAutoWindowSize.Value})";
            }
            
            if (cacheResolution.Resolution == Resolution.Full)
            {
                return "NoCache";
            }
            else
            {
                string result = GetResolutionString(cacheResolution.Resolution) + "_" + ParserHelper.GetAggregateFunctionString(cacheResolution.AggregateFunction);
                if (cacheResolution.ForceRebuild)
                {
                    result += "_REBUILD";
                }
                if (cacheResolution.IncrementalRefresh)
                {
                    result += "_REFRESHINCREMENTAL";
                }
                return result;
            }
        }

        /// <summary>
        /// Konvertiert eine Resolution in einen String
        /// </summary>
        /// <param name="resolution">Die zu konvertierende Resolution</param>
        /// <param name="cacheResolution">Die CacheResolution, die die Resolution enthält</param>
        /// <returns>Der konvertierte String</returns>
        public static string GetResolutionString(Resolution resolution, CacheResolution cacheResolution)
        {
            // Wenn die OriginalAutoWindowSize-Eigenschaft gesetzt ist, verwenden wir das AUTO-Format
            if (cacheResolution.OriginalAutoWindowSize.HasValue)
            {
                return $"AUTO({cacheResolution.OriginalAutoWindowSize.Value})";
            }
            
            switch (resolution)
            {
                case Resolution.Minutely:
                    return "MINUTELY";
                case Resolution.FiveMinutely:
                    return "FIVEMINUTELY";
                case Resolution.FifteenMinutely:
                    return "FIFTEENMINUTELY";
                case Resolution.Hourly:
                    return "HOURLY";
                case Resolution.Full:
                    return "FULL";
                default:
                    return resolution.ToString();
            }
        }
        
        /// <summary>
        /// Konvertiert eine Resolution in einen String
        /// </summary>
        /// <param name="resolution">Die zu konvertierende Resolution</param>
        /// <returns>Der konvertierte String</returns>
        internal static string GetResolutionString(Resolution resolution)
        {
            switch (resolution)
            {
                case Resolution.Minutely:
                    return "MINUTELY";
                case Resolution.FiveMinutely:
                    return "FIVEMINUTELY";
                case Resolution.FifteenMinutely:
                    return "FIFTEENMINUTELY";
                case Resolution.Hourly:
                    return "HOURLY";
                case Resolution.Full:
                    return "FULL";
                default:
                    return resolution.ToString();
            }
        }
    }

    /// <summary>
    /// Parser für JsonQuery-Operationen
    /// </summary>
    public class JsonQueryParser : KalaQlParserBase
    {
        /// <summary>
        /// Prüft, ob dieser Parser das angegebene Verb parsen kann
        /// </summary>
        /// <param name="verb">Das zu prüfende Verb</param>
        /// <returns>True, wenn dieser Parser das Verb parsen kann, sonst False</returns>
        public override bool CanParse(string verb) => verb == "Loaj";

        /// <summary>
        /// Parst eine Zeile und erstellt eine Operation
        /// </summary>
        /// <param name="line">Die zu parsende Zeile</param>
        /// <param name="fields">Die bereits aufgeteilten Felder der Zeile</param>
        /// <returns>Die erstellte Operation</returns>
        public override Op_Base Parse(string line, List<string> fields, List<IKalaQlOperation> previousOps)
        {
            // Check if NewestOnly is the last parameter
            bool newestOnly = fields.Count > 6 && fields[6] == "NewestOnly";

            if (fields.Count < 6) throw new Exception($"6 Parameters needed. Example: Load NAME: measurename 0001-01-01T00:00:00 9999-12-31T00:00:00 NoCache. But got: {line}");

            string fieldPath;
            DateTime startTime, endTime;
            string cacheResolution;

            // Handle three different field structures:
            // Structure A (6 fields): Loaj NAME: measurement start end cache
            // Structure B (7 fields): Loaj NAME: measurement fieldpath start end cache
            // Structure C (7 fields): Loaj NAME: measurement start end cache NewestOnly

            if (fields.Count >= 7 && !newestOnly)
            {
                // Structure B: fieldpath is at index 3, start at index 4, end at index 5, cache at index 6
                fieldPath = fields[3];
                startTime = ParseDateTime(fields[4], false);
                endTime = ParseDateTime(fields[5], true);
                cacheResolution = fields[6];
            }
            else
            {
                // Structure A or C: no explicit fieldpath, use default
                fieldPath = "$.*"; // Default field path when not specified
                startTime = ParseDateTime(fields[3], false);
                endTime = ParseDateTime(fields[4], true);
                cacheResolution = fields[5];
            }
            string sourceName = ParseSourceName(fields[2], previousOps);

            return new Op_JsonQuery(line, fields[1].Trim(':'), sourceName, fieldPath, startTime, endTime, ParseCacheResolution(cacheResolution), newestOnly);
        }

        /// <summary>
        /// Generiert eine Zeile aus den angegebenen Parametern
        /// </summary>
        /// <param name="parameters">Die Parameter für die Zeile</param>
        /// <returns>Die generierte Zeile</returns>
        public override string GenerateLine(object parameters)
        {
            return GenerateLine((JsonQueryParams)parameters);
        }

        /// <summary>
        /// Generiert eine Zeile aus den angegebenen Parametern
        /// </summary>
        /// <param name="parameters">Die Parameter für die Zeile</param>
        /// <returns>Die generierte Zeile</returns>
        public static string GenerateLine(JsonQueryParams parameters)
        {
            if (parameters.NewestOnly)
            {
                return $"Loaj {parameters.Name}: {parameters.Measurement} {parameters.JsonPath} NewestOnly";
            }
            else
            {
                return $"Loaj {parameters.Name}: {parameters.Measurement} {parameters.JsonPath} {parameters.From:yyyy-MM-ddTHH:mm:ss} {parameters.To:yyyy-MM-ddTHH:mm:ss} {GetCacheResolutionString(parameters.CacheResolution)}";
            }
        }

        /// <summary>
        /// Konvertiert eine CacheResolution in einen String
        /// </summary>
        /// <param name="cacheResolution">Die zu konvertierende CacheResolution</param>
        /// <returns>Der konvertierte String</returns>
        public static string GetCacheResolutionString(CacheResolution cacheResolution)
        {
            if (cacheResolution.Resolution == Resolution.Full)
            {
                return "NoCache";
            }
            else
            {
                string result = GetResolutionString(cacheResolution.Resolution) + "_" + ParserHelper.GetAggregateFunctionString(cacheResolution.AggregateFunction);
                if (cacheResolution.ForceRebuild)
                {
                    result += "_REBUILD";
                }
                if (cacheResolution.IncrementalRefresh)
                {
                    result += "_REFRESHINCREMENTAL";
                }
                return result;
            }
        }

        /// <summary>
        /// Konvertiert eine Resolution in einen String
        /// </summary>
        /// <param name="resolution">Die zu konvertierende Resolution</param>
        /// <returns>Der konvertierte String</returns>
        public static string GetResolutionString(Resolution resolution)
        {
            switch (resolution)
            {
                case Resolution.Minutely:
                    return "MINUTELY";
                case Resolution.FiveMinutely:
                    return "FIVEMINUTELY";
                case Resolution.FifteenMinutely:
                    return "FIFTEENMINUTELY";
                case Resolution.Hourly:
                    return "HOURLY";
                default:
                    throw new Exception($"Unbekannte Resolution: {resolution}");
            }
        }
    }

    /// <summary>
    /// Parser für Aggregate-Operationen
    /// </summary>
    public class AggregateParser : KalaQlParserBase
    {
        /// <summary>
        /// Prüft, ob dieser Parser das angegebene Verb parsen kann
        /// </summary>
        /// <param name="verb">Das zu prüfende Verb</param>
        /// <returns>True, wenn dieser Parser das Verb parsen kann, sonst False</returns>
        public override bool CanParse(string verb) => verb == "Aggr" || verb == "Aggregate";

        /// <summary>
        /// Parst eine Zeile und erstellt eine Operation
        /// </summary>
        /// <param name="line">Die zu parsende Zeile</param>
        /// <param name="fields">Die bereits aufgeteilten Felder der Zeile</param>
        /// <returns>Die erstellte Operation</returns>
        public override Op_Base Parse(string line, List<string> fields, List<IKalaQlOperation> previousOps)
        {
            string sourceName = ParseSourceName(fields[2], previousOps);

            return new Op_Aggregate(line, fields[1].Trim(':'), sourceName, ParseWindow(fields[3]), ParseAggregate(fields[4]), ParseEmptyWindows(fields.Count > 5 ? fields[5] : ""));
        }

        /// <summary>
        /// Generiert eine Zeile aus den angegebenen Parametern
        /// </summary>
        /// <param name="parameters">Die Parameter für die Zeile</param>
        /// <returns>Die generierte Zeile</returns>
        public override string GenerateLine(object parameters)
        {
            return GenerateLine((AggregateParams)parameters);
        }

        /// <summary>
        /// Generiert eine Zeile aus den angegebenen Parametern
        /// </summary>
        /// <param name="parameters">Die Parameter für die Zeile</param>
        /// <returns>Die generierte Zeile</returns>
        public static string GenerateLine(AggregateParams parameters)
        {
            string result = $"Aggregate {parameters.Name}: {parameters.Input} {GetWindowString(parameters.Window)} {ParserHelper.GetAggregateFunctionString(parameters.AggregateFunction)}";
            if (parameters.EmptyWindows)
            {
                result += " EmptyWindows";
            }
            return result;
        }

        /// <summary>
        /// Konvertiert ein Window in einen String
        /// </summary>
        /// <param name="window">Das zu konvertierende Window</param>
        /// <returns>Der konvertierte String</returns>
        private static string GetWindowString(Window window)
        {
            if (window == Window.Aligned_5Minutes)
            {
                return "Aligned_5Minutes";
            }
            else if (window == Window.Aligned_15Minutes)
            {
                return "Aligned_15Minutes";
            }
            else if (window == Window.Aligned_1Hour)
            {
                return "Aligned_1Hour";
            }
            else if (window == Window.Aligned_1Day)
            {
                return "Aligned_1Day";
            }
            else if (window == Window.Aligned_1Week)
            {
                return "Aligned_1Week";
            }
            else if (window == Window.Aligned_1Month)
            {
                return "Aligned_1Month";
            }
            else if (window == Window.Aligned_1YearStartAtHalf)
            {
                return "Aligned_1YearStartAtHalf";
            }
            else if (window == Window.Aligned_1Year)
            {
                return "Aligned_1Year";
            }
            else if (window == Window.Unaligned_1Month)
            {
                return "Unaligned_1Month";
            }
            else if (window == Window.Unaligned_1Year)
            {
                return "Unaligned_1Year";
            }
            else if (window == Window.Infinite)
            {
                return "Infinite";
            }
            else
            {
                // Zugriff auf die TimeSpan-Eigenschaft eines Window-Objekts
                if (window.Mode == WindowMode.FixedIntervall)
                {
                    return window.Interval.ToString();
                }
                return "Infinite";
            }
        }       
    }

    /// <summary>
    /// Parser für Interpolate-Operationen
    /// </summary>
    public class InterpolateParser : KalaQlParserBase
    {
        /// <summary>
        /// Prüft, ob dieser Parser das angegebene Verb parsen kann
        /// </summary>
        /// <param name="verb">Das zu prüfende Verb</param>
        /// <returns>True, wenn dieser Parser das Verb parsen kann, sonst False</returns>
        public override bool CanParse(string verb) => verb == "Inpo";

        /// <summary>
        /// Parst eine Zeile und erstellt eine Operation
        /// </summary>
        /// <param name="line">Die zu parsende Zeile</param>
        /// <param name="fields">Die bereits aufgeteilten Felder der Zeile</param>
        /// <returns>Die erstellte Operation</returns>
        public override Op_Base Parse(string line, List<string> fields, List<IKalaQlOperation> previousOps)
        {
            string? constantValue = fields.Count > 4 ? fields[4] : null;
            string sourceName = ParseSourceName(fields[2], previousOps);
            return new Op_Interpolate(line, fields[1].Trim(':'), sourceName, ParseInterpolationMode(fields[3]), ParseDecimalNullable(constantValue ?? "NULL"));
        }

        /// <summary>
        /// Generiert eine Zeile aus den angegebenen Parametern
        /// </summary>
        /// <param name="parameters">Die Parameter für die Zeile</param>
        /// <returns>Die generierte Zeile</returns>
        public override string GenerateLine(object parameters)
        {
            return GenerateLine((InterpolateParams)parameters);
        }

        /// <summary>
        /// Generiert eine Zeile aus den angegebenen Parametern
        /// </summary>
        /// <param name="parameters">Die Parameter für die Zeile</param>
        /// <returns>Die generierte Zeile</returns>
        public static string GenerateLine(InterpolateParams parameters)
        {
            return $"Inpo {parameters.Name}: {parameters.Input} {GetInterpolationModeString(parameters.InterpolationMode)} {GetDecimalNullableString(parameters.DefaultValue)}";
        }

        /// <summary>
        /// Konvertiert einen InterpolationMode in einen String
        /// </summary>
        /// <param name="interpolationMode">Der zu konvertierende InterpolationMode</param>
        /// <returns>Der konvertierte String</returns>
        private static string GetInterpolationModeString(InterpolationMode interpolationMode)
        {
            switch (interpolationMode)
            {
                case InterpolationMode.forwards:
                    return "FORWARDS";
                case InterpolationMode.backwards:
                    return "BACKWARDS";
                case InterpolationMode.constant:
                    return "CONSTANT";
                default:
                    throw new Exception($"Unbekannter InterpolationMode: {interpolationMode}");
            }
        }

        /// <summary>
        /// Konvertiert einen Decimal? in einen String
        /// </summary>
        /// <param name="value">Der zu konvertierende Decimal?</param>
        /// <returns>Der konvertierte String</returns>
        private static string GetDecimalNullableString(decimal? value)
        {
            if (value == null)
            {
                return "NULL";
            }
            else
            {
                return value.ToString()!;
            }
        }
    }

    /// <summary>
    /// Parser für MatView-Operationen
    /// </summary>
    public class MatViewParser : KalaQlParserBase
    {
        /// <summary>
        /// Prüft, ob dieser Parser das angegebene Verb parsen kann
        /// </summary>
        /// <param name="verb">Das zu prüfende Verb</param>
        /// <returns>True, wenn dieser Parser das Verb parsen kann, sonst False</returns>
        public override bool CanParse(string verb) => verb == "MatView";

        /// <summary>
        /// Parst eine Zeile und erstellt eine Operation
        /// </summary>
        /// <param name="line">Die zu parsende Zeile</param>
        /// <param name="fields">Die bereits aufgeteilten Felder der Zeile</param>
        /// <returns>Die erstellte Operation</returns>
        public override Op_Base Parse(string line, List<string> fields, List<IKalaQlOperation> previousOps)
        {
            string sourceName = ParseSourceName(fields[2], previousOps);
            return new Op_MatView(line, fields[1].Trim(':'), sourceName, fields[3]);
        }

        /// <summary>
        /// Generiert eine Zeile aus den angegebenen Parametern
        /// </summary>
        /// <param name="parameters">Die Parameter für die Zeile</param>
        /// <returns>Die generierte Zeile</returns>
        public override string GenerateLine(object parameters)
        {
            return GenerateLine((MatViewParams)parameters);
        }

        /// <summary>
        /// Generiert eine Zeile aus den angegebenen Parametern
        /// </summary>
        /// <param name="parameters">Die Parameter für die Zeile</param>
        /// <returns>Die generierte Zeile</returns>
        public static string GenerateLine(MatViewParams parameters)
        {
            return $"MatView {parameters.Name}: {parameters.Input} {parameters.Measurement}";
        }
    }

    /// <summary>
    /// Parser für Insert-Operationen
    /// </summary>
    public class InsertParser : KalaQlParserBase
    {
        /// <summary>
        /// Prüft, ob dieser Parser das angegebene Verb parsen kann
        /// </summary>
        /// <param name="verb">Das zu prüfende Verb</param>
        /// <returns>True, wenn dieser Parser das Verb parsen kann, sonst False</returns>
        public override bool CanParse(string verb) => verb == "Insert";

        /// <summary>
        /// Parst eine Zeile und erstellt eine Operation
        /// </summary>
        /// <param name="line">Die zu parsende Zeile</param>
        /// <param name="fields">Die bereits aufgeteilten Felder der Zeile</param>
        /// <returns>Die erstellte Operation</returns>
        public override Op_Base Parse(string line, List<string> fields, List<IKalaQlOperation> previousOps)
        {
            string sourceName = ParseSourceName(fields[2], previousOps);
            return new Op_Insert(line, fields[1].Trim(':'), sourceName, fields[3]);
        }

        /// <summary>
        /// Generiert eine Zeile aus den angegebenen Parametern
        /// </summary>
        /// <param name="parameters">Die Parameter für die Zeile</param>
        /// <returns>Die generierte Zeile</returns>
        public override string GenerateLine(object parameters)
        {
            return GenerateLine((InsertParams)parameters);
        }

        /// <summary>
        /// Generiert eine Zeile aus den angegebenen Parametern
        /// </summary>
        /// <param name="parameters">Die Parameter für die Zeile</param>
        /// <returns>Die generierte Zeile</returns>
        public static string GenerateLine(InsertParams parameters)
        {
            return $"Insert {parameters.Name}: {parameters.Measurement} {parameters.Value}";
        }
    }

    /// <summary>
    /// Parser für Expresso-Operationen
    /// </summary>
    public class ExpressoParser : KalaQlParserBase
    {
        /// <summary>
        /// Prüft, ob dieser Parser das angegebene Verb parsen kann
        /// </summary>
        /// <param name="verb">Das zu prüfende Verb</param>
        /// <returns>True, wenn dieser Parser das Verb parsen kann, sonst False</returns>
        public override bool CanParse(string verb) => verb == "Expr";

        /// <summary>
        /// Parst eine Zeile und erstellt eine Operation
        /// </summary>
        /// <param name="line">Die zu parsende Zeile</param>
        /// <param name="fields">Die bereits aufgeteilten Felder der Zeile</param>
        /// <returns>Die erstellte Operation</returns>
        public override Op_Base Parse(string line, List<string> fields, List<IKalaQlOperation> previousOps)
        {
            string sourceName = ParseSourceName(fields[2], previousOps);
            return new Op_Expresso(line, fields[1].Trim(':'), sourceName.Replace('\'', '"'));
        }

        /// <summary>
        /// Generiert eine Zeile aus den angegebenen Parametern
        /// </summary>
        /// <param name="parameters">Die Parameter für die Zeile</param>
        /// <returns>Die generierte Zeile</returns>
        public override string GenerateLine(object parameters)
        {
            return GenerateLine((ExpressoParams)parameters);
        }

        /// <summary>
        /// Generiert eine Zeile aus den angegebenen Parametern
        /// </summary>
        /// <param name="parameters">Die Parameter für die Zeile</param>
        /// <returns>Die generierte Zeile</returns>
        public static string GenerateLine(ExpressoParams parameters)
        {
            return $"Expr {parameters.Name}: {parameters.Expression.Replace('"', '\'')}";
        }
    }

    /// <summary>
    /// Parser für Publish-Operationen
    /// </summary>
    public class PublishParser : KalaQlParserBase
    {
        /// <summary>
        /// Prüft, ob dieser Parser das angegebene Verb parsen kann
        /// </summary>
        /// <param name="verb">Das zu prüfende Verb</param>
        /// <returns>True, wenn dieser Parser das Verb parsen kann, sonst False</returns>
        public override bool CanParse(string verb) => verb == "Publ" || verb == "Publish";

        /// <summary>
        /// Parst eine Zeile und erstellt eine Operation
        /// </summary>
        /// <param name="line">Die zu parsende Zeile</param>
        /// <param name="fields">Die bereits aufgeteilten Felder der Zeile</param>
        /// <returns>Die erstellte Operation</returns>
        public override Op_Base Parse(string line, List<string> fields, List<IKalaQlOperation> previousOps)
        {
            string sourceName = ParseSourceName(fields[1], previousOps);

            return new Op_Publish(line, sourceName.Split(",", StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries).ToList(), ParsePublishMode(fields[2]));
        }

        /// <summary>
        /// Generiert eine Zeile aus den angegebenen Parametern
        /// </summary>
        /// <param name="parameters">Die Parameter für die Zeile</param>
        /// <returns>Die generierte Zeile</returns>
        public override string GenerateLine(object parameters)
        {
            return GenerateLine((PublishParams)parameters);
        }

        /// <summary>
        /// Generiert eine Zeile aus den angegebenen Parametern
        /// </summary>
        /// <param name="parameters">Die Parameter für die Zeile</param>
        /// <returns>Die generierte Zeile</returns>
        public string GenerateLine(PublishParams parameters)
        {
            return $"Publish {string.Join(",", parameters.Inputs)} {GetPublishModeString(parameters.PublishMode)}";
        }

        /// <summary>
        /// Konvertiert einen PublishMode in einen String
        /// </summary>
        /// <param name="publishMode">Der zu konvertierende PublishMode</param>
        /// <returns>Der konvertierte String</returns>
        private static string GetPublishModeString(PublishMode publishMode)
        {
            switch (publishMode)
            {
                case PublishMode.CombinedResultset:
                    return "CombinedResultset";
                default:
                    return "MultipleResultsets";
            }
        }
    }
}
