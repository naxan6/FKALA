using Exceptionless.DateTimeExtensions;
using FKala.Core.Interfaces;
using FKala.Core.KalaQl.Windowing;
using FKala.Core.Model;
using NodaTime.TimeZones;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace FKala.Core.KalaQl.QueryParser
{
    /// <summary>
    /// Basisklasse für Parser, die KalaQl-Befehle parsen können
    /// </summary>
    public abstract class KalaQlParserBase : IKalaQlParser
    {
        /// <summary>
        /// Prüft, ob dieser Parser das angegebene Verb parsen kann
        /// </summary>
        /// <param name="verb">Das zu prüfende Verb</param>
        /// <returns>True, wenn dieser Parser das Verb parsen kann, sonst False</returns>
        public abstract bool CanParse(string verb);

        /// <summary>
        /// Parst eine Zeile und erstellt eine Operation
        /// </summary>
        /// <param name="line">Die zu parsende Zeile</param>
        /// <param name="fields">Die bereits aufgeteilten Felder der Zeile</param>
        /// <returns>Die erstellte Operation</returns>
        public abstract Op_Base Parse(string line, List<string> fields, List<IKalaQlOperation> previousOps);

        public static string ParseSourceName(string sourceName, List<IKalaQlOperation> previousOps)
        {
            // Dieser Regex-Pattern fängt alle drei Fälle ab:
            // 1. =N-M=> (z.B. =1-5=>) -> Gruppe 1 = N, Gruppe 2 = M
            // 2. =N=>   (z.B. =3=>)   -> Gruppe 1 = N, Gruppe 2 nicht erfolgreich
            // 3. ==>                  -> Gruppe 1 & 2 nicht erfolgreich
            string pattern = @"=(?:(?:(\d+)(?:-(\d+))?)|)=>";
            // Regex.Replace mit einem MatchEvaluator (der Lambda-Funktion)
            // wird für jeden Treffer aufgerufen.
            return Regex.Replace(sourceName, pattern, (match) =>
            {
                try
                {
                    // Fall 1: Range-Match (z.B. =1-5=>)
                    if (match.Groups[1].Success && match.Groups[2].Success)
                    {
                        int startIndex = int.Parse(match.Groups[1].Value);
                        int endIndex = int.Parse(match.Groups[2].Value);

                        // Sicherstellen, dass die kleinere Zahl vorne steht
                        if (startIndex > endIndex)
                        {
                            (startIndex, endIndex) = (endIndex, startIndex); // Tuple-Swap
                        }

                        // Prüfen, ob der höchste Index gültig ist
                        if (endIndex > previousOps.Count)
                        {
                            return $"(Error: Index {endIndex} out of range. Max is {previousOps.Count})";
                        }

                        // Eine Liste der Namen im Bereich erstellen
                        var names = Enumerable.Range(startIndex, endIndex - startIndex + 1)
                                          // C# 8+ Index-Syntax (^i bedeutet "i von hinten")
                                          .Select(i => previousOps[^i].Name);

                        return string.Join(", ", names);
                    }

                    // Fall 2: Single-Index-Match (z.B. =3=>)
                    if (match.Groups[1].Success)
                    {
                        int index = int.Parse(match.Groups[1].Value);

                        if (index > previousOps.Count || index < 1)
                        {
                            return $"(Error: Index {index} out of range. Max is {previousOps.Count})";
                        }

                        // C# 8+ Index-Syntax
                        return previousOps[^index].Name;
                    }

                    // Fall 3: Default-Match (==>)
                    if (previousOps.Count < 1)
                    {
                        return "(Error: No previous ops for '==>')";
                    }

                    // C# 8+ Index-Syntax (^1 ist das letzte Element)
                    return previousOps[^1].Name;
                }
                catch (Exception ex)
                {
                    // Allgemeines Fallback für unerwartete Fehler (z.B. leere Liste)
                    return $"(Error: {ex.Message})";
                }
            });
        }


        /// <summary>
        /// Generiert eine Zeile aus den angegebenen Parametern
        /// </summary>
        /// <param name="parameters">Die Parameter für die Zeile</param>
        /// <returns>Die generierte Zeile</returns>
        public abstract string GenerateLine(object parameters);

        /// <summary>
        /// Parst einen DateTime-String
        /// </summary>
        /// <param name="v">Der zu parsende String</param>
        /// <returns>Das geparste DateTime</returns>
        protected static DateTime ParseDateTime(string v, bool isEnd)
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
                if (DateTime.TryParseExact(v, format, CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal, out parsedDate))
                {
                    return DateTime.SpecifyKind(parsedDate, DateTimeKind.Utc);
                }
            }

            var dateTimeOffset = DateMath.Parse(v, TimeZoneInfo.Utc, isEnd);
            return dateTimeOffset.DateTime;
        }

        /// <summary>
        /// Parst eine AggregateFunction
        /// </summary>
        /// <param name="v">Der zu parsende String</param>
        /// <returns>Die geparste AggregateFunction</returns>
        protected static AggregateFunction ParseAggregate(string v)
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

        /// <summary>
        /// Parst ein Window
        /// </summary>
        /// <param name="v">Der zu parsende String</param>
        /// <returns>Das geparste Window</returns>
        protected static Window ParseWindow(string v)
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
                    else if (v.EndsWith("h") && int.TryParse(v.TrimEnd('h'), out int hours))
                    {
                        timespan = TimeSpan.FromHours(hours);
                    }
                    else
                    {
                        timespan = TimeSpan.Parse(v);
                    }

                    return new Window(WindowMode.FixedIntervall, timespan);
            }
        }

        /// <summary>
        /// Parst eine CacheResolution
        /// </summary>
        /// <param name="v">Der zu parsende String</param>
        /// <returns>Die geparste CacheResolution</returns>
        protected CacheResolution ParseCacheResolution(string v)
        {
            v = v.Trim();

            var parts = v.Split('_');

            // Speichert die ursprüngliche Fenstergröße, wenn die Resolution aus einem "AUTO(...)"-String erstellt wurde
            long? originalAutoWindowSize;

            Resolution? resolution = ParseResolution(parts[0], out originalAutoWindowSize);
            if (resolution != null && resolution != Resolution.Full)
            {
                var aggregate = ParseAggregate(parts[1]);
                var forceRebuild = parts.Length > 2 && parts[2].ToUpper().Contains("REBUILD");
                var refreshIncremental = parts.Length > 2 && parts[2].ToUpper().Contains("REFRESHINCREMENTAL");
                return new CacheResolution()
                {
                    Resolution = resolution.Value,
                    AggregateFunction = aggregate,
                    ForceRebuild = forceRebuild,
                    IncrementalRefresh = refreshIncremental,
                    OriginalAutoWindowSize = originalAutoWindowSize
                };
            }
            else
            {
                return CacheResolutionPredefined.NoCache;
            }
        }

        /// <summary>
        /// Parst eine Resolution
        /// </summary>
        /// <param name="v">Der zu parsende String</param>
        /// <param name="originalAutoWindowSize">Die ursprüngliche Fenstergröße, wenn die Resolution aus einem "AUTO(...)"-String erstellt wurde</param>
        /// <returns>Die geparste Resolution</returns>
        protected static Resolution? ParseResolution(string v, out long? originalAutoWindowSize)
        {
            originalAutoWindowSize = null;

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

                // Speichert die ursprüngliche Fenstergröße
                originalAutoWindowSize = queriedwindowsize;

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

        /// <summary>
        /// Parst eine Resolution
        /// </summary>
        /// <param name="v">Der zu parsende String</param>
        /// <returns>Die geparste Resolution</returns>
        protected Resolution? ParseResolution(string v)
        {
            long? dummy;
            return ParseResolution(v, out dummy);
        }

        /// <summary>
        /// Parst einen Boolean für EmptyWindows
        /// </summary>
        /// <param name="v">Der zu parsende String</param>
        /// <returns>True, wenn EmptyWindows aktiviert sind, sonst False</returns>
        protected static bool ParseEmptyWindows(string v)
        {
            if (v.Contains("EmptyWindows"))
            {
                return true;
            }
            return false;
        }

        /// <summary>
        /// Parst einen PublishMode
        /// </summary>
        /// <param name="v">Der zu parsende String</param>
        /// <returns>Der geparste PublishMode</returns>
        protected static PublishMode ParsePublishMode(string v)
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

        /// <summary>
        /// Parst einen Decimal oder Null
        /// </summary>
        /// <param name="v">Der zu parsende String</param>
        /// <returns>Der geparste Decimal oder Null</returns>
        protected static decimal? ParseDecimalNullable(string v)
        {
            if (v.ToUpper() == "NULL")
            {
                return null;
            }
            else
            {
                return decimal.Parse(v, NumberStyles.Any, CultureInfo.InvariantCulture);
            }
        }

        /// <summary>
        /// Parst einen InterpolationMode
        /// </summary>
        /// <param name="v">Der zu parsende String</param>
        /// <returns>Der geparste InterpolationMode</returns>
        protected static InterpolationMode ParseInterpolationMode(string v)
        {
            if (v.ToUpper() == "FORWARDS")
            {
                return InterpolationMode.forwards;
            }
            else if (v.ToUpper() == "BACKWARDS")
            {
                return InterpolationMode.backwards;
            }
            else if (v.ToUpper() == "CONSTANT")
            {
                return InterpolationMode.constant;
            }
            throw new ArgumentException($"InterpolationMode {v} is invalid");
        }

        /// <summary>
        /// Parst eine MgmtAction
        /// </summary>
        /// <param name="v">Der zu parsende String</param>
        /// <returns>Die geparste MgmtAction</returns>
        protected static MgmtAction ParseMgmtAction(string v)
        {
            if (v.ToUpper() == "LOADMEASURES" || v.ToUpper() == "LISTMEASUREMENTS")
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
            else if (v.ToUpper() == "BLACKLIST")
            {
                return MgmtAction.Blacklist;
            }
            else if (v.ToUpper() == "UNBLACKLIST")
            {
                return MgmtAction.UnBlacklist;
            }

            throw new ArgumentException($"MgmtAction {v} is invalid");
        }
    }
}
