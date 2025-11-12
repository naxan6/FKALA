using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using FKala.Core.Interfaces;


/// <summary>
/// Verarbeitet das kompakte Abfrage-Skript und erweitert es
/// zu einem ausführlichen Skript.
/// </summary>
public class QueryPreprocessor
{
    private readonly IDataLayer DataLayer;
    private readonly List<string> _generatedNames = new List<string>();
    private readonly List<string> _expandedLines = new List<string>();

    // Regex zum Parsen des <measure:...> Tags
    private static readonly Regex _nameTemplateRegex =
        new Regex(@"(.*)<measure(?::(.*?))?>(.*)", RegexOptions.Compiled);

    public QueryPreprocessor(IDataLayer dataLayer)
    {
        DataLayer = dataLayer;
    }

    /// <summary>
    /// Hauptmethode zur Verarbeitung des kompakten Skripts.
    /// </summary>
    /// <param name="compactScript">Das gesamte Skript als einzelner String.</param>
    /// <returns>Eine Liste der erweiterten Zeilen.</returns>
    public List<string> Process(string compactScript)
    {
        _generatedNames.Clear();
        _expandedLines.Clear();

        var lines = compactScript.Split(new[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries);

        foreach (var line in lines)
        {
            ProcessLine(line.Trim());
        }

        return _expandedLines;
    }

    /// <summary>
    /// Verarbeitet eine einzelne Zeile des Skripts.
    /// </summary>
    private void ProcessLine(string line)
    {
        if (string.IsNullOrWhiteSpace(line)) return;

        var parts = SplitLineRespectingQuotes(line);
        if (parts.Count == 0) return;

        // Eine Zeile ist eine Vorlage, wenn sie 'regex:' enthält.
        // Wir erwarten mindestens 3 Teile: <Verb> <Name>: <Input>
        bool isTemplate = parts.Count >= 3 && parts[2].StartsWith("regex:");

        if (!isTemplate)
        {
            // Normale Zeile, einfach hinzufügen
            _expandedLines.Add(line);
            RegisterName(parts); // Trotzdem den Namen registrieren
            return;
        }

        // --- Vorlagenverarbeitung ---
        string verb = parts[0];
        string nameTemplate = parts[1].TrimEnd(':');
        string inputTemplate = parts[2];
        string parameters = string.Join(" ", parts.Skip(3));

        string regexPattern = inputTemplate.Substring("regex:".Length);
        List<string> sourceList;

        // 'Load' durchsucht die DataLayer-Messpunkte
        if (verb.Equals("Load", StringComparison.OrdinalIgnoreCase))
        {
            sourceList = this.DataLayer.LoadMeasurementList();
        }
        // 'Aggr' (und andere) durchsuchen die bisher generierten Namen
        else
        {
            sourceList = _generatedNames;
        }

        // Führe die Regex-Suche durch
        var regex = new Regex(regexPattern);
        var matches = sourceList.Where(s => regex.IsMatch(s)).ToList();

        if (matches.Count == 0)
        {
            throw new ArgumentException($"[Warning] Preprocessor: Regex '{regexPattern}' " +
                              $"für '{verb}' fand 0 Treffer.");
        }

        // Erzeuge eine neue Zeile für jeden Treffer
        foreach (var match in matches)
        {
            // 1. Neuen Namen generieren
            string newName = GenerateNameFromTemplate(nameTemplate, match);

            // 2. Neuen Input generieren (der Treffer selbst, ggf. in Quotes)
            string newInput = QuoteIfNecessary(match);

            // 3. Zeile zusammensetzen
            string newLine = $"{verb} {newName}: {newInput} {parameters}";

            _expandedLines.Add(newLine);
            _generatedNames.Add(newName);
        }
    }

    /// <summary>
    /// Registriert einen Namen (z.B. $VAR, rPV1, aPV1, PV)
    /// aus einer Nicht-Vorlagen-Zeile.
    /// </summary>
    private void RegisterName(List<string> parts)
    {
        if (parts.Count < 2) return;

        string verb = parts[0];
        string name = parts[1].TrimEnd(':');

        // Gemäß Beispielen definieren diese Verben Namen
        if (verb.Equals("Var", StringComparison.OrdinalIgnoreCase) ||
            verb.Equals("Load", StringComparison.OrdinalIgnoreCase) ||
            verb.Equals("Aggr", StringComparison.OrdinalIgnoreCase) ||
            verb.Equals("Expr", StringComparison.OrdinalIgnoreCase))
        {
            if (!_generatedNames.Contains(name))
            {
                _generatedNames.Add(name);
            }
        }
    }

    /// <summary>
    /// Generiert einen neuen Namen basierend auf einer Vorlage
    /// (z.B. "r<measure:^.*(PV.)[kW]$>") und einem Treffer.
    /// </summary>
    private string GenerateNameFromTemplate(string nameTemplate, string sourceMatch)
    {
        var templateMatch = _nameTemplateRegex.Match(nameTemplate);

        if (!templateMatch.Success)
        {
            // Keine <measure> Vorlage, statischer Name (z.B. "rNetz")
            return nameTemplate;
        }

        string prefix = templateMatch.Groups[1].Value; // z.B. "r"
        string captureRegexPattern = templateMatch.Groups[2].Value; // z.B. "^.*(PV.)[kW]$"
        string suffix = templateMatch.Groups[3].Value; // z.B. ""

        string namePart;

        if (string.IsNullOrEmpty(captureRegexPattern))
        {
            // Fall: <measure> (nutze den vollen Treffer)
            namePart = sourceMatch;
        }
        else
        {
            // Fall: <measure:regex(gruppe)> (nutze Capture-Gruppe)
            var captureRegex = new Regex(captureRegexPattern);
            var captureMatch = captureRegex.Match(sourceMatch);

            if (captureMatch.Success && captureMatch.Groups.Count > 1)
            {
                namePart = captureMatch.Groups[1].Value; // Die erste Capture-Gruppe
            }
            else
            {
                throw new ArgumentException($"[Warning] Namens-Regex '{captureRegexPattern}' " +
                                  $"fand keine Gruppe in '{sourceMatch}'.");
                namePart = "NAME_ERROR"; // Fallback
            }
        }

        return $"{prefix}{namePart}{suffix}";
    }

    /// <summary>
    /// Teilt eine Zeile an Leerzeichen, respektiert aber Text in Anführungszeichen.
    /// Anführungszeichen werden aus den Teilen entfernt.
    /// </summary>
    private List<string> SplitLineRespectingQuotes(string line)
    {
        var parts = new List<string>();
        var currentPart = new StringBuilder();
        bool inQuote = false;

        foreach (char c in line)
        {
            if (c == '"')
            {
                inQuote = !inQuote;
                // Füge das Anführungszeichen nicht zum Teil hinzu
            }
            else if (c == ' ' && !inQuote)
            {
                if (currentPart.Length > 0)
                {
                    parts.Add(currentPart.ToString());
                    currentPart.Clear();
                }
            }
            else
            {
                currentPart.Append(c);
            }
        }

        if (currentPart.Length > 0)
        {
            parts.Add(currentPart.ToString());
        }

        return parts;
    }

    /// <summary>
    /// Fügt Anführungszeichen hinzu, wenn der Text Leerzeichen enthält
    /// und nicht bereits in Anführungszeichen steht.
    /// </summary>
    private string QuoteIfNecessary(string text)
    {
        if (text.Contains(' ') && !text.StartsWith("\"") && !text.EndsWith("\""))
        {
            return $"\"{text}\"";
        }
        return text;
    }
}