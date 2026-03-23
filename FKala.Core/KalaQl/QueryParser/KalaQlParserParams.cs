using FKala.Core.KalaQl.Windowing;
using FKala.Core.Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FKala.Core.KalaQl.QueryParser
{
    /// <summary>
    /// Parameter für AlignTimezone-Operationen
    /// </summary>
    public class AlignTimezoneParams
    {
        /// <summary>
        /// Die Zeitzone
        /// </summary>
        public string Timezone { get; set; }

        /// <summary>
        /// Erstellt eine neue Instanz der AlignTimezoneParams-Klasse
        /// </summary>
        /// <param name="timezone">Die Zeitzone</param>
        public AlignTimezoneParams(string timezone)
        {
            Timezone = timezone;
        }
    }

    /// <summary>
    /// Parameter für Var-Operationen
    /// </summary>
    public class VarParams
    {
        /// <summary>
        /// Der Name der Variable
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Der Wert der Variable
        /// </summary>
        public string Value { get; set; }

        /// <summary>
        /// Erstellt eine neue Instanz der VarParams-Klasse
        /// </summary>
        /// <param name="name">Der Name der Variable</param>
        /// <param name="value">Der Wert der Variable</param>
        public VarParams(string name, string value)
        {
            Name = name;
            Value = value;
        }
    }

    /// <summary>
    /// Parameter für Load-Operationen
    /// </summary>
    public class LoadParams
    {
        /// <summary>
        /// Der Name der Operation
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Der Name der Messung
        /// </summary>
        public string Measurement { get; set; }

        /// <summary>
        /// Der Startzeitpunkt
        /// </summary>
        public DateTime From { get; set; }

        /// <summary>
        /// Der Endzeitpunkt
        /// </summary>
        public DateTime To { get; set; }

        /// <summary>
        /// Die Cache-Auflösung
        /// </summary>
        public CacheResolution CacheResolution { get; set; }

        /// <summary>
        /// Gibt an, ob nur der neueste Wert geladen werden soll
        /// </summary>
        public bool NewestOnly { get; set; }

        /// <summary>
        /// Erstellt eine neue Instanz der LoadParams-Klasse
        /// </summary>
        /// <param name="name">Der Name der Operation</param>
        /// <param name="measurement">Der Name der Messung</param>
        /// <param name="from">Der Startzeitpunkt</param>
        /// <param name="to">Der Endzeitpunkt</param>
        /// <param name="cacheResolution">Die Cache-Auflösung</param>
        /// <param name="newestOnly">Gibt an, ob nur der neueste Wert geladen werden soll</param>
        public LoadParams(string name, string measurement, DateTime from, DateTime to, CacheResolution cacheResolution, bool newestOnly = false)
        {
            Name = name;
            Measurement = measurement;
            From = from;
            To = to;
            CacheResolution = cacheResolution;
            NewestOnly = newestOnly;
        }
    }

    /// <summary>
    /// Parameter für JsonQuery-Operationen
    /// </summary>
    public class JsonQueryParams
    {
        /// <summary>
        /// Der Name der Operation
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Der Name der Messung
        /// </summary>
        public string Measurement { get; set; }

        /// <summary>
        /// Der JSON-Pfad
        /// </summary>
        public string JsonPath { get; set; }

        /// <summary>
        /// Der Startzeitpunkt
        /// </summary>
        public DateTime From { get; set; }

        /// <summary>
        /// Der Endzeitpunkt
        /// </summary>
        public DateTime To { get; set; }

        /// <summary>
        /// Die Cache-Auflösung
        /// </summary>
        public CacheResolution CacheResolution { get; set; }

        /// <summary>
        /// Gibt an, ob nur der neueste Wert geladen werden soll
        /// </summary>
        public bool NewestOnly { get; set; }

        /// <summary>
        /// Erstellt eine neue Instanz der JsonQueryParams-Klasse
        /// </summary>
        /// <param name="name">Der Name der Operation</param>
        /// <param name="measurement">Der Name der Messung</param>
        /// <param name="jsonPath">Der JSON-Pfad</param>
        /// <param name="from">Der Startzeitpunkt</param>
        /// <param name="to">Der Endzeitpunkt</param>
        /// <param name="cacheResolution">Die Cache-Auflösung</param>
        /// <param name="newestOnly">Gibt an, ob nur der neueste Wert geladen werden soll</param>
        public JsonQueryParams(string name, string measurement, string jsonPath, DateTime from, DateTime to, CacheResolution cacheResolution, bool newestOnly = false)
        {
            Name = name;
            Measurement = measurement;
            JsonPath = jsonPath;
            From = from;
            To = to;
            CacheResolution = cacheResolution;
            NewestOnly = newestOnly;
        }
    }

    /// <summary>
    /// Parameter für Aggregate-Operationen
    /// </summary>
    public class AggregateParams
    {
        /// <summary>
        /// Der Name der Operation
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Der Name der Eingabe
        /// </summary>
        public string Input { get; set; }

        /// <summary>
        /// Das Fenster
        /// </summary>
        public Window Window { get; set; }

        /// <summary>
        /// Die Aggregatfunktion
        /// </summary>
        public AggregateFunction AggregateFunction { get; set; }

        /// <summary>
        /// Gibt an, ob leere Fenster ausgegeben werden sollen
        /// </summary>
        public bool EmptyWindows { get; set; }

        /// <summary>
        /// Erstellt eine neue Instanz der AggregateParams-Klasse
        /// </summary>
        /// <param name="name">Der Name der Operation</param>
        /// <param name="input">Der Name der Eingabe</param>
        /// <param name="window">Das Fenster</param>
        /// <param name="aggregateFunction">Die Aggregatfunktion</param>
        /// <param name="emptyWindows">Gibt an, ob leere Fenster ausgegeben werden sollen</param>
        public AggregateParams(string name, string input, Window window, AggregateFunction aggregateFunction, bool emptyWindows = false)
        {
            Name = name;
            Input = input;
            Window = window;
            AggregateFunction = aggregateFunction;
            EmptyWindows = emptyWindows;
        }
    }

    /// <summary>
    /// Parameter für Interpolate-Operationen
    /// </summary>
    public class InterpolateParams
    {
        /// <summary>
        /// Der Name der Operation
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Der Name der Eingabe
        /// </summary>
        public string Input { get; set; }

        /// <summary>
        /// Der Interpolationsmodus
        /// </summary>
        public InterpolationMode InterpolationMode { get; set; }

        /// <summary>
        /// Der Standardwert
        /// </summary>
        public decimal? DefaultValue { get; set; }

        /// <summary>
        /// Erstellt eine neue Instanz der InterpolateParams-Klasse
        /// </summary>
        /// <param name="name">Der Name der Operation</param>
        /// <param name="input">Der Name der Eingabe</param>
        /// <param name="interpolationMode">Der Interpolationsmodus</param>
        /// <param name="defaultValue">Der Standardwert</param>
        public InterpolateParams(string name, string input, InterpolationMode interpolationMode, decimal? defaultValue)
        {
            Name = name;
            Input = input;
            InterpolationMode = interpolationMode;
            DefaultValue = defaultValue;
        }
    }

    /// <summary>
    /// Parameter für MatView-Operationen
    /// </summary>
    public class MatViewParams
    {
        /// <summary>
        /// Der Name der Operation
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Der Name der Eingabe
        /// </summary>
        public string Input { get; set; }

        /// <summary>
        /// Der Name der Messung
        /// </summary>
        public string Measurement { get; set; }

        /// <summary>
        /// Erstellt eine neue Instanz der MatViewParams-Klasse
        /// </summary>
        /// <param name="name">Der Name der Operation</param>
        /// <param name="input">Der Name der Eingabe</param>
        /// <param name="measurement">Der Name der Messung</param>
        public MatViewParams(string name, string input, string measurement)
        {
            Name = name;
            Input = input;
            Measurement = measurement;
        }
    }

    /// <summary>
    /// Parameter für Insert-Operationen
    /// </summary>
    public class InsertParams
    {
        /// <summary>
        /// Der Name der Operation
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Der Name der Messung
        /// </summary>
        public string Measurement { get; set; }

        /// <summary>
        /// Der Wert
        /// </summary>
        public string Value { get; set; }

        /// <summary>
        /// Erstellt eine neue Instanz der InsertParams-Klasse
        /// </summary>
        /// <param name="name">Der Name der Operation</param>
        /// <param name="measurement">Der Name der Messung</param>
        /// <param name="value">Der Wert</param>
        public InsertParams(string name, string measurement, string value)
        {
            Name = name;
            Measurement = measurement;
            Value = value;
        }
    }

    /// <summary>
    /// Parameter für Expresso-Operationen
    /// </summary>
    public class ExpressoParams
    {
        /// <summary>
        /// Der Name der Operation
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Der Ausdruck
        /// </summary>
        public string Expression { get; set; }

        /// <summary>
        /// Erstellt eine neue Instanz der ExpressoParams-Klasse
        /// </summary>
        /// <param name="name">Der Name der Operation</param>
        /// <param name="expression">Der Ausdruck</param>
        public ExpressoParams(string name, string expression)
        {
            Name = name;
            Expression = expression;
        }
    }

    /// <summary>
    /// Parameter für Publish-Operationen
    /// </summary>
    public class PublishParams
    {
        /// <summary>
        /// Die Liste der Eingaben
        /// </summary>
        public List<string> Inputs { get; set; }

        /// <summary>
        /// Der Veröffentlichungsmodus
        /// </summary>
        public PublishMode PublishMode { get; set; }

        /// <summary>
        /// Erstellt eine neue Instanz der PublishParams-Klasse
        /// </summary>
        /// <param name="inputs">Die Liste der Eingaben</param>
        /// <param name="publishMode">Der Veröffentlichungsmodus</param>
        public PublishParams(List<string> inputs, PublishMode publishMode)
        {
            Inputs = inputs;
            PublishMode = publishMode;
        }
    }

    /// <summary>
    /// Parameter für Mgmt-Operationen
    /// </summary>
    public class MgmtParams
    {
        /// <summary>
        /// Die Aktion
        /// </summary>
        public MgmtAction Action { get; set; }

        /// <summary>
        /// Die Parameter
        /// </summary>
        public string Parameters { get; set; }

        /// <summary>
        /// Erstellt eine neue Instanz der MgmtParams-Klasse
        /// </summary>
        /// <param name="action">Die Aktion</param>
        /// <param name="parameters">Die Parameter</param>
        public MgmtParams(MgmtAction action, string parameters = "")
        {
            Action = action;
            Parameters = parameters;
        }
    }

    /// <summary>
    /// Parameter für ShiftTime-Operationen
    /// </summary>
    public class ShiftTimeParams
    {
        /// <summary>
        /// Der Name der Operation
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Der Name der Eingabe
        /// </summary>
        public string Input { get; set; }

        /// <summary>
        /// Die Zeitverschiebung
        /// </summary>
        public TimeSpan Offset { get; set; }

        /// <summary>
        /// Erstellt eine neue Instanz der ShiftTimeParams-Klasse
        /// </summary>
        /// <param name="name">Der Name der Operation</param>
        /// <param name="input">Der Name der Eingabe</param>
        /// <param name="offset">Die Zeitverschiebung</param>
        public ShiftTimeParams(string name, string input, TimeSpan offset)
        {
            Name = name;
            Input = input;
            Offset = offset;
        }
    }
}
