# FKALA Anwendungsarchitektur

Dieses Dokument beschreibt die grundlegende Architektur der FKALA-Anwendung. Es dient als Referenz für Entwickler, um sich schnell in der Codebasis zurechtzufinden und zukünftige Anpassungen zu erleichtern.

## 1. Übersicht

FKALA ist ein System zur Erfassung, Speicherung, Abfrage und Verarbeitung von Zeitreihendaten. Es besteht aus mehreren Kernkomponenten, die zusammenarbeiten, um diese Funktionalität bereitzustellen.

## 2. Hauptkomponenten

Die Anwendung ist grob in folgende Projekte/Schichten unterteilt:

*   **`FKala.Core`**: Enthält die Kernlogik der Anwendung, einschließlich Datenmodell, Datenzugriffsschicht (DataLayer), die Abfragesprache KalaQl und deren Verarbeitung.
*   **`FKala.Api`**: Stellt eine HTTP-API bereit, über die externe Clients mit dem System interagieren können (z.B. Daten einfügen, Abfragen ausführen). Beinhaltet auch Hintergrund-Jobs (z.B. via Quartz.NET).
*   **`FKala.Client.Console`**: Eine Beispiel-Konsolenanwendung zur Interaktion mit der API.
*   **`FKala.Unittests`**: Enthält Unittests für die verschiedenen Komponenten.
*   Weitere Projekte wie `FKala.Background`, `Fkala.Console`, `FKala.WebApi` (möglicherweise veraltet oder für spezifische Zwecke).

## 3. Kernkonzepte in `FKala.Core`

### 3.1. DataLayer (`IDataLayer` / `DataLayer_Readable_Caching_V1`)

*   **Verantwortlichkeit**: Speicherung und Abruf von Zeitreihendaten.
*   **Speicherformat**: Daten werden typischerweise in `.dat`-Dateien auf dem Dateisystem gespeichert, organisiert in Verzeichnissen nach Measurement-Name, Jahr und Monat.
*   **Caching**: Implementiert eine Caching-Schicht (`CachingLayer`), um häufig abgefragte Daten oder aggregierte Daten für schnelleren Zugriff zwischenzuspeichern. Verschiedene Cache-Auflösungen (Minutely, Hourly etc.) werden unterstützt.
*   **Materialized Views (`MatView`)**:
    *   `Op_MatView` in KalaQl ermöglicht das Erstellen von materialisierten Sichten.
    *   Die Definition einer MatView (die zugrundeliegende KalaQl-Query) wird in einer `viewdef.txt`-Datei im Verzeichnis des entsprechenden Measurements gespeichert.
    *   `DataLayer_Readable_Caching_V1` enthält Methoden zum Schreiben (`WriteMatViewFile`), Laden (`LoadMatViews`) und Löschen (`DeleteMeasurementAndMatViewDefinition`) dieser Sichten und ihrer Definitionen.
*   **Buffering**: Verwendet einen `BufferedWriterService` für effizientes Schreiben von Daten.
*   **Blacklisting**: Ermöglicht das Blacklisten von Measurements.

### 3.2. KalaQl - Die Abfragesprache

*   **Zweck**: Eine domänenspezifische Sprache zur Abfrage und Manipulation von Zeitreihendaten.
*   **Verarbeitungspipeline**:
    1.  **Query-String**: Eine Abfrage wird als Text-String formuliert, entweder als einzelne Zeilen oder als mit ` | ` (Pipe) verkettete Operationen.
    2.  **`KalaQuery` Objekt**:
        *   Die Klasse `FKala.Core.KalaQl.KalaQuery` repräsentiert eine Abfrage.
        *   Die Methode `FromQuery(string queryText)` parst den Text-String.
        *   `ParseQueryText(string line)` zerlegt jede einzelne Operation in Felder (Tokens).
        *   Eine `KalaQlParserRegistry` wird verwendet, um den passenden Parser für das Verb jeder Operation zu finden.
    3.  **Parser (`VerbParsers.cs`)**:
        *   Für jedes Verb (z.B. `Load`, `Aggregate`, `Publish`, `MatView`) gibt es eine spezialisierte Parser-Klasse (z.B. `LoadParser`, `PublishParser`), die von `KalaQlParserBase` erbt.
        *   Diese Parser wandeln die Text-Repräsentation einer Operation in ein entsprechendes `IKalaQlOperation`-Objekt um (z.B. `Op_Load`, `Op_Publish`).
    4.  **`IKalaQlOperation`-Objekte**:
        *   Jede Operation (z.B. `Op_Load`, `Op_Aggregate`, `Op_MatView`, `Op_Publish`) implementiert das `IKalaQlOperation`-Interface.
        *   Wichtige Methoden sind `CanExecute()`, `Execute()` und `ToLine()` (für die Rückkonvertierung in einen String).
        *   Operationen werden in einer Liste innerhalb des `KalaQuery`-Objekts gespeichert.
    5.  **Ausführung (`KalaQuery.Execute(IDataLayer dataLayer)`)**:
        *   Erstellt einen `KalaQlContext`, der den Zustand während der Abfrageausführung hält (z.B. Zwischenergebnisse).
        *   Iteriert durch die `IKalaQlOperation`-Objekte und führt sie aus, wenn ihre Eingabedaten verfügbar sind.
        *   Zwischenergebnisse werden im `KalaQlContext.IntermediateDatasources` (als `ResultPromise`-Objekte) gespeichert.
        *   Die `Op_Publish`-Operation ist typischerweise die letzte und formatiert die Endergebnisse.
*   **Wichtige Operationen**:
    *   `Load`: Lädt Rohdaten aus dem DataLayer.
    *   `Aggregate`: Führt Aggregationen über Zeitfenster durch.
    *   `MatView`: Erstellt oder verwendet eine materialisierte Sicht.
    *   `Publish`: Gibt die Ergebnisse in einem bestimmten Format aus (z.B. Tabelle, mehrere Resultsets).
    *   `Var`: Definiert Variablen, die in der Query verwendet werden können.

## 4. API-Schicht (`FKala.Api`)

*   **Technologie**: ASP.NET Core Web API.
*   **Controller**: Stellt Endpunkte für Abfragen (`QueryController`, `StreamQueryController`) und Dateneingabe (`InsertController`) bereit.
*   **Input-Formatierung**: Unterstützt benutzerdefinierte Input-Formatter (z.B. `PlainTextFormatter`).
*   **Hintergrund-Jobs (Quartz.NET)**:
    *   Verwendet Quartz.NET für geplante Aufgaben.
    *   Der `MatViewRefreshJob` ist ein Beispiel, der nächtlich alle MatViews aktualisiert. Er nutzt den `IDataLayer` und die `KalaQuery`-Logik, um MatViews zu löschen und neu zu erstellen.
*   **MQTT-Integration**: Enthält einen `MqttWorker` für die Verarbeitung von Daten über MQTT (optional, abhängig von Konfiguration).
*   **Konfiguration**: Verwendet `appsettings.json` für Einstellungen wie `DataStorage`-Pfad, MQTT-Settings etc.

## 5. Datenfluss (Beispiel Abfrage)

1.  Client sendet eine KalaQl-Query als HTTP-Request an `FKala.Api`.
2.  Der entsprechende Controller in `FKala.Api` empfängt die Anfrage.
3.  Eine `KalaQuery` wird instanziiert und mit `FromQuery()` aus dem Query-Text geparst.
4.  `KalaQuery.Execute()` wird mit einer `IDataLayer`-Instanz aufgerufen.
5.  Die einzelnen Operationen in der `KalaQuery` werden ausgeführt:
    *   `Op_Load` liest Daten vom `IDataLayer`.
    *   `Op_Aggregate` verarbeitet diese Daten.
    *   `Op_MatView` greift auf eine bestehende Sicht zu oder materialisiert sie neu, indem sie die zugrundeliegende Kette von Operationen (ggf. ohne Zeitfilter für die Materialisierung) ausführt und das Ergebnis im `IDataLayer` speichert.
    *   `Op_Publish` formatiert das Endergebnis.
6.  Das `KalaResult` wird an den Controller zurückgegeben und als HTTP-Response an den Client gesendet.

## 6. Wichtige Verzeichnisse und Dateien für Entwickler

*   **Daten**: `<DataStorage>/data/` (Unterverzeichnisse pro Measurement, Jahr, Monat)
*   **MatView-Definitionen**: `<DataStorage>/data/<viewName>/viewdef.txt`
*   **Blacklist**: `<DataStorage>/blacklist/`
*   **KalaQl-Kernlogik**: `FKala.Core/KalaQl/`
*   **DataLayer-Implementierung**: `FKala.Core/DataLayer/DataLayer_Readable_Caching_V1.cs`
*   **API-Endpunkte**: `FKala.Api/Controller/`
*   **Quartz-Jobs**: `FKala.Api/Jobs/` (Beispiel: `MatViewRefreshJob.cs`)
*   **API-Startup/Konfiguration**: `FKala.Api/Program.cs`

## 7. Zukünftige Überlegungen / Bereiche für Verbesserungen

*   **Fehlerbehandlung und Logging**: Kann in vielen Bereichen noch detaillierter und strukturierter erfolgen.
*   **Performance-Optimierungen**: Insbesondere bei komplexen Abfragen oder großen Datenmengen.
*   **Inkrementelle MatView-Updates**: Aktuell werden MatViews komplett neu gebaut.
*   **Konfigurierbarkeit**: Weitere Aspekte der Anwendung könnten konfigurierbar gemacht werden.
*   **Code-Cleanups**: Reduzierung von Warnungen (Nullability etc.).
*   **Erweiterte Testabdeckung**: Insbesondere für komplexere KalaQl-Szenarien und die API-Schicht.

---

Dieses Dokument ist ein lebendiges Artefakt und sollte bei größeren Änderungen an der Architektur aktualisiert werden.
