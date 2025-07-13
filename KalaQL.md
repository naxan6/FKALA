# KalaQl - Die FKALA Abfragesprache

KalaQl ist eine domänenspezifische Sprache (DSL), die für die Abfrage und Manipulation von Zeitreihendaten innerhalb des FKALA-Systems entwickelt wurde. Dieses Dokument beschreibt die Syntax, Operatoren und Konzepte von KalaQl.

## 1. Grundlagen

### 1.1. Query-Struktur

Eine KalaQl-Abfrage besteht aus einer oder mehreren Operationen. Operationen können entweder zeilenweise oder durch ein Pipe-Symbol (` | `) getrennt geschrieben werden.

**Zeilenweise:**
```kalaql
// Dies ist ein Kommentar
Var startDate: 2023-01-01T00:00:00Z
Load rawData: my_sensor ${startDate} 2023-01-31T23:59:59Z NoCache
Aggregate hourlyAvg: rawData Aligned_1Hour Avg
Publish hourlyAvg Table
```

**Verkettet mit Pipe:**
```kalaql
Var startDate: 2023-01-01T00:00:00Z | Load rawData: my_sensor ${startDate} 2023-01-31T23:59:59Z NoCache | Aggregate hourlyAvg: rawData Aligned_1Hour Avg | Publish hourlyAvg Table
```

### 1.2. Kommentare

Kommentare beginnen mit `//` oder `#`.

```kalaql
// Dies ist ein einzeiliger Kommentar
# Dies ist ebenfalls ein Kommentar
```

### 1.3. Bezeichner und Namen

*   **Operation Names (Output Names)**: Jede Operation, die einen Datensatz erzeugt (z.B. `Load`, `Aggregate`), muss einen eindeutigen Namen für diesen Ausgabedatensatz erhalten. Dieser Name wird nach dem Verb und vor einem Doppelpunkt (`:`) deklariert. Beispiel: `rawData` in `Load rawData: ...`.
*   **Measurement Names**: Namen von Zeitreihen im DataLayer (z.B. `my_sensor`).
*   **Variablennamen**: Namen für Variablen, die mit `Var` definiert werden.

### 1.4. Datums- und Zeitformate

Datums- und Zeitangaben werden typischerweise im ISO 8601-Format erwartet, oft mit UTC-Zeitzone (`Z`).
Beispiel: `2023-01-01T00:00:00Z`

## 2. Variablen (`Var`)

Mit `Var` können Variablen definiert werden, die später in der Abfrage wiederverwendet werden können. Dies ist nützlich für Datumsangaben, Measurement-Namen oder andere häufig verwendete Werte. Variablen werden mit `${VariablenName}` referenziert.

**Syntax:**
`Var <variablenName>: <wert>`

**Beispiel:**
```kalaql
Var sensorName: "factory1/temp/sensor_alpha"
Var fromDate: 2023-01-01T00:00:00Z
Var toDate: 2023-02-01T00:00:00Z

Load data: ${sensorName} ${fromDate} ${toDate} NoCache
Publish data Table
```

## 3. Kernoperationen (Verben)

Jede Operation in KalaQl beginnt mit einem Verb.

### 3.1. `Load`

Lädt Rohdaten aus dem DataLayer für ein spezifiziertes Measurement und einen Zeitbereich.

**Syntax:**
`Load <outputName>: <measurementName> <startTime> <endTime> <cacheResolution> [NewestOnly]`
`Load <outputName>: <measurementName> NewestOnly`

**Parameter:**

*   `<outputName>`: Name des resultierenden Datensatzes.
*   `<measurementName>`: Name des zu ladenden Measurements (Zeitreihe). Kann ein String-Literal (in Anführungszeichen, falls Leerzeichen etc. enthalten sind) oder eine Variable sein.
*   `<startTime>`: Startzeitpunkt (ISO 8601).
*   `<endTime>`: Endzeitpunkt (ISO 8601).
*   `<cacheResolution>`: Gibt an, wie der Cache verwendet werden soll. Mögliche Werte:
    *   **`NoCache`**: Lädt immer Rohdaten, kein Cache wird verwendet oder geschrieben. (Entspricht `Resolution.Full`)
    *   **`AUTO(<windowSizeInSeconds>)`**: Versucht, eine passende gecachte Auflösung zu finden oder zu erstellen, die der `windowSizeInSeconds` am nächsten kommt.
    *   **`<Resolution>_<AggregateFunction>`**: Verwendet eine spezifische gecachte Auflösung.
        *   `Resolution`:
            *   `MINUTELY`
            *   `FIVEMINUTELY`
            *   `FIFTEENMINUTELY`
            *   `HOURLY`
            *   (Andere `Resolution`-Enum-Werte wie `Daily`, `Weekly` etc. könnten existieren, sind aber im `LoadParser` nicht explizit als String-Konstanten für die Cache-Auflösung aufgeführt. `FULL` wird zu `NoCache`.)
        *   `AggregateFunction`:
            *   `AVG`
            *   `WAVG` (Weighted Average)
            *   `FIRST`
            *   `LAST`
            *   `MIN`
            *   `MAX`
            *   `COUNT`
            *   `SUM`
    *   Optionale Suffixe für Cache-Auflösungen (kombinierbar):
        *   `_REBUILD`: Erzwingt eine Neuerstellung des Caches für diese Auflösung.
        *   `_REFRESHINCREMENTAL`: Versucht, den Cache inkrementell zu aktualisieren.
*   `[NewestOnly]` (optional): Wenn angegeben, wird nur der neueste Datenpunkt des Measurements geladen. `<startTime>`, `<endTime>` und `<cacheResolution>` werden dann ignoriert.

**Beispiele:**

```kalaql
// Lädt Rohdaten für 'sensor1' für einen Tag
Load raw: sensor1 2023-01-01T00:00:00Z 2023-01-01T23:59:59Z NoCache

// Lädt den neuesten Datenpunkt von 'sensor2'
Load latest: sensor2 NewestOnly

// Verwendet stündlich aggregierten Cache (Mittelwert)
Load hourly_avg_data: sensor1 2023-01-01T00:00:00Z 2023-01-31T23:59:59Z HOURLY_AVG

// Verwendet AUTO-Cache-Mechanismus, zielt auf 5-Minuten-Fenster
Load auto_data: sensor1 2023-01-01T00:00:00Z 2023-01-31T23:59:59Z AUTO(300)

// Erzwingt Neuerstellung des 15-Minuten-Maximum-Caches
Load data_rebuild: sensor1 2023-01-01T00:00:00Z 2023-01-31T23:59:59Z FIFTEENMINUTELY_MAX_REBUILD
```

### 3.2. `Loaj` (Load JSON)

Lädt Daten aus einem Measurement und extrahiert Werte mittels eines JSONPath-Ausdrucks. Dies ist nützlich, wenn die gespeicherten Werte JSON-Objekte sind.

**Syntax:**
`Loaj <outputName>: <measurementName> <jsonPath> <startTime> <endTime> <cacheResolution> [NewestOnly]`
`Loaj <outputName>: <measurementName> <jsonPath> NewestOnly`

**Parameter:**

*   `<outputName>`, `<measurementName>`, `<startTime>`, `<endTime>`, `<cacheResolution>`, `[NewestOnly]`: Wie bei `Load`.
*   `<jsonPath>`: Ein JSONPath-Ausdruck (z.B. `$.temperature`, `$.values[0].value`).

**Beispiel:**
```kalaql
// Lädt Temperaturwerte aus JSON-Daten
Loaj temps: complex_sensor $.metrics.temperature 2023-01-01T00:00:00Z 2023-01-01T23:59:59Z NoCache
```

### 3.3. `Aggregate` (oder `Aggr`)

Aggregiert Daten aus einem Eingabe-Datensatz über definierte Zeitfenster.

**Syntax:**
`Aggregate <outputName>: <inputName> <window> <aggregateFunction> [EmptyWindows]`
`Aggr <outputName>: <inputName> <window> <aggregateFunction> [EmptyWindows]`

**Parameter:**

*   `<outputName>`: Name des resultierenden aggregierten Datensatzes.
*   `<inputName>`: Name des Eingabe-Datensatzes (typischerweise das Ergebnis einer `Load`-Operation).
*   `<window>`: Definiert das Zeitfenster für die Aggregation. Mögliche Werte:
    *   **Zeitspannen-Format**: `HH:mm:ss` (z.B. `01:00:00` für stündlich, `00:05:00` für 5-minütlich). Dies sind "fixed interval" Fenster, die sich am Zeitstempel des ersten Datenpunkts orientieren.
    *   **Ausgerichtete Fenster (Aligned Windows)**:
        *   `Aligned_5Minutes`
        *   `Aligned_15Minutes`
        *   `Aligned_1Hour`
        *   `Aligned_1Day`
        *   `Aligned_1Week`
        *   `Aligned_1Month` (orientiert sich am Kalendermonat)
        *   `Aligned_1Year` (orientiert sich am Kalenderjahr)
        *   `Aligned_1YearStartAtHalf` (spezielles Jahresfenster)
    *   **Nicht-ausgerichtete Fenster (Unaligned Windows)**:
        *   `Unaligned_1Month` (rollierender Monatszeitraum ab dem ersten Datenpunkt)
        *   `Unaligned_1Year` (rollierender Jahreszeitraum ab dem ersten Datenpunkt)
    *   **`Infinite`**: Aggregiert alle Datenpunkte im Eingabe-Datensatz zu einem einzigen Wert.
*   `<aggregateFunction>`: Die anzuwendende Aggregationsfunktion. Mögliche Werte:
    *   `Avg` (Average)
    *   `WAvg` (Weighted Average - erfordert spezielle Datenstruktur oder Annahmen)
    *   `First`
    *   `Last`
    *   `Min`
    *   `Max`
    *   `Count`
    *   `Sum`
*   `[EmptyWindows]` (optional): Wenn angegeben, werden auch für Zeitfenster ohne Datenpunkte Ergebniszeilen erzeugt (typischerweise mit `null`-Werten oder dem Standardwert der Aggregationsfunktion).

**Beispiele:**

```kalaql
Load raw: sensor_A 2023-01-01T00:00:00Z 2023-01-01T05:00:00Z NoCache

// Stündlicher Durchschnitt, ausgerichtet an der vollen Stunde
Aggregate hourly_avg: raw Aligned_1Hour Avg

// 15-Minuten-Maximum, mit festem Intervall ab dem ersten Datenpunkt
Aggr max_15m: raw 00:15:00 Max

// Tägliche Summe, inklusive Tage ohne Daten
Aggregate daily_sum: raw Aligned_1Day Sum EmptyWindows

// Gesamtzahl aller Punkte
Aggr total_count: raw Infinite Count
```

### 3.4. `Interpolate` (oder `Inpo`)

Füllt fehlende Werte in einem Datensatz durch Interpolation.

**Syntax:**
`Interpolate <outputName>: <inputName> <interpolationMode> <defaultValue>`
`Inpo <outputName>: <inputName> <interpolationMode> <defaultValue>`

**Parameter:**

*   `<outputName>`: Name des resultierenden Datensatzes mit interpolierten Werten.
*   `<inputName>`: Name des Eingabe-Datensatzes.
*   `<interpolationMode>`: Art der Interpolation. Mögliche Werte:
    *   `FORWARDS` (oder `forwards`): Füllt fehlende Werte mit dem letzten bekannten vorherigen Wert (Last Observation Carried Forward - LOCF).
    *   `BACKWARDS` (oder `backwards`): Füllt fehlende Werte mit dem nächsten bekannten folgenden Wert (Next Observation Carried Backward - NOCB).
    *   `CONSTANT` (oder `constant`): Füllt fehlende Werte mit dem `<defaultValue>`.
*   `<defaultValue>`: Der Wert, der für `CONSTANT`-Interpolation verwendet wird, oder als Fallback, wenn `FORWARDS`/`BACKWARDS` keine Werte finden (z.B. am Anfang/Ende des Datensatzes). Kann eine Zahl oder das Schlüsselwort `NULL` sein.

**Beispiel:**
```kalaql
Load data: my_sensor ... NoCache
// Füllt Lücken mit dem letzten bekannten Wert, oder 0.0 wenn am Anfang keine Daten sind
Interpolate filled_data: data FORWARDS 0.0

// Füllt Lücken mit dem Wert 100
Inpo constant_fill: data CONSTANT 100
```

### 3.5. `MatView`

Erstellt oder verwendet eine materialisierte Sicht. Eine materialisierte Sicht speichert das Ergebnis einer Unterabfrage, um zukünftige Abfragen zu beschleunigen.

**Syntax:**
`MatView <outputName>: <inputName> <viewStorageName>`

**Parameter:**

*   `<outputName>`: Name des Datensatzes, der das Ergebnis der MatView-Operation darstellt (entweder aus der existierenden Sicht gelesen oder das Ergebnis der neu materialisierten Daten).
*   `<inputName>`: Name des Eingabe-Datensatzes, der materialisiert werden soll, falls die Sicht neu erstellt werden muss.
*   `<viewStorageName>`: Der Name, unter dem die materialisierte Sicht im DataLayer gespeichert wird (und unter dem die `viewdef.txt` abgelegt wird).

**Funktionsweise:**
1.  Prüft, ob eine MatView mit `<viewStorageName>` bereits im DataLayer existiert.
2.  **Wenn ja**: Liest die Daten für den im ursprünglichen `Load`-Befehl der Abfragekette spezifizierten Zeitbereich aus der gespeicherten Sicht.
3.  **Wenn nein**:
    *   Führt die Abfragekette, die zu `<inputName>` führt, erneut aus. Dabei werden bei allen `Load`-Operationen in dieser Kette die Zeitfilter entfernt (d.h., es werden alle Daten geladen).
    *   Das Ergebnis dieser vollständigen Abfrage wird unter `<viewStorageName>` im DataLayer gespeichert (sowohl die Datenpunkte als auch eine `viewdef.txt` mit der Query).
    *   Anschließend werden die Daten für den ursprünglich angeforderten Zeitbereich aus dieser neu erstellten Sicht gelesen.

**Beispiel:**
```kalaql
// Definiere eine komplexe Berechnung
Load base_data: sensor_X 2023-01-01T00:00:00Z 2023-01-31T23:59:59Z NoCache
Aggregate daily_avg: base_data Aligned_1Day Avg
Expr processed_data: daily_avg "value * 1.8 + 32" // Umrechnung in Fahrenheit

// Erstelle/verwende eine MatView für die prozessierten Daten
// Die Sicht wird im DataLayer als 'fahrenheit_sensor_X_daily' gespeichert
MatView final_view_data: processed_data fahrenheit_sensor_X_daily

Publish final_view_data Table
```
Im obigen Beispiel: Wenn `fahrenheit_sensor_X_daily` nicht existiert, wird `base_data` für den gesamten Zeitraum geladen, aggregiert, mit `Expr` verarbeitet und dann als `fahrenheit_sensor_X_daily` gespeichert. Anschließend wird `final_view_data` mit den Daten für Januar 2023 aus dieser Sicht befüllt. Bei nachfolgenden Abfragen mit demselben `MatView`-Aufruf (und ggf. anderem Zeitfilter für `base_data`) wird direkt aus `fahrenheit_sensor_X_daily` gelesen.

### 3.6. `Insert`

Diese Operation ist typischerweise nicht Teil von Lese-Abfragen, sondern dient dem Einfügen von Daten. Ihre genaue Verwendung im Kontext einer `KalaQuery`-Pipeline für Leseoperationen ist unklar, aber sie existiert als Parser.

**Syntax (basierend auf `InsertParser`):**
`Insert <outputName_oder_DummyName>: <measurementName> <wert>`

**Parameter:**
*   `<outputName_oder_DummyName>`: Name (Bedeutung in Lese-Query unklar).
*   `<measurementName>`: Ziel-Measurement.
*   `<wert>`: Der einzufügende Wert (Format unklar, evtl. "timestamp wert"-Paar als String).

**Hinweis:** Die primäre Methode zum Einfügen von Daten ist `IDataLayer.Insert()`. Die `Op_Insert` könnte für spezielle Szenarien innerhalb einer Query-Kette gedacht sein.

### 3.7. `Expr` (Expresso)

Wendet einen NCalc-Ausdruck auf jeden Datenpunkt eines Eingabe-Datensatzes an.

**Syntax:**
`Expr <outputName>: <inputName> "<ncalcAusdruck>"`

**Parameter:**

*   `<outputName>`: Name des resultierenden Datensatzes.
*   `<inputName>`: Name des Eingabe-Datensatzes.
*   `<ncalcAusdruck>`: Ein String, der einen NCalc-kompatiblen Ausdruck enthält. Der Wert des aktuellen Datenpunkts ist typischerweise über eine Variable wie `value` oder den Namen des Eingabe-Datensatzes zugänglich. Anführungszeichen im Ausdruck sollten mit `'` (einfach) maskiert werden, wenn der gesamte Ausdruck in `"` (doppelt) steht, oder umgekehrt. Der Parser (`ExpressoParser`) ersetzt `'` durch `"` oder `"` durch `'` je nach äußerer Begrenzung.

**Beispiel:**
```kalaql
Load temps_c: sensor_temp 2023-01-01T00:00:00Z 2023-01-01T23:59:59Z NoCache
// Konvertiert Celsius in Fahrenheit
Expr temps_f: temps_c "value * 9/5 + 32"
Publish temps_f Table
```

### 3.8. `Publish` (oder `Publ`)

Gibt einen oder mehrere Datensätze aus. Dies ist typischerweise die letzte Operation in einer Abfrage.

**Syntax:**
`Publish <inputName1>[,<inputName2>,...] <mode>`
`Publ <inputName1>[,<inputName2>,...] <mode>`

**Parameter:**

*   `<inputName1>[,<inputName2>,...]`: Eine kommaseparierte Liste von Namen der Datensätze, die veröffentlicht werden sollen. Wenn die Namen Leerzeichen oder Sonderzeichen enthalten, müssen sie in der Query-Zeichenfolge möglicherweise in Anführungszeichen gesetzt werden (z.B. `Publ "mein sensor", "anderer sensor" Table`). Die `ToLine()`-Methode der Operation erzeugt sie ohne äußere Anführungszeichen, wenn es mehrere sind.
*   `<mode>`: Der Veröffentlichungsmodus. Mögliche Werte:
    *   `Table` (oder `CombinedResultset`): Kombiniert alle angegebenen Datensätze zu einer einzigen Tabelle. Datenpunkte werden anhand ihrer Zeitstempel synchronisiert.
    *   `MultipleResultsets`: Gibt jeden Datensatz als separates Ergebnis zurück.

**Beispiele:**

```kalaql
Load raw: sensor_A ...
Aggregate avg_data: raw Aligned_1Hour Avg
Aggregate max_data: raw Aligned_1Hour Max

// Veröffentlicht avg_data und max_data als eine kombinierte Tabelle
Publish avg_data, max_data Table

// Veröffentlicht avg_data und max_data als separate Resultsets
Publ avg_data, max_data MultipleResultsets
```

### 3.9. `AlTz` (Align Timezone)

Passt die Zeitzone der Zeitstempel in einem Datensatz an. (Details zur genauen Funktionsweise und Implementierung sind aus den bereitgestellten Parsern nicht vollständig ersichtlich, aber die Syntax ist bekannt.)

**Syntax:**
`AlTz <timezoneIdentifier>`

**Parameter:**
*   `<timezoneIdentifier>`: Ein String, der die Zielzeitzone identifiziert (z.B. "Europe/Berlin", "UTC").

**Beispiel (hypothetisch, da die Anwendung auf einen Datensatz fehlt):**
```kalaql
Load utc_data: my_sensor ...
// Die AlTz-Operation scheint global auf den Kontext zu wirken oder
// benötigt eine andere Syntax, um auf einen spezifischen Datensatz angewendet zu werden.
// Die aktuelle Parser-Definition nimmt nur die Zeitzone.
// Möglicherweise wird sie implizit auf alle nachfolgenden Operationen angewendet
// oder auf einen spezifischen Datensatz, der im Kontext ausgewählt ist.
// Für die Dokumentation hier die Basissyntax:
AlTz "Europe/Berlin"
Publish utc_data Table // Zeitstempel in utc_data wären jetzt als Europe/Berlin interpretiert/konvertiert
```
**Anmerkung:** Die genaue Anwendung von `AlTz` auf Datensätze müsste in `Op_AlignTimezone.cs` und seiner Verwendung im `KalaQlContext` geprüft werden. Der Parser nimmt nur die Zeitzone als Argument.

### 3.10. `Mgmt`

Führt Management-Operationen auf dem DataLayer aus.

**Syntax:**
`Mgmt <managementAction> [<parameter1> <parameter2> ...]`

**Parameter:**
*   `<managementAction>`: Die auszuführende Management-Aktion. Die tatsächlich im Code (`MgmtAction.cs`) definierten und vom `MgmtParser` sowie `Op_Mgmt.cs` verarbeiteten Aktionen und ihre Parameter sind:
    *   `LoadMeasures`: Lädt eine Liste aller Measurements.
        *   Parameter: Keine.
    *   `SortAllRaw`: Sortiert Rohdatendateien aller Measurements.
        *   Parameter: Keine (operiert auf allen Measurements).
    *   `FsChk`: Führt einen Dateisystemcheck für alle Measurements durch (prüft Lesbarkeit der Daten).
        *   Parameter: Keine (operiert auf allen Measurements).
    *   `ImportInflux <influxImportParams>`: Importiert Daten aus InfluxDB.
        *   Parameter: `<influxImportParams>` - Ein String, der die für den `InfluxLineProtocolImporter` spezifischen Parameter enthält (z.B. Dateipfad zur Influx Line Protocol Datei).
    *   `ImportMariaDbTstsfe <mariaDbImportParams>`: Importiert Daten aus einer MariaDB (tstsfe-Schema).
        *   Parameter: `<mariaDbImportParams>` - Ein String, der die für den `MigrateMariaDb_Tstsfe_Custom`-Importer spezifischen Parameter enthält (z.B. Verbindungsdetails, Tabellennamen).
    *   `Copy <sourceMeasurement> <targetMeasurement>`: Kopiert Daten von einem Quell-Measurement zu einem Ziel-Measurement.
        *   Parameter: `<sourceMeasurement>` (Name des Quell-Measurements), `<targetMeasurement>` (Name des Ziel-Measurements). Getrennt durch Leerzeichen.
    *   `Sort <measurementName>`: Sortiert Rohdatendateien für ein spezifisches Measurement.
        *   Parameter: `<measurementName>` (Name des zu sortierenden Measurements).
        *   *Anmerkung: Obwohl im Enum vorhanden, wird `Sort` nicht explizit in `Op_Mgmt.Execute` behandelt. `SortAllRaw` ist implementiert. Die Funktionalität für ein einzelnes Measurement über `Mgmt Sort <name>` ist via `IDataLayer.SortRawFiles` vorhanden, aber nicht direkt in `Op_Mgmt` verdrahtet.*
    *   `Rename <oldMeasurementName> <newMeasurementName>`: Benennt ein Measurement um.
        *   Parameter: `<oldMeasurementName>`, `<newMeasurementName>`. Getrennt durch Leerzeichen.
    *   `Clean <measurementName>`: Führt Aufräumarbeiten für ein Measurement durch.
        *   Parameter: `<measurementName>` (Name des zu bereinigenden Measurements).
        *   *Anmerkung: Obwohl im Enum vorhanden, wird `Clean` nicht explizit in `Op_Mgmt.Execute` behandelt. Die Funktionalität ist in `IDataLayer.Cleanup` vorhanden.*
    *   `BenchmarkIo`: Führt I/O-Benchmarks auf dem Datenverzeichnis aus.
        *   Parameter: Keine (operiert auf dem konfigurierten `DataDirectory`).
    *   `UnBlacklist <measurementName>`: Entfernt ein Measurement von der Blacklist.
        *   Parameter: `<measurementName>`.
    *   `Blacklist <measurementName>`: Fügt ein Measurement zur Blacklist hinzu.
        *   Parameter: `<measurementName>`.
*   Die Parameter werden als ein einzelner String nach der Action an `Op_Mgmt` übergeben und dort weiter aufgeteilt (typischerweise mit Leerzeichen als Trenner oder als Ganzes an spezialisierte Importer weitergereicht).

**Beispiele:**
```kalaql
// Liste alle Measurements auf
Mgmt LoadMeasures

// Sortiere Rohdateien aller Measurements
Mgmt SortAllRaw

// Kopiere 'sensor_A_temp' nach 'sensor_A_backup'
Mgmt Copy sensor_A_temp sensor_A_backup

// Benenne 'sensor_X_old' um zu 'sensor_X_final'
Mgmt Rename sensor_X_old sensor_X_final

// Blackliste 'defekter_sensor'
Mgmt Blacklist defekter_sensor

// Importiere Daten aus einer InfluxDB Line Protocol Datei
Mgmt ImportInflux "/pfad/zu/influxdata.txt"
```

## 4. Enumerationen und Schlüsselwörter (Zusammenfassung)

### 4.1. Cache Resolution (für `Load`)

*   `NoCache`
*   `AUTO(<seconds>)`
*   `<Resolution>_<AggregateFunction>[_REBUILD][_REFRESHINCREMENTAL]`
    *   **Resolution**: `MINUTELY`, `FIVEMINUTELY`, `FIFTEENMINUTELY`, `HOURLY`
    *   **AggregateFunction**: `AVG`, `WAVG`, `FIRST`, `LAST`, `MIN`, `MAX`, `COUNT`, `SUM`

### 4.2. Window (für `Aggregate`)

*   Zeitspanne: `HH:mm:ss`
*   Aligned: `Aligned_5Minutes`, `Aligned_15Minutes`, `Aligned_1Hour`, `Aligned_1Day`, `Aligned_1Week`, `Aligned_1Month`, `Aligned_1Year`, `Aligned_1YearStartAtHalf`
*   Unaligned: `Unaligned_1Month`, `Unaligned_1Year`
*   `Infinite`

### 4.3. AggregateFunction (für `Aggregate`)

*   `Avg`, `WAvg`, `First`, `Last`, `Min`, `Max`, `Count`, `Sum`

### 4.4. InterpolationMode (für `Interpolate`)

*   `FORWARDS` (oder `forwards`)
*   `BACKWARDS` (oder `backwards`)
*   `CONSTANT` (oder `constant`)

### 4.5. PublishMode (für `Publish`)

*   `Table` (entspricht `CombinedResultset`)
*   `MultipleResultsets`
*   `CombinedResultset` (wird vom Parser auch akzeptiert und zu `PublishMode.CombinedResultset` gemappt)

### 4.6. MgmtAction (für `Mgmt`)

Die folgenden Aktionen sind im `MgmtAction`-Enum definiert:

*   `LoadMeasures`
*   `SortAllRaw`
*   `FsChk`
*   `ImportInflux`
*   `ImportMariaDbTstsfe`
*   `Copy`
*   `Sort`
*   `Rename`
*   `Clean`
*   `BenchmarkIo`
*   `UnBlacklist`
*   `Blacklist`

Die genauen Parameter für jede Aktion werden in der `Op_Mgmt`-Klasse verarbeitet.

---
Diese Dokumentation sollte einen umfassenden Überblick über die KalaQl-Syntax und ihre Komponenten bieten.
