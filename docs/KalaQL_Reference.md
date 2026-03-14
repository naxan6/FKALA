# KalaQL Referenz

KalaQL ist eine prozedurale, zeilenbasierte Abfragesprache für Zeitreihendaten. Sie besteht aus einer Sequenz von Operationen (Ops), die nacheinander ausgeführt werden. Jede Operation verarbeitet Daten aus dem vorherigen Zustand und erzeugt ein neues Dataset im KalaQlContext. Die Syntax ist streng zeichenbasiert und erfordert exakte Groß-/Kleinschreibung.

## Grundlagen

Die Ausführung beginnt mit `KalaQuery.Start()`. Jede Operation wird mit `Add()` zur Query hinzugefügt. Die Ausgabe erfolgt über `Publish`.

### Beispiel: Einfache Abfrage
```
Load temp: room_temp 2025-12-28T00:00:00Z 2025-12-29T00:00:00Z MINUTELY_AVG
Aggr daily_avg: temp Aligned_1Day AVG
Publ daily_avg CombinedResultset
```

## Unterstützte Operationen

### Load
Lädt Rohdaten aus einer Messung.

**Syntax:**
```
Load name: measurement start end resolution
Load name: measurement NewestOnly
```

**Parameter:**
- `name`: Name des resultierenden Datasets (z. B. `temp`)
- `measurement`: Name der Messung in der Datenbank
- `start`: Startzeitpunkt im ISO 8601-Format (UTC)
- `end`: Endzeitpunkt im ISO 8601-Format (UTC)
- `resolution`: Cache-Auflösung (z. B. `MINUTELY_AVG`, `HOURLY_SUM`)
- `NewestOnly`: Lädt nur den neuesten Datenpunkt

**Beispiele:**
```
Load temp: room_temp 2025-12-28T00:00:00Z 2025-12-29T00:00:00Z MINUTELY_AVG
Load latest: sensor NewestOnly
```

### Aggr (Aggregate)
Führt eine Aggregation über ein Zeitfenster durch.

**Syntax:**
```
Aggr name: input window aggregatefunc [EmptyWindows]
```

**Parameter:**
- `name`: Name des resultierenden Datasets
- `input`: Name des Eingabedatasets
- `window`: Zeitfenstermodus (siehe unten)
- `aggregatefunc`: Aggregationsfunktion (siehe unten)
- `EmptyWindows`: Optional. Gibt leere Fenster aus, wenn kein Datenpunkt vorhanden ist

**Unterstützte Window-Modi:**
- `Aligned_1Minute`
- `Aligned_5Minutes`
- `Aligned_15Minutes`
- `Aligned_1Hour`
- `Aligned_1Day`
- `Aligned_1Week`
- `Aligned_1Month`
- `Aligned_1Year`
- `Aligned_1YearStartAtHalf`
- `Unaligned_1Month`
- `Unaligned_1Year`
- `Infinite`

**Unterstützte Aggregationsfunktionen:**
- `AVG`
- `WAVG`
- `FIRST`
- `LAST`
- `MIN`
- `MAX`
- `COUNT`
- `SUM`
- `TEXTFIRST`
- `TEXTLAST`
- `TEXTCOUNT`

**Beispiele:**
```
Aggr daily_avg: temp Aligned_1Day AVG
Aggr monthly_min: temp Aligned_1Month MIN EmptyWindows
Aggr hourly_sum: pressure Aligned_1Hour SUM
```

### Var
Definiert eine Variable für die Verwendung in anderen Operationen.

**Syntax:**
```
Var name: value
```

**Parameter:**
- `name`: Name der Variable (z. B. `threshold`)
- `value`: Wert der Variable als String

**Beispiele:**
```
Var threshold: 25.5
Var unit: °C
```

### AlTz (AlignTimezone)
Setzt die Zeitzone für alle nachfolgenden Operationen.

**Syntax:**
```
AlTz timezone
```

**Parameter:**
- `timezone`: IANA-Zeitzone (z. B. `Europe/Berlin`, `America/New_York`)

**Beispiel:**
```
AlTz Europe/Berlin
```

### InPo (Interpolate)
Interpoliert fehlende Werte in einem Dataset.

**Syntax:**
```
InPo name: input FORWARDS|BACKWARDS|CONSTANT [constantvalue]
```

**Parameter:**
- `name`: Name des resultierenden Datasets
- `input`: Name des Eingabedatasets
- `FORWARDS|BACKWARDS|CONSTANT`: Interpolationsmodus
- `constantvalue`: Optional. Wert für `CONSTANT`-Interpolation

**Unterstützte Interpolationsmodi:**
- `FORWARDS`
- `BACKWARDS`
- `CONSTANT`

**Beispiele:**
```
InPo temp_interp: raw_temps FORWARDS 0
InPo humidity_interp: hum BACKWARDS
```

### Expr (Expresso)
Führt mathematische Berechnungen mit DynamicExpresso aus.

**Syntax:**
```
Expr name: "expression"
```

**Parameter:**
- `name`: Name des resultierenden Datasets
- `expression`: Mathematischer Ausdruck mit Variablen (z. B. `temp_f`, `hum`)

**Beispiele:**
```
Expr temp_c: (temp_f - 32) * 5 / 9
Expr humidity_ratio: hum / 100
Expr alert: temp_c > threshold ? 1 : 0
```

### Loaj (JsonQuery)
Extrahiert Werte aus JSON-Daten.

**Syntax:**
```
Loaj name: measurement fieldpath start end resolution
Loaj name: measurement start end resolution
Loaj name: measurement fieldpath NewestOnly
```

**Parameter:**
- `name`: Name des resultierenden Datasets
- `measurement`: Name der Messung mit JSON-Daten
- `fieldpath`: JSON-Pfad (z. B. `/data/pressure`). Standard: `$.*`
- `start`: Startzeitpunkt im ISO 8601-Format (UTC)
- `end`: Endzeitpunkt im ISO 8601-Format (UTC)
- `resolution`: Cache-Auflösung
- `NewestOnly`: Lädt nur den neuesten JSON-Datensatz

**Unterstützte Cache-Resolutionen:**
- `MINUTELY`
- `FIVEMINUTELY`
- `FIFTEENMINUTELY`
- `HOURLY`
- `NoCache`

**Beispiele:**
```
Loaj pressure: sensor_json /data/pressure 2025-12-28T00:00:00Z 2025-12-29T00:00:00Z HOURLY_AVG
Loaj json_data: sensor_json 2025-12-28T00:00:00Z 2025-12-29T00:00:00Z MINUTELY
Loaj latest_json: sensor_json NewestOnly
```

### Publ (Publish)
Gibt die Ergebnisse aus.

**Syntax:**
```
Publ name1,name2 CombinedResultset|MultipleResultsets
```

**Parameter:**
- `name1,name2,...`: Namen der zu publizierenden Datasets
- `CombinedResultset`: Alle Datasets in einem einzigen Resultset
- `MultipleResultsets`: Jedes Dataset als eigenes Resultset

**Unterstützte Publish-Modi:**
- `CombinedResultset`
- `MultipleResultsets`

**Beispiele:**
```
Publ agg_temp,agg_hum CombinedResultset
Publ temp,hum MultipleResultsets
```

### MatView (Materialized View)
Erstellt einen zwischengespeicherten View für Performance-Optimierung.

**Syntax:**
```
MatView name: input measurement
```

**Parameter:**
- `name`: Name des resultierenden Datasets
- `input`: Name des Eingabedatasets
- `measurement`: Name der Messung, in der der View gespeichert wird

**Beispiel:**
```
MatView daily_summary: daily_avg daily_summaries
```

### Insert
Fügt einen einzelnen Datenpunkt ein.

**Syntax:**
```
Insert name: measurement value
```

**Parameter:**
- `name`: Name des resultierenden Datasets
- `measurement`: Name der Messung
- `value`: Wert als String

**Beispiel:**
```
Insert manual_entry: temperature "23.5"
```

### Mgmt (Management)
Führt Systemoperationen aus.

**Syntax:**
```
Mgmt action
```

**Unterstützte MgmtAction-Werte:**
- `LoadMeasures`
- `SortAllRaw`
- `FsChk`
- `Copy`
- `Rename`
- `Blacklist`
- `UnBlacklist`
- `ImportInflux`
- `ImportMariaDbTstsfe`
- `BenchmarkIo`
- `Statistics`

**Beispiel:**
```
Mgmt FsChk
```

## Hinweise zur Syntax

- **Groß-/Kleinschreibung**: Exakt einhalten (z. B. `Aggr`, nicht `AGGR`; `AlTz`, nicht `ALTZ`; `InPo`, nicht `INPO`; `Loaj`, nicht `LOAJ`; `Publ`, nicht `PUBL`; `Expr`, nicht `EXPR`)
- **Leerzeichen**: Nur ein Leerzeichen zwischen Parametern erlaubt
- **Variablen**: In Expresso-Ausdrücken mit `$`-Syntax referenziert (z. B. `$threshold`)
- **Zeitformate**: Immer UTC, im ISO 8601-Format (`YYYY-MM-DDTHH:mm:ssZ`)
- **Alias-Verben**: `Aggr` = `Aggregate`, `InPo` = `Interpolate`, `Loaj` = `JsonQuery`, `Publ` = `Publish`, `AlTz` = `AlignTimezone`

## Komplexes Beispiel: Temperaturüberwachung mit Alarm

Dieses Beispiel kombiniert mehrere Operationen zu einer vollständigen Analyse:

```
Load temp_raw: room_temp 2025-12-28T00:00:00Z 2025-12-29T00:00:00Z MINUTELY
AlTz Europe/Berlin
Aggr hourly_avg: temp_raw Aligned_1Hour AVG
InPo temp_interp: hourly_avg FORWARDS 0
Var threshold: 25.5
Expr alert: temp_interp > threshold ? 1 : 0
Publ temp_interp,alert CombinedResultset
```

**Erklärung:**
1. `Load`: Lädt rohe Temperaturdaten mit Minutenauflösung.
2. `AlTz`: Setzt die Zeitzone auf Berlin für korrekte Tageszeiten.
3. `Aggr`: Bildet den stündlichen Durchschnitt über die Rohdaten.
4. `InPo`: Interpoliert fehlende Stunden mit dem letzten Wert (FORWARDS).
5. `Var`: Definiert eine Schwelle von 25,5°C für einen Alarm.
6. `Expr`: Berechnet einen Alarmzustand (1 = über Schwelle, 0 = darunter).
7. `Publ`: Gibt beide Datasets (interpolierte Temperatur und Alarmzustand) als ein Resultset aus.

Dieses Beispiel zeigt die echte Macht von KalaQL: Daten laden, transformieren, berechnen und visualisieren – alles in einer einzigen, lesbaren Abfrage.