# FKala — Architektur- und Code-Review

> Erstellt: 2026-03-23  
> Scope: Vollständige Analyse aller Projekte (FKala.Core, FKala.Api, FKala.Unittests, etc.)

---

## Inhaltsverzeichnis

1. [Zusammenfassung](#zusammenfassung)
2. [Kritische Bugs](#1-kritische-bugs)
3. [Data Layer — Concurrency & Integrität](#2-data-layer--concurrency--integrität)
4. [Query Engine — Korrektheit & Sicherheit](#3-query-engine--korrektheit--sicherheit)
5. [API & Infrastruktur](#4-api--infrastruktur)
6. [Architektur & Projektstruktur](#5-architektur--projektstruktur)
7. [Verbesserungsvorschläge nach Priorität](#6-verbesserungsvorschläge-nach-priorität)

---

## Zusammenfassung

| Severity | Anzahl | Bereiche |
|----------|--------|----------|
| 🔴 Critical | 8 | Infinite Loop, Datenverlust, Race Conditions, Blacklist-Bug, Code Execution |
| 🟠 High | 18 | TOCTOU, Resource Leaks, Memory Leaks, Reconnect-Logik, Sicherheit |
| 🟡 Medium | 20 | Aggregation-Korrektheit, Cache-Inkonsistenz, Konfiguration, Doku |
| 🟢 Low | 10 | Code-Qualität, Minor Null-Safety, Dead Code |

---

## 1. Kritische Bugs

### 1.1 🔴 Infinite Loop im Exception-Handler — QueryController

**Datei:** `FKala.Api/Controller/QueryController.cs`, Zeile ~104  

```csharp
while (ie != null)
{
    exres.Add("iex " + ie.Message);
    exres.Add("iexstack: " + ie.StackTrace);
    ie = ex.InnerException; // BUG: müsste ie.InnerException sein!
}
```

`ie = ex.InnerException` setzt `ie` immer auf dieselbe Referenz — die Schleife terminiert nie. Hängt den Request-Thread und allokiert Speicher bis OOM.

**Fix:** `ie = ie.InnerException;`

---

### 1.2 🔴 `LoadMeasureBlacklist()` — Ergebnis wird nie zugewiesen

**Datei:** `FKala.Core/DataLayer/DataLayer_Readable_Caching_V1.cs`, Zeilen 362–370  

```csharp
ConcurrentDictionary<string, bool> newBl = new(); // lokale Variable
foreach (var blDir in blackListDirs)
    newBl.AddOrUpdate(Path.GetFileName(blDir), true, ...);
// MeasurementBlacklist wird NICHT zugewiesen ← BUG
```

Die Blacklist wird beim Start in eine lokale Variable geladen und dann verworfen. `MeasurementBlacklist` bleibt leer. Blacklisted Measurements werden beim Insert-Hot-Path nicht erkannt (`IsBlacklisted(measurement, false)` prüft nur den leeren Cache).

**Fix:** `this.MeasurementBlacklist = newBl;` am Ende der Methode.

---

### 1.3 🔴 Sort-on-Read ohne Cross-Query-Locking — Concurrent File Corruption

**Datei:** `FKala.Core/DataLayer/StorageAccess.cs`, Zeile 59, 146–198  

Jede `StorageAccess`-Instanz hat einen eigenen `LockManager`. Zwei gleichzeitige Queries auf denselben unsorted Day erzeugen unabhängige Locks. Beide erkennen Sortierungsbedarf, beide schreiben `.sorted`-Dateien, beide versuchen Rename/Delete auf denselben Pfaden.

**Folgen:** `IOException` bei Rename, möglicher permanenter Datenverlust wenn `.bak`-Dateien gelöscht werden bevor der Rename des `.sorted`-Files gelingt.

**Fix:** `LockManager` als Singleton oder statische Instanz teilen, nicht pro `StorageAccess`-Instanz.

---

### 1.4 🔴 Insert-Datenverlust bei Crash — kein Write-Ahead-Log

**Dateien:** `DataLayer_Readable_Caching_V1.cs` (Zeilen 38, 209–217), `BufferedWriterService.cs` (Zeile 33)  

Pipeline: `Insert()` → `_insertQueue` (in-memory) → `ProcessInsertQueueAsync` → `BufferedWriter` (in-memory buffer, 128KB) → Flush alle 10 Sek.

**Datenverlust-Fenster:** Bis zu 10 Sekunden Inserts + ein StreamWriter-Buffer (128KB). Bei Crash geht alles verloren, was noch nicht geflusht wurde.

**Fix:** Write-Ahead-Log oder mindestens `Flush(flushToDisk: true)` nach jedem Batch.

---

### 1.5 🔴 `Flush()` drains Queue parallel zu `ProcessInsertQueueAsync` — Race Condition

**Datei:** `DataLayer_Readable_Caching_V1.cs`, Zeilen 547–550  

`Flush()` wird von Reader-Threads aufgerufen und ruft `ProcessRemainingQueueItems()` auf. Gleichzeitig draint `ProcessInsertQueueAsync` dieselbe Queue. `ProcessLines` mutiert `LatestEntries` in einer nicht-atomaren Check-then-Update-Sequenz.

**Folge:** Delayed Inserts werden nicht erkannt → Datei nicht als unsorted markiert → falsche Query-Ergebnisse.

**Fix:** `Flush(filePath)` sollte nur den spezifischen Writer flushen, nicht die globale Queue drainen.

---

### 1.6 🔴 `DeveloperExceptionPage` in Produktion aktiv

**Datei:** `FKala.Api/Program.cs`, Zeile 116  

`app.UseDeveloperExceptionPage()` ohne Environment-Check. Leakt volle Stack Traces, Source-Code-Schnipsel, Environment-Variablen und Konfiguration an jeden Client.

**Fix:**
```csharp
if (app.Environment.IsDevelopment())
    app.UseDeveloperExceptionPage();
```

---

### 1.7 🔴 DynamicExpresso — Beliebige Expression-Ausführung ohne Sandbox

**Datei:** `FKala.Core/KalaQl/Op_Expresso.cs`, Zeilen 28–47  

Der `Interpreter` wird mit Default-Konfiguration instanziiert. Keine Type-Restrictions, keine Complexity-Limits, kein Timeout. Computational bombs (`Math.Pow(Math.Pow(9999999m, 9999999m), 9999999m)`) können die CPU blockieren.

**Fix:** Timeout/CancellationToken, Type-Whitelist, Expression-Längen-Limit.

---

### 1.8 🔴 CI/CD führt keine Tests aus

**Datei:** `.github/workflows/docker-image.yml`  

Der einzige Workflow baut und pusht ein Docker-Image. Kein `dotnet test`, kein `dotnet build`-Verifizierungsschritt, keine statische Analyse. Kaputter Code wird direkt zu DockerHub deployed.

**Fix:** `dotnet test` Step vor dem Docker-Push hinzufügen.

---

## 2. Data Layer — Concurrency & Integrität

### 2.1 🟠 Constructor-Ordering: Custom `writeBuffer` wird ignoriert

**Datei:** `DataLayer_Readable_Caching_V1.cs`, Zeilen 69–73  

```csharp
public DataLayer_Readable_Caching_V1(string path, int readBuf, int writeBuf) : this(path)
{
    this.WriteBuffer = writeBuf; // wird NACH dem Base-Ctor gesetzt
}
```

Der verkettete Konstruktor `this(path)` erstellt `BufferedWriterSvc` mit dem Default `WriteBuffer = 131072`. Der übergebene `writeBuffer` wird danach gesetzt, hat aber keinen Effekt mehr.

**Fix:** Buffer-Parameter vor der Service-Erstellung setzen.

---

### 2.2 🟠 TOCTOU Race bei Cache-Build — `IsLocked` / `AcquireLock`

**Datei:** `FKala.Core/DataLayer/Cache/CachingLayer.cs`, Zeilen 54–64  

```csharp
bool cacheAlreadyInWork = _lockManager.IsLocked(path); // Check ohne Lock
using (var lockHandle = _lockManager.AcquireLock(path)) // Lock erwerben
{
    if (!cacheAlreadyInWork) { /* Cache rebuilden */ }
}
```

Zwischen `IsLocked` und `AcquireLock` kann sich der Zustand ändern. Zwei Threads bauen den Cache doppelt.

**Fix:** Double-Check-Pattern innerhalb des Locks verwenden.

---

### 2.3 🟠 `BufferedWriter.Dispose()` — Flush ist Dead Code

**Datei:** `FKala.Core/DataLayer/Infrastructure/BufferedWriter.cs`, Zeilen 56–65  

`Dispose()` setzt `disposed = true` **vor** dem Aufruf von `Flush()`. `Flush()` prüft `if (!disposed)` → kehrt sofort zurück. Der explizite Flush in Dispose ist damit wirkungslos.

**Fix:** Reihenfolge umkehren: erst `Flush()`, dann `disposed = true`.

---

### 2.4 🟠 `WriteSortedFile` — Crash zwischen File.Move hinterlässt unsichtbare Daten

**Datei:** `StorageAccess.cs`, Zeilen 246–256  

```csharp
File.Move(filePath, bakFile);              // 1: Original → .bak
File.Move(filePath + ".sorted", filePath); // 2: .sorted → Original
File.Delete(bakFile);                      // 3: .bak löschen
```

Crash nach Schritt 1 vor Schritt 2: Original-Datei ist weg (`.bak`), `.sorted` nicht umbenannt. Daten sind beim Neustart unauffindbar.

**Fix:** Beim Start nach `.sorted`- und `.bak`-Dateien scannen und Recovery durchführen.

---

### 2.5 🟠 Double Enumeration in Cache-Generation — 2x Laufzeit

**Datei:** `FKala.Core/DataLayer/Cache/Cache_Base.cs`, Zeilen 70–88  

```csharp
if (rs.Any()) // Erste Enumeration: vollständige Pipeline für 1. Element
{
    foreach (var dp in rs) // Zweite Enumeration: Pipeline komplett neu
```

`rs` ist lazy-evaluated. `Any()` startet die gesamte Load+Sort-Pipeline, verwirft sie, `foreach` startet neu.

**Fix:** `Any()` entfernen oder Ergebnis materialisieren.

---

### 2.6 🟠 `_processedItemsTimestamps` — Unbegrenztes Wachstum

**Datei:** `DataLayer_Readable_Caching_V1.cs`, Zeilen 50, 290–294  

Jeder Insert enqueued einen `DateTime`. Cleanup nur in `GetStatistics()`. Ohne regelmäßigen Aufruf wächst die Queue endlos (~19 MB/Tag bei 100K Inserts/Stunde).

**Fix:** Periodisches Cleanup im `ProcessInsertQueueAsync`.

---

### 2.7 🟠 `LockManager` entfernt nie Einträge

**Datei:** `FKala.Core/DataLayer/Infrastructure/LockManager.cs`, Zeile 13  

Jeder einzigartige File-Path erzeugt ein `SemaphoreSlim`, das nie entfernt wird. Bei tausenden Measurements über Jahre wächst das Dictionary unbegrenzt.

**Fix:** Eviction-Policy oder Reference-Counting hinzufügen.

---

### 2.8 🟡 Dedup-Inkonsistenz: First-Wins vs. Last-Wins

**Datei:** `StorageAccess.cs`, Zeilen 280–329 vs. 168–173  

Inline-Merge bei gleichen Timestamps innerhalb einer Datei: **First Wins** (`retPrev.Value = retPrev.Value ?? ret.Value`). Dictionary-Dedup über mehrere Dateien: **Last Wins**. Inkonsistentes Verhalten bei Duplikaten.

---

### 2.9 🟡 Binary-Search im Cache — BUG/HACK-Kommentar

**Datei:** `Cache_Base.cs`, Zeilen 111–151  

```csharp
(current.StartTime < startTime.AddMinutes(-1) && jumpintervall < 1024) //BUG/HACK
```

Die Suche bricht bei 1024 Bytes ab, was für Minutely-Caches ~50 Minuten zusätzlichen Linear-Scan bedeutet.

---

### 2.10 🟡 `CreatedDirectories`-Cache wird bei Measurement-Löschung nicht invalidiert

**Datei:** `DataLayer_Readable_Caching_V1.cs`, Zeile 47 vs. Zeilen 651–675  

Nach `DeleteMeasure` glaubt der Directory-Cache, das Verzeichnis existiert noch. Neue Inserts für dasselbe Measurement scheitern dann beim Schreiben.

---

### 2.11 🟡 Cache-Invalidierung während Read möglich

**Datei:** `CachingLayer.cs`, Zeilen 208–223; `Cache_Base.cs`, Zeilen 53–66  

`Cache_Base.Invalidate` löscht Cache-Dateien per `File.Delete()`, während `LoadCache` die Datei ohne Lock lesen könnte. Auf Linux: Stale Data. Auf Windows: IOException.

---

### 2.12 🟡 String-Interpolation-Bug — Pfad nicht ausgegeben

**Datei:** `BufferedWriterService.cs`, Zeile 108  

```csharp
Console.WriteLine("Error at BufferedWriter with path <{filePath}>");
// Fehlt das $-Zeichen → gibt den Literal-Text "{filePath}" aus
```

---

### 2.13 🟡 `SemaphoreSlim`-Drain — Spurious Wakeups unter Last

**Datei:** `DataLayer_Readable_Caching_V1.cs`, Zeilen 298–322  

Nach dem Drain aller Queue-Items bleiben hunderte Semaphore-Counts übrig. Jede Iteration weckt den Processor, der 0 Items findet → CPU-Spinning.

---

## 3. Query Engine — Korrektheit & Sicherheit

### 3.1 🟠 Variable Substitution (Op_Var) — Injection-Risiko

**Datei:** `FKala.Core/KalaQl/Op_Var.cs`, Zeilen 35–38  

`string.Replace(VarName, VarValue)` über die gesamte Zeile. Keine Wort-Grenz-Erkennung. `VarName = "a"` ersetzt **jedes** "a" überall — in Keywords, Measurement-Namen, Expressions.

**Fix:** Variablen mit Prefix (z.B. `$`) kennzeichnen, Wort-Grenz-basierte Ersetzung.

---

### 3.2 🟠 Regex DoS (ReDoS) bei User-Supplied Patterns

**Datei:** `FKala.Core/KalaQl/QueryPreprocessor.cs`, Zeilen 111, 142, 166, 236  

User-Regex-Patterns aus `regex:`-Direktiven werden ohne Timeout kompiliert. Patterns wie `(a+)+$` können katastrophales Backtracking verursachen.

**Fix:** `new Regex(pattern, RegexOptions.None, TimeSpan.FromSeconds(2))`.

---

### 3.3 🟠 Object-Pool-Leaks an mehreren Stellen

**Dateien:** `Op_Expresso.cs`, `Op_ShiftTime.cs`, `Op_Interpolate.cs`, `DatasetsCombiner2.cs`

- **Op_Expresso:** `previousInput`-DataPoints werden nie in den Pool zurückgegeben. Kommentierter Code (`//TODO`) bestätigt das bekannte Problem.
- **Op_ShiftTime:** Klont DataPoints, gibt Originale nicht zurück.
- **Op_Interpolate:** Input-DataPoints werden nie zurückgegeben.
- **DatasetsCombiner2:** Gibt DataPoints in den Pool zurück, die noch vom Consumer referenziert werden — Use-After-Free.

---

### 3.4 🟠 `ResultPromise` Double-Enumeration + Unused Enumerators

**Dateien:** `Op_Interpolate.cs` Zeilen 58–64, `Op_Insert.cs` Zeilen 59–63  

```csharp
var enumerable = resultPromise.ResultsetFactory();
var enumerator = enumerable.GetEnumerator(); // Nie verwendet!
foreach (var dp in enumerable) // Zweite Enumeration
```

Erste Enumeration (`GetEnumerator()`) wird erzeugt und verworfen. Verschwendet I/O und kann zu Inkonsistenzen führen.

---

### 3.5 🟠 `Regex.Unescape` auf allen Query-Inputs

**Datei:** `KalaQuery.cs`, Zeile 97  

```csharp
queryText = Regex.Unescape(queryText);
```

Wird bedingungslos auf jeden Query-Text angewendet. `\n` in Measurement-Namen wird zu Newline, `\0` zu Null-Byte. Kann Parser und Datei-I/O korrumpieren.

**Fix:** Nur bei JSON-Transport unescapen, nicht pauschal.

---

### 3.6 🟠 Destruktive Mgmt-Operationen ohne Autorisierung

**Datei:** `FKala.Core/KalaQl/Op_Mgmt.cs`, Zeilen 137–152  

`DeleteMeasure`, `TruncateMeasure`, `Copy`, `Rename`, `Blacklist` sind über die Query-Sprache ohne jede Autorisierung zugänglich. Jeder mit API-Zugang kann Daten löschen.

---

### 3.7 🟡 Min/Max werden durch Null-Werte gelöscht

**Datei:** `FKala.Core/Helper/StreamingAggregator.cs`, Zeilen 149–150  

```csharp
case AggregateFunction.Min:
    aggregatedValue = aggregatedValue != null && toIntegrate != null 
        ? decimal.Min(...) : toIntegrate;
```

`Min(5, null)` ergibt `null` — ein gültiges Minimum wird durch einen Null-Wert überschrieben.

**Fix:**
```csharp
case AggregateFunction.Min:
    if (toIntegrate == null) break;
    aggregatedValue = aggregatedValue != null 
        ? decimal.Min(aggregatedValue.Value, toIntegrate.Value) 
        : toIntegrate;
    break;
```

---

### 3.8 🟡 Leerer Input erzeugt Ghost-DataPoint bei Count

**Datei:** `FKala.Core/KalaQl/Op_Aggregate.cs`, Zeilen 163–165  

Bei leerem Input wird trotzdem ein finaler DataPoint erzeugt. Für `Count` ergibt das `Value=0` bei `DateTime.MinValue`.

---

### 3.9 🟡 Unbegrenzte Regex-Expansion — Memory/Query-Explosion

**Datei:** `QueryPreprocessor.cs`, Zeilen 112–134  

`regex:.*` expandiert zu einem Load pro Measurement. Bei 100.000 Measurements → 100.000 Operationen ohne Limit.

---

### 3.10 🟡 DatasetsCombiner2 — Doppelte Einträge bei Window-Grenzen

**Datei:** `DatasetsCombiner2.cs`, Zeilen 63–73  

Zwei separate `if`-Bedingungen können denselben DataPoint doppelt in `timeMatches` aufnehmen, wenn ein Punkt genau auf einer Window-Grenze liegt.

---

### 3.11 🟡 `Op_Expresso` — Thread-Safety bei Interpreter-Wiederverwendung

**Datei:** `Op_Expresso.cs`, Zeilen 17, 47  

`Interpreter` und `Lambda` sind Instanz-Felder. Bei paralleler Verwendung (z.B. concurrent MatView-Queries) ist Thread-Safety nicht garantiert.

---

### 3.12 🟡 `Window.FastForward` — DST-Fehler bei tagesbasierten Windows

**Datei:** `FKala.Core/KalaQl/Windowing/Window.cs`, Zeilen 191–204  

FastForward berechnet per fester Sekunden-Division (`86400s/Tag`). Bei DST-Übergängen hat ein Tag 23h oder 25h — das Window-Start wird falsch platziert.

---

### 3.13 🟡 Op_JsonQuery — NullReferenceException bei fehlenden Keys

**Datei:** `FKala.Core/KalaQl/Op_JsonQuery.cs`, Zeilen 79–125  

- `item.ValueText!` → NRE bei `ValueText = null`
- `jarray!.ToList()[index]` → keine Bounds-Prüfung
- Bekannter Bug im Code: `//TODODODODODODDODODO REBUILD IST SOMIT DEFEKT`

---

## 4. API & Infrastruktur

### 4.1 🟠 Keine Authentifizierung/Autorisierung

**Datei:** `FKala.Api/Program.cs`  

Kein `UseAuthentication`/`UseAuthorization`. InsertController erlaubt jedem, beliebige Daten zu schreiben. In Kombination mit den destruktiven Mgmt-Operationen (3.6) können Daten gelöscht werden.

---

### 4.2 🟠 CORS AllowAll + Swagger in Produktion

**Dateien:** `Program.cs`, Zeilen 78–83, 112–113  

`AllowAnyOrigin().AllowAnyMethod().AllowAnyHeader()` ohne Einschränkung. Swagger UI permanent aktiv — leakt API-Schema.

---

### 4.3 🟠 GC.Collect() alle 10 Sekunden + LOH Compaction

**Datei:** `Program.cs`, Zeilen 135–143  

Erzwingt Full Blocking GC mit LOH-Kompaktierung alle 10 Sekunden. Verursacht regelmäßige Latenz-Spikes für alle Requests und blockiert alle Managed Threads.

**Fix:** Entfernen. Stattdessen Memory-Leaks mit Profiling finden und beheben.

---

### 4.4 🟠 MQTT Worker — Mehrere Robustheitsprobleme

**Datei:** `FKala.Api/Worker/MqttWorker.cs`  

| Problem | Zeile | Details |
|---------|-------|---------|
| Blocking `Thread.Sleep` in async Handler | 70 | Blockiert Thread-Pool-Thread |
| Fire-and-forget Reconnect | 72 | `ReconnectAsync` ohne `await` — bei Fehler: dauerhaft disconnected |
| Fester Client-ID `"Fkala"` | 33 | Zwei Instanzen → Broker disconnected die erste |
| `WithCleanSession()` | 34 | Alle Messages während Disconnect verloren |
| Null Payload nicht behandelt | 78 | `Encoding.UTF8.GetString(null)` → ArgumentNullException |
| Kein QoS spezifiziert | 59–61 | Default QoS 0 → Fire-and-Forget |
| `Console.WriteLine` statt `ILogger` | überall | Kein strukturiertes Logging |

---

### 4.5 🟡 MatViewRefreshJob — Nicht-atomares Delete+Recreate

**Datei:** `FKala.Api/Jobs/MatViewRefreshJob.cs`, Zeilen 55–83  

Materialized View wird gelöscht, dann neu erstellt. Bei Crash dazwischen: View für 24h verloren.

**Fix:** In temporären Namen schreiben, dann atomisch umbenennen.

---

### 4.6 🟡 StreamQuery ohne CancellationToken

**Datei:** `FKala.Api/Controller/StreamQueryController.cs`  

Keine Prüfung auf Client-Disconnect. Server verarbeitet und serialisiert Daten für tote Verbindungen bis Query-Ende.

---

### 4.7 🟡 InsertController + PlainTextFormatter — Keine Size-Limits

**Dateien:** `InsertController.cs`, `PlainTextFormatter.cs`  

Kein `[RequestSizeLimit]`. `ReadToEndAsync()` ohne Größenlimit. Multi-GB-Bodies → OOM.

---

### 4.8 🟡 Docker-Container läuft als Root

**Datei:** `Dockerfile`  

Kein `USER`-Statement. Prozess läuft als UID 0. Debug-Statements (`RUN pwd`, `RUN ls`) sind noch vorhanden.

---

### 4.9 🟡 `appsettings.Production.json` leakt Infrastruktur-Details

**Datei:** `FKala.Api/appsettings.Production.json`  

Enthält SSH-Kommandos, Hostnamen (`libreelec.fritz.box`), Docker-Run-Befehle, Volume-Mounts als Kommentare. Committed in Source Control und im Docker-Image.

---

### 4.10 🟡 `AddControllers()` doppelt aufgerufen

**Datei:** `Program.cs`, Zeilen 17 und 86  

Zwei separate `AddControllers()`-Aufrufe. Konfiguration könnte überschrieben werden.

---

## 5. Architektur & Projektstruktur

### 5.1 🟡 Tote Projekte in der Solution

| Projekt | Status |
|---------|--------|
| `FKala.Background` | Nur leere `Class1.cs` |
| `FKala.Core.Interfaces` | 0 Source-Dateien, nicht referenziert |
| `FKala.Migrate` | Nur bin/obj, Source entfernt |
| `FKala.WebApi` | Aufgegebener Prototyp, erstellt neuen DataLayer pro Request |

---

### 5.2 🟡 `IDataLayer` Interface zu groß — Leakt Implementierungsdetails

28+ Members, inklusive `BufferedWriterSvc`, `CachingLayer`, `DataDirectory`, `ReadBuffer`, `WriteBuffer`. Referenziert die konkrete Klasse `DataLayer_Readable_Caching_V1.MatView` direkt.

---

### 5.3 🟡 Keine strukturierte Logging-Strategie

- **FKala.Core:** 0 `ILogger`-Verwendungen, 25+ `Console.WriteLine`-Aufrufe
- **FKala.Api:** MQTT Worker nutzt `Console.WriteLine` statt injected `ILogger`
- Selbst-Logging in den eigenen Datastore (`kala/errors`) — fragil wenn Storage das Problem ist

---

### 5.4 🟡 Test-Coverage-Lücken

**Gut abgedeckt:** Parser, Cache-Resolutions, DurationParser, ShiftTime, Timezone-Handling

**Nicht getestet:**
- Kein einziger API-Level/Integration Test
- Kein End-to-End Insert→Query Round-Trip
- `BufferedWriterService` — der zentrale Write-Pfad
- `CachingLayer.LoadDataFromCache` + inkrementelle Updates
- `MqttWorker`, `MatViewRefreshJob`
- Keine Concurrency/Stress-Tests
- Keine Crash-Recovery-Tests

---

### 5.5 🟡 Package-Versionen veraltet/inkonsistent

| Package | Version | Problem |
|---------|---------|---------|
| `Microsoft.Extensions.ObjectPool` | 8.0.8 | .NET 8 auf .NET 9 Projekt |
| `Microsoft.AspNetCore.Mvc.NewtonsoftJson` | 8.0.8 | .NET 8 auf .NET 9 |
| `MQTTnet.AspNetCore` | 3.1.2 | Sehr alt, aktuell ist v4.x |
| `Microsoft.NET.Test.Sdk` | 17.6.0 | Sehr alt (aktuell: 17.12+) |
| `Swashbuckle.AspNetCore` | 6.4.0 / 6.7.3 | Gemischte Versionen im selben Projekt |

Dual-JSON-Libraries: `Newtonsoft.Json` und `System.Text.Json` parallel im Einsatz.

---

### 5.6 🟡 Code-Duplikation bei Cache-Implementierungen

`Cache_Minutely`, `Cache_5Minutely`, `Cache_15Minutely`, `Cache_Hourly` sind ~90% identisch. Unterscheiden sich nur in Konstanten (Window-Size, Time-Format, Refresh-Threshold).

**Fix:** `Cache_Base` parametrisieren und Subklassen eliminieren.

---

### 5.7 🟢 Namespace-Inkonsistenz

`DataLayer_Readable_Caching_V1` in `FKala.Core`, `StorageAccess` in `FKala.Core.DataLayers` (Plural), Cache-Klassen in `FKala.Core.DataLayer.Cache` (Singular), `PathSanitizer` in `FKala.Core.Logic`.

---

### 5.8 🟢 `DpPolicy.Return` — Doppeltes Clearing

**Datei:** `Pools.cs`, Zeilen 29–34  

```csharp
obj.ValueText = null; // erstes Mal
obj.Value = null;
obj.ValueText = null; // Duplikat — Copy-Paste-Fehler
```

---

## 6. Verbesserungsvorschläge nach Priorität

### Sofort-Maßnahmen (kritische Bugs)

1. **Infinite Loop fixen** — `QueryController.cs` Zeile 104: `ie = ie.InnerException`
2. **Blacklist-Assignment fixen** — `LoadMeasureBlacklist()`: `this.MeasurementBlacklist = newBl;`
3. **`DeveloperExceptionPage` gaten** — `if (app.Environment.IsDevelopment())`
4. **`dotnet test` in CI** — Step vor Docker-Push in `docker-image.yml`
5. **Constructor-Ordering fixen** — Buffer-Parameter vor `BufferedWriterSvc`-Erstellung setzen

### Kurzfristig (Stabilität & Datenintegrität)

6. **Shared LockManager** für Sort-on-Read (verhindert File-Corruption)
7. **`Flush()` nicht mehr die globale Queue drainen** lassen
8. **TOCTOU bei Cache-Build** — Double-Check-Pattern innerhalb des Locks
9. **Crash-Recovery** für `.sorted`/`.bak`-Dateien beim Start
10. **`BufferedWriter.Dispose()`** — Flush vor `disposed = true`
11. **Min/Max Null-Handling** fixen
12. **Unused `GetEnumerator()`-Aufrufe** entfernen (Op_Interpolate, Op_Insert)

### Mittelfristig (Robustheit & Sicherheit)

13. **MQTT Worker** — `await ReconnectAsync`, exponentielles Backoff, Null-Payload-Check
14. **`Console.WriteLine` → `ILogger`** in gesamtem Core + MQTT Worker
15. **Request-Size-Limits** für Insert + PlainTextFormatter
16. **Swagger + CORS** in Produktion einschränken
17. **`Regex.Unescape`** nur für JSON-Transport anwenden
18. **Regex-Timeout** für User-Patterns im QueryPreprocessor
19. **GC.Collect-Loop entfernen** — stattdessen Memory-Profiling
20. **Tote Projekte entfernen** (Background, Core.Interfaces, Migrate, WebApi)

### Langfristig (Architektur)

21. **Health-Check-Endpoint** (`/health`) hinzufügen
22. **Integration-Tests** für Insert→Query→Cache-Flows
23. **`IDataLayer`-Interface** aufteilen (Read, Write, Management)
24. **Object-Pool-Ownership-Konvention** definieren und durchsetzen
25. **Write-Ahead-Log** für Insert-Queue-Durability
26. **Cache-Subklassen** durch parametrisierte `Cache_Base` ersetzen
27. **Package-Versionen** auf .NET 9 aktualisieren, MQTTnet v4
28. **Docker-Container** als Non-Root User ausführen
