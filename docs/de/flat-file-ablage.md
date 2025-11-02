# Flat-File-Ablage in FKALA

Dieses Dokument beschreibt die komplette Flat-File-Ablage von FKALA: Ordnerstruktur, Namenskonventionen, Dateiformate, Schreib-/Lese-Semantik, Caching, Sperrkonzept, Fehler- und OOO-Handhabung, Retention, Berechtigungen sowie Best Practices. Ziel ist eine eindeutige, nachvollziehbare Datenablage, die sowohl performant als auch robust ist.

Inhalte:
- Übersicht
- Ordner- und Pfadstruktur
- Dateinamen- und Segmentierungskonzept
- Dateiformate und Kodierung
- Schreib- und Lese-Semantik
- Sortierung, Duplikate, Out-of-Order (OOO)
- Caching-Layout und Materialized Views
- Metadaten-, Blacklist- und Invalidate-Flags
- Fehlerbehandlung und Recovery
- Konfiguration und Parameter
- Retention, Archivierung, Berechtigungen
- Beispiele
- Validierung und Konsistenzregeln
- Best Practices und Troubleshooting

---

## Übersicht

FKALA speichert Zeitreihen als zeilenbasierte Textdateien im Dateisystem. Partitionierung erfolgt nach Messung (Measurement) sowie Datum (Jahr/Monat/Tag). Für schnelle Abfragen werden Aggregat-Caches gepflegt. Das System unterstützt:

- Automatische Verzeichnisanlage
- Gepuffertes Schreiben und paralleles Lesen
- Optionale automatische Sortierung und Deduplizierung
- Aggregations-Caches in mehreren Auflösungen
- Robuste Fehlerbehandlung und Wiederanlauf
- Blacklisting problematischer Messungen
- Flag-basierte Invalidierung von Caches

Diese Architektur ermöglicht einfache Portabilität, Debuggability und Nutzung externer Tools.

---

## Ordner- und Pfadstruktur

Wurzelverzeichnis: `{storagePath}`

Standard (konfigurierbar, gemäß FKala.Core/DataLayer/DataLayer_Readable_Caching_V1):
- FKala.Api: `C:\fkala`
- FKala.WebApi: `.\fkala_data`

Abgeleitet aus dem Code:
- Datenwurzel: `{storagePath}/data`
- Blacklistwurzel: `{storagePath}/blacklist`
- Cachewurzel (CachingLayer): `{storagePath}/cache`

Unterordner:

```
{storagePath}/
├─ data/                   # Rohdaten & Views
│  ├─ {measurement}/
│  │  ├─ {yyyy}/
│  │  │  ├─ {MM}/
│  │  │  │  ├─ {measurement}_{yyyy-MM-dd}.dat    # unsortiert
│  │  │  │  └─ {measurement}#_{yyyy-MM-dd}.dat   # sortiert
│  │  │  └─ ...
│  │  └─ ...
│  └─ _views_/            # Materialized Views (WriteMatViewFile, LoadMatViews)
│     └─ {viewName}/
│        └─ viewdef.txt   # KalaQL-Definition + Metadaten (z. B. newestSeen)
├─ cache/                  # Aggregat-/Abfrage-Caches
│  ├─ minutely/
│  ├─ 5minutely/
│  ├─ 15minutely/
│  └─ hourly/
│     └─ {measurement}/
│        └─ {year}/
│           ├─ {measurement}_{year}_{AggFunc}.dat
│           └─ INVALIDATE_{measurement}_{year}.flag
└─ blacklist/              # Deaktivierte/ausgeblendete Messungen
   └─ {measurement}/
```

Hinweise:
- Verzeichnisse werden bei Bedarf rekursiv erstellt.
- Ein interner Cache vermeidet wiederholte Anlagekosten (CreatedDirectories).

---

## Dateinamen- und Segmentierungskonzept

### Rohdaten (data/)
- Unsortiert: `{measurement}_{yyyy-MM-dd}.dat`
- Sortiert: `{measurement}#_{yyyy-MM-dd}.dat` (enthält `#_` vor dem Datum)

Bedeutung:
- Ein `#` kennzeichnet eine Datei als inhaltlich sortiert (aufsteigende Zeit) und dedupliziert.
- Unsortierte Dateien können in der Praxis entstehen, wenn Daten “live” angefüttert oder verspätet/parallel eingehen.

Partitionierung:
- Jahr-/Monat-/Tag-Verzeichnisstruktur
- Tagesgranularität pro Datei:
  - Datum wird aus Pfad/Dateinamen abgeleitet
  - Zeitstempel in den Zeilen führen nur die Uhrzeit, nicht das Datum

### Cache-Dateien (cache/)
- Format: `{measurement}_{year}_{AggFunc}.dat`
- Beispiel: `temperatur_2024_Avg.dat`
- Je Auflösung eigener Ordner: `minutely`, `5minutely`, `15minutely`, `hourly`

Aggregationsfunktionen:
- Avg, Sum, Min, Max, Count (Standardumfang)

---

## Dateiformate und Kodierung

### Roh- und Cache-Dateien (*.dat)
- Textzeilen, UTF-8 (ohne BOM)
- Zeilenende: Systemstandard (Windows: CRLF)
- Zeilenformat Rohdaten:
  ```
  HH:mm:ss.fffffff Wert
  ```
  Beispiel:
  ```
  23:26:45.1234567 23.5
  23:27:01.9876543 23.6
  ```

Felder:
- Zeit: `HH:mm:ss.fffffff` (7 Nachkommastellen)
- Wert: Dezimalzahl (Kultur-unabhängig empfohlen: Punkt als Dezimaltrennzeichen)

Datum:
- Implizit aus dem Tagessegment (Pfad/Dateiname)

Toleranzen:
- Parser ist robust gegen geringe Format-Abweichungen (z. B. zusätzliche Whitespaces).
- PathSanitizer ersetzt unzulässige Zeichen in Pfaden durch `$`.

Kompression:
- Derzeit nicht standardmäßig verwendet; wenn nötig über externen Prozess (z. B. Archivierung) umsetzen und FKALA darüber informieren/konfigurieren.

---

## Schreib- und Lese-Semantik

### Schreiben
- Gepuffertes, asynchrones Schreiben:
  - Globaler BufferedWriterService steuert periodische Flushes
  - Default-Flush-Intervall: ~10 Sekunden (konfigurierbar)
  - WriteBuffer-Größe konfigurierbar (Standard 131072 Bytes)
- Locking pro Zieldatei (SemaphoreSlim-basiert) verhindert Race Conditions
- Automatische Verzeichnisanlage vor dem ersten Schreibzugriff
- Atomizität:
  - Innerhalb einer Datei sorgt Locking + Flush für Konsistenz
  - Für größere Umbenennungs-/Sortiervorgänge sollte “write to temp, then move” genutzt werden (Move ist OS-atomar innerhalb desselben Volumes)

### Lesen
- Streaming-Lesen mit Puffer (Default ReadBuffer: 131072 Bytes)
- Paralleles Öffnen/Lesen mehrerer Dateien für große Zeitbereiche
- Lazy Loading: Nur tatsächlich benötigte Dateien werden geöffnet
- LastLineReader/FileFromEndProcessor existieren für rückwärts-orientierte Operationen (z. B. letzte Werte)

---

## Sortierung, Duplikate, Out-of-Order (OOO)

### Sortierung
- Unsortierte Rohdaten entstehen bei Live-Zufluss
- Periodische oder bedarfsgetriebene Sortierung ab “Alter” ≥ 1–2 Tagen
- Ergebnis ist eine sortierte Tagesdatei `{measurement}#_{yyyy-MM-dd}.dat`
- Erwartung für sortierte Dateien:
  - Strikt aufsteigende Zeit je Zeile
  - Keine Duplikate derselben Zeit
- Unerwartete Unsortiertheit in als sortiert markierten Dateien führt zu `UnexpectedlyUnsortedException`

### Duplikate
- Deduplizierung nach “Last Wins”:
  - Tritt die gleiche Zeit mehrfach auf, gewinnt der letzte Eintrag beim Sortier-/Konsolidierungsprozess

### Out-of-Order
- Verspätete Daten (z. B. für frühere Tage/Zeiten) werden:
  - In die korrekte Tagesdatei einsortiert (bei nächster Sortierung)
  - Caches betroffene Zeitfenster invalidieren/aktualisieren

---

## Caching-Layout und Materialized Views

### Aggregat-Caches (cache/)
- Auflösungen: minutely, 5minutely, 15minutely, hourly
- Pro Measurement und Jahr eine Datei pro Aggregationsfunktion:
  - `{measurement}_{year}_{AggFunc}.dat`
- Inhalt analog Rohformat, aber zeitlich entsprechend aggregiert
- Inkrementelle Updates:
  - Bei neuen Rohdaten werden nur betroffene Bereiche fortgeschrieben
- Invalidierung:
  - Flag-Dateien (siehe unten) oder heuristische Erkennung bei Sortier-/OOO-Einsätzen

### Materialized Views (data/_views_/)

Belegt durch Code:
- Schreiben: `IDataLayer.WriteMatViewFile(viewName, lines)` legt `{storagePath}/data/_views_/{viewName}/viewdef.txt` an und schreibt `lines` (erste Zeile oft “newestSeen”, erzeugt in Op_MatView)
- Laden: `LoadMatViews()` sucht rekursiv `viewdef.txt` unter `data/`

Struktur:
```
data/_views_/{viewName}/
└─ viewdef.txt
```

Hinweise:
- Partitionsdateien für Views sind nicht Standard im Codepfad; die Materialisierung erfolgt über KalaQL und Inserts ins Zielmeasurement, viewdef.txt dient der Definition/Steuerung.

---

## Metadaten-, Blacklist- und Invalidate-Flags

### Blacklist
- `{storagePath}/blacklist/{measurement}/`
- Messungen in Blacklist werden vom aktiven Betrieb ausgeschlossen (z. B. temporär defekt)
- Live-Status verwaltet durch eine interne Map (ConcurrentDictionary)

### Invalidate-Flags (belegt in CachingLayer.cs)
- Dateiname: `INVALIDATE_{measurement}_{year}.flag`
- Ort: `{storagePath}/cache/{resolution}/{measurement}/{year}/`
- Wirkung:
  - Markiert den Jahres-Cache als ungültig; beim nächsten Zugriff/Update wird der Cache für das Jahr rekonstruiert
  - Nach erfolgreichem Rebuild wird das Flag gelöscht
- Automatik:
  - CachingLayer kann bei OOO-Änderungen gezielt invalidieren (Invalidate(measurement, new DateOnly(year,1,1)))

### View-Definitionen
- `viewdef.txt` im jeweiligen View-Ordner enthält die Query/KalaQL
- Dient als Single Source of Truth für die Materialisierung

---

## Fehlerbehandlung und Recovery

- Dateifehler:
  - Beschädigte Zeilen/Dateien werden geloggt; Verarbeitung fährt mit gesunden Segmenten fort
  - Fehler werden im System geloggt und ggf. als Messung `kala/errors` erfasst
- Zugriffs-/IO-Fehler:
  - Fehlende Berechtigungen/Verzeichnisse werden erkannt
  - Automatismen versuchen Anlage/Recovery
- Shutdown:
  - Graceful Shutdown leeren alle Write-Buffer, schließen Handles
- Unerwartete Unsortiertheit:
  - `UnexpectedlyUnsortedException` triggert Fallback/Neuaufbereitung (Sortierung/Invalidierung)

---

## Konfiguration und Parameter

Relevante Konfigurationsschlüssel (Auszug):
- `DataStorage`: Basis-Pfad (`C:\fkala` oder `.\fkala_data`)
- `ReadBuffer`: Lese-Puffergröße (Default 131072)
- `WriteBuffer`: Schreib-Puffergröße (Default 131072)
- `FlushIntervalMs` o. ä.: Intervall periodischer Flushes (Default ~10000 ms)
- `AutoSortAgeDays`: Alter, ab dem eine Datei zur Sortierung herangezogen wird (z. B. 1–2 Tage)
- `CacheResolutions`: Aktivierte Aggregat-Auflösungen
- `EnableBlacklist`: Schaltet Blacklist-Mechanismen aktiv

Hinweis: Exakte Key-Namen können pro Host (Api/WebApi) leicht variieren; maßgeblich ist die jeweilige appsettings-Konfiguration.

---

## Retention, Archivierung, Berechtigungen

### Retention
- Policy-basiert je Measurement:
  - Z. B. Rohdaten > 365 Tage in Archiv, Caches für Vorjahre nur auf Anfrage
- Alte sortierte Tagesdateien können komprimiert archiviert werden (z. B. ZIP)
  - Achtung: FKALA kann komprimierte Dateien nicht direkt lesen, daher ggf. vor Abfrage dearchivieren oder separate Storage-Ebene nutzen

### Archivierung
- Empfehlung: “Cold” Storage-Pfad unterhalb `{storagePath}/archive/`
- Struktur analog `data/` für einfache Rücksicherung

### Berechtigungen
- Dienstkonto benötigt:
  - Lesen/Schreiben für `{storagePath}` inkl. Unterordner
  - Rechte für Umbenennen/Move (atomare Moves innerhalb eines Volumes)
- Antivirus/Indexer:
  - Ausschlüsse für `{storagePath}` empfohlen (Performance)

---

## Beispiele

### Beispiel 1: Rohdaten
Pfad:
```
C:/fkala/data/temperatur/2024/01/temperatur_2024-01-15.dat
```
Inhalt:
```
00:00:00.0000000 23.1
00:00:05.0000000 23.2
...
23:59:55.0000000 22.8
```

### Beispiel 2: Sortierte Rohdaten
Pfad:
```
C:/fkala/data/temperatur/2024/01/temperatur#_2024-01-15.dat
```
Eigenschaften:
- strikt aufsteigende Zeit, keine Duplikate

### Beispiel 3: Cache-Datei (hourly, Avg)
Pfad:
```
C:/fkala/cache/hourly/temperatur/2024/temperatur_2024_Avg.dat
```
Inhalt (Beispielauszug, Format analog):
```
00:00:00.0000000 23.15
01:00:00.0000000 23.20
...
```

### Beispiel 4: Invalidate-Flag
Pfad:
```
C:/fkala/cache/hourly/temperatur/2024/INVALIDATE_temperatur_2024.flag
```

---

## Validierung und Konsistenzregeln

- Pfad-Sicherheit:
  - Ungültige Zeichen werden durch `$` ersetzt (PathSanitizer)
- Tagesdatei-Konsistenz:
  - Alle Zeilen müssen innerhalb desselben Kalendertages liegen
  - Zeit aufsteigend für sortierte Dateien; unsortierte dürfen abweichen
- Deduplizierung:
  - Je Zeitstempel max. ein Wert in sortierten Dateien
- Cache-Konsistenz:
  - Änderungen in Rohdaten invalidieren betroffene Cachefenster
  - Bei OOO-Einfügen Cache-Update/Neuaufbau sicherstellen
- Fehlerresilienz:
  - Fehlerhafte Zeilen werden übersprungen oder protokolliert, nicht blockierend für restliche Verarbeitung

---

## Best Practices

- Schreibrate/Flush:
  - Flush-Intervall und Puffer je Systemlatenz und Datenvolumen abstimmen
- Sortierfenster:
  - AutoSortAgeDays so wählen, dass Echtzeit nicht behindert, aber Konsistenz zeitnah erreicht wird
- Parallelisierung:
  - Parallele Lesedegrees an CPU/IO anpassen
- Monitoring:
  - Fehler-/Warnmessungen (z. B. `kala/errors`) in Observability-Pipeline aufnehmen
- Antivirus/Backup:
  - Ausschlüsse/Quotas beachten, um IO-Latenzen zu vermeiden

---

## Troubleshooting (Kurz)

- Unerwartet langsame Abfragen:
  - Prüfen, ob Caches vorhanden/aktuell sind; ggf. Invalidate-Flags setzen
- “UnexpectedlyUnsortedException”:
  - Datei mit `#_` prüfen; wenn unsortiert, neu sortieren lassen
- Fehlende Datenpunkte:
  - Live-Puffer/Flush prüfen, WriteBuffer erhöhen, Locking-Fehler ausschließen
- Berechtigungsfehler:
  - Dienstkonto-/ACLs für `{storagePath}` prüfen

---

## Zusammenfassung

Die FKALA-Flat-File-Ablage nutzt eine klare Partitionierung pro Measurement und Tag, UTF-8-Textformate und atomare, gepufferte IO-Operationen. Ein mehrstufiges Caching mit Auflösungen von Minute bis Stunde beschleunigt Abfragen. Automatische Sortierung, Deduplizierung und OOO-Handling stellen inhaltliche Korrektheit sicher, während Blacklisting und Flag-basierte Invalidierung einen kontrollierten Betrieb und schnelle Recovery ermöglichen.
