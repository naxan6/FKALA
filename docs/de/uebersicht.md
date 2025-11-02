# FKALA - Zeitreihendatenverwaltungssystem - Übersicht

## Willkommen bei FKALA

FKALA ist ein leistungsstarkes System zur Erfassung, Speicherung, Abfrage und Verarbeitung von Zeitreihendaten. Es wurde entwickelt, um mit großen Mengen an Zeitreihendaten effizient umzugehen, insbesondere wenn komplexe analytische Operationen erforderlich sind.

## Hauptfunktionen

### Zeitreihenverwaltung
- Speicherung von Zeitreihendaten in Flat-Dateien
- Unterstützung für verschiedene Cache-Auflösungen (Minutely, Hourly, etc.)
- Materialisierte Ansichten für häufig abgefragte Datenmengen

### Abfragesprache KalaQl
- Domänenspezifische Sprache für Zeitreihendaten
- Unterstützung für komplexe Operationen (Load, Aggregate, MatView, Publish)
- Verkettung von Operationen in einer Pipeline

### API-Schnittstellen
- HTTP-API für externe Clients
- Integration mit MQTT für Echtzeitdaten
- Hintergrundprozesse zur automatischen Wartungsaufgaben

## Architektur

FKALA besteht aus folgenden Hauptkomponenten:

### 1. DataLayer (Datenlayer)
Verwaltet die Datenspeicherung und -abruf im Dateisystem

### 2. KalaQl-Engine (Abfragesprache)
Verarbeitet Abfragen in der KalaQl-Sprache

### 3. API-Schicht
Stellt HTTP-Endpunkte zur Verfügung für externe Interaktion

### 4. Hintergrundverarbeitung
Verwaltet automatische Wartungsaufgaben wie MatView-Aktualisierungen

## Datenstruktur

Zeitreihendaten werden im folgenden Format gespeichert:

```
<DataStorage>/
├── data/
│   ├── measurementName/
│   │   ├── 2023/
│   │   │   ├── 01/
│   │   │   │   └── data.dat
│   │   │   └── 02/
│   │   │       └── data.dat
│   │   └── viewdef.txt (für Materialisierte Ansichten)
├── blacklist/
└── cache/
```

## Schnellstart

### 1. Daten einfügen
```bash
PUT /api/insert
Content-Type: text/plain

temperatureOutside 2023-01-01T10:30:00Z 23.5
humidity 2023-01-01T10:30:00Z 65.2
```

### 2. Daten abfragen
```bash
GET /api/query?input=Load mydata: temperatureOutside 2023-01-01T00:00:00Z 2023-01-02T00:00:00Z FiveMinutely_WAvg
```

### 3. Komplexe Abfrage
```bash
Load mydata: temperatureOutside 2023-01-01T00:00:00Z 2023-01-02T00:00:00Z FiveMinutely_WAvg | Aggregate Avg | MatView dailyavg
```

## Technologien

### Primäre Technologien
- **Sprache:** C# (.NET Core)
- **Web-Framework:** ASP.NET Core
- **Speicherung:** Flat-Dateien im Dateisystem
- **Datenbank:** Keine (direkter Dateizugriff)
- **Caching:** Mehrstufiges Caching-System

### Unterstützte Integrationen
- **MQTT:** Für Echtzeitdatenübertragung
- **HTTP:** Für RESTful API-Schnittstellen
- **Quartz.NET:** Für geplante Hintergrundjobs

## Konfiguration

Die Anwendung wird über `appsettings.json` konfiguriert:

```json
{
  "DataStorage": "/path/to/data",
  "ReadBuffer": 16384,
  "WriteBuffer": 32768
}
```

## Leistungsmerkmale

### Performance-Optimierung
- Buffering für effizientes Schreiben von Daten
- Caching-Mechanismen für häufig abgerufene Daten
- Optimierter Datei-Zugriff
- Unterstützung für große Datensätze ohne Memory-Probleme

### Zuverlässigkeit
- Umfassende Fehlerbehandlung
- Protokollierung aller Operationen
- Datenintegrität durch atomare Operationen

## Anwendungsbereiche

FKALA eignet sich besonders für:

1. **IoT-Datenerfassung:** Sammlung und Analyse von Sensordaten
2. **Wetterdatenverwaltung:** Langzeitverfolgung von Wetterbedingungen
3. **Industrieüberwachung:** Überwachung von Produktionsdaten
4. **Finanzanalysen:** Zeitreihenanalyse von Finanzdaten

## Nächste Schritte

1. **Dokumentation durchlesen:** `docs/de/README.md`
2. **Beispiele ausprobieren:** `docs/de/verwendung-beispiele.md`
3. **Konfiguration anpassen:** `docs/de/konfiguration.md`
4. **Fehlerbehandlung verstehen:** `docs/de/troubleshooting.md`

## Support und Entwicklung

Das FKALA-Projekt ist Open-Source und wird kontinuierlich weiterentwickelt. Für Unterstützung und Beiträge:

- **GitHub Repository:** https://github.com/naxan6/FKALA
- **Issues:** GitHub Issues Tracker
- **Pull Requests:** Beiträge sind willkommen
