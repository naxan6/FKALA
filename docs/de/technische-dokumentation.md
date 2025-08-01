# Technische Dokumentation von FKALA

## Systemarchitektur

FKALA basiert auf einer modularen Architektur mit klaren Trennungen zwischen den Komponenten:

### Datenlayer (DataLayer)
- **Verantwortung:** Speicherung und Abruf von Zeitreihendaten
- **Speicherformat:** Flat-Dateien im Dateisystem
- **Caching:** Implementierte Caching-Schicht für häufig abgerufene Daten
- **Materialisierte Ansichten:** Unterstützung für vorberechnete Datenmengen

### KalaQl-Engine
- **Verantwortung:** Verarbeitung von KalaQl-Abfragen
- **Sprache:** Domänenspezifische Sprache für Zeitreihendaten
- **Operationen:** Load, Aggregate, MatView, Publish, Var

### API-Schicht
- **Technologie:** ASP.NET Core Web API
- **Endpunkte:** HTTP-Endpoints für Datenverwaltung und Abfragen
- **Integration:** Unterstützung für MQTT und Hintergrundjobs

## Datenstruktur

### Speicherung
Zeitreihendaten werden in folgender Struktur gespeichert:
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

### Zeitreihenformat
Daten werden im folgenden Format gespeichert:
```
measurementName timestamp value
temperatureOutside 2023-01-01T10:30:00Z 23.5
humidity 2023-01-01T10:30:00Z 65.2
```

## KalaQl-Sprache - Syntax

### Grundlegende Operationen

**Load**
```
Load mydata: measurementName 2023-01-01T00:00:00Z 2023-01-02T00:00:00Z FiveMinutely_WAvg
```

**Aggregate**
```
Aggregate mydata: Avg
```

**MatView**
```
MatView myview: Load data 2023-01-01T00:00:00Z 2023-01-02T00:00:00Z FiveMinutely_WAvg | Aggregate Avg
```

**Publish**
```
Publish mydata: Table
```

## API-Endpunkte

### Insert Controller
**Methoden:** PUT /api/insert
- **Eingabe:** Text im Format `measurementName,value,timestamp`
- **Ausgabe:** HTTP 200 bei Erfolg

### Query Controller
**Methoden:** GET /api/query und POST /api/query
- **Eingabe:** KalaQl-Abfrage als Text
- **Ausgabe:** Ergebnisse im JSON-Format

## Hintergrundprozesse

### MatViewRefreshJob
- **Zweck:** Aktualisiert materialisierte Ansichten
- **Zeitplan:** Täglich um 2:00 Uhr nachts
- **Prozess:** Löscht und neu erstellt alle Materialisierten Ansichten

## Konfiguration

Die Anwendung wird über folgende Einstellungen konfiguriert:

### appsettings.json
```json
{
  "DataStorage": "/path/to/data/storage",
  "ReadBuffer": 16384,
  "WriteBuffer": 32768,
  "MqttSettings": {
    "BrokerUrl": "tcp://localhost:1883",
    "ClientId": "FKALA-Client"
  }
}
```

## Fehlerbehandlung

Das System implementiert umfassende Fehlerbehandlung:

### HTTP-Fehlercodes
- **400 Bad Request:** Ungültige Abfrage oder Eingabe
- **404 Not Found:** Angeforderte Daten nicht vorhanden
- **500 Internal Server Error:** Interne Systemfehler

### Logmeldungen
Alle Operationen werden protokolliert für Debugging-Zwecke.

## Performance-Optimierung

### Caching-Mechanismen
- Mehrere Cache-Auflösungen (Minutely, Hourly, etc.)
- Automatische Cache-Invalidierung bei Datenänderungen
- Speicheroptimierte Zwischenspeicherung

### Speicherverwaltung
- Buffering für effizientes Schreiben von Daten
- Optimierter Datei-Zugriff
- Unterstützung für große Datensätze ohne Memory-Probleme

## Sicherheit

Das System implementiert folgende Sicherheitspraktiken:
- HTTPS-Unterstützung (optional)
- Authentifizierung für sensible Operationen
- Zugriffskontrolle auf Datenspeicherung
