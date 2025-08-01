# FKALA - Zeitreihendatenverwaltungssystem

Willkommen beim FKALA-System, einem leistungsstarken Zeitreihendatenmanagement-System.

## Inhaltsverzeichnis
1. [Einleitung](#einleitung)
2. [Was ist FKALA?](#was-ist-fkala)
3. [Architektur](#architektur)
4. [Verwendung](#verwendung)
5. [API-Referenz](#api-referenz)
6. [KalaQl-Sprache](#kalaql-sprache)
7. [Beispiele](#beispiele)
8. [Fehlerbehandlung](#fehlerbehandlung)
9. [Konfiguration](#konfiguration)

## Einleitung

FKALA ist ein spezialisiertes System zur Erfassung, Speicherung, Abfrage und Verarbeitung von Zeitreihendaten. Es wurde entwickelt, um mit großen Mengen an Zeitreihendaten effizient umzugehen, insbesondere wenn komplexe analytische Operationen erforderlich sind.

## Was ist FKALA?

FKALA bietet eine Vielzahl von Funktionen für Zeitreihendaten:

- Speicherung von Zeitreihendaten in Flat-Dateien
- Leistungsstarkes Abfragesystem mit eigener Sprache (KalaQl)
- Caching-Mechanismen zur Optimierung der Datenabfrage
- Materialisierte Ansichten für häufig abgefragte Datenmengen
- HTTP-API für externe Clients

## Architektur

FKALA besteht aus folgenden Hauptkomponenten:

1. **DataLayer**: Verwaltet die Datenspeicherung und -abruf
2. **KalaQl Engine**: Verarbeitet Abfragen in der KalaQl-Sprache
3. **API Layer**: Stellt HTTP-Endpunkte zur Verfügung
4. **Background Processing**: Verwaltet automatische Wartungsaufgaben

## Verwendung

## Verwendung

### Daten einfügen

Verwenden Sie die Insert-API, um Zeitreihendaten zu speichern:

```
PUT /api/insert
Content-Type: text/plain

measurementName 2023-01-01T10:30:00.0000000 value
```

### Daten abfragen

Nutzen Sie die Query-API, um Daten mit KalaQl-Abfragen abzurufen:

```
GET /api/query?input=Load mydata: mymeasurement 2023-01-01T00:00:00.0000000 2023-01-02T00:00:00.0000000 FiveMinutely_WAvg
```

## API Referenz

### Insert Controller

**Endpoint:** `PUT /api/insert`
- **Beschreibung:** Fügt Zeitreihendaten hinzu
- **Eingabe:** Text im Format `measurementName timestamp value`  
  - Timestamp: ISO 8601 Format `yyyy-MM-ddTHH:mm:ss.fffffff` (19 Zeichen)
- **Antwort:** `200 OK` bei Erfolg

### Query Controller

**Endpoint:** `GET /api/query`

**Endpoint:** `POST /api/query`

- **Beschreibung:** Führt KalaQl-Abfragen aus
- **Eingabe:** KalaQl-Abfrage als Text
- **Antwort:** Ergebnisse im JSON-Format

## KalaQl Sprache

KalaQl ist die spezialisierte Abfragesprache von FKALA. Sie ermöglicht komplexe Zeitreihenoperationen.

### Grundlegende Operationen

- `Load` - Lädt Daten aus dem Speicher
- `Aggregate` - Führt Aggregationen durch
- `MatView` - Verwaltet materialisierte Ansichten
- `Publish` - Formatiert die Ausgabedaten

### Beispiele

**Laden von Daten:**
```
Load mydata: mymeasurement 2023-01-01T00:00:00Z 2023-01-02T00:00:00Z FiveMinutely_WAvg
```

**Aggregieren von Daten:**
```
Load mydata: mymeasurement 2023-01-01T00:00:00Z 2023-01-02T00:00:00Z FiveMinutely_WAvg
| Aggregate mydata: Avg
```

## Fehlerbehandlung

Das System liefert detaillierte Fehlermeldungen. Wichtige Fehlercodes:

- `400 Bad Request` - Ungültige Eingabedaten
- `404 Not Found` - Angeforderte Daten nicht gefunden
- `500 Internal Server Error` - Interne Serverfehler

## Konfiguration

Die Anwendung wird über die `appsettings.json` konfiguriert:

```json
{
  "DataStorage": "/path/to/data/storage",
  "ReadBuffer": 16384,
  "WriteBuffer": 32768
}
