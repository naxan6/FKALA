# FKALA - Endbenutzerdokumentation

## Inhaltsverzeichnis
1. [Einleitung](#einleitung)
2. [Systemübersicht](#systemübersicht)
3. [Installation](#installation)
4. [Erste Schritte](#erste-schritte)
5. [Datenverwaltung](#datenverwaltung)
6. [Abfragen und Analysen](#abfragen-und-analysen)
7. [API-Nutzung](#api-nutzung)
8. [Fehlerbehandlung](#fehlerbehandlung)
9. [Konfiguration](#konfiguration)

## Einleitung

FKALA ist ein leistungsstarkes System zur Verwaltung von Zeitreihendaten. Es ermöglicht die effiziente Speicherung, Analyse und Abfrage großer Mengen an zeitbasierten Daten.

## Systemübersicht

Das FKALA-System besteht aus folgenden Hauptkomponenten:

- **DataLayer**: Verwaltet die Datenspeicherung und -abruf
- **KalaQl Engine**: Verarbeitet Abfragen in der KalaQl-Sprache  
- **API Layer**: Stellt HTTP-Endpunkte zur Verfügung
- **Background Processing**: Verwaltet automatische Wartungsaufgaben

## Installation

Die Installation erfolgt durch das Klonen des Repositorys:

```bash
git clone https://github.com/naxan6/FKALA.git
```

Anschließend können die Abhängigkeiten mit folgendem Befehl installiert werden:
```
dotnet restore
```

Für Docker-Installationen siehe [Docker-Konfiguration](docker-konfiguration.md).

## Erste Schritte

1. Starten Sie den Server:
   ```bash
   dotnet run --project FKala.Api
   ```

2. Erstellen Sie Ihre erste Zeitreihendaten:
   ```
   measurementName 2023-01-01T10:30:00.0000000 value
   ```

## Datenverwaltung

### Daten einfügen

Zeitreihendaten werden im folgenden Format eingefügt:

```
measurementName 2023-01-01T10:30:00.0000000 value
```

Das Format besteht aus:
- **measurementName**: Name der Messung
- **Timestamp**: ISO 8601 Format `yyyy-MM-ddTHH:mm:ss.fffffff` (19 Zeichen)
- **value**: Der Datenwert

### Daten abfragen

Verwenden Sie die Query-API, um Zeitreihendaten zu analysieren:

```
GET /api/query?input=Load mydata: mymeasurement 2023-01-01T00:00:00.0000000 2023-01-02T00:00:00.0000000 FiveMinutely_WAvg
```

## Abfragen und Analysen

Das FKALA-System unterstützt komplexe Zeitreihenoperationen durch die KalaQl-Sprache:

### Grundlegende Operationen

- **Load** - Lädt Daten aus dem Speicher
- **Aggregate** - Führt Aggregationen durch  
- **MatView** - Verwaltet materialisierte Ansichten
- **Publish** - Formatiert die Ausgabedaten

## API-Nutzung

### REST-API Endpunkte

**Daten einfügen:**
```
POST /api/insert
Content-Type: text/plain

measurementName 2023-01-01T10:30:00.0000000 value
```

**Abfragen ausführen:**
```
GET /api/query?input=Load mydata: mymeasurement 2023-01-01T00:00:00.0000000 2023-01-02T00:00:00.0000000 FiveMinutely_WAvg
```

## Fehlerbehandlung

Das System liefert detaillierte Fehlermeldungen:

- **400 Bad Request** - Ungültige Eingabedaten
- **404 Not Found** - Angeforderte Daten nicht gefunden  
- **500 Internal Server Error** - Interne Serverfehler

## Konfiguration

Die Anwendung wird über die `appsettings.json` konfiguriert:

```json
{
  "DataStorage": "/path/to/data/storage",
  "ReadBuffer": 16384,
  "WriteBuffer": 32768
}
