# FKALA Konfiguration

## Grundlegende Konfiguration

Die FKALA-Anwendung wird über die `appsettings.json` konfiguriert. Diese Datei enthält alle notwendigen Einstellungen für das Systemverhalten.

### Beispielkonfiguration

```json
{
  "DataStorage": "C:\\fkala\\data",
  "ReadBuffer": 16384,
  "WriteBuffer": 32768,
  "MqttSettings": {
    "BrokerUrl": "tcp://localhost:1883",
    "ClientId": "FKALA-Client"
  },
  "Logging": {
    "LogLevel": {
      "Default": "Information",
      "Microsoft.AspNetCore": "Warning"
    }
  }
}
```

## Konfigurationseinstellungen

### DataStorage
- **Typ:** String (Pfad)
- **Beschreibung:** Basispfad für die Datenspeicherung
- **Standard:** `C:\fkala\data`
- **Beispiel:** `"DataStorage": "/opt/fkala/data"`

### ReadBuffer
- **Typ:** Integer
- **Beschreibung:** Größe des Lesebuffers in Bytes
- **Empfehlung:** 16384 (16 KB) für optimale Leistung
- **Standard:** `16384`

### WriteBuffer
- **Typ:** Integer
- **Beschreibung:** Größe des Schreibbuffers in Bytes
- **Empfehlung:** 32768 (32 KB) für effizientes Schreiben
- **Standard:** `32768`

## MQTT-Konfiguration

FKALA unterstützt optional die Integration mit MQTT-Brokern:

### MqttSettings

**BrokerUrl**
- **Typ:** String
- **Beschreibung:** URL des MQTT-Brokers
- **Beispiel:** `"BrokerUrl": "tcp://localhost:1883"`

**ClientId**
- **Typ:** String
- **Beschreibung:** Eindeutige ID für den Client
- **Beispiel:** `"ClientId": "FKALA-Client"`

## Sicherheitskonfiguration

### Logging-Einstellungen

Die Anwendung implementiert umfassende Logging-Möglichkeiten:

```json
{
  "Logging": {
    "LogLevel": {
      "Default": "Information",
      "Microsoft.AspNetCore": "Warning"
    }
  }
}
```

## Umgebungskonfiguration

### Entwicklungsumgebung

Für die Entwicklungseinstellungen können Sie folgende Konfiguration verwenden:

```json
{
  "DataStorage": "./data",
  "ReadBuffer": 8192,
  "WriteBuffer": 16384,
  "Logging": {
    "LogLevel": {
      "Default": "Debug"
    }
  }
}
```

### Produktivumgebung

Für produktive Systeme empfiehlt sich:

```json
{
  "DataStorage": "/var/lib/fkala/data",
  "ReadBuffer": 32768,
  "WriteBuffer": 65536,
  "Logging": {
    "LogLevel": {
      "Default": "Information"
    }
  }
}
```

## Datei- und Pfadkonventionen

### Datenstruktur

Die Daten werden in folgender Struktur gespeichert:
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

## Leistungsparameter

### Buffer-Größen

Die Buffer-Größen beeinflussen die Performance:

**Lesebuffer:**
- Größe: 16384 Bytes (Standard)
- Empfehlung: Anpassung basierend auf Datenmengen pro Abfrage

**Schreibbuffer:**
- Größe: 32768 Bytes (Standard)
- Empfehlung: Anpassung basierend auf Schreibhäufigkeit

## Fehlerbehandlung

Die Konfiguration beeinflusst auch die Fehlerbehandlung:

### Protokollierung
- **Information:** Normale Systemmeldungen
- **Warning:** Warnungen, keine kritischen Fehler
- **Error:** Kritische Fehler mit möglicherweise Datenverlust

## Best Practices

1. **Pfadkonventionen:** Verwenden Sie absolute Pfade für produktive Umgebungen
2. **Buffergröße:** Passen Sie Buffergrößen an Ihre Datenvolumina an
3. **Logging:** Aktivieren Sie Logging für Fehlerdiagnose
4. **Sicherheit:** Stellen Sie sicher, dass die Datenverzeichnisse sicher sind

## Konfigurationsvalidierung

Bevor Sie FKALA starten, überprüfen Sie folgende Punkte:

1. Der DataStorage-Pfad existiert und ist beschreibbar
2. Die Buffergrößen sind im gültigen Bereich (meist 1024 bis 65536)
3. Alle erforderlichen Konfigurationseinstellungen sind vorhanden
