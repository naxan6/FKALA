# FKALA Troubleshooting - Fehlerbehandlung

## Häufige Probleme und Lösungen

### 1. Daten einfügen fehlschlägt

**Fehlermeldungen:**
- `400 Bad Request`
- `500 Internal Server Error`

**Mögliche Ursachen und Lösungen:**

#### Ungültiges Datenformat
**Problem:** Daten im falschen Format eingefügt
**Lösung:** Überprüfen Sie das Datenformat:
```
measurementName,value,timestamp
temperatureOutside,23.5,2023-01-01T10:30:00Z
```

#### Speicherplatzprobleme
**Problem:** Kein Speicherplatz verfügbar
**Lösung:** Überprüfen Sie den freien Speicher auf dem Datenträger

### 2. Datenabfragen funktionieren nicht

**Fehlermeldungen:**
- `404 Not Found`
- `400 Bad Request` bei KalaQl-Abfragen

**Mögliche Ursachen und Lösungen:**

#### Ungültige KalaQl-Syntax
**Problem:** Falsche Abfragesyntax
**Lösung:** Überprüfen Sie die KalaQl-Abfrage:
```bash
# Fehlerhaft
Load mydata: temperatureOutside 2023-01-01T00:00:00Z 2023-01-02T00:00:00Z FiveMinutely_WAvg

# Richtig
Load mydata: temperatureOutside 2023-01-01T00:00:00Z 2023-01-02T00:00:00Z FiveMinutely_WAvg
```

#### Daten nicht vorhanden
**Problem:** Zeitreihendaten wurden nicht gefunden
**Lösung:** Überprüfen Sie, ob Daten in der Datenbank vorliegen

### 3. Performance-Probleme

**Symptome:**
- Langsame Abfragen
- Hohe CPU-Nutzung
- Lange Antwortzeiten

**Mögliche Ursachen und Lösungen:**

#### Ungenügender Puffer
**Problem:** Zu kleine Buffergrößen
**Lösung:** Anpassen der Buffergrößen in appsettings.json:
```json
{
  "ReadBuffer": 32768,
  "WriteBuffer": 65536
}
```

#### Fehlende Caching
**Problem:** Keine Caching-Mechanismen aktiviert
**Lösung:** Verwenden Sie Materialisierte Ansichten:
```bash
Load mydata: temperatureOutside 2023-01-01T00:00:00Z 2023-01-02T00:00:00Z FiveMinutely_WAvg | MatView dailyavg
```

### 4. MQTT-Integration Probleme

**Fehlermeldungen:**
- `Connection failed`
- `Authentication failed`

**Mögliche Ursachen und Lösungen:**

#### Broker nicht erreichbar
**Problem:** MQTT-Broker ist nicht erreichbar
**Lösung:** Überprüfen Sie die Netzwerkkonnektivität

#### Falsche Authentifizierung
**Problem:** Falsche Anmeldedaten für MQTT
**Lösung:** Überprüfen Sie die MqttSettings in appsettings.json

## Protokollierung und Debugging

### Serverprotokolle überprüfen

Die Anwendung protokolliert alle Operationen. Überprüfen Sie:

1. **Systemprotokolle:** `logs/system.log`
2. **Fehlerprotokolle:** `logs/error.log`
3. **Zugriffsprotokolle:** `logs/access.log`

### Logmeldungen interpretieren

**Beispiele für typische Meldungen:**

```
INFO: Request received for /api/insert
ERROR: Failed to parse data line "invalid,data,format"
WARN: Cache invalidation triggered for measurement temperatureOutside
```

## Systemüberprüfung

### Vor dem Start

Bevor Sie FKALA starten, führen Sie folgende Überprüfungen durch:

1. **Pfadexistenz:** Der DataStorage-Pfad existiert
2. **Berechtigungen:** Lesen/Schreiben in den Datenspeicher
3. **Speicherplatz:** Mindestens 100MB freier Speicherplatz
4. **Netzwerk:** MQTT-Broker erreichbar (falls konfiguriert)

### Nach dem Start

Nach erfolgreichem Start überprüfen Sie:

1. **API-Endpunkte:** `GET /api/health` für Statusabfrage
2. **Datenbankverbindung:** Zugriff auf Zeitreihendaten
3. **Caching:** Funktion des Caching-Mechanismus
4. **Hintergrundjobs:** Status der geplanten Aufgaben

## Spezielle Fehlercodes

### HTTP-Fehlercodes

| Code | Beschreibung | Lösung |
|------|-------------|---------|
| 400 | Ungültige Anfrage | Überprüfen Sie die Eingabedaten |
| 404 | Ressource nicht gefunden | Daten oder Parameter überprüfen |
| 500 | Interne Serverfehler | Systemprotokolle prüfen |
| 503 | Dienst nicht verfügbar | Serverüberwachung prüfen |

### KalaQl-Fehlercodes

| Code | Beschreibung | Lösung |
|------|-------------|---------|
| Parse Error | Syntaxfehler in Abfrage | Korrektur der KalaQl-Syntax |
| Missing Input | Fehlende Eingabedaten | Überprüfen Sie die Abhängigkeiten |
| Cache Error | Caching-Probleme | Cache-Struktur überprüfen |

## Kontakt für Unterstützung

Wenn Sie Probleme mit FKALA haben, kontaktieren Sie das Support-Team:

1. **E-Mail:** support@fkala.de
2. **Dokumentation:** docs/fkala/index.html
3. **GitHub Issues:** https://github.com/naxan6/FKALA/issues

## Wiederherstellung nach Fehlern

### Datenwiederherstellung

Falls Daten beschädigt sind:

1. **Sicherungskopie:** Stellen Sie eine Sicherung Ihrer Daten her
2. **Logdatei prüfen:** Suchen Sie nach Fehlermeldungen in den Logs
3. **Datenvalidierung:** Überprüfen Sie die Integrität der Zeitreihendaten

### Systemwiederherstellung

Für schwerwiegende Probleme:

1. **Systemneustart:** `systemctl restart fkala`
2. **Protokollanalyse:** Detaillierte Analyse der Logdateien
3. **Konfigurationsprüfung:** Überprüfen Sie alle appsettings.json-Einstellungen
