# Verwendung von FKALA - Beispiele

## Erste Schritte

### Daten einfügen

Um Zeitreihendaten in FKALA zu speichern, verwenden Sie den Insert-Endpunkt:

**HTTP-Anfrage:**
```
PUT /api/insert
Content-Type: text/plain

temperatureOutside 2023-01-01T10:30:00Z 23.5
humidity 2023-01-01T10:30:00Z 65.2
```

**Antwort:** `200 OK` bei Erfolg

## Daten abfragen

### Einfache Abfrage

Um Daten auszulesen, verwenden Sie die Query-API:

**HTTP-Anfrage:**
```
GET /api/query?input=Load tempdata: temperatureOutside 2023-01-01T00:00:00Z 2023-01-02T00:00:00Z FiveMinutely_WAvg
```

**Antwort:** JSON-Formatierte Ergebnisse

## KalaQl-Sprache - Beispiele

### Zeitreihendaten laden

```
Load mydata: temperatureOutside 2023-01-01T00:00:00Z 2023-01-02T00:00:00Z FiveMinutely_WAvg
```

### Daten aggregieren

```
Load mydata: temperatureOutside 2023-01-01T00:00:00Z 2023-01-02T00:00:00Z FiveMinutely_WAvg | Aggregate mydata: Avg
```

### Materialisierte Ansicht erstellen

```
Load mydata: temperatureOutside 2023-01-01T00:00:00Z 2023-01-02T00:00:00Z FiveMinutely_WAvg | Aggregate Avg | Publish mydata: Table
```

## Komplexe Abfragen

### Mehrere Operationen in einer Kette

```
Load temp: temperatureOutside 2023-01-01T00:00:00Z 2023-01-02T00:00:00Z FiveMinutely_WAvg
| Aggregate temp: Avg
| MatView dailyavg: temp
| Publish dailyavg: Table
```

### Verwendung von Variablen

```
Var resolution: FiveMinutely_WAvg
Load mydata: temperatureOutside 2023-01-01T00:00:00Z 2023-01-02T00:00:00Z FiveMinutely_WAvg
```

## Datenformat

### Zeitreihendaten im CSV-Format

```
value,timestamp
23.5,2023-01-01T10:30:00Z
24.1,2023-01-01T10:31:00Z
```

### Zeitreihendaten im JSON-Format (für Abfragen)

```json
{
  "measurements": [
    {
      "name": "temperatureOutside",
      "data": [
        {"value": 23.5, "timestamp": "2023-01-01T10:30:00Z"},
        {"value": 24.1, "timestamp": "2023-01-01T10:31:00Z"}
      ]
    }
  ]
}
```

## Fehlerbehandlung

### Häufige Fehler und Lösungen

**Fehler 400 Bad Request:**
- Überprüfen Sie die Syntax Ihrer KalaQl-Abfrage
- Stellen Sie sicher, dass alle Parameter korrekt formatiert sind

**Fehler 404 Not Found:**
- Überprüfen Sie den Messwertnamen und die Zeitintervalle
- Stellen Sie sicher, dass die Daten tatsächlich im Speicher vorhanden sind

**Fehler 500 Internal Server Error:**
- Prüfen Sie die Serverprotokolle für detaillierte Fehlerinformationen

## Performance-Tipps

### Caching optimieren

Verwenden Sie die passende Cache-Auflösung für Ihre Abfragehäufigkeit:
- `Minutely` für hohe Frequenz
- `FiveMinutely` für mittlere Frequenz
- `Hourly` für geringe Frequenz

### Materialisierte Ansichten nutzen

Für häufig abgefragte Datenmengen erstellen Sie Materialisierte Ansichten:

```
Load mydata: temperatureOutside 2023-01-01T00:00:00Z 2023-01-02T00:00:00Z FiveMinutely_WAvg | Aggregate Avg | MatView dailyavg
```

## Monitoring und Debugging

### Systemprotokolle

Alle Operationen werden protokolliert. Überprüfen Sie die Serverprotokolle für:
- Erfolgsmeldungen
- Fehlermeldungen
- Performance-Indikatoren
