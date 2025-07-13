# Fehleranalyse für das FKALA Projekt

## Ziel
Identifizierung von logischen Fehlern und Sicherheitslücken im gesamten FKALA-Projekt.

## Methodik

1.  **Projektstruktur verstehen & Initiale Erkundung:**
    *   Überprüfung des gesamten Projektlayouts anhand der Dateilisten.
    *   Identifizierung der Schlüsselmodule und ihrer Verantwortlichkeiten (z.B. API, Kernlogik, Datenschicht, Unittests).

2.  **Gezielte Suche nach logischen Fehlern:**
    *   Fokus auf Bereiche mit komplexer Logik:
        *   KalaQl-Abfrageverarbeitung (`FKala.Core/KalaQl/`)
        *   Datenmanipulation und -speicherung (`FKala.Core/DataLayer/`)
        *   Asynchrone Operationen und Hintergrundjobs (`FKala.Api/Jobs/`, `FKala.Api/Worker/`)
        *   Zustandsmanagement innerhalb von Operationen.
    *   Verwendung von `search_files` für:
        *   Schlüsselwörter wie `TODO`, `FIXME`, `HACK`.
        *   Komplexe bedingte Anweisungen.
        *   Potenzielle Null-Dereferenzierungen (insbesondere in älterem Code, falls Nullable Reference Types nicht konsistent verwendet werden).
        *   Ressourcenmanagement (z.B. `IDisposable`-Verwendung).
    *   Überprüfung von Codedefinitionen mit `list_code_definition_names`, um Interaktionen zu verstehen.

3.  **Gezielte Suche nach Sicherheitslücken:**
    *   **Eingabevalidierung:**
        *   Untersuchung von API-Endpunkten (`FKala.Api/Controller/`) auf ordnungsgemäße Validierung eingehender Daten.
        *   Analyse des Abfrage-Parsings (`FKala.Core/KalaQl/QueryParser/`) auf potenzielle Injection-Vektoren in der KalaQl-Syntax.
    *   **Datensicherheit:**
        *   Überprüfung von Datenzugriffsmustern (`FKala.Core/DataLayer/`) auf Risiken wie unbefugten Zugriff oder Datenlecks.
        *   Suche nach hartcodierten Anmeldeinformationen oder sensiblen Informationen.
    *   **Dateisysteminteraktionen:**
        *   Analyse von `FKala.Core/Helper/FilesystemHelper.cs` und `FKala.Core/DataLayer/StorageAccess.cs` auf Path Traversal oder unsichere Dateioperationen.
    *   **Externe Interaktionen:**
        *   Überprüfung der MQTT-Integration (`FKala.Api/Worker/MqttWorker.cs`, `FKala.Api/Settings/MqttSettings.cs`) auf sichere Konfiguration und Kommunikation.
    *   **Deserialisierung:**
        *   Überprüfung der Verwendung von `JsonSerializer` auf potenzielle Schwachstellen bei der Deserialisierung nicht vertrauenswürdiger oder nicht ordnungsgemäß validierter Daten.
    *   Verwendung von `search_files` für:
        *   Muster, die auf SQL-Injection hindeuten (obwohl bei einer benutzerdefinierten QL weniger wahrscheinlich).
        *   Hartcodierte Geheimnisse.
        *   Häufig missbrauchte/unsichere Funktionen (kontextabhängig).

4.  **Detaillierte Code-Überprüfung (Hochrisikobereiche):**
    *   Basierend auf den Ergebnissen aus Schritt 2 & 3 werden spezifische Dateien für eine eingehende Überprüfung mit `read_file` ausgewählt.
    *   Manuelles Verfolgen von Datenflüssen und Ausführungspfaden.
    *   Bewertung von Fehlerbehandlungsmechanismen.

5.  **Dokumentation der Ergebnisse:**
    *   Alle Ergebnisse werden in dieser Datei (`BugAnalysis.md`) dokumentiert.
    *   Jedes Ergebnis enthält:
        *   Beschreibung des Problems.
        *   Betroffene Datei(en) und Zeilennummer(n).
        *   Mögliche Auswirkungen und Schweregrad.
        *   Vorgeschlagene Behebung (falls zutreffend).

## Ergebnisse

*(Dieser Abschnitt wird gefüllt, sobald Probleme identifiziert werden)*

### Logische Fehler
*   **Defekte Rebuild-Funktionalität in Op_JsonQuery.cs**
    *   **Datei:** `FKala.Core/KalaQl/Op_JsonQuery.cs`
    *   **Beschreibung:** Ein Kommentar `//TODODODODODODDODODO REBUILD IST SOMIT DEFEKT` deutet darauf hin, dass die Rebuild-Funktionalität fehlerhaft sein könnte.
    *   **Auswirkung:** Die `ForceRebuild`-Option funktioniert möglicherweise nicht wie erwartet, was zu veralteten oder inkonsistenten Ergebnissen bei JsonQueries führen kann.
    *   **Schweregrad:** Mittel.
*   **Ineffiziente Schleife in Cache_Base.cs**
    *   **Datei:** `FKala.Core/DataLayer/Cache/Cache_Base.cs`
    *   **Beschreibung:** Ein Kommentar `//TODO für später: breaken, sobald endtime überschritten, nicht mehr komplett durchlaufen` weist auf eine mögliche Performance-Optimierung hin. Die Schleife durchläuft möglicherweise mehr Daten als nötig.
    *   **Auswirkung:** Potenziell geringere Performance bei Cache-Operationen, insbesondere bei großen Zeitbereichen.
    *   **Schweregrad:** Niedrig.
*   **Potenzieller Bug/Hack in der Caching-Logik (jumpintervall)**
    *   **Datei:** `FKala.Core/DataLayer/Cache/Cache_Base.cs`
    *   **Beschreibung:** Kommentare `//BUG/HACK: will only work in some cases` neben Bedingungen, die `jumpintervall` betreffen. Dies deutet auf eine instabile oder unzuverlässige Logik hin.
    *   **Auswirkung:** Könnte zu fehlerhaftem Caching-Verhalten führen, z.B. falsche Daten oder ineffiziente Cache-Aktualisierungen unter bestimmten Bedingungen.
    *   **Schweregrad:** Mittel bis Hoch (abhängig von den genauen Umständen, unter denen der Fehler auftritt).
*   **Potenziell nicht beendeter Hintergrundtask in BufferedWriterService**
    *   **Datei:** `FKala.Core/DataLayer/Infrastructure/BufferedWriterService.cs`
    *   **Beschreibung:** Die Methode `FlushBuffersPeriodically` läuft in einem `Task.Run` als Endlosschleife (`while (true)`). Es gibt keinen Mechanismus (z.B. `CancellationToken`), um diesen Task sauber zu beenden, wenn die `BufferedWriterService`-Instanz disposed wird.
    *   **Auswirkung:** Der Task könnte versuchen, weiterzuarbeiten oder auf bereits disposete Ressourcen zuzugreifen, nachdem der Service beendet wurde. Dies kann zu unerwarteten Fehlern während des Herunterfahrens der Anwendung führen.
    *   **Schweregrad:** Mittel.

### Sicherheitslücken
*   **Unzureichende Eingabevalidierung im InsertController**
    *   **Datei:** `FKala.Api/Controller/InsertController.cs`
    *   **Beschreibung:** Die `Insert`-Methode validiert den `input`-String nur auf `null` oder Leerheit. Das Format des Strings (`<measurement> <timestamp> <value>`) wird nicht vor der Weitergabe an `DataLayer.Insert()` überprüft.
    *   **Auswirkung:** Ungültig formatierte Eingaben könnten zu Laufzeitfehlern (z.B. `IndexOutOfRangeException`, `FormatException`) in der `DataLayer`-Verarbeitung führen, was zu `Internal Server Error`-Antworten führt. Obwohl `PathSanitizer` für den Measurement-Namen verwendet wird, könnten unerwartete Zeichen im Wertteil oder eine falsche Struktur des Gesamtstrings Probleme verursachen.
    *   **Schweregrad:** Mittel.
    *   **Empfehlung:** Implementierung einer strengeren Formatvalidierung im Controller oder einer vorgeschalteten Schicht.
*   **Potenzielle KalaQl Injection im QueryController**
    *   **Datei:** `FKala.Api/Controller/QueryController.cs` (Methode `DoQuery`)
    *   **Beschreibung:** Der vom Benutzer bereitgestellte `query`-String wird direkt an `KalaQuery.Start().FromQuery(query)` übergeben. Wenn der KalaQl-Parser (`FKala.Core/KalaQl/QueryParser/`) nicht robust gegen Injection-Angriffe implementiert ist, könnten Angreifer die Abfragelogik manipulieren, auf nicht autorisierte Daten zugreifen oder Management-Operationen ausführen.
    *   **Auswirkung:** Abhängig von der Robustheit des Parsers, von Datenlecks über unautorisierten Datenzugriff bis hin zu Denial-of-Service oder Ausführung von administrativen Befehlen.
    *   **Schweregrad:** Hoch.
    *   **Empfehlung:** Dringendes und gründliches Audit aller KalaQl-Parser-Implementierungen auf Sicherheitslücken. Strikte Validierung und ggf. Escaping aller Benutzereingaben, die Teil der Abfrage werden.
*   **Informationspreisgabe durch detaillierte Fehlermeldungen im QueryController**
    *   **Datei:** `FKala.Api/Controller/QueryController.cs` (Methode `DoQuery`)
    *   **Beschreibung:** Im Fehlerfall (sowohl Parser-Fehler als auch Exceptions) werden detaillierte Fehlermeldungen und Stacktraces potenziell an den Client zurückgegeben.
    *   **Auswirkung:** Angreifer könnten sensible Informationen über die interne Funktionsweise der Anwendung, verwendete Bibliotheken oder spezifische Fehlerzustände erhalten, was weitere Angriffe erleichtern kann.
    *   **Schweregrad:** Mittel.
    *   **Empfehlung:** Nur generische Fehlermeldungen an den Client senden. Detaillierte Fehlerinformationen ausschließlich serverseitig loggen.
*   **Potenzielle KalaQl Injection im StreamQueryController**
    *   **Datei:** `FKala.Api/Controller/StreamQueryController.cs` (Methode `DoQuery`)
    *   **Beschreibung:** Ähnlich wie im `QueryController`, wird der Benutzereingabe-String direkt an `KalaQuery.Start(true).FromQuery(query)` übergeben. Die gleichen Risiken bezüglich KalaQl-Injection bestehen.
    *   **Auswirkung:** Datenlecks, unautorisierter Datenzugriff, Denial-of-Service (besonders relevant bei Streaming durch ressourcenintensive Abfragen).
    *   **Schweregrad:** Hoch.
    *   **Empfehlung:** Dringendes und gründliches Audit aller KalaQl-Parser-Implementierungen.
*   **Unangemessene Fehlerbehandlung und Informationspreisgabe im StreamQueryController**
    *   **Datei:** `FKala.Api/Controller/StreamQueryController.cs`
    *   **Beschreibung:**
        1.  Bei `string.IsNullOrEmpty(input)` wird eine `Exception` geworfen, was zu HTTP 500 führt, anstatt eines erwarteten HTTP 400.
        2.  Parser-Fehler (`result.Errors`) führen ebenfalls zum Werfen einer `Exception` (HTTP 500).
        3.  Im allgemeinen `catch`-Block wird `throw;` verwendet, bevor Logging oder eine strukturierte Fehlerantwort gesendet werden kann. Dies führt zu einem HTTP 500 und verhindert das Logging der Exception-Details an dieser Stelle.
    *   **Auswirkung:** Preisgabe von Server-Fehlerdetails in Entwicklungsumgebungen. Unklare Fehlerantworten für Clients. Verhindertes Logging im `catch`-Block.
    *   **Schweregrad:** Mittel.
    *   **Empfehlung:** Fehlerbehandlung überarbeiten: HTTP 400 für ungültige Client-Eingaben, strukturierte und bereinigte Fehlermeldungen für Parser-Fehler (ggf. auch HTTP 400), und generische HTTP 500-Antworten für unerwartete Serverfehler mit serverseitigem Logging. Den `throw;` im `catch` Block entfernen, um Logging und eine kontrollierte Antwort zu ermöglichen.
*   **Potenzielle Remote Code Execution (RCE) durch ExpressoParser**
    *   **Datei:** `FKala.Core/KalaQl/QueryParser/VerbParsers.cs` (Klasse `ExpressoParser`)
    *   **Beschreibung:** Der `ExpressoParser` nimmt einen Ausdrucksstring entgegen (`fields[2]`), ersetzt einfache durch doppelte Anführungszeichen und übergibt ihn an `Op_Expresso`. Wenn dieser Ausdruck später unsicher ausgewertet wird (z.B. durch Code-Interpretation oder eine Skript-Engine ohne Sandboxing), könnte dies eine RCE-Schwachstelle darstellen.
    *   **Auswirkung:** Vollständige Kompromittierung des Servers.
    *   **Schweregrad:** Kritisch.
    *   **Empfehlung:** Dringend untersuchen, wie und wo `Op_Expresso.ExpressionString` ausgewertet wird. Sicherstellen, dass eine sichere Auswertungsumgebung (Sandboxing, Whitelisting) verwendet wird oder die Funktionalität überdacht wird.
*   **Potenzielle Injection durch JsonQueryParser (JsonPath)**
    *   **Datei:** `FKala.Core/KalaQl/QueryParser/VerbParsers.cs` (Klasse `JsonQueryParser`)
    *   **Beschreibung:** Der `JsonQueryParser` nimmt einen JsonPath-Ausdruck (`fields[3]`) entgegen. Wenn die verwendete JsonPath-Auswertungsbibliothek anfällig ist oder der Ausdruck nicht validiert/bereinigt wird, könnten speziell gestaltete JsonPath-Ausdrücke zu Injection-Angriffen auf die JSON-Engine oder zu Denial-of-Service führen.
    *   **Auswirkung:** Datenlecks, Denial-of-Service, potenziell Ausführung von Code, falls die JSON-Engine dies zulässt.
    *   **Schweregrad:** Hoch.
    *   **Empfehlung:** Überprüfen der verwendeten JsonPath-Bibliothek und Implementierung von Validierung/Bereinigung für JsonPath-Ausdrücke.
*   **Unsichere Behandlung von Measurement-Namen in mehreren Parsern**
    *   **Dateien:** `FKala.Core/KalaQl/QueryParser/VerbParsers.cs` (z.B. `LoadParser`, `JsonQueryParser`, `MatViewParser`, `InsertParser`)
    *   **Beschreibung:** Measurement-Namen werden oft direkt aus den `fields` übernommen (z.B. `fields[2]`). Obwohl `PathSanitizer` später im `DataLayer` greift, könnten spezielle Zeichen in Measurement-Namen vor diesem Schritt Probleme verursachen, wenn die Namen für interne Logik, Schlüsselbildung oder Vergleiche im Parser oder den `Op_` Klassen verwendet werden, bevor sie als Pfad behandelt werden.
    *   **Auswirkung:** Potenziell fehlerhaftes Verhalten, Umgehung von Logik, Denial-of-Service.
    *   **Schweregrad:** Mittel bis Hoch (abhängig von der internen Verwendung).
    *   **Empfehlung:** Strikte Validierung von Measurement-Namen bereits auf Parser-Ebene, um nur erlaubte Zeichen/Formate zuzulassen.
*   **Potenzielle Injection-Schwachstellen durch MgmtParser-Parameter**
    *   **Datei:** `FKala.Core/KalaQl/QueryParser/MgmtParser.cs`
    *   **Beschreibung:** Der `MgmtParser` fasst alle Felder nach der Aktion zu einem einzigen `parameters`-String zusammen. Dieser String wird an `Op_Mgmt` übergeben. Wenn `Op_Mgmt` diesen String für verschiedene Aktionen (z.B. `IMPORTINFLUX`, `COPY`, `RENAME`, `BLACKLIST`) weiterverarbeitet und die darin enthaltenen Teile (Dateipfade, Measurement-Namen, Datenbankparameter etc.) ohne strikte Validierung, Bereinigung oder Escaping in sensiblen Operationen verwendet, besteht ein hohes Risiko für Injection-Angriffe (Path Traversal, SQL-Injection-ähnliche Angriffe, Command Injection je nach Kontext).
    *   **Auswirkung:** Abhängig von der spezifischen Management-Aktion und der Parameterverarbeitung, von Datenmanipulation und -diebstahl bis hin zur Kompromittierung des Dateisystems oder anderer verbundener Systeme.
    *   **Schweregrad:** Hoch bis Kritisch.
    *   **Empfehlung:** Dringende Überprüfung der `Op_Mgmt`-Implementierung für jede `MgmtAction`. Sicherstellen, dass alle aus dem `parameters`-String extrahierten Werte strikt validiert und kontextbezogen sicher behandelt werden, bevor sie in Dateioperationen, Datenbankabfragen oder anderen kritischen Funktionen verwendet werden.
*   **Potenzielles Path Traversal durch unsichere Pfadübergabe an FilesystemHelper.DirectoryCopy**
    *   **Datei:** `FKala.Core/Helper/FilesystemHelper.cs`
    *   **Beschreibung:** Die Methode `DirectoryCopy` validiert die Eingabepfade `sourceDirName` und `destDirName` nicht über ihre Basispfade hinaus. Wenn diese Pfade aus Benutzereingaben konstruiert und nicht vorher rigoros validiert und bereinigt werden (z.B. durch `Path.GetFullPath` und anschließende Überprüfung, ob der Pfad innerhalb eines erlaubten Basisverzeichnisses liegt), könnten Angreifer Path-Traversal-Sequenzen (`../`) einschleusen, um auf nicht autorisierte Verzeichnisse zuzugreifen oder in diese zu schreiben.
    *   **Auswirkung:** Lesen sensibler Dateien, Schreiben/Überschreiben von Dateien in beliebigen Verzeichnissen, abhängig von den Berechtigungen des Anwendungsprozesses.
    *   **Schweregrad:** Hoch.
    *   **Empfehlung:** Sicherstellen, dass alle Aufrufer von `DirectoryCopy` die Pfadparameter sorgfältig validieren und bereinigen. Verwendung von `Path.GetFullPath()` zur Normalisierung und anschließende Überprüfung, ob der resultierende Pfad innerhalb eines erlaubten Basisverzeichnisses liegt. Die Robustheit von `PathSanitizer.SanitizePath` ist ebenfalls entscheidend, wenn Measurement-Namen zur Pfadkonstruktion verwendet werden.
*   **Unzureichender Path-Traversal-Schutz in PathSanitizer**
    *   **Datei:** `FKala.Core/Helper/PathSanitizer.cs` (eigentlich `FKala.Core/Logic/PathSanitizer.cs`)
    *   **Beschreibung:** Die Methode `SanitizePath` ersetzt ungültige *Dateinamen*-Zeichen durch `$`. Sie verhindert jedoch nicht explizit Path-Traversal-Sequenzen wie `../` oder `..\`. Ein Pfad wie `../../secrets` würde diese Bereinigung passieren. Wenn dieser "bereinigte" Pfad dann zur Konstruktion von vollständigen Pfaden verwendet wird (z.B. als Measurement-Name), könnte dies immer noch zu Path Traversal führen, abhängig davon, wie `Path.Combine` und andere Datei-APIs damit umgehen.
    *   **Auswirkung:** Potenzieller Zugriff auf Dateien und Verzeichnisse außerhalb des vorgesehenen Datenverzeichnisses, was zu Datenlecks oder Manipulationen führen kann.
    *   **Schweregrad:** Hoch.
    *   **Empfehlung:** `PathSanitizer.SanitizePath` sollte explizit Path-Traversal-Angriffe verhindern. Dies kann durch Normalisierung des Pfades (z.B. mit `Path.GetFullPath()`) und anschließender Überprüfung, ob der Pfad innerhalb eines erlaubten Basisverzeichnisses bleibt, geschehen. Alternativ oder zusätzlich könnte eine strenge Whitelist für erlaubte Zeichen in Measurement-Namen durchgesetzt werden. Das Ersetzen von `/` und `\` durch `$` verhindert zwar, dass der Measurement-Name selbst eine Verzeichnishierarchie bildet, aber nicht das Ausbrechen mittels `../`.
*   **Potenziell unverschlüsselte MQTT-Kommunikation und fehlende Authentifizierung**
    *   **Dateien:** `FKala.Api/Worker/MqttWorker.cs`, `FKala.Api/Settings/MqttSettings.cs`
    *   **Beschreibung:** Der `MqttWorker` konfiguriert keine expliziten TLS/SSL-Optionen oder Authentifizierungsmechanismen (Benutzername/Passwort) für die Verbindung zum MQTT-Broker. Wenn die MQTT-URL nicht `mqtts://` verwendet oder der Broker keine Authentifizierung erzwingt, könnte die Kommunikation unverschlüsselt erfolgen und für unbefugte Zugriffe offen sein.
    *   **Auswirkung:** Abfangen von Daten im Transit (wenn unverschlüsselt), unbefugtes Senden oder Empfangen von MQTT-Nachrichten, wenn der Broker nicht gesichert ist.
    *   **Schweregrad:** Mittel bis Hoch (abhängig von der Sensitivität der Daten und der Broker-Konfiguration).
    *   **Empfehlung:** Implementierung und standardmäßige Aktivierung von TLS für die MQTT-Verbindung. Bereitstellung von Konfigurationsoptionen für die Authentifizierung am MQTT-Broker.
*   **Potenzielle Injection über MQTT Topic-Namen und Payloads**
    *   **Datei:** `FKala.Api/Worker/MqttWorker.cs`
    *   **Beschreibung:** Der MQTT-Topic-Name wird (nach Ersetzung von Leerzeichen) als Teil des Measurement-Namens für `DataLayer.Insert` verwendet. Der Payload wird direkt als Wert verwendet. Wenn Angreifer Topic-Namen oder Payloads kontrollieren können, könnten sie versuchen, ungültige Measurement-Namen zu erzeugen (Risiko von Path Traversal, falls `PathSanitizer` umgangen wird) oder schädliche Payloads einzuschleusen, die bei späterer Verarbeitung Probleme verursachen (z.B. XSS, wenn die Daten in einer UI angezeigt werden, oder Fehler bei der Verarbeitung durch `Op_Expresso`).
    *   **Auswirkung:** Erstellung unerwünschter Measurements, potenzielles Path Traversal, Einschleusung von schädlichen Daten, die spätere Systeme kompromittieren oder stören könnten.
    *   **Schweregrad:** Mittel bis Hoch.
    *   **Empfehlung:** Strikte Validierung und Bereinigung von MQTT-Topic-Namen, bevor sie als Measurement-Namen verwendet werden (Whitelist-Ansatz bevorzugt). Behandlung von MQTT-Payloads als potenziell unsicher und entsprechende Kodierung/Validierung je nach späterem Verwendungszweck.
*   **Potenzielle Risiken bei der JSON-Deserialisierung in Op_JsonQuery**
    *   **Datei:** `FKala.Core/KalaQl/Op_JsonQuery.cs`
    *   **Beschreibung:** `JsonConvert.DeserializeObject<Dictionary<string, object>>(item.ValueText)` wird verwendet, um den Textwert von Datenpunkten zu deserialisieren.
        1.  **TypeNameHandling**: Wenn die globalen Standardeinstellungen von Newtonsoft.Json `TypeNameHandling` auf einen unsicheren Wert (z.B. `All`, `Auto`) setzen, könnte dies zu Remote Code Execution führen, falls ein Angreifer den Inhalt von `item.ValueText` kontrollieren und ein `$type`-Feld einschleusen kann. Standardmäßig ist `TypeNameHandling.None` aktiv, was sicher ist.
        2.  **Resource Exhaustion**: Extrem große oder komplexe JSON-Strings in `item.ValueText` könnten bei der Deserialisierung zu hohem Ressourcenverbrauch (CPU, Speicher) und somit zu einem Denial-of-Service führen.
    *   **Auswirkung:** Potenziell RCE (wenn `TypeNameHandling` unsicher konfiguriert ist), Denial-of-Service.
    *   **Schweregrad:** Kritisch (für RCE-Szenario), Mittel (für DoS).
    *   **Empfehlung:**
        1.  Sicherstellen, dass `TypeNameHandling` global auf `None` gesetzt ist oder explizit `JsonSerializerSettings` mit `TypeNameHandling = TypeNameHandling.None` an `DeserializeObject` übergeben.
        2.  Implementierung von Schutzmaßnahmen gegen Resource Exhaustion, z.B. Begrenzung der maximalen Größe von `item.ValueText` vor der Deserialisierung oder Konfiguration von Limits im Deserializer (falls von Newtonsoft.Json unterstützt).
        3.  Sicherstellen, dass Daten, die als `item.ValueText` gespeichert werden, bereits bei der Eingabe validiert werden.
*(Details zu gefundenen Sicherheitslücken)*

---

**Nächste Schritte (für Cline im ACT MODE):**
*   Ausführung des oben beschriebenen Plans, beginnend mit der Überprüfung der Projektstruktur und gezielten Suchen.
*   Füllen des Abschnitts "Ergebnisse".
