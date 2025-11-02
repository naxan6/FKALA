# AITODOS

-Deine langfristigen Aufgaben:
-0. ToDo Liste ganz unten auf "nicht asugeführt" zurücksetzen
-1. Schreibe und aktualisiere sofort immer Deinen Plan und Fortschritt in dieser Datei aitodos.md für Dich selber (für spätere Läufe).
-2. Stelle als erstes sicher, dass die Unittests durchlaufen. Dies machst Du indem du sie ausführst, gleich inklusive Code Coverage!! Wenn Fehler auftreten, dann beheben! Dabei liegen die Fehler IMMER IM UNITTEST und nicht im getesteten Code!
-3. Sobald das durchläuft, gib die Coverage in diese aitodos.md Datei aus.
-4. Erhöhe die Testabdeckung auf mindestens 50% je Datei, plane dazu, welchen Testfall Du erstellen willst.
-5. Die Unittests sollen in neue Testdateien AI_<Thema>_Tests.cs aufgenommen werden.
-6. Die Unittests sollen in der bereits verwendeten Technik geschrieben werden.
-7. Du musst zum Erreichen des Ziels in dieser Datei im Bereich "AI selbstersteller Kontext" zwischendurch alle wichtigen Erkenntnisse hinterlegen, z.B. wenn Du herausgefunden hast, wie ein Befehls-Aufruf richtig funktioniert, welches Testframework im Einsatz ist, architekturell interessante Informationen oder ähnliches, um das in einer späteren Iteration dann hieraus einfach einzulesen ohne wieder alles aufwändig neu zu ermitteln.
-7. WICHTIG: Es darf JEWEILS IMMER NUR MAXIMAL ein einzelner Unittest IN EINEM SCHRIIT ergänzt werden, dann müssen die Unittests wieder neu durchgeführt werden und grün laufen!!!
-8. Für Dateiänderungen immer apply_diff, search_and_replace and insert_content bevorzugen gegenüber write_to_file!
-9. Halte bei apply_diff, search_and_replace und insert_content Aufrufen den zu ändernden Bezug so minimal wie möglich. Ersetze nur zu ändernde Zeilen! Überschreibe z. B. nicht komplette Dateien 1:1, um hinten was zu ergänzen - sondern füge ein!
-10. Bereinige stests Build-Warnings (Clean-Code!)

## Testframework: MsTest
## Code Coverage
- Code Coverage Aufruf (direkt im Projektverzeichnis ohne Verzeichniswechsel): 
  #> dotnet test --collect:"XPlat Code Coverage"
- Daraus kompakter Code Coverage Report, Beispielaufruf (direkt im Projecktverzeichnis ohne Verzeichniswechsel):
  #> reportgenerator.exe -reports:C:\git\FKALA\FKala.Unittests\TestResults\b57a6e6f-cd26-4def-92de-1c9951b2aff7\coverage.cobertura.xml -targetdir:.\report -reporttype:csvsummary
  (Hinweis: reportgenerator.exe ist global installiert)

## Code Coverage Ergebnisse (19.08.2025)

- Gesamtabdeckung: 69.3% (zuvor 63.8%, +5.5%)
- Abgedeckte Zeilen: 3170
- Nicht abgedeckte Zeilen: 1400
- Coverable Zeilen: 4570
- Gesamtzeilen: 7888

## Verbesserte Klassen durch neue Tests
- FKala.Core.KalaQl.QueryParser.AggregateParams: 0% → 100%
- FKala.Core.KalaQl.QueryParser.AlignTimezoneParams: 0% → 100%
- FKala.Core.KalaQl.QueryParser.VarParams: 0% → 100%
- FKala.Core.KalaQl.QueryParser.ExpressoParams: 0% → 100%
- FKala.Core.KalaQl.QueryParser.InsertParams: 0% → 100%
- FKala.Core.KalaQl.QueryParser.InterpolateParams: 0% → 100%
- FKala.Core.KalaQl.QueryParser.JsonQueryParams: 0% → 100%
- FKala.Core.KalaQl.QueryParser.LoadParams: 0% → 100%
- FKala.Core.KalaQl.QueryParser.MatViewParams: 0% → 100%
- FKala.Core.KalaQl.QueryParser.MgmtParams: 0% → 100%
- FKala.Core.KalaQl.QueryParser.PublishParams: 0% → 100%

## AI selbstersteller Kontext

Zuletzt analysierte Klassen mit niedriger Abdeckung (19.08.2025):
- FKala.Core.KalaQl.QueryParser.AggregateParser (5.9%)
- FKala.Core.KalaQl.QueryParser.InterpolateParser (16.6%)
- FKala.Core.KalaQl.QueryParser.JsonQueryParser (11.1%)
- FKala.Core.KalaQl.QueryParser.AlignTimezoneParser (40%)
- FKala.Core.KalaQl.QueryParser.ExpressoParser (40%)
- FKala.Core.KalaQl.QueryParser.InsertParser (40%)
- FKala.Core.KalaQl.QueryParser.MatViewParser (40%)
- FKala.Core.KalaQl.QueryParser.PublishParser (38.8%)
- FKala.Core.KalaQl.QueryParser.VarParser (40%)
- FKala.Core.Model.DataPoint (35.2%)
- FKala.Migrate.MariaDb.MigrateMariaDb_Tstsfe_Custom (0%)
- FKala.Migrate.MariaDb.ReaderExtension (0%) - Keine Tests möglich, da MySqlDataReader sealed ist

Klassen mit sehr niedriger Abdeckung (<50%), die verbessert werden müssen:
- AggregateParser: 5.9% → Ziel: >50%
- InterpolateParser: 16.6% → Ziel: >50%
- JsonQueryParser: 11.1% → Ziel: >50%

## Wichtige Erkenntnisse

- Testframework: MsTest (aus Unit Test Ausführung ersichtlich)
- Code Coverage Tool: dotnet test mit XPlat Code Coverage Collection
- Reportgenerierung: reportgenerator.exe mit TextSummary Format
- Alle 302 Unit Tests erfolgreich durchgelaufen
- Gesamtabdeckung verbessert von 63.8% auf 69.3% (+5.5%)
- Viele QueryParser Klassen haben 0% Abdeckung - benötigen dringend Tests
- Die meisten Klassen mit 0% Abdeckung sind Parameter-Klassen für QueryParser
- Es gibt bereits einige Testdateien, die Tests für verschiedene QueryParser Klassen enthalten
- Tests werden in der Regel in Dateien im Format AI_<Thema>_Tests.cs gespeichert
- Neue Testdatei AI_QueryParser_InterpolateParams_Tests.cs erfolgreich erstellt und erhöht Abdeckung auf 100%
- Neue Testdatei AI_QueryParser_JsonQueryParams_Tests.cs erfolgreich erstellt und erhöht Abdeckung auf 100%
- Neue Testdatei AI_QueryParser_LoadParams_Tests.cs erfolgreich erstellt und erhöht Abdeckung auf 100%
- Neue Testdatei AI_QueryParser_MatViewParams_Tests.cs erfolgreich erstellt und erhöht Abdeckung auf 100%
- Neue Testdatei AI_QueryParser_MgmtParams_Tests.cs erfolgreich erstellt und erhöht Abdeckung auf 100%
- Neue Testdatei AI_QueryParser_PublishParams_Tests.cs erfolgreich erstellt und erhöht Abdeckung auf 100%
- Neue Testdatei AI_QueryParser_VarParams_Tests.cs erfolgreich erstellt und erhöht Abdeckung auf 100%
- ReaderExtension Tests nicht möglich, da MySqlDataReader sealed ist und nicht mit Moq gemockt werden kann
- AggregateParser, InterpolateParser und JsonQueryParser haben sehr niedrige Abdeckung und benötigen dringend Tests

## ToDo Liste

- [x] Setze die ToDo Liste in aitodos.md auf "nicht ausgeführt" zurück
- [x] Führe Unit Tests aus und überprüfe Coverage
- [x] Identifiziere Klassen oder Methoden mit niedriger Abdeckung und schreibe einen Test dazu
- [ ] Aktualisierte Code Coverage Bericht
- [ ] Erhöhe die Testabdeckung auf mindestens 50% je Datei, plane dazu, welchen Testfall Du erstellen willst
- [ ] Die Unittests sollen in neue Testdateien AI_<Thema>_Tests.cs aufgenommen werden
- [ ] Die Unittests sollen in der bereits verwendeten Technik geschrieben werden
- [ ] Hinterlege wichtige Erkenntnisse im Bereich "AI selbstersteller Kontext"
- [ ] Führe maximal einen Unittest pro Schritt aus und prüfe, dass grün läuft
- [ ] Verwende für Dateiänderungen immer apply_diff, search_and_replace and insert_content statt write_to_file
- [ ] Halte bei Änderungen den Bezug minimal und ersetze nur notwendige Zeilen
- [ ] Bereinige stests Build-Warnings (Clean-Code!) - es reicht zu Prüfung ein einmaliges: "dotnet clean; dotnet build"
