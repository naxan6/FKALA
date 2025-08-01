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

## Code Coverage Ergebnisse (31.07.2025)

- Gesamtabdeckung: 57% (zuvor 56.5%)
- Abgedeckte Zeilen: 2608
- Nicht abgedeckte Zeilen: 1961
- Coverable Zeilen: 4569
- Gesamtzeilen: 7887

## AI selbstersteller Kontext

Zuletzt analysierte Klassen mit niedriger Abdeckung:
- FKala.Core.DataLayer.Infrastructure.IEnumerableExtensions (0%)
- FKala.Core.Helper.Benchmarker (0%) 
- FKala.Core.Helper.Msg (0%)
- FKala.Core.Logic.Fast (0%)
- FKala.Core.Logic.FileFromEndProcessor (0%)

Klassen mit sehr niedriger Abdeckung (<30%):
- FKala.Core.KalaQl.QueryParser.AggregateParams
- FKala.Core.KalaQl.QueryParser.AggregateParser 
- FKala.Core.KalaQl.QueryParser.ExpressoParams

## ToDo Liste

- [x] Führe Unit Tests aus und überprüfe Coverage
- [x] Identifiziere Klassen oder Methoden mit niedriger Abdeckung und schreibe einen Test dazu
- [x] Aktualisierte Code Coverage Bericht
- [x] Erhöhe die Testabdeckung auf mindestens 50% je Datei, plane dazu, welchen Testfall Du erstellen willst.
- [x] Die Unittests sollen in neue Testdateien AI_<Thema>_Tests.cs aufgenommen werden.
- [x] Die Unittests sollen in der bereits verwendeten Technik geschrieben werden.
- [x] Du musst zum Erreichen des Ziels in dieser Datei im Bereich "AI selbstersteller Kontext" zwischendurch alle wichtigen Erkenntnisse hinterlegen, z.B. wenn Du herausgefunden hast, wie ein Befehls-Aufruf richtig funktioniert, welches Testframework im Einsatz ist, architekturell interessante Informationen oder ähnliches, um das in einer späteren Iteration dann hieraus einfach einzulesen ohne wieder alles aufwändig neu zu ermitteln.
- [x] Es darf JEWEILS IMMER NUR MAXIMAL ein einzelner Unittest IN EINEM SCHRIIT ergänzt werden, dann müssen die Unittests wieder neu durchgeführt werden und grün laufen!!!
- [x] Für Dateiänderungen immer apply_diff, search_and_replace and insert_content bevorzugen gegenüber write_to_file!
- [x] Halte bei apply_diff, search_and_replace und insert_content Aufrufen den zu ändernden Bezug so minimal wie möglich. Ersetze nur zu ändernde Zeilen! Überschreibe z. B. nicht komplette Dateien 1:1, um hinten was zu ergänzen - sondern füge ein!
- [x] Bereinige stests Build-Warnings (Clean-Code!)
