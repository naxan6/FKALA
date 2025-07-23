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
-9. Halte apply_diff, search_and_replace und insert_content Aufrufe so minimal wie möglich. Ersetze nur zu ändernde Zeilen!
-10. Bereinige stests Build-Warnings (Clean-Code!)

## Testframework: MsTest
## Code Coverage
- Code Coverage Aufruf (direkt im Projektverzeichnis ohne Verzeichniswechsel): 
  #> dotnet test --collect:"XPlat Code Coverage"
- Daraus kompakter Code Coverage Report, Beispielaufruf (direkt im Projecktverzeichnis ohne Verzeichniswechsel):
  #> reportgenerator.exe -reports:C:\git\FKALA\FKala.Unittests\TestResults\b57a6e6f-cd26-4def-92de-1c9951b2aff7\coverage.cobertura.xml -targetdir:.\report -reporttype:csvsummary
  (Hinweis: reportgenerator.exe ist global installiert)

## Code Coverage Ergebnisse (23.07.2025)

- Gesamtabdeckung: 56.1% (56.1%)
- Abgedeckte Zeilen: 2573
- Nicht abgedeckte Zeilen: 2012
- Coverable Zeilen: 4585
- Gesamtzeilen: 7895

### Klassen mit niedriger Abdeckung (< 50%):
- Op_Expresso: 88.3% - Expression-Operationen (verbessert von 25.8%)
- Op_MatView: 18.7% - Materialized View-Operationen (verbessert von 8%)
- Op_Mgmt: 45.2% - Management-Operationen (verbessert von 4.1%)
- Op_AlignTimezone: 48% - AlignTimezone-Operationen
- Op_Var: 60% - Variable-Operationen
- Op_Publish: 54.4% - Publish-Operationen

## ToDo Liste

- [x] Führe Unit Tests aus und überprüfe Coverage
- [x] Identifiziere Klassen oder Methoden mit niedriger Abdeckung und schreibe einen Test dazu
- [x] Aktualisiere Code Coverage Bericht
