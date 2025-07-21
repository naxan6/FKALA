# AITODOS

-Deine langfristigen Aufgaben:
-1. Schreibe und aktualisiere sofort immer Deinen Plan und Fortschritt in dieser Datei aitodos.md für Dich selber (für spätere Läufe).
-2. Stelle als erstes sicher, dass die Unittests durchlaufen. Dies machst Du indem du sie ausführst!! Wenn Fehler auftreten, dann beheben! Dabei liegen die Fehler IMMER IM UNITTEST und nicht im getesteten Code!
-3. Dann miss die Code Coverage (Aufruf siehe unten) und gib die Coverage in diese aitodos.md Datei aus.
-4. Erhöhe die Testabdeckung auf mindestens 50% je Datei, plane dazu, welchen Testfall Du erstellen willst.
-5. Die Unittests sollen in neue Testdateien AI_<Thema>_Tests.cs aufgenommen werden.
-6. Die Unittests sollen in der bereits verwendeten Technik geschrieben werden.
-7. Du musst zum Erreichen des Ziels in dieser Datei im Bereich "AI selbstersteller Kontext" zwischendurch alle wichtigen Erkenntnisse hinterlegen, z.B. wenn Du herausgefunden hast, wie ein Befehls-Aufruf richtig funktioniert, welches Testframework im Einsatz ist, architekturell interessante Informationen oder ähnliches, um das in einer späteren Iteration dann hieraus einfach einzulesen ohne wieder alles aufwändig neu zu ermitteln.
-7. WICHTIG: Es darf JEWEILS IMMER NUR MAXIMAL ein einzelner Unittest IN EINEM SCHRIIT ergänzt werden, dann müssen die Unittests wieder neu durchgeführt werden und grün laufen!!!
-8. Für Dateiänderungen immer apply_diff, search_and_replace and insert_content bevorzugen gegenüber write_to_file!
9. Halt apply_diff so klein wie möglich. ersetze nicht unnötig ungeänderte Zeilen!

## Testframework: MsTest
## Code Coverage
- Code Coverage Aufruf: dotnet test --collect:"XPlat Code Coverage"
- Daraus kompakter Code Coverage Report, Beispielaufruf:
  reportgenerator.exe -reports:C:\git\FKALA\FKala.Unittests\TestResults\b57a6e6f-cd26-4def-92de-1c9951b2aff7\coverage.cobertura.xml -targetdir:.\report -reporttype:csvsummary
  (Hinweis: reportgenerator.exe ist global installiert)

## Code Coverage Ergebnisse (20.07.2025)

- Gesamtabdeckung: 45.6%
- Abgedeckte Zeilen: 2091
- Nicht abgedeckte Zeilen: 2494
- Coverable Zeilen: 4585
- Gesamtzeilen: 7895

## ToDo Liste

- [ ] Führe Unit Tests aus und überprüfe Coverage
- [ ] Identifiziere Klassen oder Methoden mit niedriger Abdeckung und schreibe einen Test dazu
- [ ] Aktualisiere Code Coverage Bericht
