Deine langfristigen Aufgaben:
1. Schreibe und aktualisiere sofort immer Deinen Plan und Fortschritt in dieser Datei aitodos.md für Dich selber (für spätere Läufe).
2. Stelleals erstes sicher, dass die Unittests durchlaufen. Wewenn Fehler auftreten, dann beheben! Dabei liegen die Fehler IMMER IM UNITTEST  und nicht im getesteten Code!
3. Dann miss die Code Coverage mit task "test_with_coverage" (siehe tasks.json) und gib die Coverage in diese aitodos.md Datei aus.
4. Erhöhe die Testabdeckung, plane dazu, welche nächsten 3 Testfälle Du erstellen willst.
5. Die Unittests sollen in neue Testdateien AI_<Thema>_Tests.cs aufgenommen werden.
6. Die Unittests sollen in der bereits verwendeten Technik geschrieben werden.
7. Du musst zum Erreichen des Ziels in dieser Datei im Bereich "AI selbstersteller Kontext" zwischendurch alle wichtigen Erkenntnisse hinterlegen, z.B. wenn Du herausgefunden hast, wie ein Befehls-Aufruf richtig funktioniert, welches Testframework im Einsatz ist, architekturell interessante Informationen oder ähnliches, um das in einer späteren Iteration dann hieraus einfach einzulesen ohne wieder alles aufwändig neu zu ermitteln.
7. WICHTIG: Es darf JEWEILS IMMER NUR MAXIMAL ein einzelner Unittest IN EINEM SCHRIIT ergänzt werden, dann müssen die Unittests wieder neu durchgeführt werden und grün laufen!!!

# AI selbstersteller Kontext:
#############################################################

## Testframework: MsTest
## Code Coverage
- Code Coverage Aufruf: dotnet test --collect:"XPlat Code Coverage"
- Daraus kompakter Code Coverage Report, Beispielaufruf:
  reportgenerator.exe -reports:C:\git\FKALA\FKala.Unittests\TestResults\b57a6e6f-cd26-4def-92de-1c9951b2aff7\coverage.cobertura.xml -targetdir:.\report -reporttype:csvsummary
  (Hinweis: reportgenerator.exe ist global installiert)

## Code Coverage Ergebnisse (13.07.2025)

### Gesamtabdeckung
- Gesamtabdeckung: 43.3%
- Abgedeckte Zeilen: 1987
- Nicht abgedeckte Zeilen: 2598
- Coverable Zeilen: 4585
- Gesamtzeilen: 7895

### Abdeckung nach Klasse/Bereich
FKala.Core: 41.8%
- FileSystemHelper: 76.9%
- FKala.Core.DataLayer_Readable_Caching_V1: 63.7%
- FKala.Core.DataLayer.Cache.Cache_Base: 86.2%
- FKala.Core.DataLayer.Cache.Cache_Hourly: 71.1%
- FKala.Core.DataLayer.Cache.Cache_Minutely: 60%
- FKala.Core.DataLayer.Cache.Cache_5Minutely: 60%
- FKala.Core.DataLayer.Cache.Cache_15Minutely: 60%
- FKala.Core.DataLayers.StorageAccess: 48.4%
- FKala.Core.KalaQl.KalaQuery: 93.1%
- FKala.Core.KalaQl.KalaResult: 100%

### Bereiche zur Verbesserung der Abdeckung
- FKala.Core.DataLayer.Cache.* (Minutely, 5Minutely, 15Minutely - alle 11,1%)
- FKala.Core.KalaQl.QueryParser.* (viele Klassen mit 0% Abdeckung)
- FKala.Core.Logic.* (mehrere Klassen mit 0% Abdeckung)

### Empfehlungen
1. Implementiere Tests für Cache-Klassen (Minutely, 5Minutely, 15Minutely)
2. Testen Sie die KalaQl.QueryParser-Klassen und -Methoden
3. Überprüfen Sie die Logic-Klassen auf fehlende Testfälle

# TODO Liste:
- [ ] Vor jeder Ergänzung eines Unittests zuerst UNittests durchführen und ggf. fixen (im Unittest, denn der Logikcode ist immer richtig!)!
- [ ] Aktualisiere Code Coverage Ergebnisse nach jedem Testlauf
- [ ] Erstelle Tests für Cache_15Minutely Klasse
- [ ] Erstelle Tests für Cache_5Minutely Klasse
- [ ] Erstelle Tests für Cache_Minutely Klasse
- [ ] Erstelle Tests für KalaQl.QueryParser-Klassen
- [ ] Erstelle Tests für Logic-Klassen

# #############################################################
