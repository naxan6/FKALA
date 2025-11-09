using FKala.Core.Interfaces;
using FKala.Core.KalaQl; // Für KalaQuery und KalaResult
using Microsoft.Extensions.Logging;
using Quartz;
using System;
using System.IO; // Für DirectoryInfo
using System.Linq; // Für Linq-Methoden wie Skip
using System.Threading.Tasks;
// Benötigt, wenn DataLayer_Readable_Caching_V1.MatView direkt verwendet wird und nicht über ein Interface
using FKala.Core;


// Annahme: Diese Datei wird im Projekt FKala.Api erstellt, z.B. in einem Ordner "Jobs"
namespace FKala.Api.Jobs
{
    [DisallowConcurrentExecution] // Verhindert, dass der Job mehrfach gleichzeitig läuft
    public class MatViewRefreshJob : IJob
    {
        private readonly IDataLayer _dataLayer;
        private readonly ILogger<MatViewRefreshJob> _logger;

        public MatViewRefreshJob(IDataLayer dataLayer, ILogger<MatViewRefreshJob> logger)
        {
            _dataLayer = dataLayer;
            _logger = logger;
        }

        public async Task Execute(IJobExecutionContext context)
        {
            _logger.LogInformation("Starte nächtlichen MatView Refresh Job um {Time}", DateTimeOffset.Now);

            try
            {
                // Expliziter Typ für Klarheit, da LoadMatViews() List<DataLayer_Readable_Caching_V1.MatView> zurückgibt
                var matViewDefinitions = _dataLayer.LoadMatViews(); 
                
                _logger.LogInformation("{Count} MatView-Definitionen gefunden.", matViewDefinitions.Count);

                if (!matViewDefinitions.Any())
                {
                    _logger.LogInformation("Keine MatViews zum Aktualisieren gefunden.");
                    await Task.CompletedTask; // Sicherstellen, dass der Task als abgeschlossen markiert wird
                    return;
                }

                foreach (var matViewDef in matViewDefinitions)
                {
                    // ViewName ist der Name des Ordners, in dem viewdef.txt liegt
                    string viewName = new DirectoryInfo(matViewDef.ViewdefFilePath).Parent!.Name;
                    _logger.LogInformation("Beginne Verarbeitung von MatView: {ViewName}", viewName);

                    try
                    {
                        // 1. Alte MatView löschen
                        _logger.LogInformation("Lösche alte MatView: {ViewName}", viewName);
                        _dataLayer.DeleteMeasurementAndMatViewDefinition(viewName);

                        // 2. MatView neu erstellen
                        _logger.LogDebug("Erstelle MatView neu: {ViewName}", viewName);
                        string queryText = matViewDef.Query;

                        if (string.IsNullOrWhiteSpace(queryText))
                        {
                            _logger.LogWarning("Query für MatView {ViewName} ist leer. Überspringe Neuerstellung.", viewName);
                            continue;
                        }
                        
                        KalaQuery newKalaQuery = KalaQuery.Start(); // Erstellt eine neue Instanz mit eigener ParserRegistry
                        newKalaQuery.FromQuery(queryText); // Parst den Text und fügt Operationen hinzu

                        // Führe die Query aus, um die MatView neu zu materialisieren
                        KalaResult executionResult = newKalaQuery.Execute(_dataLayer);
                        
                        if (executionResult.Errors.Any())
                        {
                            _logger.LogError("Fehler bei der Ausführung der KalaQuery für MatView {ViewName}: {Errors}",
                                viewName, string.Join("; ", executionResult.Errors));
                        }
                        else
                        {
                             executionResult.ConsumeResultSetsNoOutput();
                            _logger.LogInformation("MatView {ViewName} erfolgreich neu erstellt.", viewName);
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Unerwarteter Fehler bei der Aktualisierung der MatView {ViewName}.", viewName);
                        // Fortfahren mit der nächsten MatView
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Schwerwiegender Fehler im MatView Refresh Job.");
                // Hier könnte man überlegen, ob der Job erneut versucht werden soll (Quartz-Konfiguration)
            }

            _logger.LogInformation("Nächtlicher MatView Refresh Job abgeschlossen um {Time}", DateTimeOffset.Now);
            await Task.CompletedTask; // IJob erfordert Task Rückgabe
        }
    }
}
