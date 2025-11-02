# Übersicht der Code-Struktur

## FKala.Api

### Controller
- **InsertController.cs**
  - `InsertController(IDataLayer dataLayer)`
  - `IActionResult Insert([FromBody] string input)`: Fügt Daten über einen HTTP PUT-Request hinzu.
- **QueryController.cs**
  - `QueryController(IDataLayer dataLayer, ILogger<QueryController> logger)`
  - `IActionResult QueryGet([FromQuery] string input)`: Führt eine Abfrage über einen HTTP GET-Request aus.
  - `IActionResult QueryPost([FromBody] string input)`: Führt eine Abfrage über einen HTTP POST-Request aus.
  - `IActionResult DoQuery(string query)`: Verarbeitet die Abfrage und gibt das Ergebnis zurück.
  - `string ProcessString(string input)`: Verarbeitet den Eingabestring.
- **StreamQueryController.cs**
  - `StreamQueryController(IDataLayer dataLayer, ILogger<QueryController> logger)`
  - `IAsyncEnumerable<Dictionary<string, object>> QueryGet([FromQuery] string input)`: Führt eine Streaming-Abfrage über einen HTTP GET-Request aus.
  - `IAsyncEnumerable<Dictionary<string, object>> QueryPost([FromBody] string input)`: Führt eine Streaming-Abfrage über einen HTTP POST-Request aus.
  - `IAsyncEnumerable<Dictionary<string, object>> DoQuery(string query)`: Verarbeitet die Streaming-Abfrage und gibt das Ergebnis zurück.
  - `string ProcessString(string input)`: Verarbeitet den Eingabestring.

### InputFormatter
- **PlainTextFormatter.cs**
  - `PlainTextFormatter()`: Konstruktor, der unterstützte Medienarten und Encodings festlegt.
  - `Task<InputFormatterResult> ReadRequestBodyAsync(InputFormatterContext context, Encoding encoding)`: Liest den Request-Body als Plain Text.
  - `bool CanReadType(Type type)`: Prüft, ob der Typ gelesen werden kann.

### Jobs
- **MatViewRefreshJob.cs**
  - `MatViewRefreshJob(IDataLayer dataLayer, ILogger<MatViewRefreshJob> logger)`
  - `Task Execute(IJobExecutionContext context)`: Führt den Job aus, um MatViews zu aktualisieren.
  - `bool ValidateQuery(string queryText)`: Validiert die Query-Definitionen.

### Settings
- **MqttSettings.cs**
  - `MqttSettings()`: Konstruktor.
  - `void Configure(IConfiguration configuration)`: Konfiguriert die Einstellungen aus der Konfigurationsdatei.

### Worker
- **MqttWorker.cs**
  - `MqttWorker(IOptions<MqttSettings> settings, IDataLayer fkalaDataLayer)`
  - `Task StartAsync(CancellationToken cancellationToken)`: Startet den MQTT Worker und abonniert Topics.
  - `Task StopAsync(CancellationToken cancellationToken)`: Stoppt den MQTT Worker.
  - `void Dispose()`: Gibt Ressourcen frei.
  - `bool IsTopicBlacklisted(string topic)`: Prüft, ob ein Topic auf der Blacklist steht.
  - `bool IsMatch(string topic, string blacklistPattern)`: Prüft, ob einem Topic-Muster entspricht.

## FKala.Background
- **Class1.cs**
  - `Class1()`: Leere Klasse.

## FKala.Client.Console
- **Client.cs**
  - `Task Main(string[] args)`: Hauptmethode zum Parsen von Argumenten und Ausführen von Abfragen.

## FKala.Core
### DataLayer
- **DataLayer_Readable_Caching_V1.cs**
  - `DataLayer_Readable_Caching_V1(string storagePath, int readBuffer, int writeBuffer)`
  - `void Insert(string data)`: Fügt Daten ein.
  - `void InsertError(string error)`: Fügt einen Fehler ein.
  - `IEnumerable<DataPoint> Query(string measurement, DateTime startTime, DateTime endTime, KalaQlContext context)`: Führt eine Abfrage aus.
  - `IEnumerable<DataPoint> QueryStreaming(string measurement, DateTime startTime, DateTime endTime, KalaQlContext context)`: Führt eine Streaming-Abfrage aus.
  - `void DeleteMeasurementAndMatViewDefinition(string measurement)`: Löscht eine Messung und MatView-Definition.
  - `List<MatView> LoadMatViews()`: Lädt MatView-Definitionen.
  - `IEnumerable<DataPoint> LoadNewestDatapoint(string measurement)`: Lädt den neuesten Datenpunkt.
  - `IEnumerable<int> LoadAvailableYears(string measurement)`: Lädt verfügbare Jahre für eine Messung.
  - `void Flush(string filepath)`: Leert den Puffer für eine Datei.
  - `string GetInsertTargetFilepath(string measurement, string date)`: Ermittelt den Zielpfad für Einfügevorgänge.
  - `void Merge(string measurement, DateTime startTime, DateTime endTime)`: Führt Dateien zusammen.
  - `void Sort(string measurement, DateTime startTime, DateTime endTime)`: Sortiert Dateien.
  - `void Cleanup(string measurement)`: Bereinigt alte Dateien.
  - `void CreateMeasurement(string measurement)`: Erstellt eine neue Messung.
  - `bool IsBlacklisted(string measurement, bool createIfNotExists)`: Prüft, ob eine Messung auf der Blacklist steht.

- **StorageAccess.cs**
  - `StorageAccess(IDataLayer dataLayer, KalaQlContext context)`
  - `static StorageAccess ForReadMultiFile(string measurementPath, string measurementPathPart, DateTime startTime, DateTime endTime, KalaQl.KalaQlContext context)`
  - `static StorageAccess ForRead(string measurementPath, string measurementPathPart, DateTime startTime, DateTime endTime, KalaQl.KalaQlContext context, bool doSortRawFiles)`
  - `static StorageAccess ForSort(string measurementPath, string measurementPathPart, DateTime startTime, DateTime endTime, KalaQl.KalaQlContext context)`
  - `static StorageAccess ForMerging(string measurementPath, string measurementPathPart, KalaQl.KalaQlContext context)`
  - `static StorageAccess ForCleanup(string measurementPath, string measurementPathPart, KalaQl.KalaQlContext context)`
  - `SortedDictionary<DateOnly, ReaderTuple> GetFilePaths(string measurementPath, string measurementPathPart, DateTime startTime, DateTime endTime)`
  - `ILookup<DateOnly, ReaderTuple> QueryFilesForCleanup(string measurementPath, string measurementPathPart)`
  - `ILookup<DateOnly, ReaderTuple> QueryFilesForMergingAllFiles(string measurementPath, string measurementPathPart)`
  - `ILookup<DateOnly, ReaderTuple> QueryFilesForMergingMultipleFiles(string measurementPath, string measurementPathPart, DateTime startTime, DateTime endTime)`
  - `StorageAccess OpenStreamReaders()`
  - `IEnumerable<ReaderTuple> GetReaders()`
  - `List<int> GetYearFolders(string measurementDir)`
  - `IEnumerable<DataPoint> StreamMergeDataPoints()`
  - `static string SetSortMark(string filepath, bool sorted)`
  - `static bool IsSortMarkSet(string filepath)`
  - `IEnumerable<DataPoint> StreamMergeDataPoints_MaterializeSortIfNeeded(string measurement, bool dontInvalidateCache_ForUseWhileCacheRebuild)`
  - `IEnumerable<DataPoint> StreamDataPoints()`
  - `IEnumerable<DataPoint> InternalStreamDataPointsSort(ReaderTuple readerTuple, int fileyear, int filemonth, int fileday)`
  - `StorageAccess ActivateAutoSortRawFiles(IDataLayer dataLayer)`
  - `void WriteSortedFile(string filePath, IEnumerable<DataPoint> rs)`
  - `static void MarkFileAsSorted(string currentPath)`
  - `static bool IsSorted<T>(List<T> list) where T : IComparable<T>`
  - `static bool IsSortedAndWithoutDuplicates(List<DataPoint> list)`
  - `IEnumerable<DataPoint> InternalStreamDataPoints(ReaderTuple sr, int fileyear, int filemonth, int fileday, bool checkUnsorted)`
  - `void Dispose()`

- **UnexpectedlyUnsortedException.cs**
  - `UnexpectedlyUnsortedException()`
  - `UnexpectedlyUnsortedException(string? message)`
  - `UnexpectedlyUnsortedException(string? message, Exception? innerException)`

### DataLayer/Cache
- **Cache_5Minutely.cs**
  - `Cache_5Minutely(IDataLayer dataLayer)`
  - `IEnumerable<DataPoint> GetAggregateForCaching(string measurement, DateTime start, DateTime end, AggregateFunction aggrFunc)`
  - `string GetTimeFormat()`
  - `DataPoint ReadLine(int fileyear, string? line)`
  - `DateTime ShouldUpdateFromWhere(int cacheYear, DataPoint? newestInCache, DataPoint? newestInRaw)`

- **Cache_15Minutely.cs**
  - `Cache_15Minutely(IDataLayer dataLayer)`
  - `IEnumerable<DataPoint> GetAggregateForCaching(string measurement, DateTime start, DateTime end, AggregateFunction aggrFunc)`
  - `string GetTimeFormat()`
  - `DataPoint ReadLine(int fileyear, string? line)`
  - `DateTime ShouldUpdateFromWhere(int cacheYear, DataPoint? newestInCache, DataPoint? newestInRaw)`

- **Cache_Base.cs**
  - `Cache_Base(IDataLayer dataLayer)`
  - `IEnumerable<DataPoint> GetAggregateForCaching(string measurement, DateTime start, DateTime end, AggregateFunction aggrFunc)`
  - `string GetTimeFormat()`
  - `DataPoint ReadLine(int fileyear, string? line)`
  - `string CacheSubdir { get; }`
  - `IDataLayer DataLayer { get; }`
  - `DateTime ShouldUpdateFromWhere(int cacheYear, DataPoint? newestInCache, DataPoint? newestInRaw)`
  - `IEnumerable<DataPoint?> LoadNewestDatapoint(string newestFile)`
  - `void GenerateWholeYearCache(string measurement, int year, string cacheFilePath, AggregateFunction aggrFunc, bool forceRebuild)`
  - `void Invalidate(string measurement, DateOnly date)`
  - `void WriteCacheFile(string cacheFilePath, IEnumerable<DataPoint> rs, bool append)`
  - `IEnumerable<DataPoint> LoadCache(DateTime startTime, DateTime endTime, int year, string yearFilePath, int readBuffer)`
  - `void NavigateTo(int fileyear, StreamReader sr, DateTime startTime)`
  - `DataPoint? ReadNextLine(int fileyear, StreamReader sr)`
  - `string EnsureDirectory(string cacheDirectory)`
  - `void UpdateData(string measurement, DateTime rebuildFromDateTime, AggregateFunction aggrFunc, string newestCacheFile)`

- **Cache_Hourly.cs**
  - `Cache_Hourly(IDataLayer dataLayer)`
  - `IEnumerable<DataPoint> GetAggregateForCaching(string measurement, DateTime start, DateTime end, AggregateFunction aggrFunc)`
  - `string GetTimeFormat()`
  - `DataPoint ReadLine(int fileyear, string? line)`
  - `DateTime ShouldUpdateFromWhere(int cacheYear, DataPoint? newestInCache, DataPoint? newestInRaw)`

- **Cache_Minutely.cs**
  - `Cache_Minutely(IDataLayer dataLayer)`
  - `IEnumerable<DataPoint> GetAggregateForCaching(string measurement, DateTime start, DateTime end, AggregateFunction aggrFunc)`
  - `string GetTimeFormat()`
  - `DataPoint ReadLine(int fileyear, string? line)`
  - `DateTime ShouldUpdateFromWhere(int cacheYear, DataPoint? newestInCache, DataPoint? newestInRaw)`

- **CachingLayer.cs**
  - `CachingLayer(IDataLayer dataLayer, string storagePath)`
  - `IEnumerable<DataPoint> LoadDataFromCache(string measurement, DateTime startTime, DateTime endTime, CacheResolution cacheResolution, KalaQl.KalaQlContext context)`
  - `ICache GetCacheImplementation(CacheResolution cacheResolution)`
  - `void IncrementalUpdateCache(string measurement, CacheResolution cacheResolution, string cacheFilePath, KalaQlContext context)`
  - `DateTime ShouldUpdateFromWhere(string sanitizedMeasurement, Resolution resolution, AggregateFunction aggregateFunction, ICache cache, KalaQlContext context)`
  - `string GetNewestCacheFilepath(string sanitizedMeasurement, ICache cache, AggregateFunction aggregateFunction)`
  - `IEnumerable<int> FilterYearsForExistingRawData(string measurement, IEnumerable<int> years)`
  - `void Mark2Invalidate(string measurement, DateOnly year)`
  - `bool IsMarked2Invalidate(string measurement, DateOnly year)`
  - `void Invalidate(string measurement, DateOnly year)`

### DataLayer/Infrastructure
- **BufferedWriter.cs**
  - `BufferedWriter(string filePath, int writeBuffer, bool append = true)`
  - `void Append(string text)`
  - `void Append(ReadOnlySpan<char> text)`
  - `void AppendNewline()`
  - `void Flush()`
  - `void Dispose()`
  - `void Close()`

- **BufferedWriterService.cs**
  - `BufferedWriterService(int writeBuffer, IDataLayer dataLayer)`
  - `Task FlushBuffersPeriodically()`
  - `void ForceFlushWriters()`
  - `void ForceFlushWriter(string filepath)`
  - `void Dispose()`
  - `void CreateWriteDispose(string filePath, bool append, Action<IBufferedWriter> writeAction)`
  - `void DoWrite(string filePath, Action<IBufferedWriter> writeAction)`

- **DatFileParser.cs**
  - `static DataPoint ParseLine(int fileyear, int filemonth, int fileday, string? line, string filepath, int lineIdx)`

- **IEnumerableExtensions.cs**
  - `static IAsyncEnumerable<T> AsAsyncEnumerable<T>(this IEnumerable<T> input)`

- **LockManager.cs**
  - `IDisposable AcquireLock(string key)`
  - `bool IsLocked(string key)`
  - `void ReleaseLock(string key)`

- **Pools.cs**
  - `DefaultObjectPool<DataPoint> DataPoint`
  - `static Pools()`
  - `DataPoint Create()`
  - `bool Return(DataPoint obj)`

- **ReaderTuple.cs**
  - `DateOnly FileDate`
  - `required string FilePath`
  - `StreamReader? StreamReader`
  - `bool MarkedAsSorted`
  - `bool MeasurementFileDiffersToPath()`

### Helper
- **Benchmarker.cs**
  - `Benchmarker()`
  - `static BenchmarkResult Bench(string baseDir)`
  - `static Dictionary<long, TimeSpan> BenchmarkReads(string dir)`
  - `static Dictionary<long, TimeSpan> BenchmarkWrites(string dir)`

- **DatasetsCombiner.cs**
  - (Kommentierter Code)

- **DatasetsCombiner2.cs**
  - `CombineSynchronizedResults(List<ResultPromise> results)`

- **EnumerableHelpers.cs**
  - `SkipLast<T>(IEnumerable<T> source)`

- **Fast.cs**
  - `static bool IntParse(ReadOnlySpan<char> s, out int result)`

- **FileFromEndProcessor.cs**
  - `ProcessFileFromEnd(string filePath, Func<string, bool> checkFunction, string newData)`

- **FilesystemHelper.cs**
  - `ConvertToLocalPath(string relativePath)`
  - `DirectoryCopy(string sourceDirName, string destDirName, bool copySubDirs)`

- **JsonSerializer.cs**
  - `Serialize(object? serObject)`

- **LastLineReader.cs**
  - `ReadLastLine(string filePath)`
  - `RemoveByteOrderMark(string input)`

- **Msg.cs**
  - `Get(string key, string value)`

- **PathSanitizer.cs**
  - `SanitizePath(string path)`

- **StreamingAggregator.cs**
  - `StreamingAggregator(AggregateFunction aggregationFunction, Window window)`
  - `Reset(decimal? lastValuePreviousWindow)`
  - `InternalInit(Window window, decimal? lastValuePreviousWindow)`
  - `AddMeanValue(decimal? value)`
  - `AddWeightedMeanValue(DateTime time, decimal? toIntegrate)`
  - `decimal? GetAggregatedValue()`
  - `AddValue(DateTime time, decimal? toIntegrate)`

## FKala.Core.Interfaces
- (Leeres Projekt, nur Interface-Definitionen)

## FKala.Unittests
- (Viele Testdateien, die die Unit-Tests für die verschiedenen Komponenten enthalten)

## FKala.WebApi
- (Ähnlich wie FKala.Api, aber als WebApi implementiert)
