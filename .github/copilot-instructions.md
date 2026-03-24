# Copilot Instructions for FKala

FKala is a high-performance, file-based time-series database written in C#/.NET 9.0. It features a custom query language (KalaQL), multi-tier caching, MQTT ingestion, and Grafana integration.

## Build, Test, and Run

```bash
# Build
dotnet build FKALA.sln

# Run all tests (MSTest + FluentAssertions + Moq)
dotnet test

# Run a single test class
dotnet test --filter "ClassName=FKala.Unittests.BugTests"

# Run a single test by name
dotnet test --filter "Name=BugTest_EmptySensorDir_CausesSartAtDateTimeMinValue"

# Run tests matching a pattern
dotnet test --filter "FullyQualifiedName~AI_ShiftTime_Tests"

# Docker
docker build . --progress=plain
# Container exposes port 8080, mounts /kaladata volume
```

No linter is configured.

## Architecture

### Project Structure

- **FKala.Core** — All business logic: data layer, KalaQL query engine, caching, aggregation, migrations, models. This is where nearly all domain code lives.
- **FKala.Api** — Primary ASP.NET Core host. Controllers (`QueryController`, `InsertController`, `StreamQueryController`), MQTT worker, Quartz-scheduled materialized view refresh, Swagger, and a static web UI in `wwwroot/`.
- **FKala.Unittests** — 345 tests across 61 files. Uses `DataFaker` helper to create temp directories with fake time-series data for isolated tests.
- **FKala.Client.Cmd** (`FKala.Client.Console/`) — CLI HTTP client for querying the API.
- **FKala.WebApi** — Minimal API prototype (largely superseded by FKala.Api).
- **FKala.Background**, **FKala.Core.Interfaces**, **FKala.Migrate** — Empty/retired placeholder projects.

### Data Storage (File-Based, No Database)

Data is stored as flat `.dat` files on the filesystem:

```
<DataStorage>/data/<measurement>/<year>/<month>/<measurement>#<yyyy-MM-dd>.dat
```

Each line in a `.dat` file: `HH:mm:ss.fffffff <value>` (date comes from the directory/filename hierarchy). The `#` in the filename means the file is sorted; `_` means unsorted. Unsorted files are merge-sorted and deduplicated on read.

Cache files live at `<DataStorage>/cache/<resolution>/<measurement>_<year>_<aggregateFunc>.dat` with flag-file-based invalidation.

### KalaQL Query Engine

FKala uses a custom DSL called KalaQL. Queries are line-based (newline or `|` separated) and processed as a pipeline of operations:

| Verb | Operation | Purpose |
|------|-----------|---------|
| `Load` | `Op_Load` | Load measurement data with time range and cache resolution |
| `Loaj` | `Op_JsonQuery` | Load and extract JSON field from text values |
| `Aggr` | `Op_Aggregate` | Aggregate with window function (e.g., `Aligned_1Hour Avg`) |
| `Expr` | `Op_Expresso` | Evaluate C# expressions on data points (via DynamicExpresso) |
| `Publ` | `Op_Publish` | Output results (CombinedResultset or MultipleResultsets) |
| `AlTz` | `Op_AlignTimezone` | Set timezone for window alignment (via NodaTime) |
| `InPo` | `Op_Interpolate` | Fill null values (forwards, backwards, constant) |
| `Shift` | `Op_ShiftTime` | Shift timestamps by a duration |
| `Insert` | `Op_Insert` | Write computed results back as a measurement |
| `MatView` | `Op_MatView` | Create/use materialized views |
| `Var` | `Op_Var` | Define variables for substitution |
| `Mgmt` | `Op_Mgmt` | Management operations (list, sort, copy, delete, etc.) |

Each verb has a corresponding parser (`IKalaQlParser`) and operation class (extends `Op_Base` which implements `IKalaQlOperation`). Operations execute in dependency order via `CanExecute()`/`HasExecuted()` checks.

### Streaming Pipeline

The query engine uses lazy evaluation throughout. `ResultPromise` wraps a `Func<IEnumerable<DataPoint>>` factory — data streams from disk through operations without full materialization. `DataPoint` objects are pooled (100K pool) to minimize GC pressure.

### API Endpoints

- `PUT /api/Insert` — Insert data point (text/plain: `<measurement> <timestamp> <value>`)
- `POST /api/Query` — Execute KalaQL, return JSON result
- `GET /api/Query?input=...` — Same, via query string (`\n` as line separator)
- `POST /api/StreamQuery` — Execute KalaQL, stream results as `IAsyncEnumerable`

No authentication — designed for trusted networks. CORS is fully open.

### Background Services

- **MqttWorker** — Hosted service subscribing to MQTT topics, inserting received messages into the data layer. Configured via `Mqtt` section in appsettings.
- **MatViewRefreshJob** — Quartz.NET job running daily at 02:00, rebuilding all materialized views.

## Key Conventions

### Naming Patterns

- Operation classes: `Op_<Verb>` (e.g., `Op_Load`, `Op_Aggregate`, `Op_Expresso`)
- Parsers: `<Verb>Parser` (e.g., `LoadParser`, `AggregateParser`)
- Param classes: `<Verb>Params` (e.g., `LoadParams`, `AggregateParams`)
- Test classes prefixed with `AI_` are AI-generated tests

### Adding a New KalaQL Operation

1. Create a parser implementing `IKalaQlParser` (`CanParse(verb)` + `Parse(line, fields)`)
2. Register it in `KalaQlParserRegistry`
3. Create an operation class extending `Op_Base` implementing `Execute()`, `GetInputNames()`, `Clone()`, `ToLine()`
4. The operation receives inputs via `KalaQlContext.IntermediateDatasources` (a dictionary of named `ResultPromise` objects)

### Test Patterns

- Tests use `DataFaker` to create isolated temp directories with synthetic time-series data and a fresh `DataLayer_Readable_Caching_V1`
- FluentAssertions for assertions: `result.Errors.Should().BeEmpty()`
- Test data files (`.dat`) are checked in under `TestData/` subdirectories
- `FKala.Core` has `InternalsVisibleTo("FKala.Unittests")`

### Error Handling

Errors accumulate in `KalaResult.Errors` (strings) and `KalaResult.Exceptions` rather than throwing — queries return partial results with error details. Custom exceptions: `KalaErrorException`, `UnexpectedlyUnsortedException`.

### Configuration

Key settings in `appsettings.json`:
- `DataStorage` — filesystem path for data (default `C:\fkala`, Docker: `/kaladata`)
- `ReadBuffer` / `WriteBuffer` — file I/O buffer sizes
- `Mqtt:Url`, `Mqtt:Port`, `Mqtt:Topics[]`, `Mqtt:Blacklist[]`

### Key Dependencies

- **DynamicExpresso** — Runtime C# expression evaluation in `Expr` operations
- **NodaTime** — Timezone-aware date/time for `AlTz` window alignment
- **MQTTnet** — MQTT client for data ingestion
- **Quartz.NET** — Job scheduling for materialized view refresh

### Documentation

- `FKala.Api/README.MD` and `FKala.Api/README_V2.md` — API and KalaQL user docs
- `docs/KalaQL_Reference.md` — Formal KalaQL language reference (in German)
