# F Kala
## _The Last Timeseries DB Ever_

## Overview

F Kala is a high-performance time-series database system with a custom query language (Kala TQL - Time Query Language). It supports data ingestion, aggregation, caching, complex expression-based transformations, materialized views, and JSON data extraction.

## Table of Contents

- [API Endpoints](#api-endpoints)
  - [/api/Insert](#apiinsert)
  - [/api/Query](#apiquery)
  - [/api/Mgmt](#apimgmt)
- [Kala TQL Commands](#kala-tql-commands)
  - [Var](#var)
  - [Load](#load)
  - [Loaj](#loaj)
  - [Aggr](#aggr)
  - [AlTz](#altz)
  - [Expr](#expr)
  - [Insert](#insert)
  - [Interpolate](#interpolate)
  - [MatView](#matview)
  - [Publ](#publ)
- [Management Commands](#management-commands)
- [Grafana Integration](#grafana-integration)

---

## API Endpoints

### /api/Insert

Inserts data into the time-series database.

- **Method:** PUT
- **Content-Type:** `text/plain`
- **Pattern:** `<measurement> <yyyy-MM-ddTHH:mm:ss.fffffff> <data>`

#### Examples

**Numeric Value:**
```bash
curl -X 'PUT' \
  'http://naxds2:20080/api/Insert' \
  -H 'accept: */*' \
  -H 'Content-Type: text/plain' \
  -d '/sensor/temperature/1 2024-08-30T15:16:22.1234567 50.3'
```

**String Value:**
```bash
curl -X 'PUT' \
  'http://naxds2:20080/api/Insert' \
  -H 'accept: */*' \
  -H 'Content-Type: text/plain' \
  -d 'jsts/Heizöltankung/stringValue1 2024-12-10T13:00:00.0000000 Tankung Stadler 3000l'
```

**Integer Value:**
```
jsts/Heizöltankung/int1 2024-12-10T13:00:00.0000000 3000
```

**Notes:**
- Measurement paths must not contain whitespace
- String values are supported (e.g., for event logging)
- Timestamps must be in ISO 8601 format with 7-digit fractional seconds

---

### /api/Query

Executes Kala TQL queries against the database.

- **Method:** POST
- **Content-Type:** `text/plain`

#### Example Query

```kala
Load rSOC: Sofar/measure/batteryInput1/SOC_Bat1 0001-01-01T00:00:00 9999-12-31T00:00:00 NoCache
Load rSOH: Sofar/measure/batteryInput1/SOH_Bat1 0001-01-01T00:00:00 9999-12-31T00:00:00 NoCache
Aggr SOC: rSOC Aligned_1Month Avg
Aggr SOH: rSOH Aligned_1Month Avg
Publ "SOC, SOH" Table
```

---

### /api/Mgmt

Management operations for the database.

- **Method:** POST
- **Content-Type:** `text/plain`

#### Example

```bash
Mgmt LoadMeasures
```

---

## Kala TQL Commands

### Var

Sets a variable for reuse in subsequent commands. Performs simple string replacement.

#### Pattern
```
Var <$VARIABLE> <Value>
```

#### Parameters
- `Var` - The verb
- `<$VARIABLE>` - Variable name (uppercase with `$` prefix recommended)
- `<Value>` - The value to substitute

#### Example

```kala
Var $FROM "2024-09-15T17:55:45"
Var $TO 2024-09-15T18:08:45
Var $CACHE Auto(20000)_Avg
Var $AGG Avg
Var $INTERVAL 20000

Load rPV1: Sofar/measure/PVInput1/0x586_Leistung_PV1[kW] $FROM $TO $CACHE
Load rPV2: Sofar/measure/PVInput1/0x589_Leistung_PV2[kW] $FROM $TO $CACHE
Load rNetz: Sofar/measure/OnGridOutput/0x488_ActivePower_PCC_Total[kW] $FROM $TO $CACHE
Load rAkku: Sofar/measure/batteryInput1/0x606_Power_Bat1[kW] $FROM $TO $CACHE
Load rVerbrauch: Sofar/measure/OnGridOutput/0x4AF_ActivePower_Load_Sys[kW] $FROM $TO $CACHE

Aggr aPV1: rPV1 $INTERVAL $AGG
Aggr aPV2: rPV2 $INTERVAL $AGG
Aggr aNetz: rNetz $INTERVAL $AGG
Aggr aAkku: rAkku $INTERVAL $AGG
Aggr aVerbrauch: rVerbrauch $INTERVAL $AGG
```

---

### Load

Loads data from storage into a temporary dataset.

#### Pattern
```
Load <Name>: <measurement> <from> <to> <CacheResolution>
```
or
```
Load <Name>: <measurement> NewestOnly
```

#### Parameters
- `Load` - The verb
- `<Name>` - Name for the temporary dataset being created
- `<measurement>` - Name of the measurement to load
- `<from>`, `<to>` - Time range (UTC only). Supports:
  - Absolute: `"2015-01-01T16:30:41"`
  - Relative: `"now/h"`, `"now-1d/d"`
- `<CacheResolution>` - Cache strategy. Options:
  - `NoCache` - Uses raw ingested data
  - `<Resolution>_<AggregateFunction>[_Rebuild]`
    - `<Resolution>`: `Hourly`, `Minutely`
    - `<AggregateFunction>`: `Avg`, `WAvg`, `First`, `Last`, `Min`, `Max`, `Count`, `Sum`
    - `_Rebuild` - Forces cache rebuild
    - `RefreshIncremental` - Triggers incremental cache update
  - `NewestOnly` - Loads only the newest datapoint

#### Examples

**Raw Data (No Cache):**
```kala
Load rSOC: Sofar/measure/batteryInput1/SOC_Bat1 0001-01-01T00:00:00 9999-12-31T00:00:00 NoCache
```
Loads all raw values for the measurement without using any cache.

**Hourly Cache:**
```kala
Load rSOH: Sofar/measure/batteryInput1/SOH_Bat1 2024-08-01T00:00:00 2024-09-01T00:00:00 Hourly_Avg
```
Uses hourly aggregated cache with average values for faster queries.

**Force Cache Rebuild:**
```kala
Load rSOH: Sofar/measure/batteryInput1/SOH_Bat1 2024-08-01T00:00:00 2024-09-01T00:00:00 Minutely_Max_Rebuild
```
Forces recreation of minutely cache with max aggregation.

---

### Loaj

Loads JSON member data from storage. Similar to `Load` but accesses nested JSON paths.

#### Pattern
```
Loaj <Name>: <measurement> <jsonPath> <from> <to> <CacheResolution>
```

#### Parameters
- Same as `Load`, plus:
- `<jsonPath>` - Path to a JSON member (e.g., `data/general/freeHeap`)
  - Array indices supported as leaves: `"nrg[0]"` extracts first element

#### Example

```kala
Loaj rFreeHeap: nxG1`$getStatus`$response data/general/freeHeap 2024-09-22T00:00:00 2024-09-30T00:00:00 NoCache
Loaj rUptime: nxG1`$getStatus`$response data/general/uptime64 2024-09-22T00:00:00 2024-09-30T00:00:00 NoCache
Aggr a: rFreeHeap Aligned_1Hour Min
Aggr b: rUptime Aligned_1Hour Min
Publ a,b Table
```

---

### Aggr

Aggregates data into time windows.

#### Pattern
```
Aggr <Name>: <Source> <Window> <Aggregate> [EmptyWindows]
```

#### Parameters
- `Aggr` - The verb
- `<Name>` - Name for the temporary dataset
- `<Source>` - Name of the source dataset to consume
- `<Window>` - Time window definition:
  - **Aligned Windows:**
    - `Aligned_5Minutes` - Starts at previous round 5-minute mark
    - `Aligned_15Minutes` - Starts at previous round 15-minute mark
    - `Aligned_1Hour` - Starts at hour boundary
    - `Aligned_1Day` - Starts at day boundary (00:00)
    - `Aligned_1Week` - Starts at week boundary (Monday)
    - `Aligned_1Month` - Starts at month boundary
    - `Aligned_1Year` - Starts at year boundary
    - `Aligned_1YearStartAtHalf` - Starts at mid-year boundary
  - **Unaligned Windows:**
    - `Unaligned_1Month` - Starts at source start time, 1 month wide
    - `Unaligned_1Year` - Starts at source start time, 1 year wide
  - **Special:**
    - `Scalarize` or `Infinite` - Single window spanning all data
    - `DD:HH:mm:ss` - Custom timespan (e.g., `10:05:20:02`)
    - `<milliseconds>` - Numeric milliseconds (e.g., `12000` = 12 seconds)
- `<Aggregate>` - Aggregation function:
  - `Avg` - Mean (sum / count)
  - `WAvg` - Time-weighted mean (considers value duration)
  - `First` - First value in window
  - `Last` - Last value in window
  - `Min` - Minimum value
  - `Max` - Maximum value
  - `Count` - Count of values
  - `Sum` - Sum of all values
- `[EmptyWindows]` - Optional. Generates null-value windows when no data exists.

#### Aggregate Functions Explained

**Avg (Arithmetic Mean):**
```
Sum of values / Count of values
```

**WAvg (Time-Weighted Average):**
```
Sum of (value × duration) / Total duration
```
Example: Window of 10 seconds, Value 5 at 0s, Value 20 at 9s
- `Avg` = (5 + 20) / 2 = 12.5
- `WAvg` = (5×9 + 20×1) / 10 = 6.5

#### Examples

```kala
Aggr SOC: rSOC Aligned_1Month Avg
```
Monthly aggregation starting at month boundaries with average values.

```kala
Aggr SOC: rSOC Aligned_1Day Max
```
Daily aggregation with maximum values.

```kala
Aggr SOC: rSOC 6000 Last
```
6-second windows with last-value aggregation.

```kala
Aggr SOC: rSOC 10:05:20:02 Min
```
Custom window (10 days, 5 hours, 20 minutes, 2 seconds) with minimum aggregation.

---

### AlTz

Aligns aggregation windows to a specific timezone. Must be placed **before** `Aggr` commands.

#### Pattern
```
AlTz "<TimezoneId>"
```

#### Parameters
- `AlTz` - The verb (Align Timezone)
- `<TimezoneId>` - IANA timezone ID (see [NodaTime TimeZones](https://nodatime.org/TimeZones))

#### Examples

```kala
AlTz "Europe/Berlin"
Aggr Daily: rData Aligned_1Day Avg
```
Aligns daily windows to Central European Time (CET/CEST).

```kala
AlTz "America/New_York"
Aggr Daily: rData Aligned_1Day Avg
```
Aligns daily windows to Eastern Time (EST/EDT).

---

### Expr

Evaluates expressions to calculate new datasets from existing ones. Supports complex calculations, conditional logic, and stateful operations.

#### Pattern
```
Expr <Name>: "<Expression>"
```

#### Parameters
- `Expr` - The verb
- `<Name>` - Name for the resulting dataset
- `<Expression>` - Expression evaluated per datapoint/window

#### Expression Capabilities

**Basic Operations:**
- Access dataset values: `DatasetName.Value`
- Arithmetic: `+`, `-`, `*`, `/`
- Comparisons: `>`, `<`, `>=`, `<=`, `==`, `!=`
- Ternary operator: `condition ? trueValue : falseValue`
- Null handling: `Value != null ? Value : defaultValue`

**Special Variables:**

| Variable | Type | Description |
|----------|------|-------------|
| `previousInput` | `Dictionary<string, DataPoint>` | Previous input datapoints for all referenced datasets |
| `previousOutput` | `DataPoint` | Previous output datapoint from this expression |
| `skip` | `Skip` | Special marker to skip/drop the current datapoint |

**DataPoint Properties:**
- `.Value` - The numeric value
- `.StartTime` - Start timestamp
- `.EndTime` - End timestamp

#### Examples

**Simple Calculation:**
```kala
Expr StromVerbrauch_KWH: "(StromVerbrauch_WH.Value) / 1000"
```
Divides watt-hours by 1000 to get kilowatt-hours.

**Combined Datasets:**
```kala
Expr PVSumInWatt: "(PV1_Windowed.Value + PV2_Windowed.Value) * 1000"
```
Adds two datasets and converts kW to W.

**Conditional Logic:**
```kala
Expr MinVorlauf: "Aussen.StartTime.Hour >= 18 ? 17 : 37"
```
Time-dependent logic returning different values based on hour.

**Value Clamping:**
```kala
Expr Clamped: "PV1.Value > 5 ? 5 : PV1.Value"
```
Caps values at 5.

**Null Coalescing:**
```kala
Expr Filled: "PV1.Value != null ? PV1.Value : 5"
```
Replaces null values with 5.

**Using previousInput:**
```kala
Expr Delta: "previousInput['rPV1A'].Value != 0 ? rPV1A.Value - previousInput['rPV1A'].Value : 0"
```
Calculates the difference from the previous value.

**Using previousOutput:**
```kala
Expr RunningSum: "previousOutput.Value + rValue.Value"
```
Accumulates a running sum across all datapoints.

**Skipping Datapoints:**
```kala
Var $FROM 2024-08-01T00:00:00Z
Var $TO 2024-08-02T00:00:00Z
Load rPV1A: Sofar/measure/PVInput1/0x585_Current_PV1[A] $FROM $TO NoCache
Load rPV2A: Sofar/measure/PVInput1/0x588_Current_PV2[A] $FROM $TO NoCache

# Skip datapoints where value >= 4 (returns skip object)
Expr Filtered1: "rPV1A.Value < 4 ? (object)rPV1A.Value : skip"

# Replace values >= 4 with null (creates datapoint with null value)
Expr Filtered2: "rPV2A.Value < 4 ? rPV2A.Value : null"

Publ "Filtered1,Filtered2" Table
```

**Important Notes:**
- `skip` - Completely drops the datapoint from the result (requires casting to `(object)`)
- `null` - Creates a datapoint with null value (visible in results as null)
- When using multiple datasets in one expression, their windows **must align**
- `previousInput` is a dictionary keyed by dataset name
- `previousOutput` tracks the last computed output value

---

### Insert

Inserts processed data back into the database (commented in examples, may require activation).

#### Pattern
```
Insert <Target>: <Source> <measurement>
```

#### Example
```kala
#Insert Ins1: Filtered1 Sofar/measure/PVInput1/0x585_Current_PV1[A]CLEANED
```

---

### Interpolate

Interpolates missing values in datasets.

#### Pattern
```
Interpolate <Name>: <Source> <Mode>
```

#### Modes
- `Linear` - Linear interpolation between known values
- `Step` - Step interpolation (holds previous value)

---

### Publ

Publishes datasets to the query result.

#### Pattern
```
Publ "<DatasetName1,DataSetName2,...>" <OutputMode>
```

#### Parameters
- `Publ` - The verb
- `<DatasetName1,DataSetName2,...>` - Comma-separated dataset names to include
- `<OutputMode>` - Output format:
  - `Table` / `CombinedResultset` - Merged result with time and value columns
  - `Default` / `MultipleResultsets` - Separate result arrays per dataset

#### Output Formats

**Table Mode:**
```json
[
  {
    "time": "2023-07-01T00:00:00",
    "SOC": 80.08839382590738861082916245,
    "SOH": 99.99708315346113010302302025
  },
  {
    "time": "2023-08-01T00:00:00",
    "SOC": 73.389861473599678779361573888,
    "SOH": 100
  }
]
```

**MultipleResultsets Mode:**
```json
[
  {
    "name": "SOC",
    "resultset": [
      {
        "time": "2023-07-01T00:00:00",
        "value": 80.12364594485650020189863605
      },
      {
        "time": "2023-08-01T00:00:00",
        "value": 73.323210912260126694109685724
      }
    ]
  },
  {
    "name": "SOH",
    "resultset": [
      {
        "time": "2023-07-01T00:00:00",
        "value": 99.99676581656875396694491103
      },
      {
        "time": "2023-08-01T00:00:00",
        "value": 100
      }
    ]
  }
]
```

---

## Management Commands

### Mgmt LoadMeasures

Lists all available measurements in the database.

#### Example
```bash
Mgmt LoadMeasures
```

#### Example Output
```json
[
  "Sofar$measure$batteryInput1$0x604_Spannung_Bat1[V]",
  "Sofar$measure$batteryInput1$0x605_Current_Bat1[A]",
  "Sofar$measure$batteryInput1$0x606_Power_Bat1[kW]",
  "Sofar$measure$batteryInput1$0x609_SOH_Bat1[%]",
  "evcc$updated",
  "evcc$loadpoints$1$sessionEnergy"
]
```

### MgmtSortRawFiles

Marks or sorts-and-marks all raw files as sorted. Files newer than 2 days are left untouched.

#### Example
```bash
Mgmt SortRawFiles
```

---

## Grafana Integration

### Installation

1. Extract `kala-kala-datasource` to Grafana's plugins folder
2. Use Grafana 11 or later (version 10 is not compatible)
3. Edit `grafana.ini` and add:
   ```ini
   allow_loading_unsigned_plugins = kala-kala-datasource
   ```
4. Restart Grafana

### Usage

Configure the Kala datasource in Grafana to query time-series data directly.

---

## Build

```bash
C:\git\FKALA> docker build . --progress=plain --no-cache
```

---

## TODOs

- [ ] Find invalid files (invalid filename, invalid data format) - *Partially done*
- [x] Mark files as sorted - *Done*
- [ ] Merge measures (maybe with a hard cut at some point in time)
- [x] Support text values (maybe even long?) - *Done*
- [ ] Function for shifting windows
- [ ] Worker for cache-refresh schedule
