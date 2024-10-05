
View e.g. in https://dillinger.io/

# F Kala
## _The Last Timeseries DB Ever_

## TODOS
- find invalid files (invalid filename, invalid dataformat)
  - *Partially* done (in a console app for manual debug use) 
- mark files as sorted
  - * Done
- merge measures (maybe with a hard cut at some point in time)
- support textvalues (maybe even long?)
  - Done
- function for shifting windows
- worker for cache-refresh schedule


# Kala TQL - Kala Time Query Language

## Build
C:\git\FKALA> docker build . --progress=plain --no-cache

## /api/Insert
- PUT
- Body ```text/plain```
  - Example: ``` /sensor/temperature/1 2024-08-30T15:16:22.1234567 50.3 ```
  - Pattern: ```<measurement> <yyyy-MM-ddTHH:mm:ss.fffffff> <data (decimal)>```
    - measurement must be without whitespace
  
## /api/Query
- POST
- Body ```text/plain```
- Example 
  ```sh
      Load rSOC: Sofar/measure/batteryInput1/SOC_Bat1 0001-01-01T00:00:00 9999-12-31T00:00:00 NoCache
      Load rSOH: Sofar/measure/batteryInput1/SOH_Bat1 0001-01-01T00:00:00 9999-12-31T00:00:00 NoCache
      Aggr SOC: rSOC Aligned_1Month Avg
      Aggr SOH: rSOH Aligned_1Month Avg
      Publ "SOC, SOH" Table
  ```

### Var
Sets a variable you can use in later commands. Does a simple string-raplece to save you from havin to change multiple lines on recurring changes.

#### Pattern ```Var <$VARIABLE> <Value>```
- ```Var``` - Verb
- ```<$VARIABLE>``` - the name of the variable, uppercase and $-prefixed is recommended
- ```<Value>``` - the value that should be placed everywhere instead of the $VARIABLE

#### Example:
- reuse variables:
```
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
Aggr aAkkuladung: rAkku $INTERVAL $AGG
```

### Load
Loads data from storage
#### Pattern ```Load <Name>: <measurement> <from> <to> <CacheResolution>```
#### or ```Load <Name>: <measurement> NewestOnly```
 - ```Load``` Verb
 - ```<Name>``` Name for the (temporary!) dataset being created 
 - ```<measurement>``` Name of the measurement to load
 - ```<from>```, ```<to>``` timerange to load
 - ```<CacheResolution>``` which Databasis or Cache to use. Can be one of:
   - ```NoCache``` uses raw ingested data
   - ```<resolution>_<AggregateFunction>[_Rebuild]```
   - ```<Resolution>``` can Be ```Hourly``` and ```Minutely```
   - ```<AggregateFunction>``` can  be any available streamable aggregate:
       - ```Avg```, ```WAvg```, ```First```, ```Last```, ```Min```, ```Max```,```Count```, ```Sum```
   - ```_Rebuild``` can be appended to force the rebuilding of the cache
   - ```RefreshIncremental``` can be appended to trigger incremental update of the cache
   - Alternative ```NewestOnly``` - only the newest one Datapoint is loaded.
#### Examples
  - **No-Cache-Query / Raw Data**
    ```Load rSOC: Sofar/measure/batteryInput1/SOC_Bat1 0001-01-01T00:00:00 9999-12-31T00:00:00 NoCache```
    it loads data into the variable ```rSOC```
    it loads values from measurement ```Sofar/measure/batteryInput1/SOC_Bat1```
    it loads *all* value, because timerange is maximal
    it loads the raw values that were inge3sted (no cache or aggregation is used)
  - **Hourly Cache**
    ```Load rSOH: Sofar/measure/batteryInput1/SOH_Bat1 2024-08-01T00:00:00 2024-09-01T00:00:00 Hourly_Avg```  
    it loads data into the variable ```rSOH```
    it loads values from measurement ```Sofar/measure/batteryInput1/SOH_Bat1```
    it loads the values from the given interval (the August in the summer of 2024)
    it uses a cache with hourly aggregated data. "Avg" (mean) was used to condense the data to hourly values. 
    If the cache doesn't exist yet, it gets populated. if it does exists, it is used - and the query will execute much faster.
  - **Minutely Cache (Force Cache Rebuild)**  
    ```Load rSOH: Sofar/measure/batteryInput1/SOH_Bat1 2024-08-01T00:00:00 2024-09-01T00:00:00 Minutely_Max_Rebuild```  
    it loads data into the variable ```rSOH```
    it loads values from measurement ```Sofar/measure/batteryInput1/SOH_Bat1```
    it loads the values from the given interval (the August in the summer of 2024)
    it uses a cache with minutely aggregated data. "Max" was used to condense the data to minutely values. 
    if the cache doesn't exist yet, it gets populated. if it does exists, it is NOT used - but recreated (because of "_Rebuild").

### Loaj
  Nearly like Load but allows access to a member of a json value.
  See example, only a path to a json-member is added on top of Load-parameters ("data/general/freeHeap").
  Array-indices are only supported as leaves, e.g. "nrg[0]" will get you the first entry, so '{ "nrg" : [11,22,33] }' will give you 11.
  ```
  Loaj rFreeHeap: nxG1`$getStatus`$response data/general/freeHeap 2024-09-22T00:00:00 2024-09-30T00:00:00 NoCache
  Loaj rUptime: nxG1`$getStatus`$response data/general/uptime64 2024-09-22T00:00:00 2024-09-30T00:00:00 NoCache
  Aggr a: rFreeHeap  Aligned_1Hour Min
  Aggr b: rUptime  Aligned_1Hour Min
  Publ a,b Table"
  ```
### Aggr
Aggregates Data.
#### Pattern ```Aggr <Name>: <Source> <Window> <Aggregate> [EmptyWindows]```
- ```<Name>``` Name for the (temporary!) dataset being created
- ```<Source>``` Name of the dataset being `consumed (has to be the name of another command)
- ```<Window>``` how time windows are created.
  Can  be any one of the following:
  - ```Aligned_5Minutes```   - first window starts at the previous round 5 Minutes before or at the starttime of the Source and is 15 minutes wide
  - ```Aligned_15Minutes```- first window starts at the previous round 15 Minutes before or at the starttime of the Source and is 15 minutes wide
  - ```Aligned_1Hour```- first window starts at the start of the hour of the starttime of the Source and is 60 minutes wide
  - ```Aligned_1Day```- first window starts at the start of the day  of the starttime of the Source and is 1 Day wide
  - ```Aligned_1Week```- first window starts at the start of the week (Monday!)  of the starttime of the Source and is 7 days wide
  - ```Aligned_1Month```- first window starts at the start of the month of the starttime of the Source and is 1 month wide (different depending on the month)
  - ```Aligned_1Year```- first window starts at the start of the year of the starttime of the Source and is 1 year wide (different in case of a leap year)
  - ```Aligned_1YearStartAtHalf```- first window starts at the half of the year of the starttime of the Source and is 1 year wide (different in case of a leap year)
  - ```Unaligned_1Month```- first window starts at the starttime of the Source and is 1 month wide (different depending on the month)
  - ```Unaligned_1Year```- first window starts at the starttime of the Source and is 1 year wide (different in case of a leap year)
  - ```Scalarize``` or ```Infinite``` -  first window starts at the starttime of the Source and is infinite -so only one single number will result.
  - Timespan in the pattern ```DD:HH:mm:ss``` - first Window starts at the starttime of the Source.  Windows areas wide as given.
  - Timespan in ```<milliseconds>```, pure numeric - eg```12000``` means 12 Seconds. First window starts at the starttime of the Source. Windows areas wide as given.
- ```<Aggregate>``` how data is aggregated into each time window. 
  Can  be any available streamable aggregate:
  ```Avg```, ```WAvg```, ```First```, ```Last```, ```Min```, ```Max```,```Count```, ```Sum```
- [EmptyWindows] - optional, if set/added, it generates empty windows (if no datapoint is available for a window in the Source). "empty window" means: the windows value is null.

#### Aggregate-Function
- ```Avg``` - takes the sum of all values within a window and divides them by the count of the values (mean)
- ```WAvg``` - takes the sum of all values times ticks within a window and divides them by the total ticks (time-weighted mean)
  Expected to be more stable over multiple aggregations. Also takes the previous' windows last value into account until the first value in the current window (not for the very first window!).
  Example: 
    - Say you have a window of length 10 seconds. 
    - At time 0s, Value 5 is valid
    - At time 9s, Value 20 is valid
    - Avg would give you ```(5+20)/2 = 12.5```
    - WAvg would give you ```((5*9)+(1*20))/10 = 6.5``` - so WAvg takes into account that 5 was valid for much longer, which may be worthy in some cases (and bad in others).
- ```First``` - takes the first value in a window
- ```Last``` - takes the last value in a window
- ```Min``` - takes the minimum value in the window
- ```Max``` - takes the maximum value in the window
- ```Count``` - counts the values in a window
- ```Sum``` - sums all the values in a window


#### Examples
- ```Aggr SOC: rSOC Aligned_1Month Avg``` - the first windows starts at 00:00 of the first day of the month of the first datapoint in the source. Windowsize is 1 Month, so each window/month gets assigned the average value found within the month.
- ```Aggr SOC: rSOC Aligned_1Day Max``` - the first windows starts at 00:00 of the first datapoint in the source. Windowsize is 1 Day, so each window/day gets assigned the maximum value found within the day.
- ```Aggr SOC: rSOC 6000 Last``` - windowsize is 6000ms (or 6 seconds), each window gets assigned the last value found within the window.
- ```Aggr SOC: rSOC 10:05:20:02 Min``` - windowsize is 10 days, 05 hours, 20 minutes and 02 seconds, each window gets assigned the minimal value found.

### AlTz
Alignes your aggregations to e.g. your local start of day (relevant only for ```Aligned_1Day```, ```Aligned_1Week```, ```Aligned_1Month```, ```Aligned_1Year```)
Add it in *before* your Aggr-Commands.

#### Pattern ```AlTz "<TimezoneId>"```
- ```AlTz``` - the verb (for "Aligning Timezone")
- ```<TimezoneId>``` - TZID / Zone ID; see https://nodatime.org/TimeZones

#### Examples
- ```AlTz "Europe/Berlin"``` - aligns to CET

### Expr
Can be used to calculate with and combine datasets. 

#### Pattern ```Expr <Name>: "<Expression>"```
- ```Expr``` - the verb
- ```<Name>``` - name for the (temporary!) dataset being created
- ```<Expression>``` - the expression being evaluated to calculate the new value of a window
- e.g. ```SOC.Value``` you can access the dataset "SOC" - ```SOC.Value * 100``` multiplies the value of each window with 100
- e.g. ```(PV1.Value + PV2.Value) * 1000``` adds together the values of the corresponding windows of two different datasets and multiplies the result with 1000 to get the resulting windows value. - !!!Warning!!! the datasets windows MUST align if you use multiple datasets in one expression.
- e.g. ```PV1.Value > 5 ? 5 : PV1.Value``` - replace all values greater than 5 with 5
- e.g. ```PV1.Value != null ? PV1.Value : 5``` - replace null values with 5 (may be used to fill EmptyWindows (see above) with a default)
##### Examples
- ```Expr StromVerbrauch_KWH: "(StromVerbrauch_WH.Value) / 1000"``` - divide the value of StromVerbrauch_WH by 1000 an create a new dataset named StromVerbrauch_KWH by the result.
- ```Expr PVSumInWatt: "(PV1_Windowed.Value + PV2_Windowed.Value) * 1000"``` - add PV1 and PV2 together and multiply to get W instead of kW, name the result PVSumInWatt.
- ```# skipping requires casting the value to (object) and skips the datapoint, null on the other hand results in a datapoint with value null.
  Var $FROM 2024-08-01T00:00:00Z
  Var $TO 2024-08-02T00:00:00Z
  Load rPV1A: Sofar/measure/PVInput1/0x585_Current_PV1[A] $FROM $TO NoCache
  Load rPV2A: Sofar/measure/PVInput1/0x588_Current_PV2[A] $FROM $TO NoCache
  Expr Filtered1: "rPV1A.Value < 4 ? (object)rPV1A.Value : skip"
  Expr Filtered2: "rPV2A.Value < 4 ? rPV2A.Value : null"
  #Insert Ins1: Filtered1 Sofar/measure/PVInput1/0x585_Current_PV1[A]CLEANED
  #Insert Ins2: Filtered2 Sofar/measure/PVInput1/0x588_Current_PV2[A]CLEANED
  Publ "Filtered1,Filtered2" Table
  ```

### Publ
Can be used to "publish" generated temporary datasets to the result.  This is what you get.

#### Pattern ```Publ "<DatasetName1,DataSetName2,...>" <OutputMode>```
- ```Publ``` - the verb
- ```<DatasetName1,DataSetName2,...>``` - the names of the datasets you want to get in the result
- ```<OutputMode>``` - the form you want to get your datasets in. One of:
  - ```Table```/```CombinedResultset``` - you get a list of elements containing ```time``` and a variable for each resultset. E.g
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
          },
      ...
       ```
  - ```Default```/```MultipleResultsets``` - you get an array a of arrays b with an array b for each dataset. array b contains "DataPoint"-elements, containing members "time", and "value"
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
            },
       ...
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
             },
       ...
       ```

## /api/Mgmt

- POST
- Body ```text/plain```
- Example 
  ```sh
      Mgmt LoadMeasures

### Mgmt LoadMeasures
Gets you a list of all available Mesaures in the F KALA database

### MgmtSortRawFiles
Marks or Sorts-and-marks all raw files as sorted (only file newer than 2 days will be left untouched).

#### Example-Output
```json
[
  "Sofar$measure$batteryInput1$0x604_Spannung_Bat1[V]",
  "Sofar$measure$batteryInput1$0x605_Current_Bat1[A]",
  "Sofar$measure$batteryInput1$0x606_Power_Bat1[kW]",
  "Sofar$measure$batteryInput1$0x609_SOH_Bat1[%]",
  "Sofar$measure$batteryInput1$0x60A_ChargeCycle_Bat1[Zyklus]",
  "Sofar$measure$batteryInput1$0x607_Temperature_Env_Bat1[Celsius]",
  "Sofar$measure$batteryInput1$0x608_SOC_Bat1[%]",
  "evcc$updated",
  "evcc$loadpoints$1$sessionEnergy",
   ...
]
```


### Grafana-Datasource-Plugin
* extract "kala-kala-datasource" in 7z to plugins folder auf grafana
* use at least grafana 11 (10 does not work!)
* edit "grafana.ini", add:  allow_loading_unsigned_plugins = kala-kala-datasource
* restart Grafana