# View Materialization Process

## Overview

The process of materializing views in this system involves several components working together to create and maintain pre-computed results from data queries. The main components are:

1. `Op_MatView` - The KQL operation that handles the materialization logic
2. `MatViewRefreshJob` - The background job that refreshes materialized views regularly
3. `DataLayer_Readable_Caching_V1` - The data layer that stores and manages the materialized views

## Key Files and Components

### 1. FKala.Core/KalaQl/Op_MatView.cs

This is the main class responsible for view materialization:

- **Class**: `Op_MatView` (inherits from `Op_Base`, implements `IKalaQlOperation`)
- **Key Methods**:
  - `Execute()` - Entry point that sets up the materialization process
  - `InternalExecute()` - Determines if materialization is needed and calls `MaterializeFull()`
  - `MaterializeFull()` - The main method that performs the actual materialization:
    - Creates a new KalaQuery with required operations
    - Executes the query to get data
    - Inserts results into the DataLayer using `context.DataLayer.Insert()`
    - Writes view definition to file using `context.DataLayer.WriteMatViewFile()`
  - `CanExecute()` - Checks if input data is available
  - `GetInputNames()` - Returns the name of the input dataset

### 2. FKala.Api/Jobs/MatViewRefreshJob.cs

This class handles the periodic refresh of materialized views:

- **Class**: `MatViewRefreshJob` (implements `IJob`)
- **Key Methods**:
  - `Execute()` - Main method that:
    - Loads view definitions using `_dataLayer.LoadMatViews()`
    - Deletes old views with `DeleteMeasurementAndMatViewDefinition()`
    - Recreates views by executing the stored query
    - Handles retries and logging

### 3. FKala.Core/DataLayer/DataLayer_Readable_Caching_V1.cs

This class handles the data layer operations:

- **Methods**:
  - `WriteMatViewFile()` - Writes view definition to a file
  - `LoadMatViews()` - Loads view definitions from files
  - `DeleteMeasurementAndMatViewDefinition()` - Deletes view and its definition
  - `MatView` class - Represents a materialized view with properties like Query, ViewdefFilePath

## Materialization Flow

1. **Query Execution**: When a query containing `MatView` operation is executed:
   - The `Execute()` method in `Op_MatView` is called
   - It checks if the input data source exists
   - If not already materialized, it calls `InternalExecute()`

2. **Materialization Process**:
   - `InternalExecute()` checks if materialization is available using `MaterializationIsAvailable()`
   - If not available, it calls `MaterializeFull()`
   - `MaterializeFull()`:
     - Gets all intermediate data sources
     - Creates a new KalaQuery with the required operations
     - Executes the query to get results
     - Inserts each DataPoint into the DataLayer using `context.DataLayer.Insert()`
     - Writes the view definition to file

3. **View Refresh**:
   - The `MatViewRefreshJob` periodically:
     - Loads all materialized views with `LoadMatViews()`
     - Deletes old views
     - Recreates them by executing their stored query
     - Handles errors and retries

## Key Concepts

- **Materialization**: The process of pre-computing results from a query to improve performance
- **View Definition**: Stored in viewdef.txt files, containing the timestamp and query
- **DataLayer**: Responsible for storing and retrieving materialized views
- **KQL Operations**: The query language operations that define how data is processed

## Files Involved

1. `FKala.Core/KalaQl/Op_MatView.cs` - Main logic for materialization
2. `FKala.Api/Jobs/MatViewRefreshJob.cs` - Background job for periodic refreshes  
3. `FKala.Core/DataLayer/DataLayer_Readable_Caching_V1.cs` - Data layer operations