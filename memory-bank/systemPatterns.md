# System Patterns

## System Architecture
FKALA follows a layered architecture pattern with clear separation of concerns:

1. **Data Layer** (`FKala.Core/DataLayer/`) - Handles raw data storage, retrieval and caching 
2. **Query Language Engine** (`FKala.Core/KalaQl/`) - Processes KalaQl queries and executes operations
3. **API Layer** (`FKala.Api/`) - Exposes HTTP endpoints for external interaction  
4. **Background Processing** (`FKala.Api/Jobs/`) - Handles automated maintenance tasks

## Key Technical Decisions
- Flat-file storage instead of traditional databases to optimize I/O performance
- Caching at multiple resolutions (minutely, hourly, etc.) 
- Materialized views as pre-computed datasets for query optimization
- Buffering mechanisms for efficient data writing

## Design Patterns in Use
1. **Command Pattern** - Operations like `Load`, `Aggregate` implement specific commands
2. **Strategy Pattern** - Different cache resolutions and aggregation functions  
3. **Template Method Pattern** - Base operation classes define common structure with hooks for specialization
4. **Observer Pattern** - Background jobs monitor and react to system state changes

## Component Relationships
- DataLayer is the foundation, used by both KalaQl engine and background jobs
- KalaQl engine orchestrates operations using various command objects  
- Background jobs coordinate through DataLayer interfaces
- API layer delegates complex operations to KalaQl engine and DataLayer

## Critical Implementation Paths
1. `IDataLayer` interface defines all data interactions 
2. `KalaQuery.Execute()` method drives the entire query processing pipeline
3. Materialized view creation and refresh processes
4. Caching layer integration with data access patterns

## Key Abstractions
- Clear separation between data representation and business logic
- Well-defined interfaces for extensibility (IDataLayer, IKalaQlOperation)
- Consistent error handling throughout the system

## Testing Patterns and Practices
- **Test Coverage**: Comprehensive unit testing of all components using Microsoft.VisualStudio.TestTools.UnitTesting framework
- **Integration Testing**: End-to-end validation ensuring component interaction works correctly
- **Regression Testing**: Continuous test suite maintaining existing functionality  
- **Edge Case Testing**: Specific tests for error conditions, boundary values, and unusual inputs
- **Data Faking**: Use of DataFaker utility for creating realistic test data with configurable distributions
</content>
