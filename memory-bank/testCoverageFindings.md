# Test Coverage Findings - FKALA Project

## Summary
Based on code coverage analysis using ReportGenerator, several classes in the FKALA project require additional unit tests to improve system reliability and test coverage.

## Classes with 0% Coverage
- `FKala.Core.DataLayer.Infrastructure.IEnumerableExtensions`
- `FKala.Core.Helper.Benchmarker` 
- `FKala.Core.Helper.Msg`
- `FKala.Core.Logic.Fast`
- `FKala.Core.Logic.FileFromEndProcessor`

## Classes with Very Low Coverage (<30%)
- `FKala.Core.KalaQl.QueryParser.AggregateParams`
- `FKala.Core.KalaQl.QueryParser.AggregateParser` 
- `FKala.Core.KalaQl.QueryParser.ExpressoParams`
- `FKala.Core.KalaQl.QueryParser.InsertParams`
- `FKala.Core.KalaQl.QueryParser.InterpolateParams`
- `FKala.Core.KalaQl.QueryParser.JsonQueryParams`
- `FKala.Core.KalaQl.QueryParser.LoadParams`
- `FKala.Core.KalaQl.QueryParser.MatViewParams`
- `FKala.Core.KalaQl.QueryParser.MgmtParams`
- `FKala.Core.KalaQl.QueryParser.PublishParams`
- `FKala.Core.KalaQl.QueryParser.VarParams`

## Classes with Medium-Low Coverage (<50%)
- `FKala.Core.KalaQl.Op_AlignTimezone` 
- `FKala.Core.KalaQl.QueryParser.AlignTimezoneParser`
- `FKala.Core.KalaQl.QueryParser.InterpolateParser`
- `FKala.Core.KalaQl.QueryParser.JsonQueryParser`

## Migration Classes with Low Coverage
- `FKala.Core.Migration.InfluxLineProtocolImporter` (6.1%)
- `FKala.Migrate.MariaDb.EventRow` (0%)
- `FKala.Migrate.MariaDb.ReaderExtension` (0%)

## Priority Recommendations

### High Priority:
1. Core KalaQl operations that are critical to query execution
2. Parser components for KalaQl syntax validation  
3. Migration classes handling data conversion between formats

### Medium Priority: 
1. Helper methods and utility functions
2. Data processing pipelines in the core engine

## Action Items:
- [ ] Create unit tests for IEnumerableExtensions class (0% coverage)
- [ ] Implement comprehensive test suite for all KalaQl parser components
- [ ] Add integration tests for edge cases in time-series operations  
- [ ] Develop regression tests for migration functionality
