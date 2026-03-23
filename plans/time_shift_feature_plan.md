# Time-Shift Feature Plan

## Overview

This document describes the design for a time-shifting functionality in the FKALA time-series data processing system. The feature enables simulation scenarios by shifting timestamps of data points while preserving all other properties.

## Goals

- **Versatile**: Support various simulation and analysis scenarios
- **Simple**: Minimal syntax, easy to use and understand
- **Composable**: Works seamlessly with existing KalaQL operations
- **Efficient**: Stream-based processing without unnecessary memory allocation

## Architecture

### Data Flow

```
┌─────────────┐     ┌──────────────────┐     ┌─────────────────┐
│   Load Op   │────▶│  ShiftTime Op    │────▶│  Downstream Ops │
│             │     │                  │     │  (Expr, Agg,    │
│ Reads data  │     │ Shifts timestamps│     │   Publish)      │
│ from disk   │     │ by offset        │     │                 │
└─────────────┘     └──────────────────┘     └─────────────────┘
```

### Integration with Existing Operations

The `Op_ShiftTime` follows the same pattern as other KalaQL operations:

1. Inherits from [`Op_Base`](FKala.Core/KalaQl/Op_Base.cs:13)
2. Implements `CanExecute()` - checks if input data source is available
3. Implements `Execute()` - creates a `ResultPromise` with shifted data
4. Registered in [`KalaQlParserRegistry`](FKala.Core/KalaQl/KalaQuery.cs:28)

## Syntax Design

### Recommended Syntax (Option 1)

```
Shift <output_name>: <input_name> <offset>
```

**Examples**:
```kalaql
Load data: sensor 2024-01-01 2024-01-02 full
Shift shifted: data +2h          # Forward 2 hours
Shift shifted: data -30m         # Backward 30 minutes  
Shift shifted: data +1d          # Forward 1 day
Shift shifted: data +1h30m       # Forward 1 hour 30 minutes
```

### Duration Format

| Unit | Suffix | Example |
|------|--------|---------|
| Minutes | `m` | `+30m`, `-15m` |
| Hours | `h` | `+2h`, `-1h` |
| Days | `d` | `+7d`, `-1d` |
| Combined | - | `+1d12h`, `+2h30m` |

**Note**: Sign prefix is required (`+` or `-`)

## Implementation Details

### New Files

1. **`FKala.Core/KalaQl/Op_ShiftTime.cs`** - Main operation class
2. **`FKala.Core/KalaQl/QueryParser/ShiftTimeParser.cs`** - Parser for the syntax

### Op_ShiftTime Class Structure

```csharp
public class Op_ShiftTime : Op_Base, IKalaQlOperation
{
    public override string Name { get; }      // Output name
    public string InputName { get; }          // Input name
    public TimeSpan Offset { get; }           // Time shift amount
    public string OffsetString { get; }       // Original string for serialization
    
    // Constructor
    public Op_ShiftTime(string line, string name, string inputName, TimeSpan offset, string offsetString)
    
    // Override methods
    public override bool CanExecute(KalaQlContext context)
    public override void Execute(KalaQlContext context)
    public override List<string> GetInputNames()
    public override IKalaQlOperation Clone()
    public override string ToLine()
}
```

### Execute Method Logic

```csharp
public override void Execute(KalaQlContext context)
{
    var inputSource = context.IntermediateDatasources
        .First(ds => ds.Name == InputName);
    
    context.IntermediateDatasources.Add(
        new ResultPromise()
        {
            Name = this.Name,
            Creator = this,
            Query_StartTime = inputSource.Query_StartTime + Offset,
            Query_EndTime = inputSource.Query_EndTime + Offset,
            ResultsetFactory = () =>
            {
                foreach (var dp in inputSource.ResultsetFactory())
                {
                    var shifted = dp.Clone();
                    shifted.StartTime = shifted.StartTime + Offset;
                    shifted.EndTime = shifted.EndTime + Offset;
                    yield return shifted;
                }
            }
        }
    );
    this.hasExecuted = true;
}
```

### Duration Parser

```csharp
public static class DurationParser
{
    public static TimeSpan Parse(string durationString)
    {
        // Pattern: [+|-][days][hours][minutes]
        // Examples: +2h, -30m, +1d12h, +2h30m
        
        var pattern = @"(?<sign>[+-])?(?:(?<d>\d+)d)?(?:(?<h>\d+)h)?(?:(?<m>\d+)m)?";
        var match = Regex.Match(durationString, pattern);
        
        if (!match.Success) throw new FormatException(...);
        
        var sign = match.Groups["sign"].Success && match.Groups["sign"].Value == "-" ? -1 : 1;
        var days = int.Parse(match.Groups["d"].Success ? match.Groups["d"].Value : "0");
        var hours = int.Parse(match.Groups["h"].Success ? match.Groups["h"].Value : "0");
        var minutes = int.Parse(match.Groups["m"].Success ? match.Groups["m"].Value : "0");
        
        return new TimeSpan(days * sign, hours * sign, minutes * sign);
    }
}
```

## Use Cases

### 1. Basic Simulation
```kalaql
Load historical: temperature 2024-01-01 2024-01-02 full
Shift future_scenario: historical +24h
Publish result: [future_scenario] MultipleResultsets
```

### 2. Comparative Analysis
```kalaql
Load base: power_consumption 2024-01-01 2024-01-07 full
Shift shifted_forward: base +1d
Shift shifted_backward: base -1d
Expr diff_forward: "shifted_forward.Value - base.Value"
Expr diff_backward: "shifted_backward.Value - base.Value"
Publish comparison: [diff_forward, diff_backward] MultipleResultsets
```

### 3. What-If Scenario
```kalaql
Load sensor_data: readings 2024-01-01 2024-01-02 full
Shift delayed: sensor_data +2h
Expr impact: "delayed.Value - sensor_data.Value"
Publish impact_analysis: [impact] MultipleResultsets
```

### 4. Forecasting with Historical Patterns
```kalaql
Load last_week: energy 2024-01-01 2024-01-07 full
Shift projection: last_week +7d
Expr forecast: "projection.Value * 1.05"  # 5% growth assumption
Publish forecast_result: [forecast] MultipleResultsets
```

### 5. Backtesting
```kalaql
Load current_strategy: performance 2024-01-01 2024-01-07 full
Shift historical_test: current_strategy -30d
Publish backtest: [historical_test] MultipleResultsets
```

## Testing Strategy

### Unit Tests
- Duration parser with various formats
- Positive and negative offsets
- Edge cases (midnight crossing, month boundaries)
- Clone verification (ensure original DataPoint unchanged)

### Integration Tests
- Chain with Expr operations
- Chain with Aggregate operations
- Multiple shifts in sequence
- Verify streaming behavior (no full materialization)

### Example Test Cases
```csharp
[Fact]
public void ShiftTime_Forward2Hours_ShiftsCorrectly()
{
    var original = new DataPoint { StartTime = new DateTime(2024, 1, 1, 10, 0, 0), Value = 42 };
    var shifted = ShiftOperation(original, TimeSpan.FromHours(2));
    Assert.Equal(new DateTime(2024, 1, 1, 12, 0, 0), shifted.StartTime);
    Assert.Equal(42, shifted.Value);
}

[Fact]
public void ShiftTime_Negative30Minutes_ShiftsCorrectly()
{
    var original = new DataPoint { StartTime = new DateTime(2024, 1, 1, 10, 0, 0), Value = 42 };
    var shifted = ShiftOperation(original, TimeSpan.FromMinutes(-30));
    Assert.Equal(new DateTime(2024, 1, 1, 9, 30, 0), shifted.StartTime);
    Assert.Equal(42, shifted.Value);
}
```

## Implementation Steps

1. **Create `Op_ShiftTime.cs`** - Core operation class
2. **Create `ShiftTimeParser.cs`** - Query syntax parser
3. **Create `DurationParser.cs`** - Duration string parsing utility
4. **Register parser** - Add to `KalaQlParserRegistry` in [`KalaQuery.cs`](FKala.Core/KalaQl/KalaQuery.cs:28)
5. **Write unit tests** - Test parsing and execution
6. **Write integration tests** - Test with other operations
7. **Update documentation** - Add to KalaQL reference

## Considerations

### Performance
- Stream-based processing (no full materialization)
- Clone DataPoints to avoid mutating originals
- Preserve object pooling pattern

### Edge Cases
- Daylight saving time transitions (DateTime handles this automatically)
- Month/year boundaries (DateTime handles overflow)
- Very large offsets (no practical limit)

### Future Enhancements
- Calendar-aware shifts (e.g., "same day next month")
- Business day awareness
- Timezone-aware shifts
- Variable offset based on another measurement

## Conclusion

The time-shift feature provides a simple yet powerful mechanism for simulation and analysis scenarios. By following the existing KalaQL operation pattern, it integrates seamlessly with the current architecture while enabling new use cases for users.
