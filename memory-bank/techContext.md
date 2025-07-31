# Technology Context

## Technologies Used
- **Primary Language**: C# (.NET Core)
- **Database**: Flat-file storage (no traditional database)
- **Web Framework**: ASP.NET Core for API layer  
- **Scheduling**: Quartz.NET for background jobs
- **Messaging**: MQTT support for real-time data ingestion
- **Build System**: MSBuild with .sln solution files
- **Testing Framework**: Microsoft.VisualStudio.TestTools.UnitTesting and FluentAssertions

## Development Setup
- Visual Studio / VS Code with C# extensions
- Unit testing framework (xUnit or similar)
- Git for version control (GitHub repository)
- Docker containerization support

## Technical Constraints
- Must operate efficiently with limited memory resources
- File I/O handling requires careful consideration of buffering strategies  
- Concurrent access patterns need to be handled properly to maintain data integrity
- Data processing must scale appropriately for large time-series datasets

## Dependencies
- .NET Core runtime environment
- Quartz.NET for job scheduling
- MQTT client libraries (for optional MQTT integration)
- JSON serialization capabilities for configuration and data handling
- Testing frameworks for comprehensive test coverage

## Tool Usage Patterns
- Build system uses MSBuild/Visual Studio solution files (.sln)
- Testing is primarily via unit tests in FKala.Unittests project  
- Development workflow involves iterative testing through console clients
- API interaction tested using HTTP client tools or web browser

## Test Infrastructure
- **Test Framework**: Microsoft.VisualStudio.TestTools.UnitTesting and FluentAssertions for comprehensive verification
- **Data Generation**: DataFaker utility creates realistic test data with configurable time ranges 
- **Test Coverage**: Extensive unit tests covering all major components including KalaQl operations, parsing, caching, and background jobs
</content>
