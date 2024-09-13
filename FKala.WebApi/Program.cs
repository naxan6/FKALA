using FKala.TestConsole;
using FKala.TestConsole.KalaQl;


var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
// Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}


/// <summary>
/// Example:
///     Load Stromverbrauch: watt/haus/0 2024-06-01T00:00:00 2024-09-01T00:00:00 
///     Aggr StromVerbrauch_Month: Stromverbrauch Aligned_1Month Avg
///     Publ StromVerbrauch_Month Default
/// </summary>
/// <value></value>
app.MapPost("/qi", async (IConfiguration config, HttpContext context) =>
{
    var reader = new StreamReader(context.Request.Body);
    var query = await reader.ReadToEndAsync();
 
    /*
     Load Last: watt/haus/0 2024-06-01T00:00:00 2024-09-01T00:00:00
     Aggr StromVerbrauch_WH: Last Aligned_1Hour Avg  
     Expr StromVerbrauch_KWH: "(StromVerbrauch_WH.Value) / 1000"
     Aggr StromVerbrauch_Month: StromVerbrauch_KWH Aligned_1Month Sum  
     Publ StromVerbrauch_Month Default
     */
    var dataStorage = config["DataStorage"] ??  Path.Combine(".", "fkala_data");
    using var dl = new DataLayer_Readable_Caching_V1(dataStorage);

    var q = KalaQuery.Start().FromQuery(query);            
    var result = q.Execute(dl);
    // Console.WriteLine(KalaJson.Serialize(result.ResultSets));// JSON serialize
    var resultSets = result.ResultSets;
    return resultSets;

})
.WithName("qi")
.Accepts<string>("text/plain")
.WithOpenApi(op => 
{
    var example = @"
Load Last: watt/haus/0 2024-06-01T00:00:00 2024-09-01T00:00:00
Aggr StromVerbrauch_WH: Last Aligned_1Hour Avg EmptyWindows
Expr StromVerbrauch_KWH: ""(StromVerbrauch_WH.Value) / 1000""
Aggr StromVerbrauch_Month: StromVerbrauch_KWH Aligned_1Month Sum EmptyWindows
Expr StromVerbrauch_Month_0: ""StromVerbrauch_Month.Value ?? 1""
Publ StromVerbrauch_Month_0 Default
";
    // Configure Swagger to show the request body as text input
    op.RequestBody = new Microsoft.OpenApi.Models.OpenApiRequestBody
    {
        Description = @$"QUERY Example <br/>
<pre>
    {example}
</pre>
        ",
        Content = new Dictionary<string, Microsoft.OpenApi.Models.OpenApiMediaType>
        {
            ["text/plain"] = new Microsoft.OpenApi.Models.OpenApiMediaType
            {
                Schema = new Microsoft.OpenApi.Models.OpenApiSchema
                {
                    Type = "string",
                    Example = new Microsoft.OpenApi.Any.OpenApiString(example, true, true)
                }
            }
        }
    };
    return op;
});

app.Run();