using FKala.Api.Controller;
using FKala.Api.InputFormatter;
using FKala.TestConsole;
using FKala.TestConsole.Interfaces;
using Microsoft.AspNetCore.Mvc;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
// Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
builder.Services.AddControllers(
    options => options.InputFormatters.Add(new PlainTextFormatter())
    );
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

var dataStorage = builder.Configuration["DataStorage"] ?? "C:\\FKALA\\DataStore";
string dataPath = Path.Combine(dataStorage, "data");
string cachePath = Path.Combine(dataStorage, "cache");
builder.Services.AddSingleton<IDataLayer>(new DataLayer_Readable_Caching_V1(dataPath, cachePath));

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseRouting();


#pragma warning disable ASP0014 // Suggest using top level route registrations
app.UseEndpoints(endpoints =>
{
    endpoints.MapControllers();
});
#pragma warning restore ASP0014 // Suggest using top level route registrations
app.UseHttpsRedirection();


app.Run();
