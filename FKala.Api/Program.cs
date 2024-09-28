using FKala.Api.Controller;
using FKala.Api.InputFormatter;
using FKala.Api.Settings;
using FKala.Api.Worker;
using FKala.Core;
using FKala.Core.Interfaces;
using Microsoft.AspNetCore.Mvc;
using System.Runtime;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
// Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
builder.Services.AddControllers(
    options => options.InputFormatters.Add(new PlainTextFormatter())
    );
//.AddNewtonsoftJson(options =>
//        {
//            options.SerializerSettings.DateFormatString = "yyyy'-'MM'-'dd'T'HH':'mm':'ssZ";
//        }
//);
builder.Services.AddOptions<MqttSettings>().Configure((MqttSettings options, IConfiguration config) => options.Configure(config));

builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

builder.Logging.AddConsole();

var storagePath = builder.Configuration["DataStorage"] ?? "C:\\fkala";

var readBuffer = !string.IsNullOrEmpty(builder.Configuration["ReadBuffer"]) ? int.Parse(builder.Configuration["ReadBuffer"]) : 16384;
var writeBuffer = !string.IsNullOrEmpty(builder.Configuration["WriteBuffer"]) ? int.Parse(builder.Configuration["WriteBuffer"]) : 32768;


builder.Services.AddSingleton<IDataLayer>(new DataLayer_Readable_Caching_V1(storagePath, readBuffer, writeBuffer));

if (builder.Configuration.GetSection(MqttSettings.ConfigurationSection).Exists())
{
    builder.Services.AddHostedService<MqttWorker>();
}

builder.Services.AddCors(o => o.AddPolicy("AllowAll", builder =>
{
    builder.AllowAnyOrigin()
           .AllowAnyMethod()
           .AllowAnyHeader();
}));


var app = builder.Build();

// Configure the HTTP request pipeline.
app.UseSwagger();
app.UseSwaggerUI();

app.UseDeveloperExceptionPage();
app.UseRouting();
app.UseCors("AllowAll");

#pragma warning disable ASP0014 // Suggest using top level route registrations
app.UseEndpoints(endpoints =>
{
    endpoints.MapControllers();
});
#pragma warning restore ASP0014 // Suggest using top level route registrations
app.UseHttpsRedirection();

// Run GC regularly
Task task = Task.Run(async () =>
{
    while (true)
    {
        await Task.Delay(TimeSpan.FromSeconds(10));
        GCSettings.LargeObjectHeapCompactionMode = GCLargeObjectHeapCompactionMode.CompactOnce;
        GC.Collect();
    }
});

app.Run();



