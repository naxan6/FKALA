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
    ).AddJsonOptions((options) =>
    {
        //options.JsonSerializerOptions.DefaultBufferSize = 4096;
    });

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

var dl = new DataLayer_Readable_Caching_V1(storagePath, readBuffer, writeBuffer);
builder.Services.AddSingleton<IDataLayer>(dl);

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


builder.Services.AddControllers()
    .ConfigureApiBehaviorOptions(options =>
    {
        // To preserve the default behaviour, capture the original delegate to call later.
        var builtInFactory = options.InvalidModelStateResponseFactory;

        options.InvalidModelStateResponseFactory = context =>
        {
            var logger = context.HttpContext.RequestServices
                                .GetRequiredService<ILogger<Program>>();

            // Perform logging here.
            // ...

            // Invoke the default behaviour, which produces a ValidationProblemDetails
            // response.
            // To produce a custom response, return a different implementation of 
            // IActionResult instead.
            return builtInFactory(context);
        };
    });


var app = builder.Build();

// Configure the HTTP request pipeline.
app.UseSwagger();
app.UseSwaggerUI();
app.UseFileServer();

app.UseDeveloperExceptionPage();
app.UseRouting();
app.UseCors("AllowAll");

#pragma warning disable ASP0014 // Suggest using top level route registrations
app.UseEndpoints(endpoints =>
{
    endpoints.MapControllers();
});
#pragma warning restore ASP0014 // Suggest using top level route registrations
//app.UseHttpsRedirection();

IHostApplicationLifetime lifetime = app.Lifetime;
lifetime.ApplicationStopping.Register(() =>
{
    dl.Shutdown();
});

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



