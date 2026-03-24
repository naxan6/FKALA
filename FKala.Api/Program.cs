using FKala.Api.Controller;
using FKala.Api.InputFormatter;
using FKala.Api.Settings;
using FKala.Api.Worker;
using FKala.Api.Jobs;
using FKala.Core;
using FKala.Core.Interfaces;
using Microsoft.AspNetCore.Mvc;
using Quartz;
using Quartz.AspNetCore;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
// Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
builder.Services.AddControllers(
    options => options.InputFormatters.Add(new PlainTextFormatter())
    ).AddJsonOptions((options) =>
    {
        //options.JsonSerializerOptions.DefaultBufferSize = 4096;
    })
    .ConfigureApiBehaviorOptions(options =>
    {
        var builtInFactory = options.InvalidModelStateResponseFactory;

        options.InvalidModelStateResponseFactory = context =>
        {
            var logger = context.HttpContext.RequestServices
                                .GetRequiredService<ILogger<Program>>();
            return builtInFactory(context);
        };
    });

//.AddNewtonsoftJson(options =>
//        {
//            options.SerializerSettings.DateFormatString = "yyyy'-'MM'-'dd'T'HH':'mm':'ssZ";
//        }
//);
builder.Services.AddOptions<MqttSettings>().Configure((MqttSettings options, IConfiguration config) => options.Configure(config));

builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

builder.Services.AddQuartz(q =>
{
    // Sicherstellen, dass die DI Job Factory verwendet wird
    // The DI job factory is used by default. No extra call is needed.
    // q.UseMicrosoftDependencyInjectionJobFactory();

    // MatViewRefreshJob registrieren
    var jobKey = new JobKey(nameof(MatViewRefreshJob));
    q.AddJob<MatViewRefreshJob>(opts => opts.WithIdentity(jobKey));

    // Trigger für den MatViewRefreshJob hinzufügen (z.B. täglich um 2 Uhr nachts)
    // Cron-Syntax: Sekunde Minute Stunde TagMonat Monat Wochentag [Jahr]
    // "0 0 2 * * ?" bedeutet: Um 02:00:00 Uhr jeden Tag
    q.AddTrigger(opts => opts
        .ForJob(jobKey)
        .WithIdentity($"{nameof(MatViewRefreshJob)}-trigger")
        .WithCronSchedule("0 0 2 * * ?") // Täglich um 2:00 Uhr morgens
        //.WithCronSchedule("0 */1 * * * ?")
        // Alternativ für Tests alle 5 Minuten: .WithCronSchedule("0 */5 * * * ?") 
        );
});

// ASP.NET Core hosting
builder.Services.AddQuartzServer(options =>
{
    // when shutting down we want jobs to complete gracefully
    options.WaitForJobsToComplete = true;
});

builder.Logging.AddConsole();
builder.Services.AddHealthChecks();

var storagePath = builder.Configuration["DataStorage"] ?? (OperatingSystem.IsWindows() ? "C:\\fkala" : "/kaladata");

int readBuffer = 16384;
int writeBuffer = 32768;
if (int.TryParse(builder.Configuration["ReadBuffer"], out var parsedReadBuffer))
    readBuffer = parsedReadBuffer;
if (int.TryParse(builder.Configuration["WriteBuffer"], out var parsedWriteBuffer))
    writeBuffer = parsedWriteBuffer;

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


var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}
app.UseFileServer();

if (app.Environment.IsDevelopment())
{
    app.UseDeveloperExceptionPage();
}
app.UseRouting();
app.UseCors("AllowAll");

#pragma warning disable ASP0014 // Suggest using top level route registrations
app.UseEndpoints(endpoints =>
{
    endpoints.MapControllers();
    endpoints.MapHealthChecks("/health");
});
#pragma warning restore ASP0014 // Suggest using top level route registrations
//app.UseHttpsRedirection();

IHostApplicationLifetime lifetime = app.Lifetime;
lifetime.ApplicationStopping.Register(() =>
{
    dl.Shutdown();
});

app.Run();
