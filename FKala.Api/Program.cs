using FKala.Api.Controller;
using FKala.Api.InputFormatter;
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
    //); ;
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

var storagePath = builder.Configuration["DataStorage"] ?? "\\\\naxds2\\docker\\fkala";
builder.Services.AddSingleton<IDataLayer>(new DataLayer_Readable_Caching_V1(storagePath));

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseDeveloperExceptionPage();
app.UseRouting();


#pragma warning disable ASP0014 // Suggest using top level route registrations
app.UseEndpoints(endpoints =>
{
    endpoints.MapControllers();
});
#pragma warning restore ASP0014 // Suggest using top level route registrations
app.UseHttpsRedirection();

// Run GC regularly
Task task = Task.Run(async () => {
    while (true)
    {
        await Task.Delay(TimeSpan.FromSeconds(10));
        GCSettings.LargeObjectHeapCompactionMode = GCLargeObjectHeapCompactionMode.CompactOnce;
        GC.Collect();
    }
});

app.Run();



