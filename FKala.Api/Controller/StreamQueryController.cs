using FKala.Core;
using FKala.Core.Interfaces;
using FKala.Core.KalaQl;
using Microsoft.AspNetCore.Mvc;
using Swashbuckle.AspNetCore.Annotations;
using System.Runtime;
using Microsoft.Extensions.Logging;
using System.Diagnostics;
using Microsoft.Extensions.Logging.Abstractions;
using FKala.Core.DataLayer.Infrastructure;
using Microsoft.AspNetCore.Http.HttpResults;
using System.Diagnostics.Metrics;
using System;
using NodaTime.Calendars;

namespace FKala.Api.Controller
{

    [ApiController]
    [Route("api/[controller]")]
    public class StreamQueryController : ControllerBase
    {
        public IDataLayer DataLayer { get; }
        public ILogger<QueryController> Logger { get; }

        public StreamQueryController(IDataLayer dataLayer, ILogger<QueryController> logger)
        {
            this.DataLayer = dataLayer;
            this.Logger = logger;
        }

        // GET api/string
        [HttpGet]
        public IAsyncEnumerable<Dictionary<string, object?>> QueryGet([FromQuery] string input)
        {
            if (string.IsNullOrEmpty(input))
            {
                throw new Exception("Input string is required.");
            }

            // Verarbeite den String hier nach Bedarf
            var inputmultiline = ProcessString(input);

            return DoQuery(inputmultiline);
        }

        // GET api/string
        [HttpPost]
        [Consumes("text/plain")]
        //[SwaggerRequestBody("Weather forecast data", Required = true)]
        public async IAsyncEnumerable<Dictionary<string, object?>>  QueryPost([FromBody] string input)
        {
            if (string.IsNullOrEmpty(input))
            {
                throw new Exception("Input string is required.");
            }

            // Verarbeite den String hier nach Bedarf
            var inputmultiline = ProcessString(input);

            DateTime previous = DateTime.Now;
            await foreach (var retRowDict in DoQuery(inputmultiline))
            {
                // hack for streaming all 250ms too see progress with sparse data and long runtimes (additionally to "if buffer is full" as in standard asp.net core)
                if (previous.AddMilliseconds(250) < DateTime.Now)
                {
                    previous = DateTime.Now;
                    await Task.Delay(TimeSpan.FromMilliseconds(1));
                }
                yield return retRowDict;
            }

        }

        private IAsyncEnumerable<Dictionary<string, object?>> DoQuery(string query)
        {
            try
            {
                var q = KalaQuery.Start(true)
                    .FromQuery(query);

                var result = q.Execute(this.DataLayer);

                
                if (result?.Errors.Count() != 0)
                {
                    int i = 0;
                    result!.Errors.ForEach(e =>
                    {
                        i++;
                        ModelState.AddModelError($"Error {i}", e);
                        Logger.LogError($"Error {i}: " + e);
                    });
                    throw new Exception(string.Join(", " , result.Errors));
                }
                //else if (result?.MeasureList != null)
                //{
                //    throw new Exception("MeasureList streaming is not supported. Use Query endpoint.");
                //}
                else if (result?.ResultSets != null)
                {
                    throw new Exception("Resultset streaming is not supported. Use Table.");
                }
                else if (result?.StreamResult != null)
                {
                    return result.StreamResult;
                }
                else if (result?.ResultTable != null)
                {
                    return result.ResultTable.AsAsyncEnumerable();
                }
                return new List<Dictionary<string, object?>>().AsAsyncEnumerable();
            }
            catch (Exception ex)
            {
                throw;
                Logger.LogError(ex, "Exception");
                List<string> exres = new List<string>();
                exres.Add("ex: " + ex.Message);
                exres.Add("stack: " + $"{ex.StackTrace}");
                Exception? ie = ex.InnerException;
                while (ie != null)
                {
                    exres.Add("iex " + ex.Message);
                    exres.Add("iexstack: " + $"{ex.StackTrace}");
                    ie = ex.InnerException;
                }
                var retRow = new Dictionary<string, object?>
                        {
                            { "query", $"{query}" },
                            { "status", $"exception" },
                            { "msg", $"{String.Join(", ", exres)}" }

                        };
                var ret = new List<Dictionary<string, object?>>
                {
                    retRow
                };
                return ret.AsAsyncEnumerable();
            }
        }

        private string ProcessString(string input)
        {
            // Beispielhafte Verarbeitung des Strings
            // Hier könntest du den String weiterverarbeiten
            // Zum Beispiel Zeilen zählen, Zeilen umbrechen, etc.
            //return input.Replace("\\n", "\n");
            return input;
        }
    }
}
