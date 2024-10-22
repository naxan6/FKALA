﻿using FKala.Core;
using FKala.Core.Interfaces;
using FKala.Core.KalaQl;
using Microsoft.AspNetCore.Mvc;
using Swashbuckle.AspNetCore.Annotations;
using System.Runtime;
using Microsoft.Extensions.Logging;

namespace FKala.Api.Controller
{

    [ApiController]
    [Route("api/[controller]")]
    public class QueryController : ControllerBase
    {
        public IDataLayer DataLayer { get; }
        public ILogger<QueryController> Logger { get; }

        public QueryController(IDataLayer dataLayer, ILogger<QueryController> logger)
        {
            this.DataLayer = dataLayer;
            this.Logger = logger;
        }

        // GET api/string
        [HttpGet]
        public IActionResult QueryGet([FromQuery] string input)
        {
            if (string.IsNullOrEmpty(input))
            {
                return BadRequest("Input string is required.");
            }

            // Verarbeite den String hier nach Bedarf
            var inputmultiline = ProcessString(input);

            return DoQuery(inputmultiline);
        }

        // GET api/string
        [HttpPost]
        [Consumes("text/plain")]
        //[SwaggerRequestBody("Weather forecast data", Required = true)]
        public IActionResult QueryPost([FromBody] string input)
        {
            if (string.IsNullOrEmpty(input))
            {
                return BadRequest("Input string is required.");
            }

            // Verarbeite den String hier nach Bedarf
            var inputmultiline = ProcessString(input);

            return DoQuery(inputmultiline);

        }

        private IActionResult DoQuery(string query)
        {
            try
            {
                var q = KalaQuery.Start()
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
                    return Ok(result.Errors);
                }
                else if (result?.MeasureList != null)
                {
                    return Ok(result.MeasureList);
                }
                else if (result?.ResultSets != null)
                {
                    return Ok(result.ResultSets);
                }
                else if (result?.ResultTable != null)
                {
                    return Ok(result.ResultTable);
                }
                return NoContent();
            }
            catch (Exception ex)
            {
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

                return Ok(ModelState);
            }
        }

        private string ProcessString(string input)
        {
            // Beispielhafte Verarbeitung des Strings
            // Hier könntest du den String weiterverarbeiten
            // Zum Beispiel Zeilen zählen, Zeilen umbrechen, etc.
            return input.Replace("\\n", "\n");
        }
    }
}
