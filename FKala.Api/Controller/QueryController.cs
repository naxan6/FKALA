﻿using FKala.Core;
using FKala.Core.Interfaces;
using FKala.Core.KalaQl;
using Microsoft.AspNetCore.Mvc;
using Swashbuckle.AspNetCore.Annotations;
using System.Runtime;

namespace FKala.Api.Controller
{

    [ApiController]
    [Route("api/[controller]")]
    public class QueryController : ControllerBase
    {
        public IDataLayer DataLayer { get; }
        public QueryController(IDataLayer dataLayer) {
            this.DataLayer = dataLayer;
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
        //[Consumes("application/json", "text/plain")]
        [Consumes("text/plain")]
        [SwaggerOperation(
            Summary = "Create a new weather forecast",
            Description = "Creates a new weather forecast and returns the created forecast."
        )]
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
            var q = KalaQuery.Start()
                .FromQuery(query);

            var result = q.Execute(this.DataLayer);

            
            if (result?.Errors.Count() != 0)
            {
                return BadRequest(result!.Errors);
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

        private string ProcessString(string input)
        {
            // Beispielhafte Verarbeitung des Strings
            // Hier könntest du den String weiterverarbeiten
            // Zum Beispiel Zeilen zählen, Zeilen umbrechen, etc.
            return input.Replace("\\n", "\n");
        }
    }
}
