using FKala.Core;
using FKala.Core.Interfaces;
using FKala.Core.KalaQl;
using Microsoft.AspNetCore.Mvc;
using Swashbuckle.AspNetCore.Annotations;

namespace FKala.Api.Controller
{

    [ApiController]
    [Route("api/[controller]")]
    public class MgmtController : ControllerBase
    {
        public IDataLayer DataLayer { get; }
        public MgmtController(IDataLayer dataLayer) {
            this.DataLayer = dataLayer;
        }
        
        [HttpGet]
        public IActionResult LoadMeasures([FromQuery] string input)
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

            if (result?.MeasureList != null)
            {
                return Ok(result.MeasureList);
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
