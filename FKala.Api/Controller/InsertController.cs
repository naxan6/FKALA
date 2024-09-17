using FKala.Core.Interfaces;
using Microsoft.AspNetCore.Mvc;

namespace FKala.Api.Controller
{

    [ApiController]
    [Route("api/[controller]")]
    public class InsertController : ControllerBase
    {
        public IDataLayer DataLayer { get; }
        public InsertController(IDataLayer dataLayer)
        {
            this.DataLayer = dataLayer;
        }

        [HttpPut]
        [Consumes("text/plain")]
        public IActionResult Insert([FromBody] string input)
        {
            if (string.IsNullOrEmpty(input))
            {
                return BadRequest("Input string is required.");
            }

            this.DataLayer.Insert(input);

            return Ok();
        }
    }
}
