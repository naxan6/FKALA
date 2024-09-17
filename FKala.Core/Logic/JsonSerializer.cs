using FKala.Core.Model;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FKala.Core.Logic
{
    public class KalaJson
    {
        public static string Serialize(object? serObject)
        {
            var settings = new JsonSerializerSettings
            {
                NullValueHandling = NullValueHandling.Ignore,                
            };
            return JsonConvert.SerializeObject(serObject, settings);
        }
    }
}
