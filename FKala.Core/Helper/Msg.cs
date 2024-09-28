using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FKala.Core.Helper
{
    public static class Msg
    {
        public static Dictionary<string, object?> Get(string key, string value)
        {
            return new Dictionary<string, object?>() { { key, value } };
        }
    }
}
