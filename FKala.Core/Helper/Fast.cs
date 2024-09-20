using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FKala.Core.Logic
{
    public class Fast
    {
        public static bool IntParse(ReadOnlySpan<char> s, out int result)
        {
            int value = 0;
            var length = s.Length;
            for (int i = 0; i < length; i++)
            {
                var c = s[i];
                if (!char.IsDigit(c))
                {
                    result = -1;
                    return false;
                }
                value = 10 * value + (c - 48);
            }
            result = value;
            return true;
        }
    }
}
