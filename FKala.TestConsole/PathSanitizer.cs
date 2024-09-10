using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FKala.TestConsole
{
    public class PathSanitizer
    {
        public static string SanitizePath(string path)
        {
            if (path == null) throw new ArgumentNullException(nameof(path));

            var invalidChars = Path.GetInvalidFileNameChars();
            var sanitizedPath = new StringBuilder(path.Length);

            foreach (var ch in path)
            {
                if (Array.Exists(invalidChars, invalidChar => invalidChar == ch))
                {
                    sanitizedPath.Append('$');
                }
                else
                {
                    sanitizedPath.Append(ch);
                }
            }

            return sanitizedPath.ToString();
        }
    }
}
