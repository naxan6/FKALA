using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FKala.Core.Logic
{
    public class PathSanitizer
    {
        static ConcurrentDictionary<string, string> sanitizedCache = new ConcurrentDictionary<string, string>();
        public static string SanitizePath(string path)
        {
            if (!sanitizedCache.ContainsKey(path))
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

                sanitizedCache.TryAdd(path, sanitizedPath.ToString());
            }
            return sanitizedCache[path];
        }
    }
}
