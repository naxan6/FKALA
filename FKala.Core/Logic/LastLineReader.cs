using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection.Metadata.Ecma335;
using System.Text;
using System.Threading.Tasks;

namespace FKala.TestConsole.Logic
{
    public class LastLineReader
    {
        public static string ReadLastLine(string filePath)
        {
            using (FileStream fs = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
            {
                long position = fs.Length - 1;
                StringBuilder builder = new StringBuilder();
                bool foundNonWhitespace = false;

                while (position >= 0)
                {
                    fs.Position = position;
                    int charValue = fs.ReadByte();

                    if (charValue == '\n')
                    {
                        if (foundNonWhitespace)
                        {
                            break;
                        }
                        else
                        {
                            builder.Clear(); // Clear any whitespace we've accumulated
                        }
                    }
                    else if (charValue != '\r')
                    {
                        builder.Insert(0, (char)charValue);
                        if (!char.IsWhiteSpace((char)charValue))
                        {
                            foundNonWhitespace = true;
                        }
                    }

                    position--;
                }

                return RemoveByteOrderMark(builder.ToString().Trim());                
            }
        }
        private static string RemoveByteOrderMark(string input)
        {
            // UTF-8 BOM als Zeichenkette: 'ï»¿'
            if (input.StartsWith("ï»¿"))
            {
                var ret = input.Substring(3);
                return ret;
            }
            return input;
        }


    }
}
