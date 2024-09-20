using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FKala.Core.Logic
{
    public static class FileFromEndProcessor
    {

        public static void ProcessFileFromEnd(string filePath, Func<string, bool> checkFunction, string newData)
        {
            using (FileStream fs = new FileStream(filePath, FileMode.Open, FileAccess.ReadWrite))
            {
                long position = fs.Length - 1;
                StringBuilder line = new StringBuilder();
                bool foundMatch = false;

                // Von hinten durch die Datei lesen
                while (position >= 0 && !foundMatch)
                {
                    fs.Position = position;
                    int byteRead = fs.ReadByte();

                    if (byteRead == '\n' || position == 0)
                    {
                        string currentLine = line.ToString();
                        if (checkFunction(currentLine))
                        {
                            foundMatch = true;
                            // Position nach dem Zeilenumbruch setzen
                            fs.Position = position == 0 ? 0 : position + 1;
                        }
                        line.Clear();
                    }
                    else
                    {
                        line.Insert(0, (char)byteRead);
                    }

                    position--;
                }

                // Datei ab der gefundenen Position abschneiden
                fs.SetLength(fs.Position);

                //// Neue Daten schreiben
                //if (fs.Position > 0 && fs.ReadByte() != '\n')
                //{
                //    fs.WriteByte((byte)'\n');
                //}
                //byte[] newDataBytes = Encoding.UTF8.GetBytes(newData);
                //fs.Write(newDataBytes, 0, newDataBytes.Length);
            }
        }
    }
}
