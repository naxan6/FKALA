using CommandLine;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FKala.Client.Cmd
{    
    // Definiere die Optionen für das Verb 'q'
    [Verb("q", HelpText = "Führt asynchrone Abfrage aus und streamt die Antwort in die Standardausgabe.")]
    class QueryAsyncOpts
    {
        [Option('u', "url", Required = true, HelpText = "Die Api-Base-Url von Kala.")]
        public string Url { get; set; } 

        [Option('i', "input", Required = true, HelpText = "Der Eingabe-String, der verarbeitet werden soll.")]
        public string Input { get; set; }
    }
}
