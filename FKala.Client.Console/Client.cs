using CommandLine;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net.Http;
using System.Net.Http.Headers;

namespace FKala.Client.Cmd
{
    public class Client
    {
        public async static Task Main(string[] args)
        {
            // Parse die Argumente und führe die entsprechende Logik aus
            await Parser.Default.ParseArguments<QuerySyncOpts, QueryAsyncOpts>(args)
                .MapResult(
                    (QuerySyncOpts opts) => QuerySync(opts),
                    (QueryAsyncOpts opts) => QueryAsync(opts),
                    errs => Task.FromResult(0)
                );

            //.WithParsedAsync<QuerySync>(opts => QuerySync(opts));
        }

        private static async Task QuerySync(QuerySyncOpts opts)
        {
            try
            {
                using (HttpClient client = new HttpClient())
                {
                    // Erstelle den Inhalt der POST-Anfrage
                    var content = new StringContent(opts.Input);

                    Uri baseUri = new Uri(opts.Url, UriKind.Absolute);
                    Uri uri = new Uri(baseUri, "Query");

                    // Sende die POST-Anfrage
                    using (HttpResponseMessage response = await client.PostAsync(uri, content))
                    {
                        response.EnsureSuccessStatusCode();
                        // Lese die Antwort als Stream und gib sie direkt in die Konsole aus
                        using (var stream = await response.Content.ReadAsStreamAsync())
                        using (var reader = new StreamReader(stream))
                        {
                            char[] buffer = new char[8192];
                            int bytesRead;
                            while ((bytesRead = await reader.ReadAsync(buffer, 0, buffer.Length)) > 0)
                            {
                                Console.Write(buffer, 0, bytesRead);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Fehler: {ex.Message}");
            }
        }

        private static async Task QueryAsync(QueryAsyncOpts opts)
        {
            try
            {
                using (HttpClient client = new HttpClient())
                {
                    // Erstelle den Inhalt der POST-Anfrage
                    var content = new StringContent(opts.Input);

                    Uri baseUri = new Uri(opts.Url, UriKind.Absolute);
                    Uri uri = new Uri(baseUri, "StreamQuery");

                    // Sende die POST-Anfrage
                    using (HttpResponseMessage response = await client.PostAsync(uri, content))
                    {
                        response.EnsureSuccessStatusCode();
                        // Lese die Antwort als Stream und gib sie direkt in die Konsole aus
                        using (var stream = await response.Content.ReadAsStreamAsync())
                        using (var reader = new StreamReader(stream))
                        {
                            char[] buffer = new char[8192];
                            int bytesRead;
                            while ((bytesRead = await reader.ReadAsync(buffer, 0, buffer.Length)) > 0)
                            {
                                Console.Write(buffer, 0, bytesRead);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Fehler: {ex.Message}");
            }
        }
    }
}