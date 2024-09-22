using CommandLine;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;

namespace FKala.Client.Cmd
{
    public class Client
    {
        public async static Task Main(string[] args)    
        {

            Client kalaClient = new Client();
            // Parse die Argumente und führe die entsprechende Logik aus
            await Parser.Default.ParseArguments<QuerySyncOpts, QueryAsyncOpts>(args)
                .MapResult(
                    (QuerySyncOpts opts) => kalaClient.QueryAsync(opts.Url, opts.Input, "Query", opts.Debug),
                    (QueryAsyncOpts opts) => kalaClient.QueryAsync(opts.Url, opts.Input, "StreamQuery", opts.Debug),
                    errs => Task.FromResult(0)
                );

            //.WithParsedAsync<QuerySync>(opts => QuerySync(opts));
        }

        private async Task QueryAsync(string baseUrl, string input, string endpoint, bool debug = false)
        {
            try
            {
                using (HttpClient client = new HttpClient())
                {
                    // timeout to 180 minutes
                    client.Timeout = TimeSpan.FromMinutes(180);

                    // create POST-body
                    var content = new StringContent(input);

                    Uri baseUri = new Uri(baseUrl, UriKind.Absolute);
                    Uri uri = new Uri(baseUri, endpoint);
                    if (debug) Console.WriteLine($"Sending to : {uri}");
                    if (debug) Console.WriteLine($"Sending command : {input}");
                    // send request

                    var request = new HttpRequestMessage(HttpMethod.Post, uri);
                    request.Content = content;

                    using (HttpResponseMessage response = await client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead))
                    {
                        if (debug) Console.WriteLine($"Status Code: {response.StatusCode}");
                        response.EnsureSuccessStatusCode();
                        if (debug) Console.WriteLine("Antwort empfangen, lese Daten...");
                        // read response and post to console
                        using (var stream = await response.Content.ReadAsStreamAsync())
                        using (var reader = new StreamReader(stream))
                        {
                            char[] buffer = new char[1024];
                            int bytesRead;
                            while ((bytesRead = await reader.ReadAsync(buffer, 0, buffer.Length)) > 0)
                            {
                                string result = new string(buffer, 0, bytesRead);
                                result = result.Replace("}", "}\n");
                                Console.Write(result);
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