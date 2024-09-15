using FKala.TestConsole;
using FKala.TestConsole.KalaQl;
using FKala.TestConsole.Logic;

string StoragePath = "\\\\naxds2\\docker\\fkala";
System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();

sw.Start();
using var dl = new DataLayer_Readable_Caching_V1(StoragePath);

var startTime = new DateTime(2024, 06, 01, 0, 0, 0);
var endTime = new DateTime(2024, 08, 01, 0, 0, 0);

var q = KalaQuery.Start()
    .FromQuery(@"
                    Load PV1: Sofar/measure/PVInput1/0x586_Leistung_PV1[kW] 2024-06-01T00:00:00 2024-08-01T00:00:00 NoCache
                    Load PV2: Sofar/measure/PVInput1/0x589_Leistung_PV2[kW] 2024-06-01T00:00:00 2024-08-01T00:00:00 NoCache
                    Aggr PV1_Windowed: PV1 Unaligned_1Month Avg
                    Aggr PV2_Windowed: PV2 Unaligned_1Month Avg
                    Expr PVSumInWatt: ""(PV1_Windowed.Value + PV2_Windowed.Value) * 1000""
                    Publ ""PVSumInWatt, PV1_Windowed"" Default
                ");

var result = q.Execute(dl);

foreach (var rs in result.ResultSets)
{
    rs.Resultset.ToList();
}
sw.Stop();
var ts = sw.Elapsed;
string elapsedTime = String.Format("{0:00}:{1:00}:{2:00}.{3:000}", ts.Hours, ts.Minutes, ts.Seconds, ts.Milliseconds);
Console.WriteLine("Verstrichene Zeit: " + elapsedTime);

Console.WriteLine(KalaJson.Serialize(result.ResultSets));// JSON serialize