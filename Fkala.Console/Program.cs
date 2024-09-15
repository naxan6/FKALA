using FKala.TestConsole;
using FKala.TestConsole.KalaQl;
using FKala.TestConsole.Logic;

string StoragePath = "\\\\naxds2\\docker\\fkala";
System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();

sw.Start();
using var dl = new DataLayer_Readable_Caching_V1(StoragePath);

var q = KalaQuery.Start()
    .FromQuery(@"
                    Load rPV1: Sofar/measure/PVInput1/0x586_Leistung_PV1[kW] 2024-04-01T00:00:00Z 2024-09-01T00:00:00Z Hourly_WAvg
                    Load rPV2: Sofar/measure/PVInput1/0x589_Leistung_PV2[kW] 2024-04-01T00:00:00Z 2024-09-01T00:00:00Z Hourly_WAvg
                    Load rNetz: Sofar/measure/OnGridOutput/0x488_ActivePower_PCC_Total[kW] 2024-04-01T00:00:00Z 2024-09-01T00:00:00Z Hourly_WAvg
                    Load rAkku: Sofar/measure/batteryInput1/0x606_Power_Bat1[kW] 2024-04-01T00:00:00Z 2024-09-01T00:00:00Z Hourly_WAvg
                    Load rVerbrauch: Sofar/measure/OnGridOutput/0x4AF_ActivePower_Load_Sys[kW] 2024-04-01T00:00:00Z 2024-09-01T00:00:00Z Hourly_WAvg

                    Aggr aPV1: rPV1 Aligned_1Hour WAvg EmptyWindows
                    Aggr aPV2: rPV2 Aligned_1Hour WAvg EmptyWindows
                    Aggr aNetz: rNetz Aligned_1Hour WAvg EmptyWindows
                    Aggr aAkku: rAkku Aligned_1Hour WAvg EmptyWindows
                    Aggr aVerbrauch: rVerbrauch Aligned_1Hour WAvg EmptyWindows
                    Aggr aAkkuladung: rAkku Aligned_1Hour WAvg EmptyWindows


                    Expr PV: ""aPV1.Value + aPV2.Value""
                    Expr Netzbezug: ""aNetz.Value > 0 ? 0 : -aNetz.Value""
                    Expr Netzeinspeisung: ""aNetz.Value < 0 ? 0 : aNetz.Value""
                    Expr Akkuentladung: ""aAkku.Value > 0 ? 0 : -aAkku.Value""
                    Expr Akkuladung: ""aAkku.Value < 0 ? 0 : aAkku.Value""
                    Expr Verbrauch: ""aVerbrauch.Value""

                    Publ ""Netzbezug,PV,Netzeinspeisung,Akkuentladung,Akkuladung,Verbrauch"" Table
                ");

var result = q.Execute(dl);

sw.Stop();
var ts = sw.Elapsed;
string elapsedTime = String.Format("{0:00}:{1:00}:{2:00}.{3:000}", ts.Hours, ts.Minutes, ts.Seconds, ts.Milliseconds);
Console.WriteLine("Verstrichene Zeit: " + elapsedTime);

Console.WriteLine(KalaJson.Serialize(result.ResultTable));// JSON serializeON serialize