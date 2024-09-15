﻿using FKala.TestConsole;
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
                    Var $CACHE NewestOnly
                    Var $INTERVAL Infinite                   

                    Load rPV1: Sofar/measure/PVInput1/0x586_Leistung_PV1[kW] $CACHE
                    Load rPV2: Sofar/measure/PVInput1/0x589_Leistung_PV2[kW] $CACHE
                    Load rNetz: Sofar/measure/OnGridOutput/0x488_ActivePower_PCC_Total[kW] $CACHE
                    Load rAkku: Sofar/measure/batteryInput1/0x606_Power_Bat1[kW] $CACHE
                    Load rVerbrauch: Sofar/measure/OnGridOutput/0x4AF_ActivePower_Load_Sys[kW] $CACHE

                    Aggr aPV1: rPV1 $INTERVAL WAvg
                    Aggr aPV2: rPV2 $INTERVAL WAvg
                    Aggr aNetz: rNetz $INTERVAL WAvg
                    Aggr aAkku: rAkku $INTERVAL WAvg
                    Aggr aVerbrauch: rVerbrauch $INTERVAL WAvg
                    Aggr aAkkuladung: rAkku $INTERVAL WAvg


                    Expr PV: ""aPV1.Value + aPV2.Value""
                    Expr Netzbezug: ""aNetz.Value > 0 ? 0 : -aNetz.Value""
                    Expr Netzeinspeisung: ""aNetz.Value < 0 ? 0 : -aNetz.Value""
                    Expr Akkuentladung: ""aAkku.Value > 0 ? 0 : -aAkku.Value""
                    Expr Akkuladung: ""aAkku.Value < 0 ? 0 : -aAkku.Value""
                    Expr Verbrauch: ""-aVerbrauch.Value""

                    Publ ""Netzbezug,PV,Netzeinspeisung,Akkuentladung,Akkuladung,Verbrauch"" Table


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