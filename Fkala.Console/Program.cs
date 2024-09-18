#pragma warning disable CS8321 // Die lokale Funktion ist deklariert, wird aber nie verwendet.
using FKala.Core;
using FKala.Core.Interfaces;
using FKala.Core.KalaQl;
using FKala.Core.Logic;
using FKala.Migrate.MariaDb;
using FKala.Unittests;




ForProfiling();

static void ForProfiling ()
{
    KalaQl t = new KalaQl();
    //TempFolderTests_OutOfOrder.Initialize(null);
    t.KalaQl_2_Datasets();
}


static void TestQuery()
{
    string StoragePath = "\\\\naxds2\\docker\\fkala";
    using var dl = new DataLayer_Readable_Caching_V1(StoragePath);


    System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();

    sw.Start();
    var q = KalaQuery.Start()
        .FromQuery(@"Var $FROM ""2022-09-19T00:00:00""
Var $TO ""2024-09-19T00:00:00""
Var $CACHE Auto(43200)_WAvg_REBUILD
Var $AGG ""WAvg EmptyWindows""
Var $INTERVAL 43200

Load rPV1: Sofar/measure/PVInput1/0x586_Leistung_PV1[kW] $FROM $TO $CACHE
Load rPV2: Sofar/measure/PVInput1/0x589_Leistung_PV2[kW] $FROM $TO $CACHE
Load rNetz: Sofar/measure/OnGridOutput/0x488_ActivePower_PCC_Total[kW] $FROM $TO $CACHE
Load rAkku: Sofar/measure/batteryInput1/0x606_Power_Bat1[kW] $FROM $TO $CACHE
Load rVerbrauch: Sofar/measure/OnGridOutput/0x4AF_ActivePower_Load_Sys[kW] $FROM $TO $CACHE

Aggr aPV1: rPV1 $INTERVAL $AGG
Aggr aPV2: rPV2 $INTERVAL $AGG
Aggr aNetz: rNetz $INTERVAL $AGG
Aggr aAkku: rAkku $INTERVAL $AGG
Aggr aVerbrauch: rVerbrauch $INTERVAL $AGG
Aggr aAkkuladung: rAkku $INTERVAL $AGG


Expr PV: ""aPV1.Value + aPV2.Value""
Expr Netzbezug: ""aNetz.Value > 0 ? 0 : -aNetz.Value""
Expr Netzeinspeisung: ""aNetz.Value < 0 ? 0 : -aNetz.Value""
Expr Akkuentladung: ""aAkku.Value > 0 ? 0 : -aAkku.Value""
Expr Akkuladung: ""aAkku.Value < 0 ? 0 : -aAkku.Value""
Expr Verbrauch: ""-aVerbrauch.Value""""

Publ ""Netzbezug,PV,Netzeinspeisung,Akkuentladung,Akkuladung,Verbrauch"" Table

");

    var result = q.Execute(dl);

    result.ResultTable!.ToList();

    sw.Stop();
    var ts = sw.Elapsed;
    string elapsedTime = String.Format("{0:00}:{1:00}:{2:00}.{3:000}", ts.Hours, ts.Minutes, ts.Seconds, ts.Milliseconds);
    Console.WriteLine("Verstrichene Zeit: " + elapsedTime);

    Console.WriteLine(KalaJson.Serialize(result.ResultTable));// JSON serialize
}



static async void MigrateJsts(IDataLayer dl)
{
    var connString = "Server=XXX;Port=XXX;User ID=XXX;Password=XXX;Database=XXX";
    var maria = new MigrateMariaDb_Tstsfe_Custom(connString, dl);
    await maria.Migrate();
}
#pragma warning restore CS8321 // Die lokale Funktion ist deklariert, wird aber nie verwendet.