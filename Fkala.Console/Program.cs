#pragma warning disable CS8321 // Die lokale Funktion ist deklariert, wird aber nie verwendet.
using FKala.Core;
using FKala.Core.Interfaces;
using FKala.Core.KalaQl;
using FKala.Core.Logic;
using FKala.Migrate.MariaDb;
using FKala.Unittests;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Text.RegularExpressions;




ForProfiling();
//BenchmarkWrites();
//BenchmarkReads();
//RepairBuggedNames();

static void ForProfiling()
{
    KalaQl t = new KalaQl();
    //TempFolderTests_OutOfOrder.Initialize(null);
    t.KalaQl_2_Datasets();
}

// local 65536


static void BenchmarkReads()
{
    Stopwatch sw = new Stopwatch();
    Random rand = new Random();  // seed a random number generator
    int numberOfBytes = 2 << 22; //8,192KB File
    byte nextByte;
    TimeSpan min = TimeSpan.MaxValue;
    string minString = "";

    FileStreamOptions fileStreamOptions = new FileStreamOptions()
    {
        Access = FileAccess.Read,
        BufferSize = 131072,
        Mode = FileMode.Open,
        Share = FileShare.ReadWrite | FileShare.Delete
    };

    for (int i = 12; i <= 24; i++) //Limited loop to 28 to prevent out of memory
    {



        using (FileStream fs = new FileStream(
            @$"\\naxds2\docker\fkala\TEST{i}.DAT",  // name of file
            FileMode.Create,    // create or overwrite existing file
            FileAccess.Write,   // write-only access
            FileShare.None,     // no sharing
            2 << 20,             // block transfer of i=18 -> size = 256 KB
            FileOptions.None))
        {
            for (int j = 0; j < numberOfBytes; j++)
            {
                nextByte = (byte)(rand.Next() % 256); // generate a random byte
                fs.WriteByte(nextByte);               // write it
            }
        }

        fileStreamOptions.BufferSize = 2 << i;
        sw.Start();
        for (int repeat = 0; repeat < 1; repeat++)
        {

            var sr = new StreamReader(@$"\\naxds2\docker\fkala\TEST{i}.DAT", Encoding.UTF8, false, fileStreamOptions);
            var readstring = sr.ReadToEnd();            
        }
        sw.Stop();
        var outs = $"READ Buffer is 2 << {i} - {2 << i} Elapsed: {sw.Elapsed}";
        Console.WriteLine(outs);

        if (min > sw.Elapsed)
        {
            min = sw.Elapsed;
            minString = outs;
        }
        sw.Reset();
        
    }
    Console.WriteLine("FASTEST READ: " + minString);
}


static void BenchmarkWrites()
{
    Stopwatch sw = new Stopwatch();
    Random rand = new Random();  // seed a random number generator
    int numberOfBytes = 2 << 22; //8,192KB File
    byte nextByte;
    TimeSpan min = TimeSpan.MaxValue;
    string minString = "";
    for (int i = 12; i <= 24; i++) //Limited loop to 28 to prevent out of memory
    {
        sw.Start();
        for (int repeat = 0; repeat < 1; repeat++)
        {
            using (FileStream fs = new FileStream(
                String.Format(@"\\naxds2\docker\fkala\TEST{0}.DAT", i),  // name of file
                FileMode.Create,    // create or overwrite existing file
                FileAccess.Write,   // write-only access
                FileShare.None,     // no sharing
                2 << i,             // block transfer of i=18 -> size = 256 KB
                FileOptions.None))
            {
                for (int j = 0; j < numberOfBytes; j++)
                {
                    nextByte = (byte)(rand.Next() % 256); // generate a random byte
                    fs.WriteByte(nextByte);               // write it
                }
            }
        }
        sw.Stop();
        var outs = $"Buffer is 2 << {i} - {2 << i} Elapsed: {sw.Elapsed}";
        Console.WriteLine(outs);

        if (min > sw.Elapsed)
        {
            min = sw.Elapsed;
            minString = outs;
        }
        sw.Reset();
    }

    Console.WriteLine("FASTEST: " + minString);
}


static void RepairBuggedNames()
{
    string StoragePathData = "\\\\naxds2\\docker\\fkala\\data";
    EnumerationOptions optionFindFilesRecursive = new EnumerationOptions()
    {
        BufferSize = 131072,
        RecurseSubdirectories = true,
        ReturnSpecialDirectories = false,
        AttributesToSkip = FileAttributes.Hidden
    };
    var fileCandidates = Directory.GetFileSystemEntries(StoragePathData, "*.dat", optionFindFilesRecursive).ToList();

    Regex regex = new Regex(@".*(_|#)\d\d\d\d-\d\d-\d\d\.dat");

    int i = 0;
    int ren = 0;
    int total = fileCandidates.Count;
    int del = 0;
    foreach (var candidate in fileCandidates)
    {
        i++;
        var measure = new DirectoryInfo(candidate).Parent.Parent.Parent.Name;
        
        // Pattern
        if (!regex.IsMatch(Path.GetFileName(candidate)))
        {
            var size = new FileInfo(candidate).Length;
            File.Delete(candidate);
            Console.WriteLine($"Deleted (1) wronly named {candidate} in measure {measure} {size}");
            del++;
            continue;
        }

        // Measure        
        if (Path.GetFileName(candidate).IndexOf(measure) == -1)
        {
            var size = new FileInfo(candidate).Length;
            File.Delete(candidate);
            Console.WriteLine($"Deleted (2) wronly named {candidate} in measure {measure} {size}");
            del++;
            continue;
        }

        // Mark
        string mark = candidate.Substring(candidate.Length - 15, 1);
        if (mark != "_" && mark != "#")
        {
            var newname = candidate.Insert(candidate.Length - 14, "_");
            File.Move(candidate, newname);
            ren++;
        }


        if (i % 1000 == 0)
        {
            Console.WriteLine($"{i}/{total} Done. {ren} renamed. {del} deleted");
        }
    }
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