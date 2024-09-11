// See https://aka.ms/new-console-template for more information
using FKala.TestConsole;
using FKala.TestConsole.DataLayers;
using FKala.TestConsole.Migration;
using System.Diagnostics.Metrics;
using System.Text;

Console.WriteLine("Hello, World!");


using var dl = new DataLayer_Readable_V1();



//    dl.Insert("/Cars/Tesla/nxCar3/SOC 2024-09-10T09:37:16.000000 10");
//dl.Insert("/Cars/Tesla/nxCar3/SOC 2024-09-10T09:38:00.000000 11");
//dl.Insert("/Cars/Tesla/nxCar3/SOC 2024-09-10T09:43:16.000000 12");
//dl.Insert("/Cars/Tesla/nxCar3/SOC 2024-09-10T09:58:16.000000 13");
//dl.Insert("/Cars/Tesla/nxCar3/SOC 2024-09-10T11:03:16.000000 14");


//dl.Insert("/Cars/Benz/eqa/SOC 2024-09-10T09:15:16.000000 1");
//dl.Insert("/Cars/Benz/eqa/SOC 2024-09-10T09:34:00.000000 2");
//dl.Insert("/Cars/Benz/eqa/SOC 2024-09-10T09:42:16.000000 3");
//dl.Insert("/Cars/Benz/eqa/SOC 2024-09-10T09:46:16.000000 4");
//dl.Insert("/Cars/Benz/eqa/SOC 2024-09-10T11:15:16.000000 5");


//var result = dl.Query("/Cars/Tesla/nxCar3/SOC", new DateTime(2024, 09, 10, 9, 37, 0), new DateTime(2024, 09, 10, 9, 38, 0));
//Console.WriteLine(dl.SerializeDatapoints(result));

//invalid query
//result = dl.Aggregate("/Cars/Tesla/nxCar3/SOC", new DateTime(2024, 09, 10, 9, 37, 0), new DateTime(2024, 09, 10, 9, 38, 0), new TimeSpan(0,15,0), "AVG", true, -1);
//Console.WriteLine(result);

//result = dl.Aggregate("/Cars/Tesla/nxCar3/SOC", new DateTime(2024, 09, 10, 9, 0, 0), new DateTime(2024, 09, 10, 9, 45, 0), new TimeSpan(0, 15, 0), "AVG", true, 0);
//Console.WriteLine(dl.SerializeDatapoints(result));

//ABFRAGE 1a
var ql = new KalaQlHardcoded(dl);
//var result = ql.Query("Sofar$measure$PVInput1$0x586_Leistung_PV1[kW]", new DateTime(2023, 08, 01, 0, 0, 0), new DateTime(2024, 08, 01, 0, 0, 0));
var result = ql.Aggregate("Sofar$measure$PVInput1$0x586_Leistung_PV1[kW]", new DateTime(2023, 08, 01, 0, 0, 0), new DateTime(2024, 08, 01, 0, 0, 0), new TimeSpan(24, 00, 0), "MEAN", true, 0);
var list = result.ToList(); // Daten laden
var jsonResult = ql.SerializeDatapoints(list); // JSON serialize
Console.WriteLine(jsonResult);


//result = dl.AddAggregatedMeasurements("/Cars/Tesla/nxCar3/SOC", "/Cars/Benz/eqa/SOC", new DateTime(2024, 09, 10, 9, 0, 0), new DateTime(2024, 09, 10, 9, 45, 0), new TimeSpan(0, 15, 0), "AVG");
//Console.WriteLine(dl.SerializeDatapoints(result));

//var result = dl.AddAggregatedMeasurements("/Cars/Tesla/nxCar3/SOC", "/Cars/Benz/eqa/SOC", new DateTime(2024, 09, 10, 9, 0, 0), new DateTime(2024, 09, 10, 9, 45, 0), new TimeSpan(0, 15, 0), "AVG");
//Console.WriteLine(dl.SerializeDatapoints(result));



////Import InfluxBackUP
//InfluxLineProtocolImporter imp = new InfluxLineProtocolImporter(dl);
//string filePath = @"E:\backup4.txt";
//const Int32 BufferSize = 16384;
//long i = 0;
//using (var reader = new StreamReader(filePath, Encoding.UTF8, false, BufferSize))
//{
//    string line;
//    while ((line = reader.ReadLine()) != null)
//    {
//        imp.Import(line, false);
//        i++;
//        if (i % 1000000 == 0)
//        {
//            Console.WriteLine($"Lines: {i} {100 * reader.BaseStream.Position / reader.BaseStream.Length}%");
//        }
//    }
//}