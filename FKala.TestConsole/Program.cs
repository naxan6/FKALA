// See https://aka.ms/new-console-template for more information
using FKala.TestConsole;

Console.WriteLine("Hello, World!");


var dl = new DataLayer();
dl.Insert("/Cars/Tesla/nxCar3/SOC 2024-09-10T09:37:16.000000 87");
dl.Insert("/Cars/Tesla/nxCar3/SOC 2024-09-10T09:38:00.000000 86");
dl.Insert("/Cars/Tesla/nxCar3/SOC 2024-09-10T09:58:16.000000 85");


var result = dl.Query("/Cars/Tesla/nxCar3/SOC", new DateTime(2024, 09, 10, 9, 37, 0), new DateTime(2024, 09, 10, 9, 38, 0));
Console.WriteLine(result);


var result2 = dl.Aggregate("/Cars/Tesla/nxCar3/SOC", new DateTime(2024, 09, 10, 9, 37, 0), new DateTime(2024, 09, 10, 9, 38, 0), new TimeSpan(0,15,0), "AVG", true, -1);
Console.WriteLine(result2);


