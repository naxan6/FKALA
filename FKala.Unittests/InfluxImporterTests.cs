using FluentAssertions;
using FKala.Core;
using FKala.Core.KalaQl;
using FKala.Core.KalaQl.Windowing;
using FKala.Core.Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FKala.Core.Logic;
using FluentAssertions.Execution;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Runtime.CompilerServices;
using static System.Runtime.InteropServices.JavaScript.JSType;
using FKala.Core.Migrations.Influx;
using FKala.Core.Migration;
using Microsoft.VisualStudio.TestPlatform.Utilities;

namespace FKala.Unittests
{
    [TestClass]
    public class InfluxImporterTests
    {
        [TestMethod]
        public void InfluxLineProtocolParser()
        {
            var test = @"teslamate,car=Model3_2022,topic=teslamate/cars/3/active_route_destination active_route_destination=""Gemeinde Rohrbach in Oberösterreich"" 1722595052416000000";            
            InfluxLineParser ilp = new InfluxLineParser();
            ilp.Read(test);
            
            Console.WriteLine(KalaJson.Serialize(ilp));
        }

        // [TestMethod]
        // public async Task InfluxLineProtocolImporter()
        // {
        //     DataLayer_Readable_Caching_V1 dl = new DataLayer_Readable_Caching_V1(@"C:\fkala\DataStore");
        //     InfluxLineProtocolImporter ip = new InfluxLineProtocolImporter(dl);
        //     var outpout = ip.Import(@"\\naxds2\docker\fkala\import\2024-09-22_nxTesla.lp");
        //     foreach (var kvp in outpout.ToBlockingEnumerable())
        //     {
        //         Console.WriteLine(KalaJson.Serialize(kvp));
        //     }
        // }
    }
}
