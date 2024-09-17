using FKala.Core;
using FKala.Core.Interfaces;
using MySqlConnector;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FKala.Migrate.MariaDb
{
    public class MigrateMariaDb_Tstsfe_Custom
    {
        public string ConnectionString { get; }
        public IDataLayer DataLayer { get; }

        public MigrateMariaDb_Tstsfe_Custom(string connectionstring, IDataLayer dataLayer)
        {
            this.ConnectionString = connectionstring;
            this.DataLayer = dataLayer;
        }

        public async Task Migrate()
        {
            using var connection = new MySqlConnection(ConnectionString);
            connection.Open();

            using var command = new MySqlCommand(@"
SELECT   
    e.id,
    s.name, 
    s.path, 
    timestamp, 
    timeValue1, 
    timeValue2, 
    intValue1, 
    intValue2, 
    doubleValue1, 
    doubleValue2, 
    stringValue1, 
    stringValue2
FROM
event e FORCE INDEX (PRIMARY)
inner join sensors s on s.path = e.sensor
order by e.id asc;
            ", connection);
            command.CommandTimeout = 300;
            using var reader = await command.ExecuteReaderAsync();

            long i = 1;
            string kalalinedata = string.Empty;
            EventRow row;
            while (await reader.ReadAsync())
            {
                row = await ReadRowAsync(reader);

                if (row.intValue1 != null)
                {
                    kalalinedata = $"jsts/{row.sensorName.Replace(' ', '_')}/int1 {DateTimeOffset.FromUnixTimeSeconds(row.timestamp / 1000).AddTicks((row.timestamp % 1000) * 10000).ToString("yyyy-MM-ddTHH:mm:ss.fffffff")} {row.intValue1}";
                    DataLayer.Insert(kalalinedata, false);
                }
                if (row.intValue2 != null)
                {
                    kalalinedata = $"jsts/{row.sensorName.Replace(' ', '_')}/int2 {DateTimeOffset.FromUnixTimeSeconds(row.timestamp / 1000).AddTicks((row.timestamp % 1000) * 10000).ToString("yyyy-MM-ddTHH:mm:ss.fffffff")} {row.intValue2}";
                    DataLayer.Insert(kalalinedata, false);
                }
                if (row.timeValue1 != null)
                {
                    kalalinedata = $"jsts/{row.sensorName.Replace(' ', '_')}/timeValue1 {DateTimeOffset.FromUnixTimeSeconds(row.timestamp / 1000).AddTicks((row.timestamp % 1000) * 10000).ToString("yyyy-MM-ddTHH:mm:ss.fffffff")} {row.timeValue1}";
                    DataLayer.Insert(kalalinedata, false);
                }
                if (row.timeValue2 != null)
                {
                    kalalinedata = $"jsts/{row.sensorName.Replace(' ', '_')}/timeValue2 {DateTimeOffset.FromUnixTimeSeconds(row.timestamp / 1000).AddTicks((row.timestamp % 1000) * 10000).ToString("yyyy-MM-ddTHH:mm:ss.fffffff")} {row.timeValue2}";
                    DataLayer.Insert(kalalinedata, false);
                }
                if (row.doubleValue1 != null)
                {
                    kalalinedata = $"jsts/{row.sensorName.Replace(' ', '_')}/doubleValue1 {DateTimeOffset.FromUnixTimeSeconds(row.timestamp / 1000).AddTicks((row.timestamp % 1000) * 10000).ToString("yyyy-MM-ddTHH:mm:ss.fffffff")} {row.doubleValue1.Value.ToString(CultureInfo.InvariantCulture)}";
                    DataLayer.Insert(kalalinedata, false);
                }
                if (row.doubleValue2 != null)
                {
                    kalalinedata = $"jsts/{row.sensorName.Replace(' ', '_')}/doubleValue2 {DateTimeOffset.FromUnixTimeSeconds(row.timestamp / 1000).AddTicks((row.timestamp % 1000) * 10000).ToString("yyyy-MM-ddTHH:mm:ss.fffffff")} {row.doubleValue2.Value.ToString(CultureInfo.InvariantCulture)}";
                    DataLayer.Insert(kalalinedata, false);
                }
                if (row.stringValue1 != null)
                {
                    kalalinedata = $"jsts/{row.sensorName.Replace(' ', '_')}/stringValue1 {DateTimeOffset.FromUnixTimeSeconds(row.timestamp / 1000).AddTicks((row.timestamp % 1000) * 10000).ToString("yyyy-MM-ddTHH:mm:ss.fffffff")} {row.stringValue1}";
                    DataLayer.Insert(kalalinedata, false);
                }
                if (row.stringValue2 != null)
                {
                    kalalinedata = $"jsts/{row.sensorName.Replace(' ', '_')}/stringValue2 {DateTimeOffset.FromUnixTimeSeconds(row.timestamp / 1000).AddTicks((row.timestamp % 1000) * 10000).ToString("yyyy-MM-ddTHH:mm:ss.fffffff")} {row.stringValue2}";
                    DataLayer.Insert(kalalinedata, false);
                }


                if (i % 100000 == 0)
                {
                    Console.WriteLine($"ID: {row.id}, Count: {i} ### {kalalinedata}");
                }

                i++;
            }
        }

        private async Task<EventRow> ReadRowAsync(MySqlDataReader reader)
        {
            return new EventRow()
            {
                id = await reader.GetValueOrDefaultAsync<int>(0),
                sensorName = await reader.GetValueOrDefaultAsync<string>(1),
                sensorPath = await reader.GetValueOrDefaultAsync<string>(2),
                timestamp = await reader.GetValueOrDefaultAsync<long>(3),
                timeValue1 = await reader.GetValueOrDefaultAsync<long?>(4),
                timeValue2 = await reader.GetValueOrDefaultAsync<long?>(5),
                intValue1 = await reader.GetValueOrDefaultAsync<int?>(6),
                intValue2 = await reader.GetValueOrDefaultAsync<int?>(7),
                doubleValue1 = await reader.GetValueOrDefaultAsync<double?>(8),
                doubleValue2 = await reader.GetValueOrDefaultAsync<double?>(9),
                stringValue1 = await reader.GetValueOrDefaultAsync<string?>(10),
                stringValue2 = await reader.GetValueOrDefaultAsync<string?>(11),

            };
        }
    }
}
