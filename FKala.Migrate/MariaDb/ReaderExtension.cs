using MySqlConnector;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FKala.Migrate.MariaDb
{
    public static class ReaderExtension
    {
        public async static Task<T?> GetValueOrDefaultAsync<T>(this MySqlDataReader reader, string columnName)
        {
            int columnIndex = reader.GetOrdinal(columnName);
            return await reader.GetValueOrDefaultAsync<T>(columnIndex);
        }
        public async static Task<T?> GetValueOrDefaultAsync<T>(this MySqlDataReader reader, int columnIndex)
        {            
            return await reader.IsDBNullAsync(columnIndex) ? default(T) : await reader.GetFieldValueAsync<T>(columnIndex);
        }
    }
}
