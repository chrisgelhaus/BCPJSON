using Microsoft.Data.SqlClient;
using System.Reflection;

namespace bcpJson
{
    /// <summary>
    /// Reads the number of rows copied by a SqlBulkCopy instance.
    /// SqlClient has exposed this under different names across versions (public RowsCopied / RowsCopied64,
    /// or a private field), so it is resolved once by reflection. Returns -1 if no member is found.
    /// </summary>
    static class SqlBulkCopyHelper
    {
        private static readonly Func<SqlBulkCopy, long> rowsCopiedReader = CreateReader();

        private static Func<SqlBulkCopy, long> CreateReader()
        {
            Type type = typeof(SqlBulkCopy);

            PropertyInfo property = type.GetProperty("RowsCopied64", BindingFlags.Public | BindingFlags.Instance)
                ?? type.GetProperty("RowsCopied", BindingFlags.Public | BindingFlags.Instance);
            if (property != null)
            {
                return bulkCopy => Convert.ToInt64(property.GetValue(bulkCopy));
            }

            foreach (string fieldName in new[] { "_rowsCopied", "rowsCopied", "_rowsCopied64" })
            {
                FieldInfo field = type.GetField(fieldName, BindingFlags.NonPublic | BindingFlags.Instance);
                if (field != null)
                {
                    return bulkCopy => Convert.ToInt64(field.GetValue(bulkCopy));
                }
            }

            return bulkCopy => -1;
        }

        public static long GetRowsCopied(SqlBulkCopy bulkCopy)
        {
            return rowsCopiedReader(bulkCopy);
        }
    }
}
