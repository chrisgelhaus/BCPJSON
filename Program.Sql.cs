using Microsoft.Data.SqlClient;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;

namespace bcpJson
{
    partial class Program
    {
        /// <summary>A user table selected for export/copy.</summary>
        internal sealed record TableRef(int ObjectId, string Schema, string Name)
        {
            public string DisplayName
            {
                get { return Schema + "." + Name; }
            }

            public string QuotedName
            {
                get { return QuoteName(Schema) + "." + QuoteName(Name); }
            }
        }

        /// <summary>Equivalent of T-SQL QUOTENAME(): [name] with embedded ] doubled.</summary>
        internal static string QuoteName(string name)
        {
            return "[" + name.Replace("]", "]]") + "]";
        }

        /// <summary>N'...' string literal with embedded quotes doubled.</summary>
        internal static string SqlLiteral(string value)
        {
            return "N'" + value.Replace("'", "''") + "'";
        }

        /// <summary>Turns a user pattern using * as wildcard into a LIKE literal; other LIKE metacharacters are escaped.</summary>
        internal static string LikeLiteral(string pattern)
        {
            string escaped = pattern.Replace("[", "[[]").Replace("_", "[_]").Replace("%", "[%]").Replace("*", "%");
            return SqlLiteral(escaped);
        }

        /// <summary>Wraps a command-line argument in double quotes.</summary>
        private static string Arg(string value)
        {
            return "\"" + (value ?? string.Empty) + "\"";
        }

        private static void ExecuteSql(SqlConnection conn, string sql)
        {
            using (var cmd = new SqlCommand(sql, conn))
            {
                cmd.CommandTimeout = 0;
                cmd.ExecuteNonQuery();
            }
        }

        /// <summary>
        /// Builds a T-SQL batch returning (object_id, schema_name, table_name) for the user tables selected by the
        /// include/exclude filters. With no include filters, every user table is selected before exclusions apply.
        /// Literal names match "schema.table" exactly; entries containing * are treated as LIKE patterns.
        /// </summary>
        private static string BuildSourceObjectIDList(IEnumerable<string> includeTables, IEnumerable<string> excludeTables, IEnumerable<string> includeSchemas, IEnumerable<string> excludeSchemas)
        {
            includeTables = (includeTables ?? Enumerable.Empty<string>()).Select(s => s.Trim()).Where(s => s.Length > 0).ToList();
            excludeTables = (excludeTables ?? Enumerable.Empty<string>()).Select(s => s.Trim()).Where(s => s.Length > 0).ToList();
            includeSchemas = (includeSchemas ?? Enumerable.Empty<string>()).Select(s => s.Trim()).Where(s => s.Length > 0).ToList();
            excludeSchemas = (excludeSchemas ?? Enumerable.Empty<string>()).Select(s => s.Trim()).Where(s => s.Length > 0).ToList();

            const string tableName = "(s.name + N'.' + t.name)";
            const string schemaName = "s.name";
            const string fromTables = "FROM sys.tables t INNER JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE t.type = 'U'";

            var sql = new StringBuilder();
            sql.AppendLine("SET NOCOUNT ON;");
            sql.AppendLine("DECLARE @objectIDs TABLE (ID INT NOT NULL PRIMARY KEY);");

            if (!includeTables.Any() && !includeSchemas.Any())
            {
                sql.AppendLine("INSERT @objectIDs (ID) SELECT t.object_id " + fromTables + " AND t.is_ms_shipped = 0;");
            }

            foreach (string condition in BuildNameConditions(tableName, includeTables).Concat(BuildNameConditions(schemaName, includeSchemas)))
            {
                sql.AppendLine("INSERT @objectIDs (ID) SELECT t.object_id " + fromTables + " AND " + condition + " AND t.object_id NOT IN (SELECT ID FROM @objectIDs);");
            }

            foreach (string condition in BuildNameConditions(tableName, excludeTables).Concat(BuildNameConditions(schemaName, excludeSchemas)))
            {
                sql.AppendLine("DELETE @objectIDs WHERE ID IN (SELECT t.object_id " + fromTables + " AND " + condition + ");");
            }

            sql.AppendLine("SELECT o.ID AS object_id, OBJECT_SCHEMA_NAME(o.ID) AS schema_name, OBJECT_NAME(o.ID) AS table_name FROM @objectIDs o ORDER BY schema_name, table_name;");
            return sql.ToString();
        }

        private static IEnumerable<string> BuildNameConditions(string column, IEnumerable<string> names)
        {
            var literals = names.Where(n => !n.Contains("*")).ToList();
            if (literals.Count > 0)
            {
                yield return column + " IN (" + string.Join(", ", literals.Select(SqlLiteral)) + ")";
            }

            foreach (string pattern in names.Where(n => n.Contains("*")))
            {
                yield return column + " LIKE " + LikeLiteral(pattern);
            }
        }

        private static List<TableRef> GetSourceTables(string connectionString, string objectListSql)
        {
            var tables = new List<TableRef>();
            using (var conn = new SqlConnection(connectionString))
            {
                conn.Open();
                using (var cmd = new SqlCommand(objectListSql, conn))
                {
                    cmd.CommandTimeout = 0;
                    using (var reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            tables.Add(new TableRef(reader.GetInt32(0), reader.GetString(1), reader.GetString(2)));
                        }
                    }
                }
            }
            return tables;
        }

        /// <summary>
        /// Resolves a table name ("schema.table", "[schema].[table]" or "table") in the target database.
        /// Returns { schema, name, object_id, objectName } or null if not found.
        /// </summary>
        private static string[] GetTableObject(string objectName, string connectionString)
        {
            const string sql = @"SET NOCOUNT ON;
SELECT TOP (2) t.object_id, s.name AS schema_name, t.name AS table_name
FROM sys.tables t
     INNER JOIN sys.schemas s ON s.schema_id = t.schema_id
WHERE t.type = 'U'
      AND (s.name + N'.' + t.name = @name OR t.object_id = OBJECT_ID(@name));";

            using (var conn = new SqlConnection(connectionString))
            {
                conn.Open();
                using (var cmd = new SqlCommand(sql, conn))
                {
                    cmd.Parameters.Add("@name", SqlDbType.NVarChar, 512).Value = objectName;
                    var result = new DataTable();
                    using (var reader = cmd.ExecuteReader(CommandBehavior.SingleResult))
                    {
                        result.Load(reader);
                    }

                    if (result.Rows.Count != 1)
                    {
                        return null;
                    }

                    DataRow row = result.Rows[0];
                    return new[]
                    {
                        row["schema_name"].ToString(),
                        row["table_name"].ToString(),
                        row["object_id"].ToString(),
                        objectName
                    };
                }
            }
        }
    }
}
