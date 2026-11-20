using Microsoft.Data.SqlClient;
using Newtonsoft.Json;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace bcpJson
{
    partial class Program
    {
        /// <summary>
        /// Per-table metadata for JSON export: the column list and a deterministic sort order.
        /// CLR types (geography, geometry, hierarchyid) are written as text via ToString() so they stay diffable
        /// and can be re-imported with ::Parse(). Rows are ordered by the primary key, or by every sortable column
        /// when there is no primary key. Keep CustomSelect.sql in sync with this text.
        /// </summary>
        private const string TableMetadataSql = @"SET NOCOUNT ON;
DECLARE @Columns NVARCHAR(MAX), @SortOrder NVARCHAR(MAX);

SELECT   @Columns = COALESCE(@Columns + N', ', N'') +
                    CASE WHEN sc.system_type_id = 240
                         THEN QUOTENAME(sc.name) + N'.ToString() AS ' + QUOTENAME(sc.name)
                         ELSE QUOTENAME(sc.name)
                    END
FROM     sys.columns sc
WHERE    sc.object_id = @objectId
ORDER BY sc.column_id;

IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = @objectId AND is_primary_key = 1)
BEGIN
    SELECT   @SortOrder = COALESCE(@SortOrder + N', ', N'') + QUOTENAME(c.name) +
                          CASE ic.is_descending_key WHEN 1 THEN N' DESC' ELSE N' ASC' END
    FROM     sys.indexes i
             INNER JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
             INNER JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
    WHERE    i.object_id = @objectId
             AND i.is_primary_key = 1
             AND ic.key_ordinal > 0
    ORDER BY ic.key_ordinal;
END
ELSE
BEGIN
    /* No primary key: sort by every column that can appear in ORDER BY (not text, ntext, image, xml or CLR types). */
    SELECT   @SortOrder = COALESCE(@SortOrder + N', ', N'') + QUOTENAME(c.name)
    FROM     sys.columns c
    WHERE    c.object_id = @objectId
             AND c.system_type_id NOT IN (34, 35, 99, 240, 241)
    ORDER BY c.column_id;
END;

SELECT @Columns, @SortOrder;";

        private static readonly Regex BcpRowsCopied = new Regex(@"(\d+) rows copied", RegexOptions.IgnoreCase | RegexOptions.Compiled);

        private static long ExportToJSON(ExportOptions options)
        {
            long totalrowCount = 0;
            List<TableRef> tables = GetSourceTables(options.SourceConnectionString(),
                BuildSourceObjectIDList(options.IncludeTablesList, options.ExcludeTablesList, options.IncludeSchemaList, options.ExcludeSchemaList));

            if (tables.Count == 0)
            {
                PostToConsole("No tables matched the include/exclude filters.", InfoLevel.Warning);
                return 0;
            }

            Parallel.ForEach(tables, table =>
            {
                try
                {
                    using (var conn = new SqlConnection(options.SourceConnectionString()))
                    {
                        conn.Open();
                        string select = BuildTableSelect(conn, table, options.NoLock);

                        using (var cmd = new SqlCommand(select, conn))
                        {
                            cmd.CommandTimeout = 0;
                            using (var reader = cmd.ExecuteReader(CommandBehavior.SingleResult | CommandBehavior.SequentialAccess))
                            {
                                string filePath = Path.Combine(options.exportPath, table.DisplayName + ".json");
                                long rows = WriteJsonRows(reader, filePath, table.DisplayName);
                                Interlocked.Add(ref totalrowCount, rows);
                                PostToConsole(string.Format("{0}: {1:N0} (Complete)", table.DisplayName, rows), InfoLevel.Success);
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    PostToConsole(string.Format("{0}: {1}", table.DisplayName, ex.Message), InfoLevel.Error);
                }
            });

            return totalrowCount;
        }

        private static long ExportQueryToJSON(ExportOptions options)
        {
            try
            {
                using (var conn = new SqlConnection(options.SourceConnectionString()))
                {
                    conn.Open();
                    using (var cmd = new SqlCommand(options.Query, conn))
                    {
                        cmd.CommandTimeout = 0;
                        using (var reader = cmd.ExecuteReader(CommandBehavior.SingleResult | CommandBehavior.SequentialAccess))
                        {
                            string filePath = Path.Combine(options.exportPath, options.OutputName + ".json");
                            long rows = WriteJsonRows(reader, filePath, options.OutputName);
                            PostToConsole(string.Format("{0}: {1:N0} (Complete)", options.OutputName, rows), InfoLevel.Success);
                            return rows;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                PostToConsole(string.Format("{0}: {1}", options.OutputName, ex.Message), InfoLevel.Error);
                return -1;
            }
        }

        private static string BuildTableSelect(SqlConnection conn, TableRef table, bool noLock)
        {
            string columns;
            string sortOrder;

            using (var cmd = new SqlCommand(TableMetadataSql, conn))
            {
                cmd.Parameters.Add("@objectId", SqlDbType.Int).Value = table.ObjectId;
                using (var reader = cmd.ExecuteReader(CommandBehavior.SingleRow))
                {
                    if (!reader.Read())
                    {
                        throw new InvalidOperationException("Could not read table metadata.");
                    }
                    columns = reader.IsDBNull(0) ? null : reader.GetString(0);
                    sortOrder = reader.IsDBNull(1) ? null : reader.GetString(1);
                }
            }

            if (string.IsNullOrEmpty(columns))
            {
                throw new InvalidOperationException("Table has no readable columns.");
            }

            string select = "SELECT " + columns + " FROM " + table.QuotedName;
            if (noLock)
            {
                select += " WITH (NOLOCK)";
            }
            if (!string.IsNullOrEmpty(sortOrder))
            {
                select += " ORDER BY " + sortOrder;
            }
            return select;
        }

        /// <summary>
        /// Writes rows as a JSON array with one object per line, so exports can be compared with line-based diff tools.
        /// NULLs are written as null; binary as base64; dates in ISO 8601.
        /// </summary>
        private static long WriteJsonRows(SqlDataReader records, string filePath, string label)
        {
            long rows = 0;
            string[] names = Enumerable.Range(0, records.FieldCount).Select(records.GetName).ToArray();

            using (var jsonWriter = new JsonTextWriter(new StreamWriter(filePath, false)))
            {
                jsonWriter.WriteStartArray();
                while (records.Read())
                {
                    jsonWriter.WriteStartObject();
                    for (int i = 0; i < names.Length; i++)
                    {
                        jsonWriter.WritePropertyName(names[i]);
                        jsonWriter.WriteValue(records.GetValue(i));
                    }
                    jsonWriter.WriteEndObject();
                    jsonWriter.WriteWhitespace(Environment.NewLine);

                    rows++;
                    if (rows % 100000 == 0)
                    {
                        PostToConsole(string.Format("{0}: {1:N0}", label, rows));
                    }
                }
                jsonWriter.WriteEndArray();
            }

            return rows;
        }

        private static long ExportToBCP(ExportOptions options)
        {
            long totalrowCount = 0;
            List<TableRef> tables = GetSourceTables(options.SourceConnectionString(),
                BuildSourceObjectIDList(options.IncludeTablesList, options.ExcludeTablesList, options.IncludeSchemaList, options.ExcludeSchemaList));

            if (tables.Count == 0)
            {
                PostToConsole("No tables matched the include/exclude filters.", InfoLevel.Warning);
                return 0;
            }

            string connectionArgs = BcpConnectionArgs(options.srcServer, options.srcDatabase, options.srcLogin, options.srcPassword, options.TrustServerCertificate);
            string outputPath = options.exportPath;

            Parallel.ForEach(tables, table =>
            {
                string name = table.DisplayName;

                // Format file (used by import to map columns)
                string formatArgs = string.Format("{0} format nul {1} -c -x -f {2} -t|",
                    Arg(table.QuotedName), connectionArgs, Arg(name + "-c.xml"));
                if (ExecuteProcess("bcp.exe", formatArgs, outputPath) != 0)
                {
                    PostToConsole(string.Format("{0}: Error creating format file.", name), InfoLevel.Error);
                    return;
                }
                PostToConsole(string.Format("{0}: Format file created.", name), InfoLevel.Success);

                // Data
                string outputLog = name + "_output.log";
                string exportArgs = string.Format("{0} out {1} {2} -c -t| -m 1 -b 10000 -K ReadOnly -e {3} -o {4}",
                    Arg(table.QuotedName), Arg(name + ".bcp"), connectionArgs, Arg(name + "_error.log"), Arg(outputLog));
                if (ExecuteProcess("bcp.exe", exportArgs, outputPath) == 0)
                {
                    long rows = ReadBcpRowCount(Path.Combine(outputPath, outputLog));
                    if (rows > 0)
                    {
                        Interlocked.Add(ref totalrowCount, rows);
                    }
                    PostToConsole(string.Format("{0}: {1:N0}", name, rows), InfoLevel.Info);
                }
                else
                {
                    PostToConsole(string.Format("{0}: bcp export failed. See {1}_error.log and {2}.", name, name, outputLog), InfoLevel.Error);
                }
            });

            return totalrowCount;
        }

        private static long ExportQueryToBCP(ExportOptions options)
        {
            if (options.Query.Contains("\""))
            {
                PostToConsole("--source-query cannot contain double quotes when exporting with bcp.", InfoLevel.Error);
                return -1;
            }

            string connectionArgs = BcpConnectionArgs(options.srcServer, options.srcDatabase, options.srcLogin, options.srcPassword, options.TrustServerCertificate);
            string name = options.OutputName;
            string outputLog = name + "_output.log";
            string args = string.Format("{0} queryout {1} {2} -c -t| -m 1 -b 10000 -K ReadOnly -e {3} -o {4}",
                Arg(options.Query), Arg(name + ".bcp"), connectionArgs, Arg(name + "_error.log"), Arg(outputLog));

            if (ExecuteProcess("bcp.exe", args, options.exportPath) != 0)
            {
                PostToConsole(string.Format("{0}: bcp export failed. See {0}_error.log and {1}.", name, outputLog), InfoLevel.Error);
                return -1;
            }

            long rows = ReadBcpRowCount(Path.Combine(options.exportPath, outputLog));
            PostToConsole(string.Format("{0}: {1:N0}", name, rows), InfoLevel.Info);
            return rows;
        }

        /// <summary>bcp.exe connection switches. Passwords are passed on the command line (bcp has no alternative); prefer Windows Auth.</summary>
        private static string BcpConnectionArgs(string server, string database, string login, string password, bool trustServerCertificate)
        {
            string args = string.Format("-S {0} -d {1} ",
                Arg(string.IsNullOrEmpty(server) ? "localhost" : server),
                Arg(string.IsNullOrEmpty(database) ? "master" : database));

            args += string.IsNullOrEmpty(login)
                ? "-T"
                : string.Format("-U {0} -P {1}", Arg(login), Arg(password));

            if (trustServerCertificate)
            {
                args += " -u";
            }
            return args;
        }

        /// <summary>Reads "N rows copied." from a bcp -o output log. Returns -1 if it cannot be found.</summary>
        private static long ReadBcpRowCount(string outputLogPath)
        {
            try
            {
                Match match = BcpRowsCopied.Match(File.ReadAllText(outputLogPath));
                return match.Success ? long.Parse(match.Groups[1].Value, CultureInfo.InvariantCulture) : -1;
            }
            catch (IOException)
            {
                return -1;
            }
        }
    }
}
