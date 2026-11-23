using Microsoft.Data.SqlClient;
using Newtonsoft.Json;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlTypes;
using System.Globalization;
using System.IO;
using System.IO.Compression;
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
        /// and can be re-imported with ::Parse(). geography/geometry values whose SRID is not the one ::Parse() assumes
        /// (4326 / 0) are written as "SRID=n;WKT" (see <see cref="ClrFromText"/>). Rows are ordered by the primary key, or by every sortable column
        /// when there is no primary key. Keep CustomSelect.sql in sync with this text.
        /// </summary>
        private const string TableMetadataSql = @"SET NOCOUNT ON;
DECLARE @Columns NVARCHAR(MAX), @SortOrder NVARCHAR(MAX);

SELECT   @Columns = COALESCE(@Columns + N', ', N'') +
                    CASE WHEN sc.system_type_id = 240 AND TYPE_NAME(sc.user_type_id) IN (N'geography', N'geometry')
                         /* SRIDs other than the one ::Parse() assumes (geography 4326, geometry 0) get an SRID=n; prefix. */
                         THEN N'CASE WHEN ' + QUOTENAME(sc.name) + N'.STSrid = '
                              + CASE TYPE_NAME(sc.user_type_id) WHEN N'geography' THEN N'4326' ELSE N'0' END
                              + N' THEN ' + QUOTENAME(sc.name) + N'.ToString() ELSE N''SRID='' + CAST('
                              + QUOTENAME(sc.name) + N'.STSrid AS NVARCHAR(11)) + N'';'' + ' + QUOTENAME(sc.name)
                              + N'.ToString() END AS ' + QUOTENAME(sc.name)
                         WHEN sc.system_type_id = 240
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

        /// <summary>The tables of the export, minus those --resume finds already exported and intact.</summary>
        private static List<TableRef> TablesToExport(ExportOptions options, ExportManifest manifest)
        {
            List<TableRef> tables = GetSourceTables(options.SourceConnectionString(),
                BuildSourceObjectIDList(options.IncludeTablesList, options.ExcludeTablesList, options.IncludeSchemaList, options.ExcludeSchemaList));

            if (tables.Count == 0)
            {
                PostToConsole("No tables matched the include/exclude filters.", InfoLevel.Warning);
                return tables;
            }

            if (options.Resume)
            {
                List<TableRef> remaining = tables.Where(t => !manifest.IsIntact(t.DisplayName, options.exportPath, false)).ToList();
                PostToConsole(string.Format("--resume: {0} of {1} table(s) already exported, {2} to go.",
                    tables.Count - remaining.Count, tables.Count, remaining.Count));
                return remaining;
            }
            return tables;
        }

        /// <summary>A manifest entry for an exported table or query: its row count and the size and hash of its files.</summary>
        private static ManifestTable ManifestEntry(string name, long rows, string folder, params string[] files)
        {
            return new ManifestTable
            {
                Name = name,
                Rows = rows,
                Exported = DateTime.UtcNow,
                Files = files.Select(f => ManifestFile.Describe(folder, f)).ToList()
            };
        }

        private static long ExportToJSON(ExportOptions options, ExportManifest manifest)
        {
            long totalrowCount = 0;
            List<TableRef> tables = TablesToExport(options, manifest);

            Parallel.ForEach(tables, Parallelism(options.MaxParallelTasks), table =>
            {
                string fileName = options.DataFileName(table.DisplayName, ".json");
                try
                {
                    // Exporting a table only rewrites its file, so a transient failure retries the whole table.
                    var (rows, orderedBy) = WithRetries(table.DisplayName, options.Retries,
                        () => ExportTableToJson(options, table, Path.Combine(options.exportPath, fileName)), IsTransientSqlError);
                    ManifestTable entry = ManifestEntry(table.DisplayName, rows, options.exportPath, fileName);
                    entry.OrderedBy = orderedBy;
                    manifest.Record(entry, options.exportPath);
                    Interlocked.Add(ref totalrowCount, rows);
                    PostToConsole(string.Format("{0}: {1:N0} (Complete)", table.DisplayName, rows), InfoLevel.Success);
                }
                catch (Exception ex)
                {
                    PostToConsole(string.Format("{0}: {1}", table.DisplayName, ex.Message), InfoLevel.Error);
                }
            });

            return totalrowCount;
        }

        /// <summary>
        /// Writes one table to &lt;file&gt;.partial and renames it to <paramref name="filePath"/> when complete. Returns the
        /// row count and the primary key the rows are sorted by (null without one), which import uses for order hints.
        /// </summary>
        private static (long Rows, List<ManifestKeyColumn> OrderedBy) ExportTableToJson(ExportOptions options, TableRef table, string filePath)
        {
            try
            {
                using (var conn = SqlConnectionFactory.Create(options.SourceConnectionString()))
                {
                    conn.Open();
                    string select = BuildTableSelect(conn, table, options.NoLock);
                    List<ManifestKeyColumn> orderedBy = ReadKeyColumns(conn, table.ObjectId, KeyKind.PrimaryKey);

                    using (var cmd = new SqlCommand(select, conn))
                    {
                        cmd.CommandTimeout = 0;
                        using (var reader = cmd.ExecuteReader(CommandBehavior.SingleResult | CommandBehavior.SequentialAccess))
                        {
                            long rows = WriteJsonRows(reader, PartialPath(filePath), table.DisplayName, options.GzipLevel);
                            CommitPartial(filePath);
                            return (rows, orderedBy.Count > 0 ? orderedBy : null);
                        }
                    }
                }
            }
            catch
            {
                DiscardPartial(filePath);
                throw;
            }
        }

        internal enum KeyKind
        {
            /// <summary>The primary key, which JSON export sorts by.</summary>
            PrimaryKey,

            /// <summary>The clustered rowstore index, the order SQL Server stores rows in.</summary>
            Clustered
        }

        /// <summary>The key columns of a table's primary key or clustered index, in key order; empty when it has none.</summary>
        private static List<ManifestKeyColumn> ReadKeyColumns(SqlConnection conn, int objectId, KeyKind kind)
        {
            string sql = @"SELECT c.name, TYPE_NAME(c.system_type_id), ic.is_descending_key, c.collation_name
FROM sys.indexes i
     INNER JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
     INNER JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
WHERE i.object_id = @objectId
      AND " + (kind == KeyKind.PrimaryKey ? "i.is_primary_key = 1" : "i.type = 1") + @"
      AND ic.key_ordinal > 0
ORDER BY ic.key_ordinal;";

            var key = new List<ManifestKeyColumn>();
            using (var cmd = new SqlCommand(sql, conn))
            {
                cmd.Parameters.Add("@objectId", SqlDbType.Int).Value = objectId;
                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        key.Add(new ManifestKeyColumn
                        {
                            Column = reader.GetString(0),
                            Type = reader.GetString(1),
                            Descending = reader.GetBoolean(2),
                            Collation = reader.IsDBNull(3) ? null : reader.GetString(3)
                        });
                    }
                }
            }
            return key;
        }

        /// <summary>
        /// --source-query to JSON. Not retried (the query may be a procedure with side effects); only the connection
        /// open is.
        /// </summary>
        private static long ExportQueryToJSON(ExportOptions options, ExportManifest manifest)
        {
            string fileName = options.DataFileName(options.OutputName, ".json");
            string filePath = Path.Combine(options.exportPath, fileName);
            if (options.Resume && manifest.IsIntact(options.OutputName, options.exportPath, false))
            {
                PostToConsole(string.Format("--resume: {0} already exported.", fileName));
                return 0;
            }

            try
            {
                using (var conn = SqlConnectionFactory.Create(options.SourceConnectionString()))
                {
                    conn.Open();
                    using (var cmd = new SqlCommand(options.Query, conn))
                    {
                        cmd.CommandTimeout = 0;
                        using (var reader = cmd.ExecuteReader(CommandBehavior.SingleResult | CommandBehavior.SequentialAccess))
                        {
                            long rows = WriteJsonRows(reader, PartialPath(filePath), options.OutputName, options.GzipLevel);
                            CommitPartial(filePath);
                            manifest.Record(ManifestEntry(options.OutputName, rows, options.exportPath, fileName), options.exportPath);
                            PostToConsole(string.Format("{0}: {1:N0} (Complete)", options.OutputName, rows), InfoLevel.Success);
                            return rows;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                DiscardPartial(filePath);
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
        /// <summary>
        /// True when a SQL decimal fits System.Decimal (96-bit mantissa, scale up to 28). decimal(38, s) values can
        /// have up to 38 significant digits, which System.Decimal cannot hold.
        /// </summary>
        /// <summary>
        /// True for decimal/numeric. money/smallmoney also map to System.Decimal but always fit it, and their readers
        /// return SqlMoney, so they keep the System.Decimal path.
        /// </summary>
        internal static bool IsSqlDecimalType(string dataTypeName)
        {
            return string.Equals(dataTypeName, "decimal", StringComparison.OrdinalIgnoreCase)
                || string.Equals(dataTypeName, "numeric", StringComparison.OrdinalIgnoreCase);
        }

        internal static bool FitsSystemDecimal(SqlDecimal value)
        {
            return value.IsNull || (value.Data[3] == 0 && value.Scale <= 28);
        }

        /// <summary>
        /// Writes a non-NULL decimal as a JSON number. Values that fit System.Decimal are written exactly as before;
        /// larger ones are written from SqlDecimal's own (culture-invariant) text, so no digit is lost.
        /// </summary>
        internal static void WriteJsonDecimal(JsonWriter writer, SqlDecimal value)
        {
            if (FitsSystemDecimal(value))
            {
                writer.WriteValue(value.Value);
            }
            else
            {
                writer.WriteRawValue(value.ToString());
            }
        }

        /// <summary>Writes the rows as a JSON array, one row object per line; gzip-compressed when <paramref name="gzipLevel"/> is set.</summary>
        private static long WriteJsonRows(SqlDataReader records, string filePath, string label, CompressionLevel? gzipLevel)
        {
            long rows = 0;
            string[] names = Enumerable.Range(0, records.FieldCount).Select(records.GetName).ToArray();
            bool[] isDecimal = Enumerable.Range(0, records.FieldCount).Select(i => IsSqlDecimalType(records.GetDataTypeName(i))).ToArray();

            using (var jsonWriter = new JsonTextWriter(FileCompression.CreateText(filePath, gzipLevel)))
            {
                jsonWriter.WriteStartArray();
                while (records.Read())
                {
                    jsonWriter.WriteStartObject();
                    for (int i = 0; i < names.Length; i++)
                    {
                        jsonWriter.WritePropertyName(names[i]);
                        if (isDecimal[i] && !records.IsDBNull(i))
                        {
                            WriteJsonDecimal(jsonWriter, records.GetSqlDecimal(i));
                        }
                        else
                        {
                            jsonWriter.WriteValue(records.GetValue(i));
                        }
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

        private static long ExportToBCP(ExportOptions options, ExportManifest manifest)
        {
            long totalrowCount = 0;
            List<TableRef> tables = TablesToExport(options, manifest);
            string connectionArgs = BcpConnectionArgs(options.srcServer, options.srcDatabase, options.srcLogin, options.srcPassword, options.TrustServerCertificate);
            string outputPath = options.exportPath;

            Parallel.ForEach(tables, Parallelism(options.MaxParallelTasks), table =>
            {
                string name = table.DisplayName;
                try
                {
                    // bcp reports no error numbers, so any bcp failure is retried; the table's files are simply rewritten.
                    string dataFile = options.DataFileName(name, ".bcp");
                    long rows = WithRetries(name, options.Retries,
                        () => ExportTableToBcp(table, connectionArgs, outputPath, options.GzipLevel), ex => ex is BcpFailedException);
                    manifest.Record(ManifestEntry(name, rows, outputPath, dataFile, name + BcpFormatFileSuffix), outputPath);
                    if (rows > 0)
                    {
                        Interlocked.Add(ref totalrowCount, rows);
                    }
                    PostToConsole(string.Format("{0}: {1:N0}", name, rows), InfoLevel.Info);
                }
                catch (Exception ex)
                {
                    PostToConsole(string.Format("{0}: {1}", name, ex.Message), InfoLevel.Error);
                }
            });

            return totalrowCount;
        }

        /// <summary>
        /// Writes a table's format and data files as .partial files and renames both once bcp has succeeded.
        /// With <paramref name="gzipLevel"/>, the data file is then compressed to &lt;name&gt;.bcp.gz (bcp cannot write
        /// to a compressed stream). Throws <see cref="BcpFailedException"/> when bcp fails.
        /// </summary>
        private static long ExportTableToBcp(TableRef table, string connectionArgs, string outputPath, CompressionLevel? gzipLevel)
        {
            string name = table.DisplayName;
            string formatPath = Path.Combine(outputPath, name + BcpFormatFileSuffix);
            string dataPath = Path.Combine(outputPath, name + ".bcp");
            string gzipPath = dataPath + FileCompression.GzipExtension;
            try
            {
                // Format file (used by import to map columns). Non-XML, because the Linux bcp does not support -x.
                // "nul" is the no-data-file placeholder on Windows; on Linux bcp would create a file named "nul".
                string formatArgs = string.Format("{0} format {1} {2} -c -f {3} -t|",
                    Arg(table.QuotedName), OperatingSystem.IsWindows() ? "nul" : "/dev/null", connectionArgs, Arg(PartialPath(name + BcpFormatFileSuffix)));
                if (ExecuteProcess(BcpTool, formatArgs, outputPath) != 0)
                {
                    throw new BcpFailedException("Error creating format file.");
                }
                PostToConsole(string.Format("{0}: Format file created.", name), InfoLevel.Success);

                // Data
                string outputLog = name + "_output.log";
                string exportArgs = string.Format("{0} out {1} {2} -c -t| -m 1 -b 10000 -K ReadOnly -e {3}",
                    Arg(table.QuotedName), Arg(PartialPath(name + ".bcp")), connectionArgs, Arg(name + "_error.log"));
                if (ExecuteProcess(BcpTool, exportArgs, outputPath, outputLog) != 0)
                {
                    throw new BcpFailedException(string.Format("bcp export failed. See {0}_error.log and {1}.", name, outputLog));
                }

                CommitData(dataPath, gzipLevel);
                CommitPartial(formatPath);
                return ReadBcpRowCount(Path.Combine(outputPath, outputLog));
            }
            catch
            {
                DiscardPartial(formatPath);
                DiscardPartial(dataPath);
                DiscardPartial(gzipPath);
                throw;
            }
        }

        /// <summary>
        /// Renames bcp's finished &lt;data&gt;.partial into place, or with <paramref name="gzipLevel"/> compresses it to
        /// &lt;data&gt;.gz (via its own .partial) and deletes the uncompressed file.
        /// </summary>
        private static void CommitData(string dataPath, CompressionLevel? gzipLevel)
        {
            if (!gzipLevel.HasValue)
            {
                CommitPartial(dataPath);
                return;
            }

            string gzipPath = dataPath + FileCompression.GzipExtension;
            FileCompression.CompressFile(PartialPath(dataPath), PartialPath(gzipPath), gzipLevel.Value);
            CommitPartial(gzipPath);
            DiscardPartial(dataPath);
        }

        /// <summary>--source-query with bcp queryout. Not retried (the query may have side effects).</summary>
        private static long ExportQueryToBCP(ExportOptions options, ExportManifest manifest)
        {
            if (options.Query.Contains("\""))
            {
                PostToConsole("--source-query cannot contain double quotes when exporting with bcp.", InfoLevel.Error);
                return -1;
            }

            string name = options.OutputName;
            string dataFile = options.DataFileName(name, ".bcp");
            string dataPath = Path.Combine(options.exportPath, name + ".bcp");
            if (options.Resume && manifest.IsIntact(name, options.exportPath, false))
            {
                PostToConsole(string.Format("--resume: {0} already exported.", dataFile));
                return 0;
            }

            string connectionArgs = BcpConnectionArgs(options.srcServer, options.srcDatabase, options.srcLogin, options.srcPassword, options.TrustServerCertificate);
            string outputLog = name + "_output.log";
            string args = string.Format("{0} queryout {1} {2} -c -t| -m 1 -b 10000 -K ReadOnly -e {3}",
                Arg(options.Query), Arg(PartialPath(name + ".bcp")), connectionArgs, Arg(name + "_error.log"));

            if (ExecuteProcess(BcpTool, args, options.exportPath, outputLog) != 0)
            {
                DiscardPartial(dataPath);
                PostToConsole(string.Format("{0}: bcp export failed. See {0}_error.log and {1}.", name, outputLog), InfoLevel.Error);
                return -1;
            }

            try
            {
                CommitData(dataPath, options.GzipLevel);
            }
            catch (Exception ex)
            {
                DiscardPartial(dataPath);
                DiscardPartial(dataPath + FileCompression.GzipExtension);
                PostToConsole(string.Format("{0}: {1}", name, ex.Message), InfoLevel.Error);
                return -1;
            }
            long rows = ReadBcpRowCount(Path.Combine(options.exportPath, outputLog));
            manifest.Record(ManifestEntry(name, rows, options.exportPath, dataFile), options.exportPath);
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
