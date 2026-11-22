using Microsoft.Data.SqlClient;
using Newtonsoft.Json;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlTypes;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace bcpJson
{
    partial class Program
    {
        /// <summary>Default rows per committed batch for imports (--batch-size).</summary>
        internal const int DefaultBatchSize = 50000;

        /// <summary>A target column that can be loaded (computed and rowversion columns are skipped).</summary>
        private sealed record ImportColumn(string Name, string TypeName, bool IsIdentity, bool IsClrType);

        /// <summary>
        /// Returns the format file name for a .bcp data file: <c>&lt;name&gt;-c.fmt</c>, or the XML <c>&lt;name&gt;-c.xml</c>
        /// written by older Windows exports. Null if neither exists.
        /// </summary>
        internal static string FindBcpFormatFile(string folder, string baseFileName)
        {
            foreach (string suffix in new[] { BcpFormatFileSuffix, LegacyBcpFormatFileSuffix })
            {
                if (File.Exists(Path.Combine(folder, baseFileName + suffix)))
                {
                    return baseFileName + suffix;
                }
            }
            return null;
        }

        /// <summary>
        /// Command line for <c>bcp &lt;table&gt; in &lt;data&gt; -f &lt;format&gt; ...</c> (bcp requires the table first and the direction second).
        /// <list type="bullet">
        /// <item>-E keeps identity values and -k keeps NULLs, so the load is a faithful restore of the export.</item>
        /// <item>-h "TABLOCK" (unless --no-table-lock) enables minimal logging and avoids row/page locks.</item>
        /// <item>-q sets QUOTED_IDENTIFIER ON, without which SQL Server rejects inserts into tables with filtered, XML or
        /// spatial indexes (and indexed views/computed columns). With -q, bcp splits the table name on '.' and quotes
        /// each part itself, so the name must be passed unbracketed as schema.table (verified against SQL Server 2022
        /// with names containing spaces, ']' and '''). A name containing '.' or '"' cannot be expressed that way and is
        /// passed bracketed without -q, which works for every table except those index types.</item>
        /// <item>-b commits every <paramref name="batchSize"/> rows; 0 leaves it out, so the whole file is one transaction.</item>
        /// </list>
        /// </summary>
        internal static string BcpInArguments(string schema, string table, string dataFile, string formatFile, string connectionArgs, bool noTableLock, string errorLog, int batchSize)
        {
            bool splittable = schema.IndexOf('.') < 0 && table.IndexOf('.') < 0 && schema.IndexOf('"') < 0 && table.IndexOf('"') < 0;
            string tableArg = splittable ? Arg(schema + "." + table) : Arg(QuoteName(schema) + "." + QuoteName(table));
            string batch = batchSize > 0 ? " -b " + batchSize.ToString(CultureInfo.InvariantCulture) : string.Empty;
            return string.Format("{0} in {1} -f {2} {3} -E -k{4}{5}{6} -e {7}",
                tableArg, Arg(dataFile), Arg(formatFile), connectionArgs, splittable ? " -q" : string.Empty, batch, BcpLoadHints(noTableLock), Arg(errorLog));
        }

        /// <summary>The table name a data file loads into by default: the file name without .gz and its extension.</summary>
        internal static string ImportBaseName(string fileName)
        {
            return Path.GetFileNameWithoutExtension(FileCompression.WithoutGzipExtension(fileName));
        }

        /// <summary>False for the export manifest and for unfinished ".partial" files, which a wildcard like *.json or * may match.</summary>
        internal static bool IsImportableFile(string name)
        {
            return !name.Equals(ExportManifest.FileName, StringComparison.OrdinalIgnoreCase)
                && !name.EndsWith(PartialSuffix, StringComparison.OrdinalIgnoreCase);
        }

        private static FileInfo[] FindSourceFiles(ImportOptions options)
        {
            FileInfo[] files = new DirectoryInfo(options.SourcePath).GetFiles(options.SourceFile, SearchOption.TopDirectoryOnly)
                .Where(f => IsImportableFile(f.Name))
                .ToArray();
            if (files.Length == 0)
            {
                PostToConsole(string.Format("No files matching '{0}' in {1}.", options.SourceFile, options.SourcePath), InfoLevel.Warning);
            }
            return files;
        }

        private static long ImportFromBCP(ImportOptions options)
        {
            long totalrowCount = 0;
            string connectionString = options.GetTargetConnectionString();
            string connectionArgs = BcpConnectionArgs(options.TargetSQLServer, options.TargetSQLDatabase, options.TargetSQLLogin, options.TargetSQLPassword, options.TrustServerCertificate);
            string workingPath = options.SourcePath;

            Parallel.ForEach(FindSourceFiles(options), Parallelism(options.MaxParallelTasks), file =>
            {
                string baseFileName = ImportBaseName(file.Name);
                string tableName = string.IsNullOrEmpty(options.TargetTable) ? baseFileName : options.TargetTable;
                string tempFolder = null;

                try
                {
                    string[] target = GetTableObject(tableName, connectionString);
                    if (target == null)
                    {
                        PostToConsole(string.Format("{0}: Target table does not exist. Create it before importing with bcp.", tableName), InfoLevel.Error);
                        return;
                    }
                    string qualifiedName = QuoteName(target[0]) + "." + QuoteName(target[1]);
                    string label = target[0] + "." + target[1];

                    string formatFile = FindBcpFormatFile(workingPath, baseFileName);
                    if (formatFile == null)
                    {
                        PostToConsole(string.Format("{0}: Format file {1}{2} (or {1}{3}) not found next to the data file.",
                            label, baseFileName, BcpFormatFileSuffix, LegacyBcpFormatFileSuffix), InfoLevel.Error);
                        return;
                    }

                    if (options.TruncateTarget)
                    {
                        using (var conn = SqlConnectionFactory.Create(connectionString))
                        {
                            conn.Open();
                            ExecuteSql(conn, "TRUNCATE TABLE " + qualifiedName + ";");
                        }
                        PostToConsole(string.Format("{0}: Truncate Complete.", label));
                    }

                    // bcp cannot read gzip: decompress to a temporary folder (the source folder may be read-only).
                    string dataFile = file.Name;
                    if (FileCompression.IsCompressedFile(file.Name))
                    {
                        tempFolder = Path.Combine(Path.GetTempPath(), "bcpjson", Guid.NewGuid().ToString("N"));
                        Directory.CreateDirectory(tempFolder);
                        dataFile = Path.Combine(tempFolder, baseFileName + ".bcp");
                        PostToConsole(string.Format("{0}: Decompressing {1}.", label, file.Name));
                        FileCompression.DecompressFile(file.FullName, dataFile);
                    }

                    string outputLog = baseFileName + "_import_output.log";
                    string args = BcpInArguments(target[0], target[1], dataFile, formatFile, connectionArgs, options.NoTableLock, baseFileName + "_import_error.log", options.BatchSize);

                    if (ExecuteProcess(BcpTool, args, workingPath, outputLog) == 0)
                    {
                        long rows = ReadBcpRowCount(Path.Combine(workingPath, outputLog));
                        if (rows > 0)
                        {
                            Interlocked.Add(ref totalrowCount, rows);
                        }
                        PostToConsole(string.Format("{0}: {1:N0} (Import Complete)", label, rows), InfoLevel.Success);
                    }
                    else
                    {
                        PostToConsole(string.Format("{0}: Error importing data. See {1}_import_error.log and {2}.", file.FullName, baseFileName, outputLog), InfoLevel.Error);
                    }
                }
                catch (Exception ex)
                {
                    PostToConsole(string.Format("{0}: {1}", file.Name, ex.Message), InfoLevel.Error);
                }
                finally
                {
                    DeleteTempFolder(tempFolder);
                }
            });

            return totalrowCount;
        }

        private static void DeleteTempFolder(string folder)
        {
            if (folder == null)
            {
                return;
            }
            try
            {
                Directory.Delete(folder, true);
            }
            catch (Exception ex) when (ex is IOException || ex is UnauthorizedAccessException)
            {
                PostToConsole(string.Format("Could not delete temporary folder {0}: {1}", folder, ex.Message), InfoLevel.Warning);
            }
        }

        private static long ImportFromJSON(ImportOptions options)
        {
            long totalrowCount = 0;

            Parallel.ForEach(FindSourceFiles(options), Parallelism(options.MaxParallelTasks), file =>
            {
                string tableName = string.IsNullOrEmpty(options.TargetTable) ? ImportBaseName(file.Name) : options.TargetTable;
                try
                {
                    long rows = ImportJsonFile(file.FullName, tableName, options);
                    Interlocked.Add(ref totalrowCount, rows);
                    PostToConsole(string.Format("{0}: {1:N0} (Import Complete)", tableName, rows), InfoLevel.Success);
                }
                catch (Exception ex)
                {
                    PostToConsole(string.Format("{0}: {1}", file.Name, ex.Message), InfoLevel.Error);
                }
            });

            return totalrowCount;
        }

        /// <summary>
        /// Streams one exported JSON file (an array of row objects; .gz files are decompressed on the fly) into an
        /// existing table: SqlBulkCopy reads the rows through a <see cref="JsonRowReader"/>, committing every
        /// --batch-size rows. Values are converted using the target column types. Tables with CLR-type columns (geography, geometry,
        /// hierarchyid) are loaded through a #temp staging table holding those columns as text, then inserted with ::Parse().
        /// </summary>
        private static long ImportJsonFile(string filePath, string tableName, ImportOptions options)
        {
            string connectionString = options.GetTargetConnectionString();
            string[] target = GetTableObject(tableName, connectionString);
            if (target == null)
            {
                throw new InvalidOperationException("Target table does not exist. Create it before importing.");
            }

            string qualifiedName = QuoteName(target[0]) + "." + QuoteName(target[1]);
            string label = target[0] + "." + target[1];

            using (var conn = SqlConnectionFactory.Create(connectionString))
            {
                conn.Open();

                List<ImportColumn> columns = GetImportColumns(conn, int.Parse(target[2], CultureInfo.InvariantCulture));
                if (columns.Count == 0)
                {
                    throw new InvalidOperationException("Target table has no loadable columns.");
                }

                if (options.TruncateTarget)
                {
                    ExecuteSql(conn, "TRUNCATE TABLE " + qualifiedName + ";");
                    PostToConsole(string.Format("{0}: Truncate Complete.", label));
                }

                bool useStaging = columns.Any(c => c.IsClrType);
                string bulkTarget = qualifiedName;
                if (useStaging)
                {
                    string stageColumns = string.Join(", ", columns.Select(c => c.IsClrType
                        ? "CAST(NULL AS NVARCHAR(MAX)) AS " + QuoteName(c.Name)
                        : QuoteName(c.Name)));
                    ExecuteSql(conn, "SELECT TOP (0) " + stageColumns + " INTO #bcpjson_stage FROM " + qualifiedName + ";");
                    bulkTarget = "#bcpjson_stage";
                }

                List<(string Name, Type Type)> bulkColumns = GetBulkColumns(conn, bulkTarget, columns);
                long rows;
                using (var bulkCopy = new SqlBulkCopy(conn, LoadOptions(options.NoTableLock), null))
                {
                    bulkCopy.DestinationTableName = bulkTarget;
                    bulkCopy.BulkCopyTimeout = 0;
                    bulkCopy.BatchSize = options.BatchSize;
                    bulkCopy.NotifyAfter = options.BatchSize > 0 ? options.BatchSize : DefaultBatchSize;
                    bulkCopy.SqlRowsCopied += (sender, e) => PostToConsole(string.Format("{0}: {1:N0}", label, e.RowsCopied));
                    foreach (var column in bulkColumns)
                    {
                        bulkCopy.ColumnMappings.Add(column.Name, column.Name);
                    }

                    using (StreamReader text = FileCompression.OpenText(filePath))
                    using (var reader = new JsonRowReader(text, bulkColumns.Select(c => c.Name).ToList(), bulkColumns.Select(c => c.Type).ToList()))
                    {
                        bulkCopy.WriteToServer(reader);
                        rows = reader.RowCount;
                    }
                }

                if (useStaging)
                {
                    bool hasIdentity = columns.Any(c => c.IsIdentity);
                    string columnList = string.Join(", ", columns.Select(c => QuoteName(c.Name)));
                    string selectList = string.Join(", ", columns.Select(c => c.IsClrType
                        ? c.TypeName + "::Parse(" + QuoteName(c.Name) + ")"
                        : QuoteName(c.Name)));

                    string sql = (hasIdentity ? "SET IDENTITY_INSERT " + qualifiedName + " ON; " : string.Empty)
                        + "INSERT INTO " + qualifiedName + " WITH (TABLOCK) (" + columnList + ") SELECT " + selectList + " FROM #bcpjson_stage; "
                        + (hasIdentity ? "SET IDENTITY_INSERT " + qualifiedName + " OFF; " : string.Empty)
                        + "DROP TABLE #bcpjson_stage;";
                    ExecuteSql(conn, sql);
                }

                return rows;
            }
        }

        private static List<ImportColumn> GetImportColumns(SqlConnection conn, int objectId)
        {
            const string sql = @"SELECT c.name, t.name AS type_name, c.is_identity, t.is_assembly_type
FROM sys.columns c
     INNER JOIN sys.types t ON t.user_type_id = c.user_type_id
WHERE c.object_id = @objectId
      AND c.is_computed = 0
      AND c.system_type_id <> 189 /* timestamp/rowversion */
ORDER BY c.column_id;";

            var columns = new List<ImportColumn>();
            using (var cmd = new SqlCommand(sql, conn))
            {
                cmd.Parameters.Add("@objectId", SqlDbType.Int).Value = objectId;
                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        columns.Add(new ImportColumn(reader.GetString(0), reader.GetString(1), reader.GetBoolean(2), reader.GetBoolean(3)));
                    }
                }
            }
            return columns;
        }

        /// <summary>Names and .NET types of the bulk copy destination's columns (what JSON values are converted to).</summary>
        private static List<(string Name, Type Type)> GetBulkColumns(SqlConnection conn, string bulkTarget, List<ImportColumn> columns)
        {
            var result = new List<(string Name, Type Type)>();
            string sql = "SELECT TOP (0) " + string.Join(", ", columns.Select(c => QuoteName(c.Name))) + " FROM " + bulkTarget + ";";
            using (var cmd = new SqlCommand(sql, conn))
            using (var reader = cmd.ExecuteReader())
            {
                for (int i = 0; i < reader.FieldCount; i++)
                {
                    // decimal(p > 28) values may not fit System.Decimal, so decimal columns are buffered as SqlDecimal.
                    Type type = reader.GetFieldType(i);
                    result.Add((reader.GetName(i), IsSqlDecimalType(reader.GetDataTypeName(i)) ? typeof(SqlDecimal) : type));
                }
            }
            return result;
        }

        /// <summary>Parses decimal text exactly (up to 38 digits); exponent notation goes through System.Decimal.</summary>
        internal static SqlDecimal ParseSqlDecimal(string text)
        {
            text = text.Trim();
            if (text.IndexOfAny(new[] { 'e', 'E' }) >= 0)
            {
                return new SqlDecimal(decimal.Parse(text, NumberStyles.Float, CultureInfo.InvariantCulture));
            }
            return SqlDecimal.Parse(text);
        }

        /// <summary>Reads the next JSON value (the reader is on a property name) and converts it to the column type.</summary>
        internal static object ReadJsonValue(JsonTextReader json, Type type)
        {
            if (type == typeof(SqlDecimal))
            {
                // ReadAsString keeps the number's text, so up to 38 significant digits survive.
                string number = json.ReadAsString();
                return number == null ? (object)DBNull.Value : ParseSqlDecimal(number);
            }
            if (type == typeof(decimal))
            {
                decimal? value = json.ReadAsDecimal();
                return value.HasValue ? (object)value.Value : DBNull.Value;
            }
            if (type == typeof(double))
            {
                double? value = json.ReadAsDouble();
                return value.HasValue ? (object)value.Value : DBNull.Value;
            }
            if (type == typeof(int))
            {
                int? value = json.ReadAsInt32();
                return value.HasValue ? (object)value.Value : DBNull.Value;
            }
            if (type == typeof(bool))
            {
                bool? value = json.ReadAsBoolean();
                return value.HasValue ? (object)value.Value : DBNull.Value;
            }
            if (type == typeof(byte[]))
            {
                byte[] value = json.ReadAsBytes();
                return value ?? (object)DBNull.Value;
            }

            string text = json.ReadAsString();
            if (text == null)
            {
                return DBNull.Value;
            }

            if (type == typeof(string) || type == typeof(object))
            {
                return text;
            }
            if (type == typeof(long))
            {
                return long.Parse(text, NumberStyles.Integer, CultureInfo.InvariantCulture);
            }
            if (type == typeof(short))
            {
                return short.Parse(text, NumberStyles.Integer, CultureInfo.InvariantCulture);
            }
            if (type == typeof(byte))
            {
                return byte.Parse(text, NumberStyles.Integer, CultureInfo.InvariantCulture);
            }
            if (type == typeof(float))
            {
                return float.Parse(text, NumberStyles.Float, CultureInfo.InvariantCulture);
            }
            if (type == typeof(Guid))
            {
                return Guid.Parse(text);
            }
            if (type == typeof(DateTime))
            {
                return DateTime.Parse(text, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);
            }
            if (type == typeof(DateTimeOffset))
            {
                return DateTimeOffset.Parse(text, CultureInfo.InvariantCulture, DateTimeStyles.None);
            }
            if (type == typeof(TimeSpan))
            {
                return TimeSpan.Parse(text, CultureInfo.InvariantCulture);
            }

            return Convert.ChangeType(text, type, CultureInfo.InvariantCulture);
        }
    }
}
