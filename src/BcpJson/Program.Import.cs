using Microsoft.Data.SqlClient;
using Newtonsoft.Json;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace bcpJson
{
    partial class Program
    {
        private const int JsonImportBatchSize = 50000;

        /// <summary>A target column that can be loaded (computed and rowversion columns are skipped).</summary>
        private sealed record ImportColumn(string Name, string TypeName, bool IsIdentity, bool IsClrType);

        private static FileInfo[] FindSourceFiles(ImportOptions options)
        {
            FileInfo[] files = new DirectoryInfo(options.SourcePath).GetFiles(options.SourceFile, SearchOption.TopDirectoryOnly);
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

            Parallel.ForEach(FindSourceFiles(options), file =>
            {
                string baseFileName = Path.GetFileNameWithoutExtension(file.Name);
                string tableName = string.IsNullOrEmpty(options.TargetTable) ? baseFileName : options.TargetTable;

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

                    string formatFile = baseFileName + "-c.xml";
                    if (!File.Exists(Path.Combine(workingPath, formatFile)))
                    {
                        PostToConsole(string.Format("{0}: Format file {1} not found next to the data file.", label, formatFile), InfoLevel.Error);
                        return;
                    }

                    if (options.TruncateTarget)
                    {
                        using (var conn = new SqlConnection(connectionString))
                        {
                            conn.Open();
                            ExecuteSql(conn, "TRUNCATE TABLE " + qualifiedName + ";");
                        }
                        PostToConsole(string.Format("{0}: Truncate Complete.", label));
                    }

                    // -E keeps identity values and -k keeps NULLs, so the load is a faithful restore of the export.
                    string outputLog = baseFileName + "_import_output.log";
                    string args = string.Format("{0} in {1} -f {2} {3} -E -k -b 10000 -e {4} -o {5}",
                        Arg(qualifiedName), Arg(file.Name), Arg(formatFile), connectionArgs, Arg(baseFileName + "_import_error.log"), Arg(outputLog));

                    if (ExecuteProcess(BcpTool, args, workingPath) == 0)
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
            });

            return totalrowCount;
        }

        private static long ImportFromJSON(ImportOptions options)
        {
            long totalrowCount = 0;

            Parallel.ForEach(FindSourceFiles(options), file =>
            {
                string tableName = string.IsNullOrEmpty(options.TargetTable) ? Path.GetFileNameWithoutExtension(file.Name) : options.TargetTable;
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
        /// Streams one exported JSON file (an array of row objects) into an existing table with SqlBulkCopy.
        /// Values are converted using the target column types. Tables with CLR-type columns (geography, geometry,
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

            using (var conn = new SqlConnection(connectionString))
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

                DataTable buffer = CreateBuffer(conn, bulkTarget, columns);
                var ordinals = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
                foreach (DataColumn dc in buffer.Columns)
                {
                    ordinals[dc.ColumnName] = dc.Ordinal;
                }

                long rows = 0;
                var bulkOptions = SqlBulkCopyOptions.KeepIdentity | SqlBulkCopyOptions.KeepNulls | SqlBulkCopyOptions.TableLock;
                using (var bulkCopy = new SqlBulkCopy(conn, bulkOptions, null))
                {
                    bulkCopy.DestinationTableName = bulkTarget;
                    bulkCopy.BulkCopyTimeout = 0;
                    bulkCopy.BatchSize = JsonImportBatchSize;
                    foreach (DataColumn dc in buffer.Columns)
                    {
                        bulkCopy.ColumnMappings.Add(dc.ColumnName, dc.ColumnName);
                    }

                    using (var streamReader = new StreamReader(filePath))
                    using (var json = new JsonTextReader(streamReader))
                    {
                        json.DateParseHandling = DateParseHandling.None;
                        json.FloatParseHandling = FloatParseHandling.Decimal;

                        if (!json.Read() || json.TokenType != JsonToken.StartArray)
                        {
                            throw new InvalidDataException("Expected a JSON array of row objects.");
                        }

                        while (json.Read() && json.TokenType != JsonToken.EndArray)
                        {
                            if (json.TokenType != JsonToken.StartObject)
                            {
                                throw new InvalidDataException(string.Format("Expected a row object at line {0}.", json.LineNumber));
                            }

                            DataRow row = buffer.NewRow();
                            while (json.Read() && json.TokenType != JsonToken.EndObject)
                            {
                                string property = (string)json.Value;
                                if (ordinals.TryGetValue(property, out int ordinal))
                                {
                                    row[ordinal] = ReadJsonValue(json, buffer.Columns[ordinal].DataType);
                                }
                                else
                                {
                                    json.Read();
                                    json.Skip();
                                }
                            }
                            buffer.Rows.Add(row);
                            rows++;

                            if (buffer.Rows.Count >= JsonImportBatchSize)
                            {
                                bulkCopy.WriteToServer(buffer);
                                buffer.Clear();
                                PostToConsole(string.Format("{0}: {1:N0}", label, rows));
                            }
                        }
                    }

                    if (buffer.Rows.Count > 0)
                    {
                        bulkCopy.WriteToServer(buffer);
                        buffer.Clear();
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

        /// <summary>Creates an empty DataTable whose column types match the bulk copy destination.</summary>
        private static DataTable CreateBuffer(SqlConnection conn, string bulkTarget, List<ImportColumn> columns)
        {
            var buffer = new DataTable();
            string sql = "SELECT TOP (0) " + string.Join(", ", columns.Select(c => QuoteName(c.Name))) + " FROM " + bulkTarget + ";";
            using (var cmd = new SqlCommand(sql, conn))
            using (var reader = cmd.ExecuteReader())
            {
                for (int i = 0; i < reader.FieldCount; i++)
                {
                    buffer.Columns.Add(reader.GetName(i), reader.GetFieldType(i));
                }
            }
            return buffer;
        }

        /// <summary>Reads the next JSON value (the reader is on a property name) and converts it to the column type.</summary>
        internal static object ReadJsonValue(JsonTextReader json, Type type)
        {
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
