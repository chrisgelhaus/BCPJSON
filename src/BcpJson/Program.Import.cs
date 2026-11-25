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

        /// <summary>The table name a data file loads into by default: the file name without .gz, its extension and a .partNNNN suffix.</summary>
        internal static string ImportBaseName(string fileName)
        {
            return PartFiles.TableNameOf(fileName);
        }

        /// <summary>False for the export manifest and for unfinished ".partial" files, which a wildcard like *.json or * may match.</summary>
        internal static bool IsImportableFile(string name)
        {
            return !name.Equals(ExportManifest.FileName, StringComparison.OrdinalIgnoreCase)
                && !name.EndsWith(PartialSuffix, StringComparison.OrdinalIgnoreCase)
                && !name.EndsWith(HeapLoadScript.Suffix, StringComparison.OrdinalIgnoreCase);
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

        /// <summary>One target table of an import: its data files (part files in part order) and its per-table steps.</summary>
        private sealed class ImportTable
        {
            public string Name;
            public string Schema;
            public string Table;
            public int ObjectId;
            public List<FileInfo> Files;
            public TableWork Work;

            public string QualifiedName => QuoteName(Schema) + "." + QuoteName(Table);
            public string Label => Schema + "." + Table;

            /// <summary>The name a file's progress and completion messages use: <paramref name="single"/> when the table has one file, else the file's own name.</summary>
            public string FileLabel(FileInfo file, string single) => Files.Count > 1 ? PartFiles.StemOf(file.Name) : single;
        }

        /// <summary>A unit of work: one file (parallel mode) or all files of a table in order (sequential mode).</summary>
        private sealed record ImportItem(ImportTable Table, List<FileInfo> Files)
        {
            public long Size => Files.Sum(f => f.Length);
        }

        /// <summary>
        /// Imports the source files table by table: a serial planning pass groups the files by target (part files of a
        /// split table together), chooses each table's load mode and the indexes to disable, then the items run largest
        /// first. A table is truncated and its indexes disabled once before its first file, and rebuilt after its last.
        /// <paramref name="loadFile"/> loads one file, reports its own errors and returns its rows.
        /// </summary>
        private static long ImportFiles(ImportOptions options, bool json, Func<ImportTable, FileInfo, long> loadFile)
        {
            long totalrowCount = 0;
            FileInfo[] files = FindSourceFiles(options);
            if (files.Length == 0)
            {
                return 0;
            }

            List<ImportItem> items;
            try
            {
                items = PlanImport(options, files, json);
            }
            catch (Exception ex)
            {
                PostToConsole(ex.Message, InfoLevel.Error);
                return 0;
            }

            // The action catches and reports everything itself, so RunLargestFirst has no exception to collect.
            WorkQueue.RunLargestFirst(items, i => i.Size, options.MaxParallelTasks, item =>
            {
                try
                {
                    item.Table.Work.Run(() =>
                    {
                        foreach (FileInfo file in item.Files)
                        {
                            Interlocked.Add(ref totalrowCount, loadFile(item.Table, file));
                        }
                    });
                }
                catch (Exception ex)
                {
                    PostToConsole(ex.Message, InfoLevel.Error);
                }
            });
            return totalrowCount;
        }

        /// <summary>The planning pass: one connection, one table after another.</summary>
        private static List<ImportItem> PlanImport(ImportOptions options, FileInfo[] files, bool json)
        {
            var byName = files.ToDictionary(f => f.Name, StringComparer.Ordinal);
            var items = new List<ImportItem>();
            var firstFileOfTable = new Dictionary<int, string>();
            // The manifest of the export, if the folder has one, says which files belong to it (see PartFiles.SelectImportFiles).
            ExportManifest manifest = ExportManifest.Load(options.SourcePath);
            string manifestPath = ExportManifest.PathIn(options.SourcePath);
            using (var conn = SqlConnectionFactory.Create(options.GetTargetConnectionString()))
            {
                conn.Open();
                foreach (ImportGroup group in PartFiles.GroupImportFiles(files.Select(f => f.Name), options.TargetTable))
                {
                    ImportSelection selection = PartFiles.SelectImportFiles(group.Files, manifest, manifestPath);
                    selection.Warnings.ForEach(w => PostToConsole(w, InfoLevel.Warning));
                    selection.Errors.ForEach(e => PostToConsole(e, InfoLevel.Error));
                    if (selection.Files.Count == 0)
                    {
                        continue;
                    }
                    List<FileInfo> groupFiles = selection.Files.Select(n => byName[n]).ToList();
                    try
                    {
                        items.AddRange(PlanImportTable(conn, options, group.TableName, groupFiles, json, firstFileOfTable));
                    }
                    catch (Exception ex)
                    {
                        PostToConsole(string.Format("{0}: {1}", group.TableName, ex.Message), InfoLevel.Error);
                    }
                }
            }
            return items;
        }

        /// <summary>
        /// Plans one group of files. <paramref name="firstFileOfTable"/> maps each target already planned to its first
        /// file: a second group resolving to the same table (e.g. T.json and dbo.T.json) is refused, because its
        /// truncation and load would race the first group's.
        /// </summary>
        private static List<ImportItem> PlanImportTable(SqlConnection conn, ImportOptions options, string tableName, List<FileInfo> files, bool json,
            Dictionary<int, string> firstFileOfTable)
        {
            string[] target = GetTableObject(tableName, conn);
            if (target == null)
            {
                if (json)
                {
                    foreach (FileInfo file in files)
                    {
                        PostToConsole(string.Format("{0}: Target table does not exist. Create it before importing.", file.Name), InfoLevel.Error);
                    }
                }
                else
                {
                    PostToConsole(string.Format("{0}: Target table does not exist. Create it before importing with bcp.", tableName), InfoLevel.Error);
                }
                return new List<ImportItem>();
            }

            var table = new ImportTable
            {
                Name = tableName,
                Schema = target[0],
                Table = target[1],
                ObjectId = int.Parse(target[2], CultureInfo.InvariantCulture)
            };

            if (firstFileOfTable.TryGetValue(table.ObjectId, out string firstFile))
            {
                PostToConsole(string.Format("{0}: files {1} and {2} both load into this table; import them separately.", table.Label, firstFile, files[0].Name), InfoLevel.Error);
                return new List<ImportItem>();
            }
            firstFileOfTable[table.ObjectId] = files[0].Name;

            if (!CheckHeapLoadLeftovers(conn, options, table))
            {
                return new List<ImportItem>();
            }

            if (!json)
            {
                // Checked before anything touches the target, so a missing format file never leaves it truncated.
                files = files.Where(file =>
                {
                    string baseFileName = ImportBaseName(file.Name);
                    if (FindBcpFormatFile(options.SourcePath, baseFileName) != null)
                    {
                        return true;
                    }
                    PostToConsole(string.Format("{0}: Format file {1}{2} (or {1}{3}) not found next to the data file.",
                        table.Label, baseFileName, BcpFormatFileSuffix, LegacyBcpFormatFileSuffix), InfoLevel.Error);
                    return false;
                }).ToList();
                if (files.Count == 0)
                {
                    return new List<ImportItem>();
                }
            }
            table.Files = files;

            bool staged = json && GetImportColumns(conn, table.ObjectId).Any(c => c.IsClrType);
            // --load-as-heap applies to split tables only; every other table loads as with --disable-indexes.
            HeapLoadPlan heapPlan = options.LoadAsHeap && files.Count > 1 ? PlanHeapLoad(conn, options, table, staged) : null;

            var toDisable = new List<IndexInfo>();
            if (heapPlan == null && (options.DisableIndexes || options.LoadAsHeap))
            {
                if (IsMemoryOptimized(conn, table.ObjectId))
                {
                    PostToConsole(string.Format("{0}: memory-optimized; --disable-indexes ignored.", table.Label));
                }
                else
                {
                    toDisable = LoadPlanning.IndexesToDisable(ReadIndexes(conn, table.ObjectId));
                }
            }

            TargetShape shape = ReadTargetShape(conn, table.ObjectId, toDisable.Select(i => i.IndexId).ToHashSet());
            if (heapPlan != null)
            {
                shape = new TargetShape(false, false, 0, shape.Partitioned);   // the shape after the drop: a heap
            }
            string sequentialReason = LoadPlanning.SequentialReason(shape, !options.NoTableLock, staged);

            if (files.Count > 1)
            {
                PostToConsole(sequentialReason == null
                    ? string.Format("{0}: {1} parts, loading in parallel.", table.Label, files.Count)
                    : string.Format("{0}: {1} parts, loading one after another ({2}).", table.Label, files.Count, sequentialReason));
                if (options.NoTableLock && shape.Partitioned
                    && !string.Equals(LockEscalation(conn, table.ObjectId), "AUTO", StringComparison.OrdinalIgnoreCase))
                {
                    PostToConsole(string.Format("{0}: partitioned; ALTER TABLE {1} SET (LOCK_ESCALATION = AUTO) lets parts in different partitions load without blocking each other.",
                        table.Label, table.QualifiedName));
                }
            }

            List<ImportItem> items = sequentialReason == null
                ? files.Select(f => new ImportItem(table, new List<FileInfo> { f })).ToList()
                : new List<ImportItem> { new ImportItem(table, files) };

            Action before, after;
            if (heapPlan != null)
            {
                before = () => PrepareHeapLoad(options, table, heapPlan, files.Count);
                after = () => FinishHeapLoad(options, table, heapPlan);
            }
            else
            {
                before = options.TruncateTarget || toDisable.Count > 0 ? () => PrepareImportTarget(options, table, toDisable) : null;
                after = toDisable.Count > 0 ? () => RebuildDisabledIndexes(options, table, toDisable) : null;
            }
            table.Work = new TableWork(table.Label, items.Count, before, after);
            return items;
        }

        /// <summary>Runs once before a table's first file: truncates it (--truncate-target), then disables indexes (--disable-indexes).</summary>
        private static void PrepareImportTarget(ImportOptions options, ImportTable table, List<IndexInfo> toDisable)
        {
            using (var conn = SqlConnectionFactory.Create(options.GetTargetConnectionString()))
            {
                conn.Open();
                if (options.TruncateTarget)
                {
                    ExecuteSql(conn, "TRUNCATE TABLE " + table.QualifiedName + ";");
                    PostToConsole(string.Format("{0}: Truncate Complete.", table.Label));
                }
                if (toDisable.Count > 0)
                {
                    DisableIndexes(conn, table.QualifiedName, toDisable);
                    PostToConsole(string.Format("{0}: Disabled indexes: {1}.", table.Label, string.Join(", ", toDisable.Select(i => i.Name))));
                }
            }
        }

        /// <summary>
        /// Runs once after a table's last file, whether or not the load succeeded: rebuilds the indexes of
        /// <paramref name="toDisable"/> that are disabled now (preparation may have stopped part way).
        /// </summary>
        private static void RebuildDisabledIndexes(ImportOptions options, ImportTable table, List<IndexInfo> toDisable)
        {
            var ids = new HashSet<int>(toDisable.Select(i => i.IndexId));
            SqlConnection conn = null;
            List<IndexInfo> disabled;
            try
            {
                conn = SqlConnectionFactory.Create(options.GetTargetConnectionString());
                conn.Open();
                disabled = ReadIndexes(conn, table.ObjectId).Where(i => i.IsDisabled && ids.Contains(i.IndexId)).ToList();
            }
            catch (Exception ex)
            {
                // Reported here, not thrown: TableWork drops after's exception when the load failed too.
                conn?.Dispose();
                PostToConsole(RebuildNotStartedMessage(table.Label, table.QualifiedName, toDisable.Select(i => i.Name), ex.Message), InfoLevel.Error);
                return;
            }

            using (conn)
            {
                if (disabled.Count > 0)
                {
                    RebuildIndexes(conn, table.QualifiedName, disabled);
                }
            }
        }

        private static long ImportFromBCP(ImportOptions options)
        {
            string connectionArgs = BcpConnectionArgs(options.TargetSQLServer, options.TargetSQLDatabase, options.TargetSQLLogin, options.TargetSQLPassword, options.TrustServerCertificate);
            string workingPath = options.SourcePath;

            return ImportFiles(options, false, (table, file) =>
            {
                // The format file is shared by a table's parts; logs and temporary files are named after the file itself.
                string baseFileName = ImportBaseName(file.Name);
                string fileStem = PartFiles.StemOf(file.Name);
                string label = table.Label;
                string tempFolder = null;

                try
                {
                    string formatFile = FindBcpFormatFile(workingPath, baseFileName);

                    // bcp cannot read gzip: decompress to a temporary folder (the source folder may be read-only).
                    string dataFile = file.Name;
                    if (FileCompression.IsCompressedFile(file.Name))
                    {
                        tempFolder = Path.Combine(Path.GetTempPath(), "bcpjson", Guid.NewGuid().ToString("N"));
                        Directory.CreateDirectory(tempFolder);
                        dataFile = Path.Combine(tempFolder, fileStem + ".bcp");
                        PostToConsole(string.Format("{0}: Decompressing {1}.", label, file.Name));
                        FileCompression.DecompressFile(file.FullName, dataFile);
                    }

                    string outputLog = fileStem + "_import_output.log";
                    string args = BcpInArguments(table.Schema, table.Table, dataFile, formatFile, connectionArgs, options.NoTableLock, fileStem + "_import_error.log", options.BatchSize);

                    if (ExecuteProcess(BcpTool, args, workingPath, outputLog) == 0)
                    {
                        long rows = ReadBcpRowCount(Path.Combine(workingPath, outputLog));
                        PostToConsole(string.Format("{0}: {1:N0} (Import Complete)", table.FileLabel(file, label), rows), InfoLevel.Success);
                        return rows > 0 ? rows : 0;
                    }
                    PostToConsole(string.Format("{0}: Error importing data. See {1}_import_error.log and {2}.", file.FullName, fileStem, outputLog), InfoLevel.Error);
                }
                catch (Exception ex)
                {
                    PostToConsole(string.Format("{0}: {1}", file.Name, ex.Message), InfoLevel.Error);
                }
                finally
                {
                    DeleteTempFolder(tempFolder);
                }
                return 0;
            });
        }

        /// <summary>The export manifest next to the files being imported, or null (none, or unreadable: import works without it).</summary>
        private static ExportManifest ManifestForImport(string folder)
        {
            try
            {
                return ExportManifest.Load(folder);
            }
            catch (Exception ex)
            {
                PostToConsole(string.Format("Ignoring {0}: {1}", ExportManifest.FileName, ex.Message), InfoLevel.Warning);
                return null;
            }
        }

        /// <summary>The key the manifest says this file's rows are sorted by, or null.</summary>
        internal static List<ManifestKeyColumn> SortKeyOf(ExportManifest manifest, string fileName)
        {
            return manifest?.Tables
                .FirstOrDefault(t => t.Files != null && t.Files.Any(f => string.Equals(f.Name, fileName, StringComparison.OrdinalIgnoreCase)))
                ?.OrderedBy;
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
            ExportManifest manifest = ManifestForImport(options.SourcePath);

            return ImportFiles(options, true, (table, file) =>
            {
                try
                {
                    long rows = ImportJsonFile(file, table, options, SortKeyOf(manifest, file.Name));
                    PostToConsole(string.Format("{0}: {1:N0} (Import Complete)", table.FileLabel(file, table.Name), rows), InfoLevel.Success);
                    return rows;
                }
                catch (Exception ex)
                {
                    PostToConsole(string.Format("{0}: {1}", file.Name, ex.Message), InfoLevel.Error);
                    return 0;
                }
            });
        }

        /// <summary>
        /// Streams one exported JSON file (an array of row objects; .gz files are decompressed on the fly) into an
        /// existing table: SqlBulkCopy reads the rows through a <see cref="JsonRowReader"/>, committing every
        /// --batch-size rows. Values are converted using the target column types. Tables with CLR-type columns (geography, geometry,
        /// hierarchyid) are loaded through a #temp staging table holding those columns as text, then inserted with
        /// ::Parse() (<see cref="ClrFromText"/>). When <paramref name="sortedBy"/> (from the manifest) matches the target's
        /// clustered index, SqlBulkCopy gets column order hints so SQL Server can skip sorting the rows.
        /// Truncation (--truncate-target) is a per-table step done before the table's first file, not here.
        /// </summary>
        private static long ImportJsonFile(FileInfo file, ImportTable table, ImportOptions options, List<ManifestKeyColumn> sortedBy)
        {
            string connectionString = options.GetTargetConnectionString();
            string qualifiedName = table.QualifiedName;
            string label = table.Label;
            string progressLabel = table.FileLabel(file, label);

            using (var conn = SqlConnectionFactory.Create(connectionString))
            {
                conn.Open();

                List<ImportColumn> columns = GetImportColumns(conn, table.ObjectId);
                if (columns.Count == 0)
                {
                    throw new InvalidOperationException("Target table has no loadable columns.");
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
                    bulkCopy.SqlRowsCopied += (sender, e) => PostToConsole(string.Format("{0}: {1:N0}", progressLabel, e.RowsCopied));
                    foreach (var column in bulkColumns)
                    {
                        bulkCopy.ColumnMappings.Add(column.Name, column.Name);
                    }

                    // Only for the real table (the staging table is a heap) and only when the export's sort key is
                    // exactly the clustered key: SQL Server checks the hint and fails the load if rows are out of order.
                    if (!useStaging && sortedBy != null)
                    {
                        List<ManifestKeyColumn> clustered = ReadKeyColumns(conn, table.ObjectId, KeyKind.Clustered);
                        if (ManifestKeyColumn.SameOrder(sortedBy, clustered)
                            && clustered.All(k => bulkColumns.Any(c => string.Equals(c.Name, k.Column, StringComparison.OrdinalIgnoreCase))))
                        {
                            foreach (ManifestKeyColumn key in clustered)
                            {
                                bulkCopy.ColumnOrderHints.Add(key.Column, key.Descending ? SortOrder.Descending : SortOrder.Ascending);
                            }
                            PostToConsole(string.Format("{0}: Rows are in clustered index order; loading with an order hint.", label));
                        }
                    }

                    using (StreamReader text = FileCompression.OpenText(file.FullName))
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
                        ? ClrFromText(c.TypeName, QuoteName(c.Name))
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

        /// <summary>
        /// T-SQL turning a staged text column back into its CLR type. geography/geometry text may carry an "SRID=n;"
        /// prefix (export writes one when the SRID is not ::Parse()'s default), which becomes STGeomFromText(wkt, n).
        /// </summary>
        internal static string ClrFromText(string typeName, string column)
        {
            string parse = typeName + "::Parse(" + column + ")";
            if (!string.Equals(typeName, "geography", StringComparison.OrdinalIgnoreCase)
                && !string.Equals(typeName, "geometry", StringComparison.OrdinalIgnoreCase))
            {
                return parse;
            }
            string semicolon = "CHARINDEX(N';', " + column + ")";
            return "CASE WHEN " + column + " LIKE N'SRID=%;%' THEN " + typeName + "::STGeomFromText(SUBSTRING(" + column + ", "
                + semicolon + " + 1, 2147483647), CAST(SUBSTRING(" + column + ", 6, " + semicolon + " - 6) AS INT)) ELSE "
                + parse + " END";
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
