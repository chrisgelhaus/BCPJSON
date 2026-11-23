using Microsoft.Data.SqlClient;
using Newtonsoft.Json;
using System.Collections.Concurrent;
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

            WarnUnmatchedSplitRequests(options.SplitRequests, tables);

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

        /// <summary>One unit of export work: a whole table (Range null) or one key range of a split table.</summary>
        private sealed record ExportItem(TableRef Table, KeyRange Range, TableSplit Split, long Size);

        /// <summary>
        /// The work items of a table export, sized by estimated rows: one per unsplit table and one per part of a
        /// split table (--split-tables, or the split --resume finds in the manifest). A table whose planning fails is
        /// reported and left out; when planning cannot start (no connection), the error is reported and nothing is exported.
        /// <paramref name="extension"/> is the data file extension (.json or .bcp).
        /// </summary>
        private static List<ExportItem> PlanExportItems(ExportOptions options, ExportManifest manifest, List<TableRef> tables, string extension,
            Func<SqlConnection, TableRef, string> splitProblem = null)
        {
            var items = new List<ExportItem>();
            if (tables.Count == 0)
            {
                return items;
            }

            try
            {
                using (var conn = SqlConnectionFactory.Create(options.SourceConnectionString()))
                {
                    conn.Open();
                    Dictionary<int, long> rowCounts = TableRowCounts(conn);
                    string[] folderFiles = options.Resume ? null : Directory.GetFiles(options.exportPath).Select(Path.GetFileName).ToArray();
                    foreach (TableRef table in tables)
                    {
                        PlanExportTable(options, manifest, conn, table, rowCounts, extension, folderFiles, splitProblem, items);
                    }
                }
            }
            catch (Exception ex)
            {
                PostToConsole(ex.Message, InfoLevel.Error);
                return new List<ExportItem>();
            }
            return items;
        }

        /// <summary>
        /// Adds one table's items to <paramref name="items"/> and, unless --resume (<paramref name="folderFiles"/> null),
        /// warns about data files of the table left in the folder by an earlier export that this run will not rewrite.
        /// </summary>
        private static void PlanExportTable(ExportOptions options, ExportManifest manifest, SqlConnection conn, TableRef table,
            Dictionary<int, long> rowCounts, string extension, string[] folderFiles, Func<SqlConnection, TableRef, string> splitProblem, List<ExportItem> items)
        {
            long rows = rowCounts.TryGetValue(table.ObjectId, out long counted) ? counted : 0;
            try
            {
                TableSplit split = ChooseSplit(options, manifest, conn, table, splitProblem, out List<int> partsToExport);
                if (folderFiles != null)
                {
                    IEnumerable<string> written = split == null
                        ? new[] { options.DataFileName(table.DisplayName, extension) }
                        : split.Ranges.Select(r => options.DataFileName(PartFiles.PartName(table.DisplayName, r.Part), extension));
                    foreach (string leftover in PartFiles.Leftovers(folderFiles, table.DisplayName, extension, written))
                    {
                        PostToConsole(string.Format("{0}: {1} is left from an earlier export; delete it (import ignores it while the manifest is present).",
                            table.DisplayName, leftover), InfoLevel.Warning);
                    }
                }
                if (split == null)
                {
                    items.Add(new ExportItem(table, null, null, rows));
                    return;
                }
                for (int i = 0; i < split.Ranges.Count; i++)
                {
                    KeyRange range = split.Ranges[i];
                    if (partsToExport == null || partsToExport.Contains(range.Part))
                    {
                        long estimate = split.EstimatedRows[i] >= 0 ? split.EstimatedRows[i] : rows / split.Ranges.Count;
                        items.Add(new ExportItem(table, range, split, estimate));
                    }
                }
            }
            catch (Exception ex)
            {
                PostToConsole(string.Format("{0}: {1}", table.DisplayName, ex.Message), InfoLevel.Error);
            }
        }

        /// <summary>
        /// How a table is exported: null for one file, or its split. With --resume, a split recorded in the manifest is
        /// kept (partsToExport then lists the parts still missing) and a table recorded unsplit stays unsplit.
        /// partsToExport is null when every part is to be exported. <paramref name="splitProblem"/> (may be null) gives a
        /// format-specific reason not to split a table.
        /// </summary>
        private static TableSplit ChooseSplit(ExportOptions options, ExportManifest manifest, SqlConnection conn, TableRef table,
            Func<SqlConnection, TableRef, string> splitProblem, out List<int> partsToExport)
        {
            partsToExport = null;
            string name = table.DisplayName;
            int? requested = SplitRequest.PartsFor(options.SplitRequests, name);

            ManifestTable recorded = options.Resume ? manifest.Find(name) : null;
            if (recorded != null)
            {
                if (recorded.Split == null)
                {
                    return null;
                }
                TableSplit kept = TableSplit.FromManifest(recorded.Split);
                if (requested != kept.Ranges.Count)
                {
                    PostToConsole(string.Format("{0}: --resume keeps its recorded split ({1} parts).", name, kept.Ranges.Count), InfoLevel.Warning);
                }
                partsToExport = manifest.PartsToRedo(name, options.exportPath);
                if (partsToExport.Count == 0)
                {
                    // Every part is intact but the table is not (a shared file such as the format file): redo it all.
                    partsToExport = null;
                }
                PostToConsole(string.Format("{0}: {1} part(s) to go.", name, partsToExport?.Count ?? kept.Ranges.Count));
                return kept;
            }

            if (requested == null)
            {
                return null;
            }
            return SplitOrWhole(name, () =>
            {
                string problem = splitProblem?.Invoke(conn, table);
                if (problem != null)
                {
                    return (null, problem);
                }
                TableSplit planned = PlanSplit(conn, table, requested.Value, out string reason);
                return (planned, reason);
            });
        }

        /// <summary>
        /// Runs a split plan for a table and reports the outcome. When the plan gives no split, or fails (a SQL error,
        /// unexpected storage), the table is exported as one file with a "not split" warning.
        /// </summary>
        internal static TableSplit SplitOrWhole(string name, Func<(TableSplit Split, string Reason)> plan)
        {
            TableSplit split;
            string reason;
            try
            {
                (split, reason) = plan();
            }
            catch (Exception ex)
            {
                (split, reason) = (null, ex.Message.Trim().TrimEnd('.'));
            }

            if (split == null)
            {
                PostToConsole(string.Format("{0}: not split: {1}.", name, reason), InfoLevel.Warning);
                return null;
            }
            PostToConsole(string.Format("{0}: split into {1} parts by {2} ({3}).", name, split.Ranges.Count, split.Column.Name, split.Kind));
            return split;
        }

        /// <summary>Warns about each --split-tables entry that matches none of <paramref name="tables"/>: a typo would otherwise leave the run unsplit.</summary>
        private static void WarnUnmatchedSplitRequests(IReadOnlyList<SplitRequest> requests, List<TableRef> tables)
        {
            foreach (SplitRequest request in SplitRequest.Unmatched(requests, tables.Select(t => t.DisplayName)))
            {
                PostToConsole(string.Format("--split-tables {0}: no table matched.", request.Pattern), InfoLevel.Warning);
            }
        }

        /// <summary>One TableWork per split table, with <paramref name="before"/> (may be null) giving its preparation.</summary>
        private static Dictionary<int, TableWork> SplitTableWork(List<ExportItem> items, Func<TableRef, Action> before)
        {
            return items.Where(i => i.Range != null)
                .GroupBy(i => i.Table.ObjectId)
                .ToDictionary(g => g.Key, g => new TableWork(g.First().Table.DisplayName, g.Count(), before?.Invoke(g.First().Table), null));
        }

        /// <summary>Runs the items largest first. <paramref name="run"/> reports its own failures; nothing is expected to escape.</summary>
        private static void RunExportItems(List<ExportItem> items, int maxTasks, Action<ExportItem> run)
        {
            try
            {
                WorkQueue.RunLargestFirst(items, i => i.Size, maxTasks, run);
            }
            catch (AggregateException ex)
            {
                foreach (Exception inner in ex.InnerExceptions)
                {
                    PostToConsole(inner.Message, InfoLevel.Error);
                }
            }
        }

        private static long ExportToJSON(ExportOptions options, ExportManifest manifest)
        {
            long totalrowCount = 0;
            List<TableRef> tables = TablesToExport(options, manifest);
            List<ExportItem> items = PlanExportItems(options, manifest, tables, ".json");
            Dictionary<int, TableWork> works = SplitTableWork(items, null);

            RunExportItems(items, options.MaxParallelTasks, item =>
            {
                TableRef table = item.Table;
                if (item.Range == null)
                {
                    string fileName = options.DataFileName(table.DisplayName, ".json");
                    try
                    {
                        // Exporting a table only rewrites its file, so a transient failure retries the whole table.
                        var (rows, orderedBy) = WithRetries(table.DisplayName, options.Retries,
                            () => ExportTableToJson(options, table, Path.Combine(options.exportPath, fileName), table.DisplayName, null), IsTransientSqlError);
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
                    return;
                }

                string partName = PartFiles.PartName(table.DisplayName, item.Range.Part);
                try
                {
                    works[table.ObjectId].Run(() =>
                    {
                        // A part only rewrites its own file, so a transient failure retries just this part.
                        string partFile = options.DataFileName(partName, ".json");
                        string where = item.Range.Predicate(item.Split.Column);
                        var (rows, orderedBy) = WithRetries(partName, options.Retries,
                            () => ExportTableToJson(options, table, Path.Combine(options.exportPath, partFile), partName, where), IsTransientSqlError);
                        var shell = new ManifestTable { Name = table.DisplayName, OrderedBy = orderedBy, Split = item.Split.ToManifest() };
                        manifest.RecordPart(shell, ManifestPart.From(item.Range, rows, new[] { partFile }),
                            new[] { ManifestFile.Describe(options.exportPath, partFile) }, options.exportPath);
                        Interlocked.Add(ref totalrowCount, rows);
                        PostToConsole(string.Format("{0}: {1:N0} (Complete)", partName, rows), InfoLevel.Success);
                    });
                }
                catch (Exception ex)
                {
                    PostToConsole(string.Format("{0}: {1}", partName, ex.Message), InfoLevel.Error);
                }
            });

            return totalrowCount;
        }

        /// <summary>
        /// Writes one table (or, with <paramref name="where"/>, one key range of it) to &lt;file&gt;.partial and renames it
        /// to <paramref name="filePath"/> when complete. Returns the row count and the primary key the rows are sorted by
        /// (null without one), which import uses for order hints. <paramref name="label"/> names the progress messages.
        /// </summary>
        private static (long Rows, List<ManifestKeyColumn> OrderedBy) ExportTableToJson(ExportOptions options, TableRef table, string filePath, string label, string where)
        {
            try
            {
                using (var conn = SqlConnectionFactory.Create(options.SourceConnectionString()))
                {
                    conn.Open();
                    string select = BuildTableSelect(conn, table, options.NoLock, where);
                    List<ManifestKeyColumn> orderedBy = ReadKeyColumns(conn, table.ObjectId, KeyKind.PrimaryKey);

                    using (var cmd = new SqlCommand(select, conn))
                    {
                        cmd.CommandTimeout = 0;
                        using (var reader = cmd.ExecuteReader(CommandBehavior.SingleResult | CommandBehavior.SequentialAccess))
                        {
                            long rows = WriteJsonRows(reader, PartialPath(filePath), label, options.GzipLevel);
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

        /// <summary>The JSON export query of a table; <paramref name="where"/> (a key range predicate, or null) selects a part of it.</summary>
        private static string BuildTableSelect(SqlConnection conn, TableRef table, bool noLock, string where)
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
            if (!string.IsNullOrEmpty(where))
            {
                select += " WHERE " + where;
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
            // Parts name every column on the bcp command line, so their names must be safe there.
            List<ExportItem> items = PlanExportItems(options, manifest, tables, ".bcp", (conn, table) =>
                ReadColumnNames(conn, table.ObjectId).All(SplitColumn.IsCommandLineSafe)
                    ? null
                    : "a column name contains characters bcp cannot take on its command line");

            // A split table's parts share one format file and select list, prepared once before its first part.
            var formatFiles = new ConcurrentDictionary<int, ManifestFile>();
            var selectLists = new ConcurrentDictionary<int, string>();
            Dictionary<int, TableWork> works = SplitTableWork(items, table => () =>
            {
                using (var conn = SqlConnectionFactory.Create(options.SourceConnectionString()))
                {
                    conn.Open();
                    selectLists[table.ObjectId] = string.Join(", ", ReadColumnNames(conn, table.ObjectId).Select(QuoteName));
                }
                formatFiles[table.ObjectId] = WithRetries(table.DisplayName, options.Retries,
                    () => WriteBcpFormatFile(table, connectionArgs, outputPath), ex => ex is BcpFailedException);
            });

            RunExportItems(items, options.MaxParallelTasks, item =>
            {
                TableRef table = item.Table;
                string name = table.DisplayName;
                if (item.Range == null)
                {
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
                    return;
                }

                string partName = PartFiles.PartName(name, item.Range.Part);
                try
                {
                    works[table.ObjectId].Run(() =>
                    {
                        string dataFile = options.DataFileName(partName, ".bcp");
                        long rows = WithRetries(partName, options.Retries,
                            () => ExportPartToBcp(options, item, selectLists[table.ObjectId], partName, connectionArgs, outputPath), ex => ex is BcpFailedException);
                        var shell = new ManifestTable
                        {
                            Name = name,
                            Split = item.Split.ToManifest(),
                            Files = new List<ManifestFile> { formatFiles[table.ObjectId] }
                        };
                        manifest.RecordPart(shell, ManifestPart.From(item.Range, rows, new[] { dataFile }),
                            new[] { ManifestFile.Describe(outputPath, dataFile) }, outputPath);
                        if (rows > 0)
                        {
                            Interlocked.Add(ref totalrowCount, rows);
                        }
                        PostToConsole(string.Format("{0}: {1:N0}", partName, rows), InfoLevel.Info);
                    });
                }
                catch (Exception ex)
                {
                    PostToConsole(string.Format("{0}: {1}", partName, ex.Message), InfoLevel.Error);
                }
            });

            return totalrowCount;
        }

        /// <summary>Writes a split table's shared format file (via .partial) and describes it for the manifest.</summary>
        private static ManifestFile WriteBcpFormatFile(TableRef table, string connectionArgs, string outputPath)
        {
            string formatFile = table.DisplayName + BcpFormatFileSuffix;
            string formatPath = Path.Combine(outputPath, formatFile);
            try
            {
                RunBcpFormat(table, connectionArgs, outputPath);
                CommitPartial(formatPath);
            }
            catch
            {
                DiscardPartial(formatPath);
                throw;
            }
            PostToConsole(string.Format("{0}: Format file created.", table.DisplayName), InfoLevel.Success);
            return ManifestFile.Describe(outputPath, formatFile);
        }

        /// <summary>
        /// Writes one key range of a table with bcp queryout to &lt;part&gt;.bcp (via .partial; gzip as for tables).
        /// <paramref name="selectList"/> names the columns the format file lists, in column_id order. The range's boundaries are typed literals in the query, because bcp takes no parameters.
        /// </summary>
        private static long ExportPartToBcp(ExportOptions options, ExportItem item, string selectList, string partName, string connectionArgs, string outputPath)
        {
            string dataPath = Path.Combine(outputPath, partName + ".bcp");
            try
            {
                string query = "SELECT " + selectList + " FROM " + item.Table.QuotedName + (options.NoLock ? " WITH (NOLOCK)" : string.Empty)
                    + " WHERE " + item.Range.Predicate(item.Split.Column);
                string outputLog = partName + "_output.log";
                string args = string.Format("{0} queryout {1} {2} -c -t| -m 1 -b 10000 -K ReadOnly -e {3}",
                    Arg(query), Arg(PartialPath(partName + ".bcp")), connectionArgs, Arg(partName + "_error.log"));
                if (ExecuteProcess(BcpTool, args, outputPath, outputLog) != 0)
                {
                    throw new BcpFailedException(string.Format("bcp export failed. See {0}_error.log and {1}.", partName, outputLog));
                }

                CommitData(dataPath, options.GzipLevel);
                return ReadBcpRowCount(Path.Combine(outputPath, outputLog));
            }
            catch
            {
                DiscardPartial(dataPath);
                DiscardPartial(dataPath + FileCompression.GzipExtension);
                throw;
            }
        }

        /// <summary>
        /// The columns bcp format lists for a table, in column_id order: every column except HIDDEN ones (temporal period
        /// columns), which bcp leaves out like SELECT *.
        /// </summary>
        private static List<string> ReadColumnNames(SqlConnection conn, int objectId)
        {
            var names = new List<string>();
            using (var cmd = new SqlCommand("SET NOCOUNT ON; SELECT name FROM sys.columns WHERE object_id = @objectId AND is_hidden = 0 ORDER BY column_id;", conn))
            {
                cmd.Parameters.Add("@objectId", SqlDbType.Int).Value = objectId;
                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        names.Add(reader.GetString(0));
                    }
                }
            }
            return names;
        }

        /// <summary>Runs bcp format for a table into &lt;table&gt;-c.fmt.partial. Throws <see cref="BcpFailedException"/> when bcp fails.</summary>
        private static void RunBcpFormat(TableRef table, string connectionArgs, string outputPath)
        {
            // Format file (used by import to map columns). Non-XML, because the Linux bcp does not support -x.
            // "nul" is the no-data-file placeholder on Windows; on Linux bcp would create a file named "nul".
            string formatArgs = string.Format("{0} format {1} {2} -c -f {3} -t|",
                Arg(table.QuotedName), OperatingSystem.IsWindows() ? "nul" : "/dev/null", connectionArgs, Arg(PartialPath(table.DisplayName + BcpFormatFileSuffix)));
            if (ExecuteProcess(BcpTool, formatArgs, outputPath) != 0)
            {
                throw new BcpFailedException("Error creating format file.");
            }
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
                RunBcpFormat(table, connectionArgs, outputPath);
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
