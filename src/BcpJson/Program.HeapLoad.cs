using Microsoft.Data.SqlClient;
using Microsoft.SqlServer.Management.Common;
using Microsoft.SqlServer.Management.Smo;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using Index = Microsoft.SqlServer.Management.Smo.Index;

namespace bcpJson
{
    partial class Program
    {
        /// <summary>One index of the target as scripted before load-as-heap drops it (Statement: one batch, lines joined with "\n").</summary>
        private sealed record SavedIndex(string Name, bool IsClustered, bool IsConstraint, string Statement);

        /// <summary>What load-as-heap drops and recreates for one table, and where its recovery script is.</summary>
        private sealed class HeapLoadPlan
        {
            /// <summary>In creation order: the clustered index first.</summary>
            public List<SavedIndex> Indexes;
            public string ScriptPath;
        }

        /// <summary>Full path of a table's recovery script in --source-path.</summary>
        private static string HeapLoadScriptPath(ImportOptions options, ImportTable table)
        {
            return Path.Combine(options.SourcePath, HeapLoadScript.FileName(table.Schema, table.Table));
        }

        /// <summary>
        /// Planning checks every import makes: a target marked by an earlier --load-as-heap run is refused
        /// (false); a recovery script without markers is only a warning.
        /// </summary>
        private static bool CheckHeapLoadLeftovers(SqlConnection conn, ImportOptions options, ImportTable table)
        {
            string scriptPath = HeapLoadScriptPath(options, table);
            if (HasHeapLoadMarkers(conn, table.Schema, table.Table))
            {
                PostToConsole(string.Format("{0}: an earlier --load-as-heap run left it without some of its indexes; run the script in {1} (it recreates what is missing and removes the markers; if the file is lost, the statements are in the extended properties {2}*, drop those after running them), then import again.",
                    table.Label, scriptPath, HeapLoadScript.MarkerPrefix), InfoLevel.Error);
                return false;
            }
            if (File.Exists(scriptPath))
            {
                PostToConsole(string.Format("{0} is left over and {1} has no markers; if all its indexes exist, delete it.", scriptPath, table.Label), InfoLevel.Warning);
            }
            return true;
        }

        /// <summary>
        /// Decides whether a split table is loaded as a heap. When it is, the recovery script has been written and the
        /// plan is returned; otherwise null, with a warning when there is a reason (the table then loads as with --disable-indexes).
        /// </summary>
        private static HeapLoadPlan PlanHeapLoad(SqlConnection conn, ImportOptions options, ImportTable table, bool staged)
        {
            HeapLoadFacts facts = ReadHeapLoadFacts(conn, table.ObjectId);
            // The index-length check is the last rule, so the indexes are scripted only for a table that passes all others.
            HeapLoadDecision decision = LoadPlanning.DecideHeapLoad(facts, !options.NoTableLock, staged, options.TruncateTarget, null);
            HeapLoadPlan plan = null;
            if (decision.LoadAsHeap)
            {
                string database = conn.Database;
                List<SavedIndex> indexes = ScriptTargetIndexes(options, database, table.Schema, table.Table, HypotheticalIndexNames(conn, table.ObjectId));
                string tooLong = indexes.FirstOrDefault(i => i.Statement.Length > LoadPlanning.MaxIndexStatementLength)?.Name;
                decision = LoadPlanning.DecideHeapLoad(facts, !options.NoTableLock, staged, options.TruncateTarget, tooLong);
                if (decision.LoadAsHeap)
                {
                    string path = HeapLoadScriptPath(options, table);
                    try
                    {
                        HeapLoadScript.Write(path, HeapLoadScript.Build(database, table.Schema, table.Table, indexes.Select(i => (i.Name, i.Statement)), DateTime.UtcNow));
                        plan = new HeapLoadPlan { Indexes = indexes, ScriptPath = path };
                    }
                    catch (Exception ex)
                    {
                        decision = new HeapLoadDecision(false, string.Format("cannot write {0}: {1}", path, ex.Message.Trim().TrimEnd('.')));
                    }
                }
            }
            if (plan == null && decision.Reason != null)
            {
                PostToConsole(string.Format("{0}: not loaded as a heap: {1}; disabling indexes instead.", table.Label, decision.Reason), InfoLevel.Warning);
            }
            return plan;
        }

        /// <summary>The catalog facts <see cref="LoadPlanning.DecideHeapLoad"/> needs, in one batch. Names are the first found, by id or name.</summary>
        static HeapLoadFacts ReadHeapLoadFacts(SqlConnection conn, int objectId)
        {
            // The emptiness check needs the table in FROM: its name is quoted by QUOTENAME on the server, from the object id.
            const string sql = @"DECLARE @empty BIT = 1;
DECLARE @emptySql NVARCHAR(MAX) = N'SELECT @empty = CASE WHEN EXISTS (SELECT 1 FROM '
    + QUOTENAME(OBJECT_SCHEMA_NAME(@objectId)) + N'.' + QUOTENAME(OBJECT_NAME(@objectId)) + N') THEN 0 ELSE 1 END;';
EXEC sys.sp_executesql @emptySql, N'@empty BIT OUTPUT', @empty = @empty OUTPUT;

SELECT CAST(CASE WHEN EXISTS (SELECT 1 FROM sys.indexes i WHERE i.object_id = t.object_id AND i.type = 1 AND i.is_hypothetical = 0) THEN 1 ELSE 0 END AS BIT) AS clustered_rowstore,
       CAST(CASE WHEN EXISTS (SELECT 1 FROM sys.indexes i WHERE i.object_id = t.object_id AND i.type = 5 AND i.is_hypothetical = 0) THEN 1 ELSE 0 END AS BIT) AS clustered_columnstore,
       t.is_memory_optimized,
       (SELECT TOP (1) fk.name FROM sys.foreign_keys fk WHERE fk.referenced_object_id = t.object_id ORDER BY fk.name) AS referencing_fk,
       (SELECT TOP (1) i.name FROM sys.indexes i WHERE i.object_id = t.object_id AND i.type IN (3, 4) ORDER BY i.index_id) AS xml_or_spatial,
       (SELECT TOP (1) i.name FROM sys.indexes i WHERE i.object_id = t.object_id AND i.index_id > 0 AND i.is_disabled = 1 ORDER BY i.index_id) AS disabled_index,
       (SELECT TOP (1) OBJECT_SCHEMA_NAME(v.object_id) + N'.' + v.name
        FROM sys.sql_expression_dependencies d
             INNER JOIN sys.views v ON v.object_id = d.referencing_id
        WHERE d.referenced_id = t.object_id
              AND EXISTS (SELECT 1 FROM sys.indexes vi WHERE vi.object_id = v.object_id AND vi.index_id = 1)
        ORDER BY OBJECT_SCHEMA_NAME(v.object_id), v.name) AS indexed_view,
       CAST(CASE WHEN t.temporal_type <> 0 THEN 1 ELSE 0 END AS BIT) AS temporal,
       CAST(CASE WHEN EXISTS (SELECT 1 FROM sys.change_tracking_tables ct WHERE ct.object_id = t.object_id) THEN 1 ELSE 0 END AS BIT) AS change_tracking,
       CAST(CASE WHEN t.is_replicated = 1 OR t.is_merge_published = 1 OR t.is_sync_tran_subscribed = 1 THEN 1 ELSE 0 END AS BIT) AS replicated,
       t.is_tracked_by_cdc,
       CAST(CASE WHEN t.filestream_data_space_id IS NOT NULL
                   OR EXISTS (SELECT 1 FROM sys.columns c WHERE c.object_id = t.object_id AND c.is_filestream = 1) THEN 1 ELSE 0 END AS BIT) AS filestream,
       CAST(CASE WHEN EXISTS (SELECT 1 FROM sys.fulltext_indexes f WHERE f.object_id = t.object_id) THEN 1 ELSE 0 END AS BIT) AS full_text,
       @empty AS is_empty
FROM sys.tables t
WHERE t.object_id = @objectId;";

            using (var cmd = new SqlCommand(sql, conn))
            {
                cmd.Parameters.Add("@objectId", SqlDbType.Int).Value = objectId;
                using (var reader = cmd.ExecuteReader())
                {
                    if (!reader.Read())
                    {
                        throw new InvalidOperationException("Target table not found in sys.tables.");
                    }
                    string Name(int i) => reader.IsDBNull(i) ? null : reader.GetString(i);
                    return new HeapLoadFacts(
                        ClusteredRowstore: reader.GetBoolean(0),
                        ClusteredColumnstore: reader.GetBoolean(1),
                        MemoryOptimized: reader.GetBoolean(2),
                        ReferencingForeignKey: Name(3),
                        XmlOrSpatialIndex: Name(4),
                        DisabledIndex: Name(5),
                        IndexedView: Name(6),
                        Temporal: reader.GetBoolean(7),
                        ChangeTracking: reader.GetBoolean(8),
                        Replicated: reader.GetBoolean(9),
                        ChangeDataCapture: reader.GetBoolean(10),
                        Filestream: reader.GetBoolean(11),
                        FullText: reader.GetBoolean(12),
                        Empty: reader.GetBoolean(13));
                }
            }
        }

        /// <summary>
        /// Every index and key constraint of the target, scripted by SMO in creation order (clustered first, then
        /// <see cref="IndexCreationRank"/>). Filegroup or partition scheme, compression, fill factor, filters and
        /// included columns are kept, hypothetical indexes skipped: unlike copy, the indexes come back in the same database.
        /// </summary>
        static List<SavedIndex> ScriptTargetIndexes(ImportOptions options, string databaseName, string schema, string table, HashSet<string> hypothetical)
        {
            string connectionString = options.GetTargetConnectionString();

            var scripting = new ScriptingOptions
            {
                IncludeIfNotExists = false,
                ScriptBatchTerminator = false,
                ScriptDataCompression = true
            };

            using (var sql = SqlConnectionFactory.Create(connectionString))
            {
                var server = new Server(new ServerConnection(sql));
                try
                {
                    Table target = server.Databases[databaseName]?.Tables[table, schema];
                    if (target == null)
                    {
                        throw new InvalidOperationException("Target table not found by SMO.");
                    }
                    return target.Indexes.Cast<Index>()
                        .Where(i => !hypothetical.Contains(i.Name))
                        .OrderBy(i => IndexCreationRank(i.IndexType, i.IsClustered))
                        .ThenBy(i => i.ID)
                        .Select(i => new SavedIndex(i.Name, i.IsClustered, i.IndexKeyType != IndexKeyType.None,
                            string.Join("\n", i.Script(scripting).Cast<string>())))
                        .ToList();
                }
                finally
                {
                    server.ConnectionContext.Disconnect();
                }
            }
        }

        /// <summary>The extended properties on a table (class 1, minor_id 0) that load-as-heap uses as markers.</summary>
        private const string HeapLoadMarkersSql = @"SELECT ep.name
FROM sys.extended_properties ep
WHERE ep.class = 1
      AND ep.minor_id = 0
      AND ep.major_id = OBJECT_ID(QUOTENAME(@schema) + N'.' + QUOTENAME(@table))
      AND ep.name LIKE N'bcpjson[_]load[_]as[_]heap[_]%'";

        private static SqlCommand HeapLoadMarkersCommand(SqlConnection conn, string sql, string schema, string table)
        {
            var cmd = new SqlCommand(sql, conn);
            cmd.Parameters.Add("@schema", SqlDbType.NVarChar, 128).Value = schema;
            cmd.Parameters.Add("@table", SqlDbType.NVarChar, 128).Value = table;
            return cmd;
        }

        static bool HasHeapLoadMarkers(SqlConnection conn, string schema, string table)
        {
            using (SqlCommand cmd = HeapLoadMarkersCommand(conn, "SELECT CAST(CASE WHEN EXISTS (" + HeapLoadMarkersSql + ") THEN 1 ELSE 0 END AS BIT);", schema, table))
            {
                return (bool)cmd.ExecuteScalar();
            }
        }

        /// <summary>One marker per saved index, in creation order, whose value is that index's statement.</summary>
        static void SetHeapLoadMarkers(SqlConnection conn, string schema, string table, List<SavedIndex> indexes)
        {
            for (int i = 0; i < indexes.Count; i++)
            {
                using (SqlCommand cmd = ExtendedPropertyCommand(conn, "sys.sp_addextendedproperty", HeapLoadScript.MarkerName(i + 1), schema, table))
                {
                    // sql_variant holds at most 8,000 bytes: statements are at most MaxIndexStatementLength characters.
                    cmd.Parameters.Add("@value", SqlDbType.NVarChar, 4000).Value = indexes[i].Statement;
                    cmd.ExecuteNonQuery();
                }
            }
        }

        /// <summary>Removes every bcpjson_load_as_heap_% extended property of the table.</summary>
        static void RemoveHeapLoadMarkers(SqlConnection conn, string schema, string table)
        {
            var names = new List<string>();
            using (SqlCommand cmd = HeapLoadMarkersCommand(conn, HeapLoadMarkersSql + " ORDER BY ep.name;", schema, table))
            using (var reader = cmd.ExecuteReader())
            {
                while (reader.Read())
                {
                    names.Add(reader.GetString(0));
                }
            }
            foreach (string name in names)
            {
                using (SqlCommand cmd = ExtendedPropertyCommand(conn, "sys.sp_dropextendedproperty", name, schema, table))
                {
                    cmd.ExecuteNonQuery();
                }
            }
        }

        private static SqlCommand ExtendedPropertyCommand(SqlConnection conn, string procedure, string name, string schema, string table)
        {
            var cmd = new SqlCommand(procedure, conn) { CommandType = CommandType.StoredProcedure };
            cmd.Parameters.Add("@name", SqlDbType.NVarChar, 128).Value = name;
            cmd.Parameters.Add("@level0type", SqlDbType.VarChar, 128).Value = "SCHEMA";
            cmd.Parameters.Add("@level0name", SqlDbType.NVarChar, 128).Value = schema;
            cmd.Parameters.Add("@level1type", SqlDbType.VarChar, 128).Value = "TABLE";
            cmd.Parameters.Add("@level1name", SqlDbType.NVarChar, 128).Value = table;
            return cmd;
        }

        /// <summary>
        /// TableWork before: truncate (--truncate-target), check the table is still empty, log the statements, set the
        /// markers, then drop the nonclustered indexes (reverse creation order) and the clustered one.
        /// </summary>
        static void PrepareHeapLoad(ImportOptions options, ImportTable table, HeapLoadPlan plan, int parts)
        {
            using (var conn = SqlConnectionFactory.Create(options.GetTargetConnectionString()))
            {
                conn.Open();
                if (options.TruncateTarget)
                {
                    ExecuteSql(conn, "TRUNCATE TABLE " + table.QualifiedName + ";");
                    PostToConsole(string.Format("{0}: Truncate Complete.", table.Label));
                }

                using (var cmd = new SqlCommand("SELECT TOP (1) 1 FROM " + table.QualifiedName + ";", conn))
                {
                    cmd.CommandTimeout = 0;
                    if (cmd.ExecuteScalar() != null)
                    {
                        DeleteHeapLoadScript(plan);
                        throw new InvalidOperationException(string.Format("{0}: rows were added after planning; not loaded.", table.Label));
                    }
                }

                foreach (SavedIndex index in plan.Indexes)
                {
                    PostToConsole(string.Format("{0}: {1}", table.Label, index.Statement));
                }
                SetHeapLoadMarkers(conn, table.Schema, table.Table, plan.Indexes);

                List<SavedIndex> dropOrder = plan.Indexes.Where(i => !i.IsClustered).Reverse()
                    .Concat(plan.Indexes.Where(i => i.IsClustered))
                    .ToList();
                foreach (SavedIndex index in dropOrder)
                {
                    ExecuteSql(conn, index.IsConstraint
                        ? "ALTER TABLE " + table.QualifiedName + " DROP CONSTRAINT " + QuoteName(index.Name) + ";"
                        : "DROP INDEX " + QuoteName(index.Name) + " ON " + table.QualifiedName + ";");
                }
                PostToConsole(string.Format("{0}: {1} index(es) dropped; loading {2} parts in parallel as a heap.", table.Label, dropOrder.Count, parts));
            }
        }

        /// <summary>
        /// TableWork after, whether or not the load succeeded, deciding only from the indexes that exist now: the
        /// clustered index first (if it cannot be created, stop: nonclustered indexes built on the heap would be
        /// rebuilt again), then each missing nonclustered one. Markers and script are removed only when every saved
        /// index exists. Errors are reported here, never thrown.
        /// </summary>
        static void FinishHeapLoad(ImportOptions options, ImportTable table, HeapLoadPlan plan)
        {
            SqlConnection conn = null;
            HashSet<string> existing;
            try
            {
                conn = SqlConnectionFactory.Create(options.GetTargetConnectionString());
                conn.Open();
                existing = ExistingIndexNames(conn, table.ObjectId);
            }
            catch (Exception ex)
            {
                conn?.Dispose();
                PostToConsole(RecreateFailedMessage(table.Label, string.Join(", ", plan.Indexes.Select(i => i.Name)), ex, plan.ScriptPath), InfoLevel.Error);
                return;
            }

            using (conn)
            {
                int created = 0;
                SavedIndex clustered = plan.Indexes.FirstOrDefault(i => i.IsClustered);
                if (clustered != null && !existing.Contains(clustered.Name))
                {
                    if (!TryCreateIndex(conn, table, clustered, plan.ScriptPath))
                    {
                        return;
                    }
                    created++;
                }

                bool allExist = true;
                foreach (SavedIndex index in plan.Indexes.Where(i => !i.IsClustered && !existing.Contains(i.Name)))
                {
                    if (TryCreateIndex(conn, table, index, plan.ScriptPath))
                    {
                        created++;
                    }
                    else
                    {
                        allExist = false;
                    }
                }
                if (!allExist)
                {
                    return;
                }

                try
                {
                    RemoveHeapLoadMarkers(conn, table.Schema, table.Table);
                }
                catch (Exception ex)
                {
                    PostToConsole(string.Format("{0}: every index exists, but its extended properties {1}* could not be removed: {2}. Remove them before the next import.",
                        table.Label, HeapLoadScript.MarkerPrefix, ReasonOf(ex)), InfoLevel.Error);
                    return;
                }
                DeleteHeapLoadScript(plan);
                // Nothing created: "before" stopped ahead of the drop (e.g. rows were added), so there is nothing to announce.
                if (created > 0)
                {
                    PostToConsole(string.Format("{0}: clustered index {1} and {2} other index(es) recreated.",
                        table.Label, clustered?.Name, plan.Indexes.Count(i => !i.IsClustered)));
                }
            }
        }

        private static bool TryCreateIndex(SqlConnection conn, ImportTable table, SavedIndex index, string scriptPath)
        {
            try
            {
                ExecuteSql(conn, index.Statement);   // CommandTimeout = 0
                return true;
            }
            catch (Exception ex)
            {
                PostToConsole(RecreateFailedMessage(table.Label, index.Name, ex, scriptPath), InfoLevel.Error);
                return false;
            }
        }

        private static string RecreateFailedMessage(string label, string name, Exception ex, string scriptPath)
        {
            return string.Format("{0}: Could not recreate {1}: {2}. Run the script in {3}.", label, name, ReasonOf(ex), scriptPath);
        }

        /// <summary>The first SQL error says why; SQL Server adds "The statement has been terminated." on a line of its own.</summary>
        private static string ReasonOf(Exception ex)
        {
            string reason = ex is SqlException sqlEx && sqlEx.Errors.Count > 0 ? sqlEx.Errors[0].Message : ex.Message;
            return reason.Trim().TrimEnd('.');
        }

        private static HashSet<string> ExistingIndexNames(SqlConnection conn, int objectId)
        {
            return IndexNames(conn, objectId, "");
        }

        /// <summary>Hypothetical indexes (Database Tuning Advisor) hold no data: load-as-heap neither drops nor recreates them.</summary>
        private static HashSet<string> HypotheticalIndexNames(SqlConnection conn, int objectId)
        {
            return IndexNames(conn, objectId, " AND i.is_hypothetical = 1");
        }

        private static HashSet<string> IndexNames(SqlConnection conn, int objectId, string condition)
        {
            var names = new HashSet<string>(StringComparer.Ordinal);
            using (var cmd = new SqlCommand("SELECT i.name FROM sys.indexes i WHERE i.object_id = @objectId AND i.name IS NOT NULL" + condition + ";", conn))
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

        private static void DeleteHeapLoadScript(HeapLoadPlan plan)
        {
            try
            {
                File.Delete(plan.ScriptPath);
            }
            catch (Exception ex) when (ex is IOException || ex is UnauthorizedAccessException)
            {
                PostToConsole(string.Format("Could not delete {0}: {1}", plan.ScriptPath, ex.Message), InfoLevel.Warning);
            }
        }
    }
}
