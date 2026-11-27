using Microsoft.Data.SqlClient;
using Microsoft.SqlServer.Management.Common;
using Microsoft.SqlServer.Management.Smo;
using System.Collections.Concurrent;
using System.Collections.Generic;
using Index = Microsoft.SqlServer.Management.Smo.Index;
using ActivationOrder = Microsoft.SqlServer.Management.Smo.Agent.ActivationOrder;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace bcpJson
{
    partial class Program
    {
        private static long BulkCopyTables(CopyOptions setting)
        {
            long totalrowCount = 0;
            List<TableRef> tables = GetSourceTables(setting.SourceConnectionString(),
                BuildSourceObjectIDList(setting.IncludeTablesList, setting.ExcludeTablesList, setting.IncludeSchemaList, setting.ExcludeSchemaList));

            if (tables.Count == 0)
            {
                PostToConsole("No tables matched the include/exclude filters.", InfoLevel.Warning);
                return 0;
            }

            int maxTasks = setting.MaxParallelTasks;
            PostToConsole(maxTasks > 0 ? string.Format("Parallel table task operations: {0}", maxTasks) : "Parallel table task operations: Unlimited", InfoLevel.Info);

            if (setting.DropTarget)
            {
                DropForeignKeysOwnedBy(setting, tables);
                DropTargetTables(setting, tables);
            }

            var createdTables = new ConcurrentBag<TableRef>();
            List<CopyItem> items = PlanCopyItems(setting, tables, createdTables);

            // Every item reports its own failures, so RunLargestFirst has nothing to collect but a failed preparation,
            // which is reported once per table.
            WorkQueue.RunLargestFirst(items, i => i.Size, maxTasks, item =>
            {
                try
                {
                    item.Work.Run(() => Interlocked.Add(ref totalrowCount, CopyItemRanges(setting, item)));
                }
                catch (InvalidOperationException ex) when (ex.InnerException is HeapLoadRefusedException)
                {
                    // TableWork wraps the failed preparation; CheckHeapLoadLeftovers has reported it already.
                }
                catch (Exception ex)
                {
                    if (Interlocked.Exchange(ref item.Context.FailureReported, 1) == 0)
                    {
                        PostToConsole(ex.Message, InfoLevel.Error);
                    }
                }
            });

            // Foreign keys span tables, so they can only be created once every table (and its keys) exists.
            if (setting.ForeignKeys && !createdTables.IsEmpty)
            {
                CreateTargetForeignKeys(setting, createdTables.OrderBy(t => t.DisplayName, StringComparer.Ordinal).ToList());
            }

            // Triggers last: created after every load, they never fire on the copied rows.
            if (setting.Triggers && !createdTables.IsEmpty)
            {
                CreateTargetTriggers(setting, createdTables.OrderBy(t => t.DisplayName, StringComparer.Ordinal).ToList());
            }

            return totalrowCount;
        }

        /// <summary>
        /// --drop-target: removes the foreign keys defined on the target tables that are about to be dropped and
        /// recreated. They would be dropped with their tables anyway, but while they exist they can block dropping
        /// the tables they reference (e.g. when re-running a copy made with --foreign-keys). Foreign keys on tables
        /// outside the copy are left alone; if one of those blocks a drop, that table reports an error.
        /// </summary>
        private static void DropForeignKeysOwnedBy(CopyOptions setting, List<TableRef> tables)
        {
            const string sql = @"SELECT fk.name
FROM sys.foreign_keys fk
     JOIN sys.tables t ON t.object_id = fk.parent_object_id
     JOIN sys.schemas s ON s.schema_id = t.schema_id
WHERE s.name = @schema AND t.name = @table;";

            int dropped = 0;
            using (var conn = SqlConnectionFactory.Create(setting.TargetConnectionString()))
            {
                conn.Open();
                foreach (TableRef table in tables)
                {
                    var names = new List<string>();
                    using (var cmd = new SqlCommand(sql, conn))
                    {
                        cmd.Parameters.Add("@schema", SqlDbType.NVarChar, 128).Value = table.Schema;
                        cmd.Parameters.Add("@table", SqlDbType.NVarChar, 128).Value = table.Name;
                        using (var reader = cmd.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                names.Add(reader.GetString(0));
                            }
                        }
                    }

                    foreach (string name in names)
                    {
                        try
                        {
                            ExecuteSql(conn, "ALTER TABLE " + table.QuotedName + " DROP CONSTRAINT " + QuoteName(name) + ";");
                            dropped++;
                        }
                        catch (Exception ex)
                        {
                            PostToConsole(string.Format("{0}: Could not drop foreign key {1}: {2}", table.DisplayName, name, ex.Message), InfoLevel.Error);
                        }
                    }
                }
            }

            if (dropped > 0)
            {
                PostToConsole(string.Format("Dropped {0} foreign key(s) on target tables that will be recreated (--drop-target).", dropped));
            }
        }

        /// <summary>
        /// --drop-target: drops the existing target tables one at a time before the parallel copy starts. Dropping
        /// them concurrently deadlocks on SQL Server 2019 (the drops lock shared metadata such as dependent triggers),
        /// and a deadlock victim would leave its table uncopied. The parallel copy then only creates tables.
        /// </summary>
        private static void DropTargetTables(CopyOptions setting, List<TableRef> tables)
        {
            using (var conn = SqlConnectionFactory.Create(setting.TargetConnectionString()))
            {
                conn.Open();
                foreach (TableRef table in tables)
                {
                    try
                    {
                        using (var cmd = new SqlCommand("SELECT CASE WHEN OBJECT_ID(@name, N'U') IS NULL THEN 0 ELSE 1 END;", conn))
                        {
                            cmd.Parameters.Add("@name", SqlDbType.NVarChar, 520).Value = table.QuotedName;
                            if ((int)cmd.ExecuteScalar() == 0)
                            {
                                continue;
                            }
                        }
                        PostToConsole(string.Format("{0}: Target table exists. Dropping.", table.DisplayName));
                        ExecuteSql(conn, "DROP TABLE " + table.QuotedName + ";");
                    }
                    catch (Exception ex)
                    {
                        PostToConsole(string.Format("{0}: Could not drop the target table: {1}", table.DisplayName, ex.Message), InfoLevel.Error);
                    }
                }
            }
        }

        /// <summary>
        /// --foreign-keys: recreates the foreign keys of every table this run created, one at a time (each takes
        /// schema locks on both tables). SMO scripts them from the source, so multi-column keys, references to
        /// unique constraints, cross-schema references, ON DELETE/ON UPDATE actions and the enabled/trusted state
        /// carry over; trusted keys are created WITH CHECK, which validates the copied rows. A key whose referenced
        /// table or key is missing in the target is reported as an error and the others are still created.
        /// </summary>
        private static void CreateTargetForeignKeys(CopyOptions setting, List<TableRef> createdTables)
        {
            var scripting = new ScriptingOptions
            {
                DriForeignKeys = true,
                SchemaQualifyForeignKeysReferences = true,
                IncludeIfNotExists = false,
                ScriptBatchTerminator = false
            };

            int total = 0;
            int createdCount = 0;
            string sourceConnectionString = setting.SourceConnectionString();
            using (var sourceSql = SqlConnectionFactory.Create(sourceConnectionString))
            using (var targetConn = SqlConnectionFactory.Create(setting.TargetConnectionString()))
            {
                var sourceServer = new Server(new ServerConnection(sourceSql));
                try
                {
                    targetConn.Open();
                    Database sourceDatabase = sourceServer.Databases[new SqlConnectionStringBuilder(sourceConnectionString).InitialCatalog];
                    foreach (TableRef tableRef in createdTables)
                    {
                        Table sourceTable = sourceDatabase.Tables.ItemById(tableRef.ObjectId);
                        foreach (ForeignKey foreignKey in sourceTable.ForeignKeys.Cast<ForeignKey>().OrderBy(fk => fk.Name, StringComparer.Ordinal))
                        {
                            total++;
                            try
                            {
                                foreach (string statement in foreignKey.Script(scripting).Cast<string>())
                                {
                                    ExecuteSql(targetConn, statement);
                                }
                                createdCount++;
                            }
                            catch (Exception ex)
                            {
                                PostToConsole(string.Format("{0}: Could not create foreign key {1} (references {2}.{3}): {4}",
                                    tableRef.DisplayName, foreignKey.Name, foreignKey.ReferencedTableSchema, foreignKey.ReferencedTable, ex.GetBaseException().Message), InfoLevel.Error);
                            }
                        }
                    }
                }
                finally
                {
                    sourceServer.ConnectionContext.Disconnect();
                }
            }

            PostToConsole(string.Format("{0} of {1} foreign key(s) created.", createdCount, total),
                createdCount == total ? InfoLevel.Success : InfoLevel.Warning);
        }

        /// <summary>
        /// Recreates the source table's check constraints on the newly created target table, after the load.
        /// Trusted constraints are created WITH CHECK (validating the copied rows, which already satisfied them in
        /// the source); disabled or untrusted ones keep that state. A constraint that cannot be created (for example
        /// one that calls a function missing in the target) is reported as an error and the others are still created.
        /// </summary>
        private static void CreateTargetCheckConstraints(CopyOptions setting, Table sourceTable, TableRef tableRef)
        {
            List<Check> checks = sourceTable.Checks.Cast<Check>().OrderBy(c => c.Name, StringComparer.Ordinal).ToList();
            if (checks.Count == 0)
            {
                return;
            }

            var scripting = new ScriptingOptions
            {
                DriChecks = true,
                IncludeIfNotExists = false,
                ScriptBatchTerminator = false
            };

            int createdCount = 0;
            using (var conn = SqlConnectionFactory.Create(setting.TargetConnectionString()))
            {
                conn.Open();
                foreach (Check check in checks)
                {
                    try
                    {
                        foreach (string statement in check.Script(scripting).Cast<string>())
                        {
                            ExecuteSql(conn, statement);
                        }
                        createdCount++;
                    }
                    catch (Exception ex)
                    {
                        PostToConsole(string.Format("{0}: Could not create check constraint {1}: {2}", tableRef.DisplayName, check.Name, ex.GetBaseException().Message), InfoLevel.Error);
                    }
                }
            }

            PostToConsole(string.Format("{0}: {1} of {2} check constraint(s) created.", tableRef.DisplayName, createdCount, checks.Count),
                createdCount == checks.Count ? InfoLevel.Info : InfoLevel.Warning);
        }

        /// <summary>
        /// --triggers: recreates the DML triggers of every table this run created, after all tables are loaded so
        /// they never fire on the copied rows. Each trigger is scripted by SMO with its definition; the enabled state
        /// and FIRST/LAST firing order are then applied explicitly. Encrypted triggers cannot be scripted, and
        /// triggers referencing objects missing in the target fail; both are reported as errors.
        /// </summary>
        private static void CreateTargetTriggers(CopyOptions setting, List<TableRef> createdTables)
        {
            var scripting = new ScriptingOptions
            {
                IncludeIfNotExists = false,
                ScriptBatchTerminator = false
            };

            int total = 0;
            int createdCount = 0;
            string sourceConnectionString = setting.SourceConnectionString();
            using (var sourceSql = SqlConnectionFactory.Create(sourceConnectionString))
            using (var targetConn = SqlConnectionFactory.Create(setting.TargetConnectionString()))
            {
                var sourceServer = new Server(new ServerConnection(sourceSql));
                try
                {
                    targetConn.Open();
                    Database sourceDatabase = sourceServer.Databases[new SqlConnectionStringBuilder(sourceConnectionString).InitialCatalog];
                    foreach (TableRef tableRef in createdTables)
                    {
                        Table sourceTable = sourceDatabase.Tables.ItemById(tableRef.ObjectId);
                        foreach (Trigger trigger in sourceTable.Triggers.Cast<Trigger>().OrderBy(t => t.Name, StringComparer.Ordinal))
                        {
                            total++;
                            string quotedTrigger = QuoteName(tableRef.Schema) + "." + QuoteName(trigger.Name);
                            try
                            {
                                if (trigger.IsEncrypted)
                                {
                                    throw new InvalidOperationException("the trigger is encrypted (WITH ENCRYPTION), so its definition cannot be copied.");
                                }

                                foreach (string statement in trigger.Script(scripting).Cast<string>())
                                {
                                    ExecuteSql(targetConn, statement);
                                }

                                foreach (string orderSql in TriggerOrderStatements(quotedTrigger, trigger.InsertOrder, trigger.UpdateOrder, trigger.DeleteOrder))
                                {
                                    ExecuteSql(targetConn, orderSql);
                                }

                                if (!trigger.IsEnabled)
                                {
                                    ExecuteSql(targetConn, "DISABLE TRIGGER " + quotedTrigger + " ON " + tableRef.QuotedName + ";");
                                }
                                createdCount++;
                            }
                            catch (Exception ex)
                            {
                                PostToConsole(string.Format("{0}: Could not create trigger {1}: {2}", tableRef.DisplayName, trigger.Name, ex.GetBaseException().Message), InfoLevel.Error);
                            }
                        }
                    }
                }
                finally
                {
                    sourceServer.ConnectionContext.Disconnect();
                }
            }

            PostToConsole(string.Format("{0} of {1} trigger(s) created.", createdCount, total),
                createdCount == total ? InfoLevel.Success : InfoLevel.Warning);
        }

        /// <summary>sp_settriggerorder calls for the statement types whose firing order is First or Last.</summary>
        internal static IEnumerable<string> TriggerOrderStatements(string quotedTrigger, ActivationOrder insertOrder, ActivationOrder updateOrder, ActivationOrder deleteOrder)
        {
            foreach (var (statementType, order) in new[] { ("INSERT", insertOrder), ("UPDATE", updateOrder), ("DELETE", deleteOrder) })
            {
                if (order == ActivationOrder.First || order == ActivationOrder.Last)
                {
                    yield return string.Format("EXEC sp_settriggerorder @triggername = {0}, @order = N'{1}', @stmttype = N'{2}';",
                        SqlLiteral(quotedTrigger), order, statementType);
                }
            }
        }

        /// <summary>One unit of copy work: a whole table (Range null) or one key range of a split table.</summary>
        private sealed record CopyItem(TableRef Table, KeyRange Range, TableSplit Split, long Size, CopyTableContext Context, TableWork Work);

        /// <summary>What a table's preparation (TableWork's before) found out, shared by the table's items.</summary>
        private sealed class CopyTableContext
        {
            /// <summary>The columns copied: computed and rowversion columns are left out.</summary>
            public List<string> Columns;
            public string TargetName;
            public bool Created;
            public bool NeedsBcp;
            public LoadMode Mode;
            /// <summary>Set at the end of a successful preparation.</summary>
            public bool Prepared;
            public int FailedParts;
            /// <summary>Sequential mode: 1 once an item has taken every part.</summary>
            public int SequentialClaimed;
            /// <summary>1 once a failed preparation was reported (every item of the table sees it).</summary>
            public int FailureReported;
            /// <summary>--load-as-heap: set before the indexes are dropped, so the finish recreates them whatever happens next.</summary>
            public HeapLoadTarget HeapTarget;
            public HeapLoadPlan HeapPlan;
            /// <summary>--disable-indexes (or the --load-as-heap fallback): set before the first index is disabled, so the finish rebuilds them whatever happens next.</summary>
            public List<IndexInfo> Disabled;
            public int TargetObjectId;
        }

        /// <summary>
        /// The copy work items, sized by rows: one per unsplit table and one per part of a table matched by
        /// --split-tables. Ranges are planned on the source (the snapshot with --snapshot). A table copied through bcp
        /// is never split. When planning cannot start (no connection), the error is reported and nothing is copied.
        /// </summary>
        private static List<CopyItem> PlanCopyItems(CopyOptions setting, List<TableRef> tables, ConcurrentBag<TableRef> createdTables)
        {
            WarnUnmatchedSplitRequests(setting.SplitRequests, tables);
            var items = new List<CopyItem>();
            try
            {
                using (var conn = SqlConnectionFactory.Create(setting.SourceConnectionString()))
                {
                    conn.Open();
                    Dictionary<int, long> rowCounts = TableRowCounts(conn);
                    foreach (TableRef table in tables)
                    {
                        PlanCopyTable(setting, conn, table, rowCounts, createdTables, items);
                    }
                }
            }
            catch (Exception ex)
            {
                PostToConsole(ex.Message, InfoLevel.Error);
                return new List<CopyItem>();
            }
            return items;
        }

        /// <summary>Adds one table's copy items to <paramref name="items"/>; a table whose planning fails is reported and left out.</summary>
        private static void PlanCopyTable(CopyOptions setting, SqlConnection conn, TableRef table, Dictionary<int, long> rowCounts,
            ConcurrentBag<TableRef> createdTables, List<CopyItem> items)
        {
            long rows = rowCounts.TryGetValue(table.ObjectId, out long counted) ? counted : 0;
            try
            {
                TableSplit split = null;
                int? requested = SplitRequest.PartsFor(setting.SplitRequests, table.DisplayName);
                if (requested != null)
                {
                    split = SplitOrWhole(table.DisplayName, () =>
                    {
                        if (HasBcpOnlyColumns(conn, table.ObjectId))
                        {
                            return (null, "copied through bcp (geography/geometry/hierarchyid columns)");
                        }
                        TableSplit planned = PlanSplit(conn, table, requested.Value, out string reason);
                        return (planned, reason);
                    });
                }

                var context = new CopyTableContext();
                int parts = split?.Ranges.Count ?? 1;
                var work = new TableWork(table.DisplayName, parts,
                    () => PrepareCopyTarget(setting, table, context, parts),
                    () => FinishCopiedTable(setting, table, context, parts, createdTables));
                if (split == null)
                {
                    items.Add(new CopyItem(table, null, null, rows, context, work));
                    return;
                }
                for (int i = 0; i < split.Ranges.Count; i++)
                {
                    long estimate = split.EstimatedRows[i] >= 0 ? split.EstimatedRows[i] : rows / split.Ranges.Count;
                    items.Add(new CopyItem(table, split.Ranges[i], split, estimate, context, work));
                }
            }
            catch (Exception ex)
            {
                PostToConsole(string.Format("{0}: {1}", table.DisplayName, ex.Message), InfoLevel.Error);
            }
        }

        /// <summary>Whether the table has geography, geometry or hierarchyid columns, which SqlBulkCopy cannot stream (copied through bcp).</summary>
        private static bool HasBcpOnlyColumns(SqlConnection conn, int objectId)
        {
            const string sql = @"SET NOCOUNT ON;
SELECT CASE WHEN EXISTS (SELECT 1 FROM sys.columns c INNER JOIN sys.types t ON t.user_type_id = c.user_type_id
    WHERE c.object_id = @objectId AND t.is_user_defined = 0 AND t.name IN (N'geography', N'geometry', N'hierarchyid')) THEN 1 ELSE 0 END;";
            using (var cmd = new SqlCommand(sql, conn))
            {
                cmd.Parameters.Add("@objectId", SqlDbType.Int).Value = objectId;
                return (int)cmd.ExecuteScalar() == 1;
            }
        }

        /// <summary>
        /// A table's preparation, once before its first part: drops/truncates/creates the target (PrepareTargetTable)
        /// and works out the columns, whether bcp is needed and, for a split table, the load mode of the target as it
        /// is now (a table this run created is a heap until its indexes are built after the load).
        /// </summary>
        private static void PrepareCopyTarget(CopyOptions setting, TableRef tableRef, CopyTableContext context, int parts)
        {
            string sourceConnectionString = setting.SourceConnectionString();
            string targetConnectionString = setting.TargetConnectionString();

            // A target left without some of its indexes by an earlier --load-as-heap run is refused before anything
            // touches it, unless --drop-target replaces it anyway.
            if (!setting.DropTarget)
            {
                using (var conn = SqlConnectionFactory.Create(targetConnectionString))
                {
                    conn.Open();
                    if (!CheckHeapLoadLeftovers(conn, CopyHeapTarget(setting, tableRef.Schema, tableRef.Name, 0)))
                    {
                        throw new HeapLoadRefusedException();
                    }
                }
            }

            using (var sourceSql = SqlConnectionFactory.Create(sourceConnectionString))
            using (var targetSql = SqlConnectionFactory.Create(targetConnectionString))
            {
                var sourceServer = new Server(new ServerConnection(sourceSql));
                var targetServer = new Server(new ServerConnection(targetSql));
                try
                {
                    Table sourceTable = SourceTable(sourceServer, sourceConnectionString, tableRef);

                    Database targetDatabase = targetServer.Databases[new SqlConnectionStringBuilder(targetConnectionString).InitialCatalog];
                    if (targetDatabase == null)
                    {
                        throw new InvalidOperationException("Target database not found.");
                    }

                    Table targetTable = PrepareTargetTable(setting, sourceTable, targetDatabase, targetServer, out bool created);
                    context.Created = created;
                    context.TargetName = QuoteName(targetTable.Schema) + "." + QuoteName(targetTable.Name);
                    context.NeedsBcp = sourceTable.Columns.Cast<Column>().Any(col =>
                        col.DataType.SqlDataType == SqlDataType.Geography ||
                        col.DataType.SqlDataType == SqlDataType.Geometry ||
                        col.DataType.SqlDataType == SqlDataType.HierarchyId);

                    // Computed and rowversion columns cannot be written; they are recalculated by the target.
                    context.Columns = sourceTable.Columns.Cast<Column>()
                        .Where(c => !c.Computed && c.DataType.SqlDataType != SqlDataType.Timestamp)
                        .Select(c => c.Name)
                        .ToList();
                }
                finally
                {
                    sourceServer.ConnectionContext.Disconnect();
                    targetServer.ConnectionContext.Disconnect();
                }
            }

            bool mayDisable = !context.Created && (setting.DisableIndexes || setting.LoadAsHeap);
            if (parts > 1 || mayDisable)
            {
                using (var conn = SqlConnectionFactory.Create(targetConnectionString))
                {
                    conn.Open();
                    int objectId;
                    using (var cmd = new SqlCommand("SELECT OBJECT_ID(@name, N'U');", conn))
                    {
                        cmd.Parameters.Add("@name", SqlDbType.NVarChar, 520).Value = context.TargetName;
                        object id = cmd.ExecuteScalar();
                        if (id == null || id == DBNull.Value)
                        {
                            throw new InvalidOperationException("Target table not found after preparing it.");
                        }
                        objectId = (int)id;
                    }
                    context.TargetObjectId = objectId;
                    if (parts > 1 && setting.LoadAsHeap && !context.Created)
                    {
                        // Copy has truncated the table already (PrepareTargetTable), so the heap load does not truncate.
                        HeapLoadTarget heapTarget = CopyHeapTarget(setting, tableRef.Schema, tableRef.Name, objectId);
                        HeapLoadPlan plan = PlanHeapLoad(conn, heapTarget, false, out string notReason);
                        if (plan != null)
                        {
                            context.HeapTarget = heapTarget;
                            context.HeapPlan = plan;
                            PrepareHeapLoad(heapTarget, plan, parts);
                            context.Mode = LoadMode.Parallel;
                            context.Prepared = true;
                            return;
                        }
                        if (notReason != null)
                        {
                            PostToConsole(string.Format("{0}: not loaded as a heap: {1}; disabling indexes instead.", tableRef.DisplayName, notReason), InfoLevel.Warning);
                        }
                    }

                    var disabledIds = new HashSet<int>();
                    if (LoadPlanning.CopyDisablesIndexes(context.Created, setting.DisableIndexes, false, setting.LoadAsHeap))
                    {
                        if (IsMemoryOptimized(conn, objectId))
                        {
                            PostToConsole(string.Format("{0}: memory-optimized; --disable-indexes ignored.", tableRef.DisplayName));
                        }
                        else
                        {
                            List<IndexInfo> toDisable = LoadPlanning.IndexesToDisable(ReadIndexes(conn, objectId));
                            if (toDisable.Count > 0)
                            {
                                context.Disabled = toDisable;
                                DisableIndexes(conn, context.TargetName, toDisable);
                                PostToConsole(string.Format("{0}: Disabled indexes: {1}.", tableRef.DisplayName, string.Join(", ", toDisable.Select(i => i.Name))));
                                disabledIds = toDisable.Select(i => i.IndexId).ToHashSet();
                            }
                        }
                    }

                    if (parts > 1)
                    {
                        TargetShape shape = ReadTargetShape(conn, objectId, disabledIds);
                        string sequentialReason = LoadPlanning.SequentialReason(shape, !setting.NoTableLock, false);
                        context.Mode = sequentialReason == null ? LoadMode.Parallel : LoadMode.Sequential;
                        PostToConsole(sequentialReason == null
                            ? string.Format("{0}: {1} parts, loading in parallel.", tableRef.DisplayName, parts)
                            : string.Format("{0}: {1} parts, loading one after another ({2}).", tableRef.DisplayName, parts, sequentialReason));
                    }
                }
            }
            context.Prepared = true;
        }

        /// <summary>Copy's load-as-heap view of a target table: never truncated by the heap load, scripts in --recovery-path.</summary>
        private static HeapLoadTarget CopyHeapTarget(CopyOptions setting, string schema, string table, int objectId)
        {
            return new HeapLoadTarget("copy", setting.TargetConnectionString(), schema, table, objectId,
                !setting.NoTableLock, false, string.IsNullOrWhiteSpace(setting.RecoveryPath) ? null : setting.RecoveryPath);
        }

        /// <summary>The source table through SMO.</summary>
        private static Table SourceTable(Server sourceServer, string sourceConnectionString, TableRef tableRef)
        {
            Database sourceDatabase = sourceServer.Databases[new SqlConnectionStringBuilder(sourceConnectionString).InitialCatalog];
            Table sourceTable = sourceDatabase?.Tables.ItemById(tableRef.ObjectId);
            if (sourceTable == null)
            {
                throw new InvalidOperationException("Source table not found.");
            }
            return sourceTable;
        }

        /// <summary>
        /// Copies what one item covers and returns the rows copied: its own range, or the whole table (Range null).
        /// In sequential mode the first item of a table to start copies every part in key order and the others do
        /// nothing. Failures are reported here and counted in the table's context.
        /// </summary>
        private static long CopyItemRanges(CopyOptions setting, CopyItem item)
        {
            CopyTableContext context = item.Context;
            IEnumerable<KeyRange> ranges;
            if (item.Range == null || context.Mode == LoadMode.Parallel)
            {
                ranges = new[] { item.Range };
            }
            else if (Interlocked.Exchange(ref context.SequentialClaimed, 1) == 0)
            {
                ranges = item.Split.Ranges;
            }
            else
            {
                return 0;
            }

            long total = 0;
            foreach (KeyRange range in ranges)
            {
                string label = range == null ? item.Table.DisplayName : PartFiles.PartName(item.Table.DisplayName, range.Part);
                try
                {
                    if (context.NeedsBcp && range != null)
                    {
                        // bcp copies whole tables; planning never splits such a table, so this would copy it once per part.
                        throw new InvalidOperationException("this table is copied through bcp and cannot be copied in parts.");
                    }
                    long copied = context.NeedsBcp
                        ? CopyTableViaBcp(setting, item.Table)
                        : CopyTableViaBulkCopy(setting, context, item.Table, range, item.Split?.Column, label);
                    if (copied < 0)
                    {
                        Interlocked.Increment(ref context.FailedParts);
                    }
                    else
                    {
                        total += copied;
                    }
                }
                catch (Exception ex)
                {
                    Interlocked.Increment(ref context.FailedParts);
                    PostToConsole(string.Format("{0}: {1}", label, ex.Message), InfoLevel.Error);
                }
            }
            return total;
        }

        /// <summary>
        /// A table's finish, once after its last part: on a table this run created, and only when every part was
        /// copied, builds the keys, indexes and check constraints and records the table for foreign keys and triggers.
        /// Keys and indexes are built after the load (bulk loading a heap is fastest); an existing target keeps its own.
        /// </summary>
        private static void FinishCopiedTable(CopyOptions setting, TableRef tableRef, CopyTableContext context, int parts, ConcurrentBag<TableRef> createdTables)
        {
            // Load as heap: recreate the indexes whatever happened, even when the preparation failed after the drop.
            if (context.HeapPlan != null)
            {
                FinishHeapLoad(context.HeapTarget, context.HeapPlan);
            }

            // Disabled indexes are rebuilt whatever happened, even when the preparation failed after disabling some.
            if (context.Disabled?.Count > 0)
            {
                RebuildDisabledIndexes(setting.TargetConnectionString(), context.TargetObjectId, tableRef.DisplayName, context.TargetName, context.Disabled);
            }

            if (!context.Prepared)
            {
                return;   // the failed preparation is reported by the items
            }

            int failed = Volatile.Read(ref context.FailedParts);
            if (failed > 0)
            {
                // An unsplit table's failure is already reported; a split table also says what it left behind.
                if (parts > 1)
                {
                    PostToConsole(string.Format(context.Created
                            ? "{0}: copy incomplete ({1} of {2} parts failed); keys, indexes and check constraints not created."
                            : "{0}: copy incomplete ({1} of {2} parts failed).",
                        tableRef.DisplayName, failed, parts), InfoLevel.Error);
                }
                return;
            }

            if (!context.Created)
            {
                return;
            }

            try
            {
                string sourceConnectionString = setting.SourceConnectionString();
                using (var sourceSql = SqlConnectionFactory.Create(sourceConnectionString))
                {
                    var sourceServer = new Server(new ServerConnection(sourceSql));
                    try
                    {
                        Table sourceTable = SourceTable(sourceServer, sourceConnectionString, tableRef);
                        if (setting.NoIndexes)
                        {
                            PostToConsole(string.Format("{0}: Primary key and indexes not created (--no-indexes).", tableRef.DisplayName));
                        }
                        else
                        {
                            CreateTargetIndexes(setting, sourceTable, tableRef);
                        }

                        if (setting.NoCheckConstraints)
                        {
                            if (sourceTable.Checks.Count > 0)
                            {
                                PostToConsole(string.Format("{0}: Check constraints not created (--no-check-constraints).", tableRef.DisplayName));
                            }
                        }
                        else
                        {
                            CreateTargetCheckConstraints(setting, sourceTable, tableRef);
                        }
                        createdTables.Add(tableRef);
                    }
                    finally
                    {
                        sourceServer.ConnectionContext.Disconnect();
                    }
                }
            }
            catch (Exception ex)
            {
                PostToConsole(string.Format("{0}: {1}", tableRef.DisplayName, ex.Message), InfoLevel.Error);
            }
        }

        /// <summary>
        /// Drops/truncates/creates the target table according to the options and returns it.
        /// <paramref name="created"/> is true when this call created the table (new, or dropped and recreated).
        /// </summary>
        private static Table PrepareTargetTable(CopyOptions setting, Table sourceTable, Database targetDatabase, Server targetServer, out bool created)
        {
            created = false;
            string label = sourceTable.Schema + "." + sourceTable.Name;
            bool targetTableExists = targetDatabase.Tables.Contains(sourceTable.Name, sourceTable.Schema);

            if (targetTableExists && setting.DropTarget)
            {
                PostToConsole(string.Format("{0}: Target table exists. Dropping.", label));
                targetDatabase.Tables[sourceTable.Name, sourceTable.Schema].Drop();
                targetDatabase.Tables.Refresh();
                targetTableExists = targetDatabase.Tables.Contains(sourceTable.Name, sourceTable.Schema);
            }

            if (targetTableExists)
            {
                PostToConsole(string.Format("{0}: Target table exists.", label), InfoLevel.Info);
                Table existing = targetDatabase.Tables[sourceTable.Name, sourceTable.Schema];
                if (setting.TruncateTarget)
                {
                    PostToConsole(string.Format("{0}: Truncating table.", label), InfoLevel.Info);
                    existing.TruncateData();
                }
                return existing;
            }

            var targetTable = new Table(targetDatabase, sourceTable.Name, sourceTable.Schema);
            foreach (Column source in sourceTable.Columns)
            {
                var column = new Column(targetTable, source.Name, source.DataType);
                column.Collation = source.Collation;
                column.Nullable = source.Nullable;
                column.Computed = source.Computed;
                column.ComputedText = source.ComputedText;
                column.Default = source.Default;

                if (source.DefaultConstraint != null)
                {
                    column.AddDefaultConstraint(targetTable.Name + "_" + source.DefaultConstraint.Name);
                    column.DefaultConstraint.Text = source.DefaultConstraint.Text;
                }

                column.IsPersisted = source.IsPersisted;
                column.DefaultSchema = source.DefaultSchema;
                column.RowGuidCol = source.RowGuidCol;

                if (source.Identity)
                {
                    column.Identity = true;
                    column.IdentitySeed = source.IdentitySeed;
                    column.IdentityIncrement = source.IdentityIncrement;
                }

                if (targetServer.VersionMajor >= 10)
                {
                    column.IsFileStream = source.IsFileStream;
                    column.IsSparse = source.IsSparse;
                    column.IsColumnSet = source.IsColumnSet;
                }

                targetTable.Columns.Add(column);
            }

            // A partitioned source lands on the same partition scheme when the target has one of that name: indexes are
            // scripted with their scheme, and a clustered columnstore index must be aligned with its table.
            if (sourceTable.IsPartitioned && targetDatabase.PartitionSchemes.Contains(sourceTable.PartitionScheme))
            {
                targetTable.PartitionScheme = sourceTable.PartitionScheme;
                foreach (PartitionSchemeParameter parameter in sourceTable.PartitionSchemeParameters)
                {
                    targetTable.PartitionSchemeParameters.Add(new PartitionSchemeParameter(targetTable, parameter.Name));
                }
            }
            targetTable.Create();
            created = true;
            PostToConsole(string.Format("{0}: Target table created.", label), InfoLevel.Info);
            return targetTable;
        }

        /// <summary>
        /// Recreates the source table's primary key, unique constraints and indexes on the newly created target
        /// table, after the data has been loaded. Each index is scripted by SMO (so filters, included columns,
        /// sort order, compression, columnstore, XML and spatial indexes carry over) without its ON &lt;filegroup&gt;
        /// clause, because the target database may not have the same filegroups or partition schemes.
        /// A failing index is reported as an error and the remaining indexes are still created.
        /// </summary>
        private static void CreateTargetIndexes(CopyOptions setting, Table sourceTable, TableRef tableRef)
        {
            List<Index> indexes = sourceTable.Indexes.Cast<Index>()
                .OrderBy(i => IndexCreationRank(i.IndexType, i.IsClustered))
                .ThenBy(i => i.ID)
                .ToList();
            if (indexes.Count == 0)
            {
                return;
            }

            var scripting = new ScriptingOptions
            {
                NoFileGroup = true,
                IncludeIfNotExists = false,
                ScriptBatchTerminator = false
            };

            int createdCount = 0;
            using (var conn = SqlConnectionFactory.Create(setting.TargetConnectionString()))
            {
                conn.Open();
                foreach (Index index in indexes)
                {
                    try
                    {
                        foreach (string statement in index.Script(scripting).Cast<string>())
                        {
                            ExecuteSql(conn, statement);
                        }
                        createdCount++;
                    }
                    catch (Exception ex)
                    {
                        PostToConsole(string.Format("{0}: Could not create index {1}: {2}", tableRef.DisplayName, index.Name, ex.GetBaseException().Message), InfoLevel.Error);
                    }
                }
            }

            PostToConsole(string.Format("{0}: {1} of {2} primary key/unique constraint/index(es) created.", tableRef.DisplayName, createdCount, indexes.Count),
                createdCount == indexes.Count ? InfoLevel.Info : InfoLevel.Warning);
        }

        /// <summary>
        /// Order in which indexes must be created: the clustered index (often the primary key) first, because
        /// creating it rebuilds the table and XML/spatial indexes require a clustered primary key; then other
        /// relational and columnstore indexes; then primary XML indexes, which secondary XML indexes depend on.
        /// </summary>
        internal static int IndexCreationRank(IndexType type, bool isClustered)
        {
            if (isClustered || type == IndexType.ClusteredIndex || type == IndexType.ClusteredColumnStoreIndex)
            {
                return 0;
            }

            switch (type)
            {
                case IndexType.NonClusteredIndex:
                case IndexType.NonClusteredColumnStoreIndex:
                case IndexType.NonClusteredHashIndex:
                    return 1;
                case IndexType.PrimaryXmlIndex:
                case IndexType.SelectiveXmlIndex:
                    return 2;
                case IndexType.SecondaryXmlIndex:
                case IndexType.SecondarySelectiveXmlIndex:
                    return 3;
                default:
                    return 4; // spatial and anything newer
            }
        }

        /// <summary>The source query of a copy: the given columns of the table, only the range's rows when range is not null.</summary>
        internal static string CopySelectSql(IEnumerable<string> columns, string quotedTable, bool noLock, KeyRange range, SplitColumn column)
        {
            return "SELECT " + string.Join(", ", columns.Select(QuoteName)) + " FROM " + quotedTable
                + (noLock ? " WITH (NOLOCK)" : string.Empty)
                + (range == null ? string.Empty : " WHERE " + range.Predicate(column));
        }

        /// <summary>Streams the table (range null) or one key range of it from the source into the prepared target.</summary>
        private static long CopyTableViaBulkCopy(CopyOptions setting, CopyTableContext context, TableRef tableRef, KeyRange range, SplitColumn column, string label)
        {
            string select = CopySelectSql(context.Columns, tableRef.QuotedName, setting.NoLock, range, column);

            using (var sourceConn = SqlConnectionFactory.Create(setting.SourceConnectionString()))
            {
                sourceConn.Open();
                using (var cmd = new SqlCommand(select, sourceConn))
                {
                    cmd.CommandTimeout = 0;
                    using (var reader = cmd.ExecuteReader(CommandBehavior.SingleResult))
                    using (var bulkCopy = new SqlBulkCopy(setting.TargetConnectionString(), LoadOptions(setting.NoTableLock)))
                    {
                        bulkCopy.DestinationTableName = context.TargetName;
                        bulkCopy.BatchSize = 10000;
                        bulkCopy.BulkCopyTimeout = 0;
                        bulkCopy.EnableStreaming = true;
                        foreach (string name in context.Columns)
                        {
                            bulkCopy.ColumnMappings.Add(name, name);
                        }

                        bulkCopy.WriteToServer(reader);

                        long rowsCopied = SqlBulkCopyHelper.GetRowsCopied(bulkCopy);
                        PostToConsole(string.Format("{0}: {1:N0}", label, rowsCopied), InfoLevel.Info);
                        return rowsCopied;
                    }
                }
            }
        }

        /// <summary>
        /// Tables with CLR-type columns are moved with bcp (export to a temp folder, then import),
        /// because SqlBulkCopy cannot stream those types. The target table must already exist (PrepareTargetTable).
        /// </summary>
        private static long CopyTableViaBcp(CopyOptions setting, TableRef tableRef)
        {
            PostToConsole(string.Format("{0}: Table contains geography/geometry/hierarchyid columns. Using bcp export/import.", tableRef.DisplayName), InfoLevel.Warning);

            string workDir = Path.Combine(Path.GetTempPath(), "bcpjson", Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(workDir);
            try
            {
                var exportOptions = new ExportOptions
                {
                    srcServer = setting.srcServer,
                    srcDatabase = setting.srcDatabase,
                    srcLogin = setting.srcLogin,
                    srcPassword = setting.srcPassword,
                    TrustServerCertificate = setting.TrustServerCertificate,
                    IncludeTablesList = new[] { tableRef.DisplayName },
                    ExcludeTablesList = Enumerable.Empty<string>(),
                    IncludeSchemaList = Enumerable.Empty<string>(),
                    ExcludeSchemaList = Enumerable.Empty<string>(),
                    FileFormat = "bcp",
                    exportPath = workDir,
                    Retries = setting.Retries
                };
                // The temporary export is retried like any export; its manifest is discarded with the folder.
                if (ExportToBCP(exportOptions, new ExportManifest()) < 0)
                {
                    return -1;
                }

                var importOptions = new ImportOptions
                {
                    FileFormat = "bcp",
                    SourcePath = workDir,
                    SourceFile = tableRef.DisplayName + ".bcp",
                    TargetSQLServer = setting.TargetSQLServer,
                    TargetSQLDatabase = setting.TargetSQLDatabase,
                    TargetSQLLogin = setting.TargetSQLLogin,
                    TargetSQLPassword = setting.TargetSQLPassword,
                    TrustServerCertificate = setting.TrustServerCertificate,
                    TargetTable = tableRef.DisplayName,
                    TruncateTarget = false,
                    NoTableLock = setting.NoTableLock
                };
                return ImportFromBCP(importOptions);
            }
            finally
            {
                try
                {
                    Directory.Delete(workDir, true);
                }
                catch (Exception ex) when (ex is IOException || ex is UnauthorizedAccessException)
                {
                    // Leave temp files behind rather than fail the copy.
                }
            }
        }
    }
}
