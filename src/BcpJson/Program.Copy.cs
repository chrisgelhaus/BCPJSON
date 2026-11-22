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
            }

            var createdTables = new ConcurrentBag<TableRef>();
            Parallel.ForEach(tables, Parallelism(maxTasks), table =>
            {
                try
                {
                    long copied = CopyTable(setting, table, createdTables);
                    if (copied > 0)
                    {
                        Interlocked.Add(ref totalrowCount, copied);
                    }
                }
                catch (Exception ex)
                {
                    PostToConsole(string.Format("{0}: {1}", table.DisplayName, ex.Message), InfoLevel.Error);
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

        private static long CopyTable(CopyOptions setting, TableRef tableRef, ConcurrentBag<TableRef> createdTables)
        {
            string sourceConnectionString = setting.SourceConnectionString();
            string targetConnectionString = setting.TargetConnectionString();

            using (var sourceSql = SqlConnectionFactory.Create(sourceConnectionString))
            using (var targetSql = SqlConnectionFactory.Create(targetConnectionString))
            {
                var sourceServer = new Server(new ServerConnection(sourceSql));
                var targetServer = new Server(new ServerConnection(targetSql));
                try
                {
                    Database sourceDatabase = sourceServer.Databases[new SqlConnectionStringBuilder(sourceConnectionString).InitialCatalog];
                    Table sourceTable = sourceDatabase?.Tables.ItemById(tableRef.ObjectId);
                    if (sourceTable == null)
                    {
                        throw new InvalidOperationException("Source table not found.");
                    }

                    Database targetDatabase = targetServer.Databases[new SqlConnectionStringBuilder(targetConnectionString).InitialCatalog];
                    if (targetDatabase == null)
                    {
                        throw new InvalidOperationException("Target database not found.");
                    }

                    Table targetTable = PrepareTargetTable(setting, sourceTable, targetDatabase, targetServer, out bool created);

                    bool needsBcp = sourceTable.Columns.Cast<Column>().Any(col =>
                        col.DataType.SqlDataType == SqlDataType.Geography ||
                        col.DataType.SqlDataType == SqlDataType.Geometry ||
                        col.DataType.SqlDataType == SqlDataType.HierarchyId);

                    long copied = needsBcp
                        ? CopyTableViaBcp(setting, tableRef)
                        : CopyTableViaBulkCopy(setting, sourceTable, targetTable, tableRef);

                    // Keys and indexes are built after the load (bulk loading a heap is fastest), and only on tables
                    // this run created: an existing target keeps its own indexes.
                    if (created && copied >= 0)
                    {
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

                    return copied;
                }
                finally
                {
                    sourceServer.ConnectionContext.Disconnect();
                    targetServer.ConnectionContext.Disconnect();
                }
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

        private static long CopyTableViaBulkCopy(CopyOptions setting, Table sourceTable, Table targetTable, TableRef tableRef)
        {
            // Computed and rowversion columns cannot be written; they are recalculated by the target.
            List<string> columns = sourceTable.Columns.Cast<Column>()
                .Where(c => !c.Computed && c.DataType.SqlDataType != SqlDataType.Timestamp)
                .Select(c => c.Name)
                .ToList();

            string select = "SELECT " + string.Join(", ", columns.Select(QuoteName)) + " FROM " + tableRef.QuotedName
                + (setting.NoLock ? " WITH (NOLOCK)" : string.Empty);

            using (var sourceConn = SqlConnectionFactory.Create(setting.SourceConnectionString()))
            {
                sourceConn.Open();
                using (var cmd = new SqlCommand(select, sourceConn))
                {
                    cmd.CommandTimeout = 0;
                    using (var reader = cmd.ExecuteReader(CommandBehavior.SingleResult))
                    using (var bulkCopy = new SqlBulkCopy(setting.TargetConnectionString(), LoadOptions(setting.NoTableLock)))
                    {
                        bulkCopy.DestinationTableName = QuoteName(targetTable.Schema) + "." + QuoteName(targetTable.Name);
                        bulkCopy.BatchSize = 10000;
                        bulkCopy.BulkCopyTimeout = 0;
                        bulkCopy.EnableStreaming = true;
                        foreach (string column in columns)
                        {
                            bulkCopy.ColumnMappings.Add(column, column);
                        }

                        bulkCopy.WriteToServer(reader);

                        long rowsCopied = SqlBulkCopyHelper.GetRowsCopied(bulkCopy);
                        PostToConsole(string.Format("{0}: {1:N0}", tableRef.DisplayName, rowsCopied), InfoLevel.Info);
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
