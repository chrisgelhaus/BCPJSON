using Microsoft.Data.SqlClient;
using Microsoft.SqlServer.Management.Common;
using Microsoft.SqlServer.Management.Smo;
using System.Collections.Generic;
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

            Parallel.ForEach(tables, Parallelism(maxTasks), table =>
            {
                try
                {
                    long copied = CopyTable(setting, table);
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

            return totalrowCount;
        }

        private static long CopyTable(CopyOptions setting, TableRef tableRef)
        {
            string sourceConnectionString = setting.SourceConnectionString();
            string targetConnectionString = setting.TargetConnectionString();

            using (var sourceSql = new SqlConnection(sourceConnectionString))
            using (var targetSql = new SqlConnection(targetConnectionString))
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

                    Table targetTable = PrepareTargetTable(setting, sourceTable, targetDatabase, targetServer);

                    bool needsBcp = sourceTable.Columns.Cast<Column>().Any(col =>
                        col.DataType.SqlDataType == SqlDataType.Geography ||
                        col.DataType.SqlDataType == SqlDataType.Geometry ||
                        col.DataType.SqlDataType == SqlDataType.HierarchyId);

                    return needsBcp
                        ? CopyTableViaBcp(setting, tableRef)
                        : CopyTableViaBulkCopy(setting, sourceTable, targetTable, tableRef);
                }
                finally
                {
                    sourceServer.ConnectionContext.Disconnect();
                    targetServer.ConnectionContext.Disconnect();
                }
            }
        }

        /// <summary>Drops/truncates/creates the target table according to the options and returns it.</summary>
        private static Table PrepareTargetTable(CopyOptions setting, Table sourceTable, Database targetDatabase, Server targetServer)
        {
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
            PostToConsole(string.Format("{0}: Target table created.", label), InfoLevel.Info);
            return targetTable;
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

            using (var sourceConn = new SqlConnection(setting.SourceConnectionString()))
            {
                sourceConn.Open();
                using (var cmd = new SqlCommand(select, sourceConn))
                {
                    cmd.CommandTimeout = 0;
                    using (var reader = cmd.ExecuteReader(CommandBehavior.SingleResult))
                    using (var bulkCopy = new SqlBulkCopy(setting.TargetConnectionString(), SqlBulkCopyOptions.KeepIdentity | SqlBulkCopyOptions.KeepNulls))
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
                    exportPath = workDir
                };
                if (ExportToBCP(exportOptions) < 0)
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
                    TruncateTarget = false
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
