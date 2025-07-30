using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data;
using Microsoft.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.SqlServer.Management.Smo;
using Microsoft.SqlServer.Management.Common;
using CommandLine;

namespace bcpJson
{
        class Program
        {
        private static readonly object _logLock = new object();
        private static string _logFilePath;
                static void Main(string[] args)
                {
            CommandLine.Parser.Default.ParseArguments<ExportOptions,ImportOptions,CopyOptions>(args)
                .WithParsed<ExportOptions>(opts => ExportData(opts))
                .WithParsed<ImportOptions>(opts => ImportData(opts))
                .WithParsed<CopyOptions>(opts => CopyData(opts));
        }

        private static void ExportData(ExportOptions opts)
        {
            if (opts.Valid())
            {
                SetLogFile(opts.outputLogFile);
                Stopwatch sw = new Stopwatch();
                sw.Start();

                //Generate2(opts);
                //var result = CollectionGenerate2(opts);

                long result = -1;
                switch (opts.FileFormat.ToLowerInvariant())
                {
                    case "bcp":
                        result = ExportToBCP(opts);
                        break;
                    case "json":
                        result = ExportToJSON(opts);
                        break;
                    default:
                        break;
                }
                sw.Stop();
                PostToConsole(string.Format("Total Exported: {0}\tRows in {1}", result, sw.Elapsed), InfoLevel.Success);
            }
        }

        private static void ImportData(ImportOptions opts)
        {
            if (opts.Valid())
            {
                SetLogFile(opts.LogFile);
                Stopwatch sw = new Stopwatch();
                sw.Start();

                long result = -1;
                switch (opts.FileFormat.ToLowerInvariant())
                {
                    case "bcp":
                        result = ImportFromBCP(opts);
                        break;
                    case "json":
                        //result = ImportFromJSON(opts);
                        break;
                    default:
                        break;
                }
                sw.Stop();
                PostToConsole(string.Format("Total Imported {0}\tRows in {1}", result, sw.Elapsed), InfoLevel.Success);
            }
        }

        private static void CopyData(CopyOptions opts)
        {
            SetLogFile(opts.outputLogFile);
            Stopwatch sw = new Stopwatch();
            sw.Start();

            //Generate2(opts);
            //var result = CollectionGenerate2(opts);
            var result = BulkCopyTables(opts);
            sw.Stop();
            PostToConsole(string.Format("Total Copied {0}\tRows in {1}", result, sw.Elapsed), InfoLevel.Success);
        }

        private static long Generate(ExportOptions setting)
		{
			var count = 0L;

			using (var conn = new SqlConnection(setting.SourceConnectionString()))
			{
				using (var cmd = new SqlCommand("select name from sys.tables", conn))
				{
					conn.Open();
					var tables = new Dictionary<string, string>();
					string dir;

					if (setting.MultiExport.HasValue)
					{
						if (setting.MultiExport.Value)
						{
							dir = setting.exportPath;

							using(var reader = cmd.ExecuteReader(CommandBehavior.SingleResult))
							{
								while (reader.Read())
								{
									var table = reader.GetString(0);
									tables.Add(string.Format("[{0}]", table), Path.Combine(setting.exportPath, table) + ".json");
								}
							}
						}
						else
						{
							dir = Path.GetDirectoryName(setting.exportPath);
							tables.Add(string.Format("({0})", setting.Query), setting.exportPath);
						}
					}
					else
					{
						throw new ArgumentNullException();
					}

					if (tables.Count > 0)
					{
						try
						{
							if (!Directory.Exists(dir))
							{
								Directory.CreateDirectory(dir);
							}							
						}
						catch (Exception)
						{							
							throw;
						}

						tables.ToList().ForEach(t => {
							cmd.CommandText = string.Format("SELECT * FROM {0} t", t.Key);

                            var result = CollectionGenerate(cmd, t.Value);
                            count += result;
                            PostToConsole(string.Format("Exported {0}\tRows of [{1}]", result.ToString("N0"), t.Key));
                        });
					}
				}
			}

			return count;
		}

        private static long Generate2(ExportOptions setting)
        {
            using (SqlConnection connection = new SqlConnection(setting.SourceConnectionString()))
            {
                //connection.Open();
                Server sqlServer = new Server(new ServerConnection(new SqlConnection(setting.SourceConnectionString())));
                SqlConnectionStringBuilder connectionStringBuilder = new SqlConnectionStringBuilder(setting.SourceConnectionString());
                Database db = new Database(sqlServer, connectionStringBuilder.InitialCatalog);
                db.Refresh();

                //Test
                Server tgtServer = new Server(new ServerConnection(new SqlConnection("Data Source=HO-SQLVM97;Initial Catalog=CMG001;Integrated Security=True;")));
                Database tgtDatabase = new Database(tgtServer, "CMG001");
                tgtDatabase.Refresh();

                foreach (Table srcTable in db.Tables)
                {
                    if (srcTable.Name == "pc_account" && srcTable.Schema == "dbo")
                    {
                        PostToConsole(string.Format("{0}", tgtDatabase.Tables.Contains(srcTable.Name, srcTable.Schema)), InfoLevel.Info);
                        if (!tgtDatabase.Tables.Contains(srcTable.Name, srcTable.Schema))
                        {
                            Transfer trnsfrTable = new Transfer(db);
                            trnsfrTable.CopyAllObjects = false;
                            trnsfrTable.ObjectList.Clear();
                            //foreach (Check check in srcTable.Checks)
                            //{
                            //    srcTable.Checks.Remove(check);
                            //}
                            trnsfrTable.ObjectList.Add(srcTable);
                            foreach (Microsoft.SqlServer.Management.Smo.Index index in srcTable.Indexes)
                            {
                                trnsfrTable.ObjectList.Add(index);
                            }
                            trnsfrTable.DestinationServer = "HO-SQLVM97";
                            trnsfrTable.DestinationDatabase = "cmg001";
                            trnsfrTable.TransferData();
                        }
                    }
                }

                //Test

                foreach (Table srcTable in db.Tables)
                {
                    var scriptingOptions = new ScriptingOptions();
                    scriptingOptions.ScriptDrops = true;
                    scriptingOptions.IncludeIfNotExists = true;

                    PostToConsole(string.Format("{0}", srcTable.Owner));
                    PostToConsole(string.Format("{0}", srcTable.Name));
                    PostToConsole(string.Format("{0}", srcTable.HasIndex));
                    PostToConsole(string.Format("{0}", srcTable.Columns.Count));

                    var tableScripts = srcTable.Script();
                    foreach (var script in tableScripts)
                    {
                        PostToConsole(string.Format("{0}", script));
                    }

                    foreach (Column column in srcTable.Columns)
                    {
                        PostToConsole(string.Format("{0},{1}", column.ID, column.Name));
                    }
                    if (srcTable.HasIndex)
                    {
                        if (srcTable.HasClusteredIndex)
                        {
                            foreach (Microsoft.SqlServer.Management.Smo.Index index in srcTable.Indexes)
                            {
                                if (index.IsClustered)
                                {
                                    PostToConsole(string.Format("{0}", index.Name));
                                    foreach (IndexedColumn indexcolumn in index.IndexedColumns)
                                    {
                                        PostToConsole(string.Format("{0}, {1}", indexcolumn.Name, indexcolumn.Descending));
                                    }
                                }
                            }
                        }
                    }
                    else
                    {

                    }
                }
            }
            return 1;
        }

		private static long CollectionGenerate(SqlCommand cmd, string path)
		{
			var rowCount = 0L;

			using (var reader = cmd.ExecuteReader(CommandBehavior.SingleResult))
			{
				var fieldFields = Enumerable.Range(0, reader.FieldCount).Select(d => new { Index = d, Name = reader.GetName(d), Type = reader.GetFieldType(d).Name }).ToList();

				using (var jsonWriter = new JsonTextWriter(new StreamWriter(path, false)))
				{
                    //  Added StartArray and EndArary functions to make the ourput a valid JSON document
                    jsonWriter.WriteStartArray();
                    while (reader.Read())
					{
                        jsonWriter.WriteStartObject();

						fieldFields.ForEach(d =>
						{
                            jsonWriter.WritePropertyName(d.Name);
                            if (d.Type != "SqlGeography")
                            {
                                jsonWriter.WriteValue(reader.GetValue(d.Index));
                            }
                            else
                            {
                                jsonWriter.WriteNull();
                            }
                        });

						jsonWriter.WriteEndObject();
                        jsonWriter.WriteWhitespace(Environment.NewLine);

						rowCount++;
					}
                    jsonWriter.WriteEndArray();
                }
            }

			return rowCount;
		}

        private static long CollectionGenerate2(ExportOptions setting)
        {
            var totalrowCount = 0L;

            using (var conn = new SqlConnection(setting.SourceConnectionString()))
            {
                // Build table inclusion list
                string sql_IncludeTableList = "";

                if(setting.IncludeTablesList.Any())
                {
                    //string[] includetablelist = setting.IncludeTablesList.Split(',');
                    sql_IncludeTableList = $" AND S.name + '.' + T.name IN ('{string.Join("','", setting.IncludeTablesList).Trim()}')";
                }

                // Build table exclusion list
                string sql_ExcludeTableList = "";
                if (setting.ExcludeTablesList.Any())
                {
                    //string[] excludetablelist = setting.ExcludeTablesList.Split(',');
                    sql_ExcludeTableList = $" AND S.name + '.' + T.name NOT IN ('{string.Join("','", setting.ExcludeTablesList).Trim()}')";
                }

                string query = $"SELECT T.object_id FROM sys.tables T INNER JOIN sys.schemas S ON S.schema_id = T.schema_id WHERE T.type = 'U' {sql_IncludeTableList} {sql_ExcludeTableList};";

                using (var cmd = new SqlCommand(query, conn))
                {
                    conn.Open();
                    DataTable dt = new DataTable();
                    //var tables = new Dictionary<string, string>();
                    string dir = setting.exportPath;

                    using (var reader = cmd.ExecuteReader(CommandBehavior.SingleResult))
                    {
                        dt.Load(reader);
                        var list = dt.AsEnumerable().Select(r => r.Field<int>("object_id")).ToArray();

                        Parallel.ForEach(
                            list,
                            new ParallelOptions { MaxDegreeOfParallelism = setting.MaxParallelTasks },
                            table =>
                        {
                            Thread.Sleep(10);

                            string schema = "";
                            string tablename = "";
                            string columnlist = "";
                            string sortorder = "";
                            var tablerowCount = 0L;

                            var tableConn = new SqlConnection(setting.SourceConnectionString());
                            tableConn.Open();
                            var tableCommand = new SqlCommand();
                            tableCommand.Connection = tableConn;

                            string tableQuery = String.Format(@"DECLARE @Schema VARCHAR(MAX) = '';
                                   DECLARE @TableName VARCHAR(MAX) = '';
                                   DECLARE @Columns VARCHAR(MAX) = '';
                                   DECLARE @SortOrder VARCHAR(MAX) = '';
								   DECLARE @indexID INT

                                   SELECT   @Columns = COALESCE(@Columns + ', ', '') + CASE sc.system_type_id
                                                                                       WHEN 240
                                                                                       THEN sc.name + '.STAsText() AS ' + sc.name
                                                                                       ELSE sc.name
                                                                                     END
                                   FROM     sys.tables st
                                            INNER JOIN sys.columns sc ON sc.object_id = st.object_id
                                   WHERE    st.object_id = {0}
                                   ORDER BY sc.column_id;

                                   SELECT   @Schema = ss.name 
                                   FROM     sys.tables st
                                            INNER JOIN sys.schemas ss ON ss.schema_id = st.schema_id
                                   WHERE    st.object_id = {0};

                                   SELECT   @TableName = st.name 
                                   FROM     sys.tables st
                                   WHERE    st.object_id = {0};

								   SELECT  @indexID = object_id
									FROM    sys.indexes
									WHERE   object_id = {0}
											AND is_primary_key = 1;

									IF @indexID IS NULL
									BEGIN	
										SELECT    @SortOrder = COALESCE(@SortOrder + ', ', '') + sc.name
										  FROM      sys.tables st
													INNER JOIN sys.columns sc ON sc.object_id = st.object_id
                                                    INNER JOIN sys.types sty ON sty.system_type_id = sc.system_type_id
										  WHERE     st.object_id = {0}
                                                    AND sty.system_type_id NOT IN (241);
									END
									ELSE
									BEGIN 
										SELECT    @SortOrder = COALESCE(@SortOrder + ', ', '') + sicn.name + ' '
													+ CASE sic.is_descending_key
														WHEN 0 THEN 'ASC'
														ELSE 'DESC'
													  END
										  FROM      sys.indexes si
													INNER JOIN sys.index_columns sic ON sic.object_id = si.object_id
																						AND sic.index_id = si.index_id
													INNER JOIN sys.columns sicn ON sicn.object_id = si.object_id
																				   AND sicn.column_id = sic.column_id
										  WHERE     si.object_id = @indexID
													AND is_primary_key = 1
									END;

                                   SELECT @Schema, @TableName, @Columns, @SortOrder;", table);
                            tableCommand.CommandText = tableQuery;
                            var tableSelect = tableCommand.ExecuteReader(CommandBehavior.SingleRow);

                            try
                            {
                                tableSelect.Read();
                                schema = tableSelect[0].ToString();
                                tablename = tableSelect[1].ToString();
                                columnlist = tableSelect[2].ToString();
                                sortorder = tableSelect[3].ToString();
                                tableSelect.Close();
                            }
                            catch (Exception)
                            {
                                throw;
                            }

                            string tableSelectStatement = "SELECT " + columnlist.Substring(1, (columnlist.Length) - 1) + " FROM [" + schema + "].[" + tablename + "]" + " WITH(NOLOCK) ORDER BY " + sortorder.Substring(1, (sortorder.Length) - 1);
                            PostToConsole(tableSelectStatement, InfoLevel.Info);

                            tableQuery = tableSelectStatement;
                            tableCommand.CommandText = tableQuery;

                            using (var records = tableCommand.ExecuteReader(CommandBehavior.SingleResult))
                            {
                                // Bulk Copy
                                using (SqlConnection destinationConnection = new SqlConnection("Data Source=HO-SQLVM97;Initial Catalog=CMG001;Integrated Security=True;"))
                                {
                                    destinationConnection.Open();
                                    SqlCommand truncatedsttable = new SqlCommand("truncate table dbo.pc_user", destinationConnection);
                                    truncatedsttable.ExecuteNonQuery();

                                    using (SqlBulkCopy bulkCopy = new SqlBulkCopy(destinationConnection))
                                    {
                                        bulkCopy.DestinationTableName = "dbo.pc_user";
                                        bulkCopy.BatchSize = 1000;
                                        try
                                        {
                                            bulkCopy.NotifyAfter = 1000;
                                            bulkCopy.WriteToServer(records);

                                            PostToConsole(string.Format(schema + "." + tablename + ": " + tablerowCount.ToString("N0")), InfoLevel.Info);
                                        }
                                        catch (Exception ex)
                                        {
                                            PostToConsole(string.Format(ex.Message), InfoLevel.Error);
                                        }
                                        finally
                                        {

                                        }
                                    }
                                }
                            }
                            tableConn.Close();
                            totalrowCount += tablerowCount;
                        }
                        );
                    }
                }
            }
            return totalrowCount;
        }

        private static long BulkCopyTables(CopyOptions setting)
        {
            var totalrowCount = 0L;

            using (var conn = new SqlConnection(setting.SourceConnectionString()))
            {
                string query = BuildSourceObjectIDList(setting.IncludeTablesList, setting.ExcludeTablesList, setting.IncludeSchemaList, setting.ExcludeSchemaList);

                using (var cmd = new SqlCommand(query, conn))
                {
                    conn.Open();
                    DataTable dt = new DataTable();

                    using (var reader = cmd.ExecuteReader(CommandBehavior.SingleResult))
                    {
                        string consoleMsg;

                        dt.Load(reader);
                        var list = dt.AsEnumerable().Select(r => r.Field<int>("object_id")).ToArray();

                        int maxTasks = setting.MaxParallelTasks;

                        consoleMsg = maxTasks > 0 ? string.Format("Parallel table task operations: {0}", maxTasks) : "Parallel table task operations: Unlimited";
                        PostToConsole(consoleMsg, InfoLevel.Info);

                        Parallel.ForEach(
                            list,
                            new ParallelOptions { MaxDegreeOfParallelism = maxTasks},
                            table =>
                        {
                            Thread.Sleep(10);
                            //Options
                            bool dropTargetTable = setting.DropTarget;
                            bool truncateTargetTable = setting.TruncateTarget;

                            string sourceSQLServer = setting.srcServer;
                            string sourceSQLDatabase = setting.srcDatabase;
                            string sourceSQLConnectionString = setting.SourceConnectionString();

                            string targetSQLServer = setting.TargetSQLServer;
                            string targetSQLDatabase = setting.TargetSQLDatabase;
                            string targetSQLConnectionString = setting.TargetConnectionString();

                            int rowsCopied = 0;

                            //Get Source Object
                            //TODO: Add code to check --source-query option for unsupported columns????

                            ServerConnection sourceConnection = new ServerConnection(sourceSQLServer);
                            Server sourceServer = new Server(sourceConnection);
                            Database sourceDatabase = sourceServer.Databases[sourceSQLDatabase];
                            IEnumerable<Table> sourceTablelist = sourceDatabase.Tables.Cast<Table>().Where(x => x.ID == table);
                            if (sourceTablelist.Count() == 1)
                            {
                                Table sourceTable = sourceTablelist.Single<Table>();

                                //Check if source object contains any unsupported columns for direct bulk copy load
                                IEnumerable<Column> unsupportedcolumns = sourceTable.Columns.OfType<Column>()
                                    .Where(col => col.DataType.SqlDataType == SqlDataType.Geography ||
                                    col.DataType.SqlDataType == SqlDataType.Geometry);
                                if (unsupportedcolumns.Count() > 0)
                                {
                                    PostToConsole(string.Format("{0}.{1}: Table contains unsupported colums types. Reverting to BCP export/import mode.", sourceTable.Schema, sourceTable.Name), InfoLevel.Warning);
                                    ExportOptions exOpts = new ExportOptions();
                                    exOpts.srcServer = setting.srcServer;
                                    exOpts.srcDatabase = setting.srcDatabase;
                                    exOpts.srcLogin = setting.srcLogin;
                                    exOpts.srcPassword = setting.srcPassword;
                                    exOpts.ExcludeSchemaList = Enumerable.Empty<string>();
                                    exOpts.ExcludeTablesList = Enumerable.Empty<string>();
                                    exOpts.Query = setting.Query;
                                    exOpts.IncludeSchemaList = Enumerable.Empty<string>();
                                    exOpts.IncludeTablesList = Enumerable.Empty<string>();
                                    exOpts.IncludeTablesList = new[] { string.Format("{0}.{1}", sourceTable.Schema, sourceTable.Name) };
                                    exOpts.FileFormat = "bcp";

                                    ExportData(exOpts);

                                    ImportOptions imOpts = new ImportOptions();
                                    imOpts.FileFormat = "bcp";
                                    imOpts.SourcePath = System.IO.Directory.GetCurrentDirectory().ToString();
                                    imOpts.SourceFile = string.Format("{0}.{1}.bcp", sourceTable.Schema, sourceTable.Name);
                                    imOpts.TargetSQLServer = setting.TargetSQLServer;
                                    imOpts.TargetSQLDatabase = setting.TargetSQLDatabase;
                                    imOpts.TargetSQLLogin = setting.TargetSQLLogin;
                                    imOpts.TargetSQLPassword = setting.TargetSQLPassword;
                                    imOpts.TargetTable = string.Format("{0}.{1}", sourceTable.Schema, sourceTable.Name);
                                    imOpts.TruncateTarget = true;

                                    ImportData(imOpts);

                                }
                                else
                                {
                                    //Setup target object
                                    ServerConnection targetConnection = new ServerConnection(targetSQLServer);
                                    Server targetServer = new Server(targetConnection);
                                    Database targetDatabase = targetServer.Databases[targetSQLDatabase];
                                    Table targetTable;
                                    //Check if tatrget table exists
                                    bool targetTableExists = targetDatabase.Tables.Contains(sourceTable.Name, sourceTable.Schema);
                                    //Drop target table
                                    if (targetTableExists && dropTargetTable)
                                    {
                                        PostToConsole(string.Format("{1}.{0}: Target table exists. Dropping.", sourceTable.Name, sourceTable.Schema));
                                        targetDatabase.Tables[sourceTable.Name, sourceTable.Schema].Drop();
                                        targetTableExists = targetDatabase.Tables.Contains(sourceTable.Name, sourceTable.Schema);
                                    }

                                    //Check target database fot table
                                    if (targetTableExists)
                                    {
                                        PostToConsole(string.Format("{1}.{0}: Target table exists.", sourceTable.Name, sourceTable.Schema), InfoLevel.Info);
                                        targetTable = targetDatabase.Tables[sourceTable.Name, sourceTable.Schema];
                                        //truncate target data
                                        if (truncateTargetTable)
                                        {
                                            PostToConsole(string.Format("{1}.{0}: Truncating table.", sourceTable.Name, sourceTable.Schema), InfoLevel.Info);
                                            targetTable.TruncateData();
                                        }
                                    }
                                    else
                                    {
                                        targetTable = new Table(targetDatabase, sourceTable.Name, sourceTable.Schema);
                                        foreach (Column source in sourceTable.Columns)
                                        {
                                            Column column = new Column(targetTable, source.Name, source.DataType);
                                            column.Collation = source.Collation;
                                            column.Nullable = source.Nullable;
                                            column.Computed = source.Computed;
                                            column.ComputedText = source.ComputedText;
                                            column.Default = source.Default;

                                            if (source.DefaultConstraint != null)
                                            {
                                                string tabname = targetTable.Name;
                                                string constrname = source.DefaultConstraint.Name;
                                                column.AddDefaultConstraint(tabname + "_" + constrname);
                                                column.DefaultConstraint.Text = source.DefaultConstraint.Text;
                                            }

                                            column.IsPersisted = source.IsPersisted;
                                            column.DefaultSchema = source.DefaultSchema;
                                            column.RowGuidCol = source.RowGuidCol;

                                            if (targetServer.VersionMajor >= 10)
                                            {
                                                column.IsFileStream = source.IsFileStream;
                                                column.IsSparse = source.IsSparse;
                                                column.IsColumnSet = source.IsColumnSet;
                                            }

                                            targetTable.Columns.Add(column);
                                        }
                                        targetTable.Create();
                                    }

                                    using (SqlBulkCopy bulkCopy = new SqlBulkCopy(targetSQLConnectionString, SqlBulkCopyOptions.KeepIdentity))
                                    {
                                        bulkCopy.DestinationTableName = string.Format("{1}.{0}", targetTable.Name, targetTable.Schema);
                                        bulkCopy.BatchSize = 1000;
                                        try
                                        {
                                            var tableConn = new SqlConnection(sourceSQLConnectionString);
                                            tableConn.Open();
                                            var tableCommand = tableConn.CreateCommand();
                                            tableCommand.CommandText = string.Format("select * from {1}.{0} ", sourceTable.Name, sourceTable.Schema);
                                            tableCommand.CommandTimeout = 0;
                                            var records = tableCommand.ExecuteReader(CommandBehavior.SingleResult);

                                            bulkCopy.WriteToServer(records);

                                            rowsCopied = SqlBulkCopyHelper.GetRowsCopied(bulkCopy);

                                            PostToConsole(string.Format("{0}.{1}: {2}",targetTable.Schema, targetTable.Name, rowsCopied.ToString("N0")), InfoLevel.Info);

                                            tableConn.Close();
                                        }
                                        catch (Exception ex)
                                        {
                                            PostToConsole(string.Format("{0}.{1}: {2}", targetTable.Schema, targetTable.Name, ex.Message), InfoLevel.Error);
                                        }
                                        finally
                                        {

                                        }
                                    }

                                    targetConnection.SqlConnectionObject.Close();
                                }

                                totalrowCount += rowsCopied;
                            }

                            sourceConnection.SqlConnectionObject.Close();
                        }
                        );
                    }
                }
            }
            return totalrowCount;
        }

        private static long ExportToBCP(ExportOptions options)
        {
            var totalrowCount = 0L;

            using (var conn = new SqlConnection(options.SourceConnectionString()))
            {
                string query = BuildSourceObjectIDList(options.IncludeTablesList, options.ExcludeTablesList, options.IncludeSchemaList, options.ExcludeSchemaList);

                using (var cmd = new SqlCommand(query, conn))
                {
                    conn.Open();
                    DataTable dt = new DataTable();
                    string outputPath = options.exportPath;

                    using (var reader = cmd.ExecuteReader(CommandBehavior.SingleResult))
                    {
                        dt.Load(reader);
                        var list = dt.AsEnumerable().Select(r => r.Field<int>("object_id")).ToArray();

                        Parallel.ForEach(
                            list,
                            new ParallelOptions { MaxDegreeOfParallelism = options.MaxParallelTasks },
                            objectID =>
                        {
                            Thread.Sleep(10);

                            //Connect to SQL Server
                            Server SourceSQLServer = new Server(new ServerConnection(new SqlConnection(options.SourceConnectionString())));
                            SqlConnectionStringBuilder connectionStringBuilder = new SqlConnectionStringBuilder(options.SourceConnectionString());
                            Database db = new Database(SourceSQLServer, connectionStringBuilder.InitialCatalog);
                            db.Refresh();

                            Table SourceTable = db.Tables.ItemById(objectID);

                            //Source BCP Login settings
                            string bcpLogin;
                            if (string.IsNullOrEmpty(options.srcLogin) && string.IsNullOrEmpty(options.srcPassword))
                            {
                                bcpLogin = "-T";
                            }
                            else
                            {
                                bcpLogin = string.Format("-U {0} -P {1}", options.srcLogin, options.srcPassword);
                            }

                            //Create Format File
                            string bcpfmtargs = String.Format("{0}.{1} format nul -S {2} -d {3} {4} -c -x -f {0}.{1}-c.xml -t|", SourceTable.Schema, SourceTable.Name, options.srcServer, options.srcDatabase, bcpLogin);
                            int bcpfmtrc = ExecuteProcess("bcp.exe", bcpfmtargs, outputPath);
                            if (bcpfmtrc == 0)
                            {
                                PostToConsole(string.Format("{0}.{1}: Format file created.", SourceTable.Schema, SourceTable.Name), InfoLevel.Success);

                                //Export Data
                                string bcpexpargs = String.Format("{0}.{1} out {0}.{1}.bcp -S {2} -d {3} -T -c -t| -m 1 -b 10000 -K ReadOnly -e {0}.{1}_error.log -o {0}.{1}_output.log", SourceTable.Schema, SourceTable.Name, options.srcServer, options.srcDatabase);
                                int bcpexprc = ExecuteProcess("bcp.exe", bcpexpargs, outputPath);

                                if (bcpexprc == 0)
                                {
                                    PostToConsole(string.Format("{0}.{1}: {2:n0}", SourceTable.Schema, SourceTable.Name, SourceTable.RowCount), InfoLevel.Info);
                                    totalrowCount += SourceTable.RowCount;
                                }
                                else
                                {
                                    PostToConsole(string.Format("{0}.{1}: ERROR. Attempted to process {2:n0} rows.", SourceTable.Schema, SourceTable.Name, SourceTable.RowCount), InfoLevel.Error);
                                }
                            }
                            else
                            {
                                PostToConsole(string.Format("{0}.{1}: Error creating format file.)", SourceTable.Schema, SourceTable.Name));
                            }

                            SourceSQLServer.ConnectionContext.Disconnect();
                        }
                        );
                    }
                }
            }
            return totalrowCount;
        }

        private static long ExportToJSON(ExportOptions options)
        {
            var totalrowCount = 0L;

            using (var conn = new SqlConnection(options.SourceConnectionString()))
            {
                string query = BuildSourceObjectIDList(options.IncludeTablesList, options.ExcludeTablesList, options.IncludeSchemaList, options.ExcludeSchemaList);

                using (var cmd = new SqlCommand(query, conn))
                {
                    conn.Open();
                    DataTable dt = new DataTable();
                    string dir = options.exportPath;

                    using (var reader = cmd.ExecuteReader(CommandBehavior.SingleResult))
                    {
                        dt.Load(reader);
                        var list = dt.AsEnumerable().Select(r => r.Field<int>("object_id")).ToArray();

                        Parallel.ForEach(list, table =>
                        {
                            Thread.Sleep(10);

                            string schema = "";
                            string tablename = "";
                            string columnlist = "";
                            string sortorder = "";
                            var tablerowCount = 0L;

                            var tableConn = new SqlConnection(options.SourceConnectionString());
                            tableConn.Open();
                            var tableCommand = new SqlCommand();
                            tableCommand.Connection = tableConn;

                            string tableQuery = String.Format(@"DECLARE @Schema VARCHAR(MAX) = '';
                                   DECLARE @TableName VARCHAR(MAX) = '';
                                   DECLARE @Columns VARCHAR(MAX) = '';
                                   DECLARE @SortOrder VARCHAR(MAX) = '';
								   DECLARE @indexID INT

                                   SELECT   @Columns = COALESCE(@Columns + ', ', '') + CASE sc.system_type_id
                                                                                       WHEN 240
                                                                                       THEN sc.name + '.STAsText() AS ' + sc.name
                                                                                       ELSE sc.name
                                                                                     END
                                   FROM     sys.tables st
                                            INNER JOIN sys.columns sc ON sc.object_id = st.object_id
                                   WHERE    st.object_id = {0}
                                   ORDER BY sc.column_id;

                                   SELECT   @Schema = ss.name 
                                   FROM     sys.tables st
                                            INNER JOIN sys.schemas ss ON ss.schema_id = st.schema_id
                                   WHERE    st.object_id = {0};

                                   SELECT   @TableName = st.name 
                                   FROM     sys.tables st
                                   WHERE    st.object_id = {0};

								   SELECT  @indexID = object_id
									FROM    sys.indexes
									WHERE   object_id = {0}
											AND is_primary_key = 1;

									IF @indexID IS NULL
									BEGIN	
										SELECT    @SortOrder = COALESCE(@SortOrder + ', ', '') + sc.name
										  FROM      sys.tables st
													INNER JOIN sys.columns sc ON sc.object_id = st.object_id
                                                    INNER JOIN sys.types sty ON sty.system_type_id = sc.system_type_id
										  WHERE     st.object_id = {0}
                                                    AND sty.system_type_id NOT IN (241);
									END
									ELSE
									BEGIN 
										SELECT    @SortOrder = COALESCE(@SortOrder + ', ', '') + sicn.name + ' '
													+ CASE sic.is_descending_key
														WHEN 0 THEN 'ASC'
														ELSE 'DESC'
													  END
										  FROM      sys.indexes si
													INNER JOIN sys.index_columns sic ON sic.object_id = si.object_id
																						AND sic.index_id = si.index_id
													INNER JOIN sys.columns sicn ON sicn.object_id = si.object_id
																				   AND sicn.column_id = sic.column_id
										  WHERE     si.object_id = @indexID
													AND is_primary_key = 1
									END;

                                   SELECT @Schema, @TableName, @Columns, @SortOrder;", table);
                            tableCommand.CommandText = tableQuery;
                            var tableSelect = tableCommand.ExecuteReader(CommandBehavior.SingleRow);

                            try
                            {
                                tableSelect.Read();
                                schema = tableSelect[0].ToString();
                                tablename = tableSelect[1].ToString();
                                columnlist = tableSelect[2].ToString();
                                sortorder = tableSelect[3].ToString();
                                tableSelect.Close();
                            }
                            catch (Exception)
                            {
                                throw;
                            }

                            string tableSelectStatement = "SELECT " + columnlist.Substring(1, (columnlist.Length) - 1) + " FROM [" + schema + "].[" + tablename + "]" + " WITH(NOLOCK) ORDER BY " + sortorder.Substring(1, (sortorder.Length) - 1);

                            tableQuery = tableSelectStatement;
                            tableCommand.CommandText = tableQuery;

                            using (var records = tableCommand.ExecuteReader(CommandBehavior.SingleResult))
                            {
                                //JSON output
                                var fieldFields = Enumerable.Range(0, records.FieldCount).Select(d => new { Index = d, Name = records.GetName(d), Type = records.GetFieldType(d).Name }).ToList();
                                using (var jsonWriter = new JsonTextWriter(new StreamWriter(Path.Combine(options.exportPath, schema + "." + tablename) + ".json", false)))
                                {
                                    //  Added StartArray and EndArary functions to make the output a valid JSON document
                                    jsonWriter.WriteStartArray();
                                    while (records.Read())
                                    {
                                        jsonWriter.WriteStartObject();

                                        fieldFields.ForEach(d =>
                                        {
                                            jsonWriter.WritePropertyName(d.Name);
                                            jsonWriter.WriteValue(records.GetValue(d.Index));
                                        });

                                        jsonWriter.WriteEndObject();
                                        jsonWriter.WriteWhitespace(Environment.NewLine);

                                        tablerowCount++;
                                        if (tablerowCount % 100000 == 0)
                                        {
                                            PostToConsole(string.Format("{0}.{1}: {2}", schema, tablename, tablerowCount.ToString("N0")));
                                        }
                                    }
                                    jsonWriter.WriteEndArray();
                                }
                                PostToConsole(string.Format("{0}.{1}: {2} (Complete)", schema, tablename, tablerowCount.ToString("N0")));
                            }
                            tableConn.Close();
                            totalrowCount += tablerowCount;
                        }
                        );
                    }
                }
            }
            return totalrowCount;
        }

        private static long ImportFromBCP(ImportOptions options)
        {
            var totalrowCount = 0L;

            if (Directory.Exists(options.SourcePath))
            {
                DirectoryInfo workingFolder = new DirectoryInfo(options.SourcePath);
                FileInfo[] files = workingFolder.GetFiles(string.Format("{0}", options.SourceFile), SearchOption.TopDirectoryOnly);
                var list = files.AsEnumerable().Select(f => f.Name).ToArray();

                Parallel.ForEach(
                    files,
                    new ParallelOptions { MaxDegreeOfParallelism = options.MaxParallelTasks },
                    file =>
                {
                    Thread.Sleep(10);

                    string outputPath = options.SourcePath;
                    string baseFileName = Path.GetFileNameWithoutExtension(file.Name);

                    //Target BCP Login settings
                    string bcpLogin;
                    string sqlcmdLogin;
                    if (string.IsNullOrEmpty(options.TargetSQLLogin) && string.IsNullOrEmpty(options.TargetSQLPassword))
                    {
                        bcpLogin = "-T";
                        sqlcmdLogin = "-E";
                    }
                    else
                    {
                        bcpLogin = string.Format("-U {0} -P {1}", options.TargetSQLLogin, options.TargetSQLPassword);
                        sqlcmdLogin = string.Format("-U {0} -P {1}", options.TargetSQLLogin, options.TargetSQLPassword);
                    }

                    //Get target table object
                    string[] TargetTable; // = new string[3];
                    if (string.IsNullOrEmpty(options.TargetTable))
                    {
                        PostToConsole(string.Format("Using table name from file {0}", baseFileName), InfoLevel.Info);
                        TargetTable = GetTableObject(baseFileName, options.TargetSQLServer, options.TargetSQLDatabase, options.GetTargetConnectionString());
                    }
                    else
                    {
                        PostToConsole(string.Format("Using supplied target table name {0}", options.TargetTable), InfoLevel.Info);
                        TargetTable = GetTableObject(options.TargetTable, options.TargetSQLServer, options.TargetSQLDatabase, options.GetTargetConnectionString());
                    }

                    if (TargetTable == null)
                    {
                        PostToConsole(string.Format("{0}: Target table does not exist. Cannot use BCP to import.", (string.IsNullOrEmpty(options.TargetTable) ? baseFileName : options.TargetTable)), InfoLevel.Error);
                        return;
                    }

                    //Truncate target table
                    if (options.TruncateTarget)
                    {
                        string truncsql = string.Format("truncate table {0}.{1}", TargetTable[0], TargetTable[1]);
                        string truncargs = String.Format("-S {0} {1} -d {2} -l 60 -b -o {3}.{4}_truncate.log -e -Q \"{5}\"", options.TargetSQLServer, sqlcmdLogin, options.TargetSQLDatabase, TargetTable[0], TargetTable[1], truncsql);
                        int truncrc = ExecuteProcess("sqlcmd.exe", truncargs, outputPath);
                        if (truncrc == 0)
                        {
                            PostToConsole(string.Format("{0}.{1}: Truncate Complete.", TargetTable[0], TargetTable[1]));
                        }
                        else
                        {
                            PostToConsole(string.Format("{0}: Truncate Error.", file.FullName), InfoLevel.Error);
                        }
                    }

                    //Execute BCP Import
                    string bcpargs = String.Format("{0}.{1} in {2} -f {3}-c.xml -S {4} -d {5} {6} -e {3}_import_error.log -o {3}_import_output.log", TargetTable[0], TargetTable[1], options.SourceFile, baseFileName, options.TargetSQLServer, options.TargetSQLDatabase, bcpLogin);
                    int bcprc = ExecuteProcess("bcp.exe", bcpargs, outputPath);

                    if (bcprc == 0)
                    {
                        //Setup target object
                        ServerConnection targetConnection = new ServerConnection(options.TargetSQLServer);
                        Server targetServer = new Server(targetConnection);
                        Database targetDatabase = targetServer.Databases[options.TargetSQLDatabase];
                        Table targetTable = targetDatabase.Tables[TargetTable[1], TargetTable[0]];
                        totalrowCount += targetTable.RowCount;

                        PostToConsole(string.Format("{0}.{1}: Import Complete.", TargetTable[0], TargetTable[1]), InfoLevel.Success);
                    }
                    else
                    {
                        PostToConsole(string.Format("{0}: Error Importing data.", file.FullName), InfoLevel.Error);
                    }
                });
                return totalrowCount;
            }
            else
            {
                return -1;
            }
        }

        private static string BuildSourceObjectIDList(IEnumerable<string> IncludeTablesList, IEnumerable<string> ExcludeTablesList, IEnumerable<string> IncludeSchemasList, IEnumerable<string> ExcludeSchemasList)
        {
            IEnumerable<string> sIncludedTablesLiteral = IncludeTablesList.Where(o => !o.Contains("*"));
            IEnumerable<string> sIncludedTablesPartial = IncludeTablesList.Where(o => o.Contains("*"));
            IEnumerable<string> sExcludedTablesLiteral = ExcludeTablesList.Where(o => !o.Contains("*"));
            IEnumerable<string> sExcludedTablesPartial = ExcludeTablesList.Where(o => o.Contains("*"));

            IEnumerable<string> sIncludedSchemasLiteral = IncludeSchemasList.Where(o => !o.Contains("*"));
            IEnumerable<string> sIncludedSchemasPartial = IncludeSchemasList.Where(o => o.Contains("*"));
            IEnumerable<string> sExcludedSchemasLiteral = ExcludeSchemasList.Where(o => !o.Contains("*"));
            IEnumerable<string> sExcludedSchemasPartial = ExcludeSchemasList.Where(o => o.Contains("*"));

            string sMergeSQL = @"SET NOCOUNT ON;
                DECLARE @objectIDs TABLE(ID INT NOT NULL)";

            if (sIncludedTablesLiteral.Any())
            {
                sMergeSQL += string.Format(@"
                /* Include Literal Table Names*/
                MERGE @objectIDs AS target
                USING
                (
                    SELECT incTT.object_id
                    FROM sys.tables incTT
                        INNER JOIN sys.schemas incTS
                            ON incTS.schema_id = incTT.schema_id
                    WHERE incTT.type = 'U'
                          AND QUOTENAME(incTS.name + '.' + incTT.name) IN (QUOTENAME('{0}'))
                ) AS source
                (ID)
                ON(target.ID = source.ID)
                WHEN NOT MATCHED THEN
                    INSERT(ID)
                    VALUES(source.ID);", string.Join("'),QUOTENAME('", sIncludedTablesLiteral).Trim());
            }
            if (sIncludedTablesPartial.Any())
            {
                foreach (string partialtable in sIncludedTablesPartial)
                {
                    sMergeSQL += string.Format(@"
                    /* Include Partial Table Names*/
                    MERGE @objectIDs AS target
                    USING
                    (
                        SELECT incTT.object_id
                        FROM sys.tables incTT
                            INNER JOIN sys.schemas incTS
                                ON incTS.schema_id = incTT.schema_id
                        WHERE incTT.type = 'U'
                              AND incTS.name + '.' + incTT.name LIKE '{0}'
                    ) AS source
                    (ID)
                    ON(target.ID = source.ID)
                    WHEN NOT MATCHED THEN
                        INSERT(ID)
                        VALUES(source.ID);", (partialtable.Replace("_", "[_]")).Replace('*', '%'));
                }
            }

            if (sIncludedSchemasLiteral.Any())
            {
                sMergeSQL += string.Format(@"
                /* Include Literal Schema Names*/
                MERGE @objectIDs AS target
                USING
                (
                    SELECT exTT.object_id
                    FROM sys.tables exTT
                        INNER JOIN sys.schemas exTS
                            ON exTS.schema_id = exTT.schema_id
                    WHERE exTT.type = 'U'
                            AND QUOTENAME(exTS.name) IN (QUOTENAME('{0}'))
                ) AS source
                (ID)
                ON(target.ID = source.ID)
                WHEN NOT MATCHED THEN
                    INSERT(ID)
                    VALUES(source.ID);", string.Join("'),QUOTENAME('", sIncludedSchemasLiteral).Trim());
            }
            if (sIncludedSchemasPartial.Any())
            {
                foreach (string partialschema in sIncludedSchemasPartial)
                {
                    sMergeSQL += string.Format(@"
                    /* Include Partial Schema Names*/
                    MERGE @objectIDs AS target
                    USING
                    (
                        SELECT incTT.object_id
                        FROM sys.tables incTT
                            INNER JOIN sys.schemas incTS
                                ON incTS.schema_id = incTT.schema_id
                        WHERE incTT.type = 'U'
                              AND incTS.name LIKE '{0}'
                    ) AS source
                    (ID)
                    ON(target.ID = source.ID)
                    WHEN NOT MATCHED THEN
                        INSERT(ID)
                        VALUES(source.ID);", (partialschema.Replace("_", "[_]")).Replace('*', '%'));
                }
            }

            if (sExcludedTablesLiteral.Any())
            {
                sMergeSQL += string.Format(@"
                /* Exclude Literal Table Names*/
                MERGE @objectIDs AS target
                USING
                (
                    SELECT exTT.object_id
                    FROM sys.tables exTT
                        INNER JOIN sys.schemas exTS
                            ON exTS.schema_id = exTT.schema_id
                    WHERE exTT.type = 'U'
                            AND QUOTENAME(exTS.name + '.' + exTT.name) IN (QUOTENAME('{0}'))
                ) AS source
                (ID)
                ON(target.ID = source.ID)
                WHEN MATCHED THEN
                    DELETE;", string.Join("'),QUOTENAME('", sExcludedTablesLiteral).Trim());
            }
            if (sExcludedTablesPartial.Any())
            {
                foreach (string partialtable in sExcludedTablesPartial)
                {
                    sMergeSQL += string.Format(@"
                    /* Exclude Partial Table Names*/
                    MERGE @objectIDs AS target
                USING
                (
                    SELECT exTT.object_id
                    FROM sys.tables exTT
                        INNER JOIN sys.schemas exTS
                            ON exTS.schema_id = exTT.schema_id
                    WHERE exTT.type = 'U'
                            AND exTS.name + '.' + exTT.name LIKE '{0}'
                ) AS source
                (ID)
                ON(target.ID = source.ID)
                WHEN MATCHED THEN
                    DELETE;", (partialtable.Replace("_", "[_]")).Replace('*', '%'));
                }
            }

            if (sExcludedSchemasLiteral.Any())
            {
                sMergeSQL += string.Format(@"
                /* Exclude Literal Schema Names*/
                MERGE @objectIDs AS target
                USING
                (
                    SELECT exTT.object_id
                    FROM sys.tables exTT
                        INNER JOIN sys.schemas exTS
                            ON exTS.schema_id = exTT.schema_id
                    WHERE exTT.type = 'U'
                            AND QUOTENAME(exTS.name) IN(QUOTENAME('{0}'))
                ) AS source
                (ID)
                ON(target.ID = source.ID)
                WHEN MATCHED THEN
                    DELETE;", string.Join("'),QUOTENAME('", sExcludedSchemasLiteral).Trim());
            }
            if (sExcludedSchemasPartial.Any())
            {
                foreach (string partialschema in sExcludedSchemasPartial)
                {
                    sMergeSQL += string.Format(@"
                    /* Exclude Partial Schema Names*/
                MERGE @objectIDs AS target
                USING
                (
                    SELECT exTT.object_id
                    FROM sys.tables exTT
                        INNER JOIN sys.schemas exTS
                            ON exTS.schema_id = exTT.schema_id
                    WHERE exTT.type = 'U'
                            AND exTS.name LIKE '{0}'
                ) AS source
                (ID)
                ON(target.ID = source.ID)
                WHEN MATCHED THEN
                    DELETE;", (partialschema.Replace("_", "[_]")).Replace('*', '%'));
                }
            }

            sMergeSQL += @"SELECT ID as object_id FROM @objectIDs;";

            return sMergeSQL;
        }

        private static string BuildSourceObjectIDList(string ExportQuery)
        {
            return ExportQuery;
        }

        private static string[] GetTableObject(string objectName, string SQLServer, string Database, string ConnectionString)
        {
            string cmdQueryString = string.Format(@"SET NOCOUNT ON;
                            DECLARE @objectIDs TABLE(ID INT NOT NULL, objSchema sysname NOT NULL, objName sysname NOT NULL)
                            /* Include Literal Table Names*/
                            MERGE @objectIDs AS target
                            USING
                            (
                                SELECT TT.object_id, TS.Name, TT.Name
                                FROM sys.tables TT
                                    INNER JOIN sys.schemas TS
                                        ON TS.schema_id = TT.schema_id
                                WHERE TT.type = 'U'
                                      AND QUOTENAME(TS.name + '.' + TT.name) = QUOTENAME('{0}')
                            ) AS source
                            (ID, objSchema, Object)
                            ON (target.ID = source.ID)
                            WHEN NOT MATCHED THEN
                                INSERT
                                (
                                    ID, objSchema, objName
                                )
                                VALUES
                                (source.ID, source.objSchema, source.Object);
                            SELECT ID as object_id, objName, objSchema FROM @objectIDs;", objectName);
            SqlConnection conn = new SqlConnection(ConnectionString);
            conn.Open();
            SqlCommand cmd = new SqlCommand(cmdQueryString, conn);
            SqlDataReader reader = cmd.ExecuteReader(CommandBehavior.SingleResult);
            DataTable result = new DataTable();
            result.Load(reader);

            if (result.Rows.Count == 1)
            {
                ServerConnection smoConnection = new ServerConnection(SQLServer);
                Server smoServer = new Server(SQLServer);
                Database smoDatabase = smoServer.Databases[Database];
                string[] dbObject = new string[4];

                //Check if tatrget table exists
                if (smoDatabase.Tables.Contains(result.Rows[0]["objName"].ToString(), result.Rows[0]["objSchema"].ToString()))
                {
                    Table srcTable = smoDatabase.Tables[result.Rows[0]["objName"].ToString(), result.Rows[0]["objSchema"].ToString()];
                    dbObject[0] = srcTable.Schema;
                    dbObject[1] = srcTable.Name;
                    dbObject[2] = srcTable.ID.ToString();
                    dbObject[3] = objectName;
                }

                conn.Close();
                smoServer.ConnectionContext.Disconnect();

                return dbObject;
            }

            return null;
        }

        private static int ExecuteProcess(string Command, string Arguments, string OutputPath)
        {
            string strOutput;
            int exitCode;

            //Create process
            System.Diagnostics.Process pProcess = new System.Diagnostics.Process();

            //strCommand is path and file name of command to run
            pProcess.StartInfo.FileName = Command;

            //strCommandParameters are parameters to pass to program
            pProcess.StartInfo.Arguments = Arguments;

            pProcess.StartInfo.UseShellExecute = false;

            pProcess.StartInfo.CreateNoWindow = true;

            //Set output of program to be written to process output stream
            pProcess.StartInfo.RedirectStandardOutput = true;

            //Optional
            pProcess.StartInfo.WorkingDirectory = OutputPath;

            //Start the process
            pProcess.Start();

            //Get program output
            strOutput = pProcess.StandardOutput.ReadToEnd();

            //Wait for process to finish
            pProcess.WaitForExit();
            exitCode = pProcess.ExitCode;

            pProcess.Close();

            return exitCode;
        }

        private static void SetLogFile(string path)
        {
            _logFilePath = string.IsNullOrWhiteSpace(path) ? null : path;
            if (!string.IsNullOrEmpty(_logFilePath))
            {
                try
                {
                    File.AppendAllText(_logFilePath, $"Log started {DateTime.Now}{Environment.NewLine}");
                }
                catch
                {
                    _logFilePath = null;
                }
            }
        }

        public enum InfoLevel
        {
            Info,
            Warning,
            Success,
            Error
        }
        private static bool PostToConsole(string Message, InfoLevel MessageLevel = InfoLevel.Info)
        {
            ConsoleColor consoleColor = ConsoleColor.Gray;
            switch(MessageLevel)
            {
                case InfoLevel.Info:
                    consoleColor = ConsoleColor.Cyan;
                    break;
                case InfoLevel.Warning:
                    consoleColor = ConsoleColor.Yellow;
                    break;
                case InfoLevel.Success:
                    consoleColor = ConsoleColor.Green;
                    break;
                case InfoLevel.Error:
                    consoleColor = ConsoleColor.Red;
                    break;
                default:
                    break;
            }

            Console.ForegroundColor = consoleColor;
            Console.WriteLine(Message);
            Console.ResetColor();
            if (!string.IsNullOrEmpty(_logFilePath))
            {
                lock (_logLock)
                {
                    File.AppendAllText(_logFilePath, Message + Environment.NewLine);
                }
            }
            return true;
        }
    }
}
