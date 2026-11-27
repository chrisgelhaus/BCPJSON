using CommandLine;
using System.Collections.Generic;

namespace bcpJson
{
    [Verb("copy", HelpText = "Copy tables from one SQL Server database to another.")]
    class CopyOptions
    {
        [Option("source-server", Required = false, Default = "localhost", HelpText = "Specifies the source instance of SQL Server to which to connect.")]
        public string srcServer { get; set; }

        [Option("source-database", Required = false, HelpText = "Specifies the source database to read from.")]
        public string srcDatabase { get; set; }

        [Option("source-login", Default = "", Required = false, HelpText = "Specifies the login ID used to connect to source SQL Server. Leave blank for Windows Auth.")]
        public string srcLogin { get; set; }

        [Option("source-password", Default = "", Required = false, HelpText = "Specifies the password for the source login ID. Leave blank for Windows Auth.")]
        public string srcPassword { get; set; }

        [Option("target-server", Required = false, Default = "localhost", HelpText = "Specifies the target instance of SQL Server to which to connect.")]
        public string TargetSQLServer { get; set; }

        [Option("target-database", Required = false, HelpText = "Specifies the target database to write to.")]
        public string TargetSQLDatabase { get; set; }

        [Option("target-login", Default = "", Required = false, HelpText = "Specifies the login ID used to connect to target SQL Server. Leave blank for Windows Auth.")]
        public string TargetSQLLogin { get; set; }

        [Option("target-password", Default = "", Required = false, HelpText = "Specifies the password for the target login ID. Leave blank for Windows Auth.")]
        public string TargetSQLPassword { get; set; }

        [Option("source-query", Required = false, HelpText = "Not supported by copy yet; ignored.")]
        public string Query { get; set; }

        [Option("include-tables", Separator = ',', Required = false, HelpText = "Comma separated list of schema.tablename to include. * is a wildcard. If no include filters are given, all user tables are copied.")]
        public IEnumerable<string> IncludeTablesList { get; set; }

        [Option("include-schemas", Separator = ',', Required = false, HelpText = "Comma separated list of schemas to include. * is a wildcard.")]
        public IEnumerable<string> IncludeSchemaList { get; set; }

        [Option("exclude-tables", Separator = ',', Required = false, HelpText = "Comma separated list of schema.tablename to exclude. * is a wildcard.")]
        public IEnumerable<string> ExcludeTablesList { get; set; }

        [Option("exclude-schemas", Separator = ',', Required = false, HelpText = "Comma separated list of schemas to exclude. * is a wildcard.")]
        public IEnumerable<string> ExcludeSchemaList { get; set; }

        [Option("nolock", Default = false, Required = false, HelpText = "Add WITH (NOLOCK) to source table reads. Can cause dirty reads.")]
        public bool NoLock { get; set; }

        [Option("drop-target", Default = false, Required = false, HelpText = "Drop target table if it exists.")]
        public bool DropTarget { get; set; }

        [Option("truncate-target", Default = false, Required = false, HelpText = "Truncate target table.")]
        public bool TruncateTarget { get; set; }

        [Option("max-tasks", Default = -1, Required = false, HelpText = "Maximum tables copied in parallel (0 or -1 = unlimited).")]
        public int MaxParallelTasks { get; set; }

        [Option("trust-server-certificate", Default = false, Required = false, HelpText = "Trust the servers' TLS certificates without validation (e.g. local instances with self-signed certificates).")]
        public bool TrustServerCertificate { get; set; }

        [Option("log-file", Required = false, HelpText = "Append console messages to this log file.")]
        public string outputLogFile { get; set; }

        [Option("no-table-lock", Default = false, Required = false, HelpText = "Load without a table lock (TABLOCK). By default each target table is locked for the load, which is fastest and allows minimal logging; use this when other sessions must keep using the target tables during the copy.")]
        public bool NoTableLock { get; set; }

        [Option("no-indexes", Default = false, Required = false, HelpText = "Do not recreate the source's primary keys, unique constraints and indexes on target tables that copy creates. By default they are built after the data is loaded. Tables that already exist keep their own indexes either way.")]
        public bool NoIndexes { get; set; }

        [Option("foreign-keys", Default = false, Required = false, HelpText = "Also recreate the foreign keys of the tables copy creates, after all tables are loaded. Trusted keys are created WITH CHECK, which validates the copied rows: combine with --snapshot so parent and child tables are copied from the same point in time, and make sure referenced tables are part of the copy (or already exist in the target).")]
        public bool ForeignKeys { get; set; }

        [Option("no-check-constraints", Default = false, Required = false, HelpText = "Do not recreate the source's check constraints on target tables that copy creates. By default they are created after the data is loaded (trusted ones WITH CHECK).")]
        public bool NoCheckConstraints { get; set; }

        [Option("triggers", Default = false, Required = false, HelpText = "Also recreate the DML triggers of the tables copy creates, after all tables are loaded (so they do not fire on the copied rows). Off by default because triggers run on every later write in the target and may reference objects that do not exist there.")]
        public bool Triggers { get; set; }

        [Option("snapshot", Default = false, Required = false, HelpText = "Copy from a database snapshot of the source so every table reflects the same point in time. Needs CREATE DATABASE permission and SQL Server 2016 SP1+ (any edition); the snapshot is dropped afterwards.")]
        public bool Snapshot { get; set; }

        [Option("split-tables", Separator = ',', Required = false, HelpText = "Split tables into key ranges copied in parallel: schema.table[:parts], * wildcard, parts 2-200 (default 8).")]
        public IEnumerable<string> SplitTables { get; set; }

        public List<SplitRequest> SplitRequests { get; private set; } = new List<SplitRequest>();

        [Option("load-as-heap", Default = false, Required = false, HelpText = "For split tables copied into an existing table with a clustered index: drop its indexes, copy the parts in parallel into the heap, then recreate the indexes. Needs --recovery-path.")]
        public bool LoadAsHeap { get; set; }

        [Option("recovery-path", Required = false, HelpText = "Folder for the recovery scripts of --load-as-heap (created if missing).")]
        public string RecoveryPath { get; set; }

        [Option("retries", Default = SqlConnectionFactory.DefaultRetries, Required = false, HelpText = "Retries when opening a connection fails transiently (network error, failover, throttling), with exponential backoff. Copies themselves are not retried. 0 = no retries.")]
        public int Retries { get; set; }

        public string SourceConnectionString()
        {
            return SqlConnectionFactory.Build(this.srcServer, this.srcDatabase, this.srcLogin, this.srcPassword, true, this.TrustServerCertificate);
        }

        public string TargetConnectionString()
        {
            return SqlConnectionFactory.Build(this.TargetSQLServer, this.TargetSQLDatabase, this.TargetSQLLogin, this.TargetSQLPassword, false, this.TrustServerCertificate);
        }

        /// <summary>Parses --split-tables into SplitRequests. Returns the error (or null) and the snapshot warning (or null). No connection is made.</summary>
        internal (string Error, string Warning) ValidateSplit()
        {
            if (!SplitRequest.TryParse(this.SplitTables, out List<SplitRequest> requests, out string error))
            {
                return (error, null);
            }

            this.SplitRequests = requests;
            return (null, SplitRequest.SnapshotWarning(requests.Count > 0, this.Snapshot, "copy"));
        }

        /// <summary>The error for --load-as-heap without --recovery-path, or null. No connection is made.</summary>
        internal string ValidateLoadAsHeap()
        {
            return this.LoadAsHeap && string.IsNullOrWhiteSpace(this.RecoveryPath)
                ? "--load-as-heap needs --recovery-path: the folder where copy writes each table's recovery script."
                : null;
        }

        public bool Valid()
        {
            this.IncludeTablesList ??= new List<string>();
            this.IncludeSchemaList ??= new List<string>();
            this.ExcludeTablesList ??= new List<string>();
            this.ExcludeSchemaList ??= new List<string>();

            if (!SqlConnectionFactory.ApplyRetries(this.Retries))
            {
                return false;
            }

            if (string.IsNullOrEmpty(this.srcDatabase) || string.IsNullOrEmpty(this.TargetSQLDatabase))
            {
                Program.PostToConsole("Specify both --source-database and --target-database.", Program.InfoLevel.Error);
                return false;
            }

            if (this.ForeignKeys && this.NoIndexes)
            {
                Program.PostToConsole("--foreign-keys cannot be combined with --no-indexes: foreign keys need the primary keys and unique constraints that --no-indexes skips.", Program.InfoLevel.Error);
                return false;
            }

            if (this.ForeignKeys && !this.Snapshot)
            {
                Program.PostToConsole("--foreign-keys without --snapshot: if the source changes during the copy, a child row can be copied without its parent and the foreign key check will fail. Consider adding --snapshot.", Program.InfoLevel.Warning);
            }

            (string splitError, string splitWarning) = this.ValidateSplit();
            if (splitError != null)
            {
                Program.PostToConsole(splitError, Program.InfoLevel.Error);
                return false;
            }

            if (splitWarning != null)
            {
                Program.PostToConsole(splitWarning, Program.InfoLevel.Warning);
            }

            string heapError = this.ValidateLoadAsHeap();
            if (heapError != null)
            {
                Program.PostToConsole(heapError, Program.InfoLevel.Error);
                return false;
            }

            if (!string.IsNullOrWhiteSpace(this.RecoveryPath))
            {
                try
                {
                    this.RecoveryPath = Path.GetFullPath(this.RecoveryPath);
                    Directory.CreateDirectory(this.RecoveryPath);
                }
                catch (Exception ex)
                {
                    Program.PostToConsole(string.Format("Cannot create --recovery-path {0}: {1}", this.RecoveryPath, ex.Message), Program.InfoLevel.Error);
                    return false;
                }
            }

            if (!string.IsNullOrEmpty(this.Query))
            {
                Program.PostToConsole("--source-query is not supported by copy and will be ignored.", Program.InfoLevel.Warning);
            }

            if (this.MaxParallelTasks == 0)
            {
                this.MaxParallelTasks = -1;
            }

            return SqlConnectionFactory.TryOpen(this.SourceConnectionString(), "source server")
                && SqlConnectionFactory.TryOpen(this.TargetConnectionString(), "target server");
        }
    }
}
