using CommandLine;

namespace bcpJson
{
    [Verb("import", HelpText = "Import data into SQL Server from flat files.")]
    class ImportOptions
    {
        [Option("file-format", Required = true, HelpText = "Source file format: bcp or json.")]
        public string FileFormat { get; set; }

        [Option("source-path", Required = false, HelpText = "Folder containing the source file(s). Defaults to the current directory.")]
        public string SourcePath { get; set; }

        [Option("source-file", Required = true, HelpText = "Source file to import. Wildcards are allowed, e.g. *.json.")]
        public string SourceFile { get; set; }

        [Option("target-server", Required = false, Default = "localhost", HelpText = "Specifies the target instance of SQL Server to which to connect.")]
        public string TargetSQLServer { get; set; }

        [Option("target-database", Required = false, HelpText = "Specifies the target database to load into.")]
        public string TargetSQLDatabase { get; set; }

        [Option("target-login", Default = "", Required = false, HelpText = "Specifies the login ID used to connect to target SQL Server. Leave blank for Windows Auth.")]
        public string TargetSQLLogin { get; set; }

        [Option("target-password", Default = "", Required = false, HelpText = "Specifies the password for the target login ID. Leave blank for Windows Auth.")]
        public string TargetSQLPassword { get; set; }

        [Option("target-table", Required = false, HelpText = "Target table name (schema.table). Defaults to the file name without extension.")]
        public string TargetTable { get; set; }

        [Option("truncate-target", Required = false, HelpText = "Truncate target table before data load.")]
        public bool TruncateTarget { get; set; }

        [Option("trust-server-certificate", Default = false, Required = false, HelpText = "Trust the server's TLS certificate without validation (e.g. local instances with self-signed certificates).")]
        public bool TrustServerCertificate { get; set; }

        [Option("log-file", Required = false, HelpText = "Append console messages to this log file.")]
        public string LogFile { get; set; }

        [Option("max-tasks", Default = -1, Required = false, HelpText = "Maximum files imported in parallel (0 or -1 = unlimited).")]
        public int MaxParallelTasks { get; set; }

        [Option("no-table-lock", Default = false, Required = false, HelpText = "Load without a table lock (TABLOCK). By default the target is locked for the load, which is fastest and allows minimal logging; use this when other sessions must keep reading or writing the table during the load, or when several files load into one table with a clustered index.")]
        public bool NoTableLock { get; set; }

        [Option("retries", Default = SqlConnectionFactory.DefaultRetries, Required = false, HelpText = "Retries when opening a connection fails transiently (network error, failover, throttling), with exponential backoff. Loads themselves are not retried. 0 = no retries.")]
        public int Retries { get; set; }

        [Option("batch-size", Default = Program.DefaultBatchSize, Required = false, HelpText = "Rows per committed batch during the load (JSON and bcp). Larger batches load faster; a failure rolls back only the current batch. 0 = the whole file in one transaction.")]
        public int BatchSize { get; set; } = Program.DefaultBatchSize;

        public string GetTargetConnectionString()
        {
            return SqlConnectionFactory.Build(this.TargetSQLServer, this.TargetSQLDatabase, this.TargetSQLLogin, this.TargetSQLPassword, false, this.TrustServerCertificate);
        }

        public bool Valid()
        {
            if (!SqlConnectionFactory.ApplyRetries(this.Retries))
            {
                return false;
            }

            if (this.BatchSize < 0)
            {
                Program.PostToConsole("--batch-size must be 0 (one transaction per file) or more.", Program.InfoLevel.Error);
                return false;
            }

            if (string.IsNullOrEmpty(this.SourcePath))
            {
                this.SourcePath = Directory.GetCurrentDirectory();
            }

            if (!Directory.Exists(this.SourcePath))
            {
                Program.PostToConsole(string.Format("Source path '{0}' does not exist.", this.SourcePath), Program.InfoLevel.Error);
                return false;
            }

            return SqlConnectionFactory.TryOpen(this.GetTargetConnectionString(), "target server");
        }
    }
}
