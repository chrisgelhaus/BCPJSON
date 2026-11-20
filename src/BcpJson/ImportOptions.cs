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

        public string GetTargetConnectionString()
        {
            return SqlConnectionFactory.Build(this.TargetSQLServer, this.TargetSQLDatabase, this.TargetSQLLogin, this.TargetSQLPassword, false, this.TrustServerCertificate);
        }

        public bool Valid()
        {
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
