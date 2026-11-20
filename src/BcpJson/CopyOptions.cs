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

        [Option("max-tasks", Default = -1, Required = false, HelpText = "Maximum parallel tasks (-1 = unlimited).")]
        public int MaxParallelTasks { get; set; }

        [Option("trust-server-certificate", Default = false, Required = false, HelpText = "Trust the servers' TLS certificates without validation (e.g. local instances with self-signed certificates).")]
        public bool TrustServerCertificate { get; set; }

        [Option("log-file", Required = false, HelpText = "Append console messages to this log file.")]
        public string outputLogFile { get; set; }

        public string SourceConnectionString()
        {
            return SqlConnectionFactory.Build(this.srcServer, this.srcDatabase, this.srcLogin, this.srcPassword, true, this.TrustServerCertificate);
        }

        public string TargetConnectionString()
        {
            return SqlConnectionFactory.Build(this.TargetSQLServer, this.TargetSQLDatabase, this.TargetSQLLogin, this.TargetSQLPassword, false, this.TrustServerCertificate);
        }

        public bool Valid()
        {
            this.IncludeTablesList ??= new List<string>();
            this.IncludeSchemaList ??= new List<string>();
            this.ExcludeTablesList ??= new List<string>();
            this.ExcludeSchemaList ??= new List<string>();

            if (string.IsNullOrEmpty(this.srcDatabase) || string.IsNullOrEmpty(this.TargetSQLDatabase))
            {
                Program.PostToConsole("Specify both --source-database and --target-database.", Program.InfoLevel.Error);
                return false;
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
