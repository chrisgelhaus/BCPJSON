using CommandLine;
using System.Collections.Generic;

namespace bcpJson
{
    [Verb("export", HelpText = "Export data from SQL Server to flat files (one file per table, or one file for --source-query).")]
    class ExportOptions
    {
        [Option("source-server", Required = false, Default = "localhost", HelpText = "Specifies the source instance of SQL Server to which to connect.")]
        public string srcServer { get; set; }

        [Option("source-database", Required = false, HelpText = "Specifies the source database to read from.")]
        public string srcDatabase { get; set; }

        [Option("source-login", Default = "", Required = false, HelpText = "Specifies the login ID used to connect to source SQL Server. Leave blank for Windows Auth.")]
        public string srcLogin { get; set; }

        [Option("source-password", Default = "", Required = false, HelpText = "Specifies the password for the source login ID. Leave blank for Windows Auth.")]
        public string srcPassword { get; set; }

        [Option("source-query", Required = false, HelpText = "Transact-SQL query/procedure to export instead of tables. Only the first result set is written; the file is named by --output-name.")]
        public string Query { get; set; }

        [Option("output-name", Required = false, Default = "query", HelpText = "Base file name used when exporting --source-query.")]
        public string OutputName { get; set; }

        [Option('k', Required = false, HelpText = "Reserved. JSON exports always write NULL values explicitly.")]
        public bool ExportNull { get; set; }

        [Option("include-tables", Separator = ',', Required = false, HelpText = "Comma separated list of schema.tablename to include in the export. * is a wildcard. If no include filters are given, all user tables are exported.")]
        public IEnumerable<string> IncludeTablesList { get; set; }

        [Option("include-schemas", Separator = ',', Required = false, HelpText = "Comma separated list of table schemas to include in the export. * is a wildcard.")]
        public IEnumerable<string> IncludeSchemaList { get; set; }

        [Option("exclude-tables", Separator = ',', Required = false, HelpText = "Comma separated list of schema.tablename to exclude from the export. * is a wildcard.")]
        public IEnumerable<string> ExcludeTablesList { get; set; }

        [Option("exclude-schemas", Separator = ',', Required = false, HelpText = "Comma separated list of table schemas to exclude from the export. * is a wildcard.")]
        public IEnumerable<string> ExcludeSchemaList { get; set; }

        [Option("nolock", Default = false, Required = false, HelpText = "Add WITH (NOLOCK) to source table reads (json format). Can cause dirty reads.")]
        public bool NoLock { get; set; }

        [Option("file-format", Required = false, Default = "bcp", HelpText = "Target file format: bcp or json.")]
        public string FileFormat { get; set; }

        [Option("export-path", Required = false, HelpText = "Folder for exported files. Defaults to the current directory; created if missing.")]
        public string exportPath { get; set; }

        [Option("trust-server-certificate", Default = false, Required = false, HelpText = "Trust the server's TLS certificate without validation (e.g. local instances with self-signed certificates).")]
        public bool TrustServerCertificate { get; set; }

        [Option("log-file", Required = false, HelpText = "Append console messages to this log file.")]
        public string outputLogFile { get; set; }

        [Option("max-tasks", Default = -1, Required = false, HelpText = "Maximum tables exported in parallel (0 or -1 = unlimited).")]
        public int MaxParallelTasks { get; set; }

        [Option("snapshot", Default = false, Required = false, HelpText = "Export from a database snapshot of the source so every table reflects the same point in time, however long the export runs. Needs CREATE DATABASE permission and SQL Server 2016 SP1+ (any edition); the snapshot is dropped afterwards.")]
        public bool Snapshot { get; set; }

        public bool IsQueryExport
        {
            get { return !string.IsNullOrEmpty(this.Query); }
        }

        public string SourceConnectionString()
        {
            return SqlConnectionFactory.Build(this.srcServer, this.srcDatabase, this.srcLogin, this.srcPassword, true, this.TrustServerCertificate);
        }

        public bool Valid()
        {
            this.IncludeTablesList ??= new List<string>();
            this.IncludeSchemaList ??= new List<string>();
            this.ExcludeTablesList ??= new List<string>();
            this.ExcludeSchemaList ??= new List<string>();

            if (string.IsNullOrEmpty(this.Query) && string.IsNullOrEmpty(this.srcDatabase))
            {
                Program.PostToConsole("Specify --source-database (table export) or --source-query (query export).", Program.InfoLevel.Error);
                return false;
            }

            if (this.Snapshot && string.IsNullOrEmpty(this.srcDatabase))
            {
                Program.PostToConsole("--snapshot needs --source-database (the database to snapshot).", Program.InfoLevel.Error);
                return false;
            }

            if (string.IsNullOrEmpty(this.exportPath))
            {
                this.exportPath = Directory.GetCurrentDirectory();
            }

            try
            {
                Directory.CreateDirectory(this.exportPath);
            }
            catch (Exception ex)
            {
                Program.PostToConsole(string.Format("Export path '{0}' is not usable: {1}", this.exportPath, ex.Message), Program.InfoLevel.Error);
                return false;
            }

            return SqlConnectionFactory.TryOpen(this.SourceConnectionString(), "source server");
        }
    }
}
