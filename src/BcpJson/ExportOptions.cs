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

        [Option("split-tables", Separator = ',', Required = false, HelpText = "Split tables into key ranges exported in parallel: schema.table[:parts], * wildcard, parts 2-200 (default 8).")]
        public IEnumerable<string> SplitTables { get; set; }

        public List<SplitRequest> SplitRequests { get; private set; } = new List<SplitRequest>();

        [Option("retries", Default = SqlConnectionFactory.DefaultRetries, Required = false, HelpText = "Retries after a transient failure (network error, failover, throttling), with exponential backoff. Applies to opening connections and to exporting a table, which is redone from the start. --source-query exports are not retried. 0 = no retries.")]
        public int Retries { get; set; }

        [Option("resume", Default = false, Required = false, HelpText = "Continue an interrupted export into the same --export-path: tables already recorded in its bcpjson-manifest.json with their files intact are skipped. Must use the same --file-format, --compress, --source-database and --source-query.")]
        public bool Resume { get; set; }

        [Option("compress", Default = FileCompression.None, Required = false, HelpText = "Compress exported data files: none or gzip. gzip writes <name>.json.gz or <name>.bcp.gz (bcp data is compressed after bcp has written it); format files and the manifest stay plain. Import reads .gz files directly. Off by default so exports stay diffable line by line.")]
        public string Compress { get; set; } = FileCompression.None;

        [Option("compression-level", Default = FileCompression.DefaultLevel, Required = false, HelpText = "gzip level with --compress gzip: fastest (most of the size reduction at the highest speed), optimal or smallest.")]
        public string CompressionLevel { get; set; } = FileCompression.DefaultLevel;

        /// <summary>The gzip level when --compress gzip is set; null for uncompressed files.</summary>
        public System.IO.Compression.CompressionLevel? GzipLevel
        {
            get { return FileCompression.IsGzip(this.Compress) ? FileCompression.ParseLevel(this.CompressionLevel) : null; }
        }

        /// <summary>The data file name for a table or query: &lt;name&gt;&lt;extension&gt;, plus .gz when compressing.</summary>
        public string DataFileName(string name, string extension)
        {
            return name + extension + (FileCompression.IsGzip(this.Compress) ? FileCompression.GzipExtension : string.Empty);
        }

        public bool IsQueryExport
        {
            get { return !string.IsNullOrEmpty(this.Query); }
        }

        public string SourceConnectionString()
        {
            return SqlConnectionFactory.Build(this.srcServer, this.srcDatabase, this.srcLogin, this.srcPassword, true, this.TrustServerCertificate);
        }

        /// <summary>Parses --split-tables into SplitRequests. Returns the error (or null) and the snapshot warning (or null). No connection is made.</summary>
        internal (string Error, string Warning) ValidateSplit()
        {
            if (!SplitRequest.TryParse(this.SplitTables, out List<SplitRequest> requests, out string error))
            {
                return (error, null);
            }

            this.SplitRequests = requests;

            if (requests.Count > 0 && this.IsQueryExport)
            {
                return ("--split-tables cannot be combined with --source-query: only table exports can be split.", null);
            }

            return (null, SplitRequest.SnapshotWarning(requests.Count > 0, this.Snapshot, "export"));
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

            if (!SqlConnectionFactory.ApplyRetries(this.Retries))
            {
                return false;
            }

            if (!FileCompression.IsValidMethod(this.Compress))
            {
                Program.PostToConsole("--compress must be none or gzip.", Program.InfoLevel.Error);
                return false;
            }

            if (FileCompression.ParseLevel(this.CompressionLevel) == null)
            {
                Program.PostToConsole("--compression-level must be fastest, optimal or smallest.", Program.InfoLevel.Error);
                return false;
            }

            if (this.Snapshot && string.IsNullOrEmpty(this.srcDatabase))
            {
                Program.PostToConsole("--snapshot needs --source-database (the database to snapshot).", Program.InfoLevel.Error);
                return false;
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
