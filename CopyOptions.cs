using CommandLine;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;

namespace bcpJson
{
    [Verb("copy", HelpText = "Copy data from one SQL Server table to another.")]
    class CopyOptions
    {
        [Option("source-server", Required = false, Default = "localhost", HelpText = "Specifies the source instance of SQL Server to which to connect.")]
        public string srcServer { get; set; }

        [Option("source-database", Required = false, HelpText = "Specifies the source database of SQL Server which read from.")]
        public string srcDatabase { get; set; }

        [Option("source-login", Default = "", Required = false, HelpText = "Specifies the login ID used to connect to source SQL Server. Leave blank for Windows Auth.")]
        public string srcLogin { get; set; }

        [Option("source-password ", Default = "", Required = false, HelpText = "Specifies the password for the source login ID. Leave blank for Windows Auth.")]
        public string srcPassword { get; set; }

        [Option("target-server", Required = false, HelpText = "Specifies the target instance of SQL Server to which to connect.")]
        public string TargetSQLServer { get; set; }

        [Option("target-database", Required = false, HelpText = "Specifies the target database of SQL Server which read from.")]
        public string TargetSQLDatabase { get; set; }

        [Option("target-login", Default = "", Required = false, HelpText = "Specifies the login ID used to connect to target SQL Server. Leave blank for Windows Auth.")]
        public string TargetSQLLogin { get; set; }

        [Option("target-password ", Default = "", Required = false, HelpText = "Specifies the password for the target login ID. Leave blank for Windows Auth.")]
        public string TargetSQLPassword { get; set; }

        [Option("source-query", Required = false, HelpText = "Transact-SQL query/procedure. If the query returns multiple result sets, only the first result set is copied to the data file; subsequent result sets are ignored.")]
        public string Query { get; set; }

        [Option("include-tables", Separator = ',', Required = false, HelpText = "Comma separated list of schema.tablename to include in the export")]
        public IEnumerable<string> IncludeTablesList { get; set; }

        [Option("include-schemas", Separator = ',', Required = false, HelpText = "Comma separated list of schemas to include in the export.")]
        public IEnumerable<string> IncludeSchemaList { get; set; }

        [Option("exclude-tables", Separator = ',', Required = false, HelpText = "Comma separated list of schema.tablename to exclude from the export")]
        public IEnumerable<string> ExcludeTablesList { get; set; }

        [Option("exclude-schemas", Separator = ',', Required = false, HelpText = "Comma separated list of schemas to exclude from the export")]
        public IEnumerable<string> ExcludeSchemaList { get; set; }

        [Option("nolock", Default = false, Required = false, HelpText = "Add WITH (NOLOCK) to source table connections. Can cause dirty reads.")]
        public bool NoLock { get; set; }

        [Option("drop-target", Default = false, Required = false, HelpText = "Drop target table if it exists.")]
        public bool DropTarget { get; set; }

        [Option("truncate-target", Default = false, Required = false, HelpText = "Truncate target table.")]
        public bool TruncateTarget { get; set; }

        [Option("max-tasks", Default = -1, Required = false, HelpText = "Maximum parallel tasks.")]
        public int MaxParallelTasks { get; set; }

        [Option("log-file", Required = false, HelpText = "Name of output log file.")]
        public string outputLogFile { get; set; }
        public string SourceConnectionString()
        {
            if (this.srcLogin == "" && this.srcPassword == "")
            {
                return string.Format("Data Source={0};Initial Catalog={1};Integrated Security=True;applicationintent=ReadOnly;", this.srcServer, string.IsNullOrEmpty(this.srcDatabase) ? "master" : this.srcDatabase);
            }
            else
            {
                return string.Format("Data Source={0};User ID={1};Password={2};Initial Catalog={3};applicationintent=ReadOnly;", this.srcServer, this.srcLogin, this.srcPassword, string.IsNullOrEmpty(this.srcDatabase) ? "master" : this.srcDatabase);
            }
        }
        public string TargetConnectionString()
        {
            if (this.TargetSQLLogin == "" && this.TargetSQLPassword == "")
            {
                return string.Format("Data Source={0};Initial Catalog={1};Integrated Security=True;applicationintent=ReadWrite;", this.TargetSQLServer, string.IsNullOrEmpty(this.TargetSQLDatabase) ? "master" : this.TargetSQLDatabase);
            }
            else
            {
                return string.Format("Data Source={0};User ID={1};Password={2};Initial Catalog={3};applicationintent=ReadWrite;", this.TargetSQLServer, this.TargetSQLLogin, this.TargetSQLPassword, string.IsNullOrEmpty(this.TargetSQLDatabase) ? "master" : this.TargetSQLDatabase);
            }
        }
    }
}
