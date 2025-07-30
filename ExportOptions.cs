using CommandLine;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;

namespace bcpJson
{
    [Verb("export", HelpText = "Export data from SQL Server to flat file.")]
    class ExportOptions
    {
        [Option("source-server", Required = false, Default = "localhost", HelpText = "Specifies the source instance of SQL Server to which to connect.")]
        public string srcServer { get; set; }

        [Option("source-database", Required = false, HelpText = "Specifies the source database of SQL Server which read from.")]
		public string srcDatabase { get; set; }

        [Option("source-login", Default = "", Required = false, HelpText = "Specifies the login ID used to connect to source SQL Server. Leave blank for Windows Auth.")]
        public string srcLogin { get; set; }

        [Option("source-password", Default = "", Required = false, HelpText = "Specifies the password for the source login ID. Leave blank for Windows Auth.")]
        public string srcPassword { get; set; }

        [Option("source-query", Required = false, HelpText = "Transact-SQL query/procedure. If the query returns multiple result sets, only the first result set is copied to the data file; subsequent result sets are ignored.")]
        public string Query { get; set; }

        [Option('k', Required = false, HelpText = "Specifies that empty columns should retain a null value during the operation, rather than omit the columns inserted. ")]
        public bool ExportNull { get; set; }

        [Option("include-tables", Separator = ',', Required = false, HelpText = "Comma separated list of schema.tablename to include in the export")]
        public IEnumerable<string> IncludeTablesList { get; set; }

        [Option("include-schemas", Separator = ',', Required = false, HelpText = "Comma separated list of table schemas to include in the export.")]
        public IEnumerable<string> IncludeSchemaList { get; set; }

        [Option("exclude-tables", Separator = ',', Required = false, HelpText = "Comma separated list of schema.tablename to exclude from the export")]
        public IEnumerable<string> ExcludeTablesList { get; set; }

        [Option("exclude-schemas", Separator = ',', Required = false, HelpText = "Comma separated list of table schemas to exclude from the export")]
        public IEnumerable<string> ExcludeSchemaList { get; set; }

        [Option("nolock", Default = false, Required = false, HelpText = "Add WITH (NOLOCK) to source table connections. Can cause dirty reads.")]
        public bool NoLock { get; set; }

        [Option("file-format", Required = true, Default = "bcp", HelpText = "Target file format.")]
        public string FileFormat { get; set; }

        [Option("export-path", Required = false, HelpText = "Path for data export.")]
        public string exportPath { get; set; }

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

        public bool? MultiExport
        {
            get
            {
                if (string.IsNullOrEmpty(this.Query) && !string.IsNullOrEmpty(this.srcDatabase))
                {
                    return true;
                }
                else if (!string.IsNullOrEmpty(this.Query) && string.IsNullOrEmpty(this.srcDatabase))
                {
                    return false;
                }
                else
                {
                    return null;
                }
            }
        }

        public bool Valid()
        {
            if (string.IsNullOrEmpty(this.Query) && string.IsNullOrEmpty(this.srcDatabase))
            {
                return false;
            }

            if (string.IsNullOrWhiteSpace(this.exportPath))
            {
                return false;
            }

            try
            {
                if (!Directory.Exists(this.exportPath))
                {
                    Directory.CreateDirectory(this.exportPath);
                }
            }
            catch
            {
                return false;
            }

            using (var srcconn = new SqlConnection(this.SourceConnectionString()))
            {
                try
                {
                    srcconn.Open();
                    srcconn.Close();
                }
                catch
                {
                    return false;
                }
            }

            return true;
        }
    }

}
