using CommandLine;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;


namespace bcpJson
{
    [Verb("import", HelpText = "Import data into SQL Server from flat file.")]
    class ImportOptions
    {
        [Option("file-format", Required = true, HelpText = "Source file format.")]
        public string FileFormat { get; set; }

        [Option("source-path", Required = false, HelpText = "Source file location.")]
        public string SourcePath { get; set; }

        [Option("source-file", Required = true, HelpText = "Source file to import.")]
        public string SourceFile { get; set; }

        [Option("target-server", Required = false, Default = "localhost", HelpText = "Specifies the target instance of SQL Server to which to connect.")]
        public string TargetSQLServer { get; set; }

        [Option("target-database", Required = false, HelpText = "Specifies the target database of SQL Server which read from.")]
        public string TargetSQLDatabase { get; set; }

        [Option("target-login", Default = "", Required = false, HelpText = "Specifies the login ID used to connect to target SQL Server. Leave blank for Windows Auth.")]
        public string TargetSQLLogin { get; set; }

        [Option("target-password ", Default = "", Required = false, HelpText = "Specifies the password for the target login ID. Leave blank for Windows Auth.")]
        public string TargetSQLPassword { get; set; }

        [Option("target-table", Required = false, HelpText = "Target table name.")]
        public string TargetTable { get; set; }

        [Option("truncate-target", Required = false, HelpText = "Truncate target table before data load.")]
        public bool TruncateTarget { get; set; }

        [Option("log-file", Required = false, HelpText = "Name of output log file.")]
        public string LogFile { get; set; }

        public string GetTargetConnectionString()
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

        public bool Valid()
        {
            //if (string.IsNullOrEmpty(this.Query) && string.IsNullOrEmpty(this.srcDatabase))
            //{
            //    return false;
            //}

            //TODO: PATH

            using (var tgtconn = new SqlConnection(this.GetTargetConnectionString()))
            {
                try
                {
                    tgtconn.Open();
                    tgtconn.Close();
                }
                catch (Exception)
                {
                    return false;
                }
            }

            return true;
        }
    }

}
