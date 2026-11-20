using Microsoft.Data.SqlClient;

namespace bcpJson
{
    /// <summary>
    /// Builds connection strings for the source/target options classes and tests connectivity.
    /// Uses SqlConnectionStringBuilder so that values containing ';' or quotes are escaped correctly.
    /// </summary>
    static class SqlConnectionFactory
    {
        public static string Build(string server, string database, string login, string password, bool readOnly, bool trustServerCertificate)
        {
            var builder = new SqlConnectionStringBuilder
            {
                DataSource = string.IsNullOrEmpty(server) ? "localhost" : server,
                InitialCatalog = string.IsNullOrEmpty(database) ? "master" : database,
                ApplicationIntent = readOnly ? ApplicationIntent.ReadOnly : ApplicationIntent.ReadWrite,
                ApplicationName = "bcpJson",
                TrustServerCertificate = trustServerCertificate
            };

            if (string.IsNullOrEmpty(login))
            {
                builder.IntegratedSecurity = true;
            }
            else
            {
                builder.UserID = login;
                builder.Password = password ?? string.Empty;
            }

            return builder.ConnectionString;
        }

        public static bool TryOpen(string connectionString, string description)
        {
            try
            {
                using (var conn = new SqlConnection(connectionString))
                {
                    conn.Open();
                }
                return true;
            }
            catch (Exception ex)
            {
                Program.PostToConsole(string.Format("Cannot connect to {0}: {1}", description, ex.Message), Program.InfoLevel.Error);
                return false;
            }
        }
    }
}
