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
                TrustServerCertificate = trustServerCertificate,
                // Without this, a failed open makes the pool rethrow the cached error for up to a minute without
                // contacting the server, so connection retries could never succeed.
                PoolBlockingPeriod = PoolBlockingPeriod.NeverBlock
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

        /// <summary>Retries for transient connection failures (--retries). Set once per run, before any connection opens.</summary>
        internal static int Retries = DefaultRetries;

        internal const int DefaultRetries = 3;

        /// <summary>Validates --retries (0-59) and applies it to every connection created afterwards.</summary>
        internal static bool ApplyRetries(int retries)
        {
            if (retries < 0 || retries > 59)
            {
                Program.PostToConsole("--retries must be between 0 and 59.", Program.InfoLevel.Error);
                return false;
            }
            Retries = retries;
            return true;
        }

        /// <summary>
        /// A connection whose Open() retries transient errors (SqlClient's default list: network errors, deadlock
        /// victims, Azure SQL throttling/failover, "cannot open database" while a database comes online) with
        /// exponential backoff. Commands are not retried: a load or DDL statement is not safe to replay.
        /// </summary>
        public static SqlConnection Create(string connectionString)
        {
            return new SqlConnection(connectionString) { RetryLogicProvider = CreateRetryProvider(Retries) };
        }

        /// <summary>Exponential backoff (about 1, 2, 4, 8 ... s, at most 30 s) for <paramref name="retries"/> retries; none for 0.</summary>
        internal static SqlRetryLogicBaseProvider CreateRetryProvider(int retries)
        {
            if (retries <= 0)
            {
                return SqlConfigurableRetryFactory.CreateNoneRetryProvider();
            }

            SqlRetryLogicBaseProvider provider = SqlConfigurableRetryFactory.CreateExponentialRetryProvider(new SqlRetryLogicOption
            {
                NumberOfTries = Math.Min(retries + 1, 60),
                DeltaTime = TimeSpan.FromSeconds(1),
                MaxTimeInterval = TimeSpan.FromSeconds(30)
            });
            provider.Retrying += (sender, e) =>
            {
                Exception last = e.Exceptions.Count > 0 ? e.Exceptions[e.Exceptions.Count - 1] : null;
                Program.PostToConsole(string.Format("Connection failed ({0}); retry {1} of {2} in {3:0.#} s.",
                    last?.Message.Trim(), e.RetryCount, retries, e.Delay.TotalSeconds), Program.InfoLevel.Warning);
            };
            return provider;
        }

        public static bool TryOpen(string connectionString, string description)
        {
            try
            {
                using (var conn = Create(connectionString))
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
