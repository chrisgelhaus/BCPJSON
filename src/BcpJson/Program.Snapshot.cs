using Microsoft.Data.SqlClient;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Text.RegularExpressions;

namespace bcpJson
{
    partial class Program
    {
        private static readonly Regex UnsafeFileNameChars = new Regex(@"[^A-Za-z0-9_.-]", RegexOptions.Compiled);

        /// <summary>A data file of the source database: logical name and server-side physical path.</summary>
        internal sealed record DataFile(string LogicalName, string PhysicalName);

        /// <summary>
        /// A database snapshot used as a point-in-time source for export/copy (--snapshot), so tables read late in a
        /// long run are consistent with tables read at the start. Dispose drops the snapshot.
        /// </summary>
        internal sealed class DatabaseSnapshot : IDisposable
        {
            private readonly string adminConnectionString;

            public DatabaseSnapshot(string adminConnectionString, string name, string sourceDatabase)
            {
                this.adminConnectionString = adminConnectionString;
                Name = name;
                SourceDatabase = sourceDatabase;
            }

            public string Name { get; }

            public string SourceDatabase { get; }

            public void Dispose()
            {
                // Pooled connections (and SMO) may still hold the snapshot open; DROP DATABASE would fail.
                SqlConnection.ClearAllPools();
                try
                {
                    using (var conn = SqlConnectionFactory.Create(adminConnectionString))
                    {
                        conn.Open();
                        ExecuteSql(conn, "DROP DATABASE " + QuoteName(Name) + ";");
                    }
                    PostToConsole(string.Format("Snapshot {0} dropped.", Name));
                }
                catch (Exception ex)
                {
                    PostToConsole(string.Format("Could not drop snapshot {0}: {1} Drop it manually (DROP DATABASE {2};).", Name, ex.Message, QuoteName(Name)), InfoLevel.Error);
                }
            }
        }

        /// <summary>Snapshot database name: &lt;source&gt;_bcpjson_&lt;UTC timestamp&gt;, truncated to 128 characters.</summary>
        internal static string SnapshotName(string sourceDatabase, DateTime utcNow)
        {
            string suffix = "_bcpjson_" + utcNow.ToString("yyyyMMddHHmmss", CultureInfo.InvariantCulture);
            int room = 128 - suffix.Length;
            return (sourceDatabase.Length > room ? sourceDatabase.Substring(0, room) : sourceDatabase) + suffix;
        }

        /// <summary>
        /// Sparse file path for one snapshot data file: next to the source file (same folder, so the server can
        /// write there), named after the snapshot and logical file with unsafe characters replaced.
        /// Handles both Windows and Linux server paths.
        /// </summary>
        internal static string SnapshotFilePath(string sourcePhysicalName, string snapshotName, string logicalName)
        {
            int separator = Math.Max(sourcePhysicalName.LastIndexOf('/'), sourcePhysicalName.LastIndexOf('\\'));
            string folder = separator >= 0 ? sourcePhysicalName.Substring(0, separator + 1) : string.Empty;
            return folder + UnsafeFileNameChars.Replace(snapshotName + "_" + logicalName, "_") + ".ss";
        }

        /// <summary>CREATE DATABASE ... AS SNAPSHOT OF statement covering every data file of the source.</summary>
        internal static string BuildCreateSnapshotSql(string sourceDatabase, string snapshotName, IEnumerable<DataFile> dataFiles)
        {
            string files = string.Join(", ", dataFiles.Select(f =>
                "(NAME = " + QuoteName(f.LogicalName) + ", FILENAME = " + SqlLiteral(SnapshotFilePath(f.PhysicalName, snapshotName, f.LogicalName)) + ")"));
            return "CREATE DATABASE " + QuoteName(snapshotName) + " ON " + files + " AS SNAPSHOT OF " + QuoteName(sourceDatabase) + ";";
        }

        /// <summary>
        /// Creates a database snapshot of <paramref name="database"/>. Returns null (after reporting an error) when it
        /// cannot be created, e.g. missing CREATE DATABASE permission, Azure SQL Database, or memory-optimized data.
        /// </summary>
        internal static DatabaseSnapshot CreateSnapshot(string server, string database, string login, string password, bool trustServerCertificate)
        {
            string adminConnectionString = SqlConnectionFactory.Build(server, "master", login, password, false, trustServerCertificate);
            string name = SnapshotName(database, DateTime.UtcNow);

            try
            {
                using (var conn = SqlConnectionFactory.Create(adminConnectionString))
                {
                    conn.Open();

                    var dataFiles = new List<DataFile>();
                    string filesSql = "SELECT name, physical_name FROM " + QuoteName(database) + ".sys.database_files WHERE type = 0 ORDER BY file_id;";
                    using (var cmd = new SqlCommand(filesSql, conn))
                    using (var reader = cmd.ExecuteReader(CommandBehavior.SingleResult))
                    {
                        while (reader.Read())
                        {
                            dataFiles.Add(new DataFile(reader.GetString(0), reader.GetString(1)));
                        }
                    }

                    if (dataFiles.Count == 0)
                    {
                        throw new InvalidOperationException("No data files found for the source database.");
                    }

                    ExecuteSql(conn, BuildCreateSnapshotSql(database, name, dataFiles));
                }

                PostToConsole(string.Format("Snapshot {0} of {1} created at {2:yyyy-MM-dd HH:mm:ss} UTC; all tables are read as of this moment.", name, database, DateTime.UtcNow), InfoLevel.Success);
                return new DatabaseSnapshot(adminConnectionString, name, database);
            }
            catch (Exception ex)
            {
                PostToConsole(string.Format("Could not create a database snapshot of {0}: {1}", database, ex.Message), InfoLevel.Error);
                return null;
            }
        }
    }
}
