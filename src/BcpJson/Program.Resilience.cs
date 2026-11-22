using Microsoft.Data.SqlClient;
using System.Collections.Generic;
using System.IO;
using System.Threading;

namespace bcpJson
{
    partial class Program
    {
        /// <summary>
        /// SQL Server error numbers treated as transient for whole-table export retries. Same list as SqlClient's
        /// configurable retry logic default (network errors, deadlock/lock timeouts, Azure SQL throttling and
        /// failover, "cannot open database").
        /// </summary>
        private static readonly HashSet<int> TransientSqlErrors = new HashSet<int>
        {
            1204, 1205, 1222, 49918, 49919, 49920, 4060, 4221, 40143, 40613, 40501, 40540, 40197,
            10929, 10928, 10060, 10054, 10053, 997, 233, 64, 20, 0
        };

        internal static bool IsTransientSqlErrorNumber(int number)
        {
            return TransientSqlErrors.Contains(number);
        }

        /// <summary>True for a SqlException (anywhere in the inner-exception chain) carrying a transient error number.</summary>
        internal static bool IsTransientSqlError(Exception ex)
        {
            for (Exception e = ex; e != null; e = e.InnerException)
            {
                if (e is SqlException sql)
                {
                    foreach (SqlError error in sql.Errors)
                    {
                        if (IsTransientSqlErrorNumber(error.Number))
                        {
                            return true;
                        }
                    }
                    return IsTransientSqlErrorNumber(sql.Number);
                }
            }
            return false;
        }

        /// <summary>Wait before retry number <paramref name="attempt"/> (0-based): 2, 4, 8, 16, then 30 s.</summary>
        internal static TimeSpan RetryDelay(int attempt)
        {
            double seconds = Math.Min(30, 2 * Math.Pow(2, Math.Min(attempt, 10)));
            return TimeSpan.FromSeconds(seconds);
        }

        /// <summary>
        /// Runs <paramref name="action"/>, retrying up to <paramref name="retries"/> times when it throws an exception
        /// <paramref name="isTransient"/> accepts. Only for idempotent work (exporting a table rewrites its file).
        /// </summary>
        internal static T WithRetries<T>(string label, int retries, Func<T> action, Func<Exception, bool> isTransient, Action<TimeSpan> sleep = null)
        {
            sleep ??= Thread.Sleep;
            for (int attempt = 0; ; attempt++)
            {
                try
                {
                    return action();
                }
                catch (Exception ex) when (attempt < retries && isTransient(ex))
                {
                    TimeSpan wait = RetryDelay(attempt);
                    PostToConsole(string.Format("{0}: {1} Retry {2} of {3} in {4:0} s.", label, ex.Message.Trim(), attempt + 1, retries, wait.TotalSeconds), InfoLevel.Warning);
                    sleep(wait);
                }
            }
        }

        /// <summary>Thrown when bcp exits with an error; bcp gives no error numbers, so every bcp failure is retried.</summary>
        internal sealed class BcpFailedException : Exception
        {
            public BcpFailedException(string message) : base(message) { }
        }

        /// <summary>
        /// Exports are written to "&lt;file&gt;.partial" and renamed when complete, so a crash or a failed table never
        /// leaves a truncated file under the real name. Imports skip ".partial" files.
        /// </summary>
        internal const string PartialSuffix = ".partial";

        internal static string PartialPath(string path)
        {
            return path + PartialSuffix;
        }

        /// <summary>Renames "&lt;path&gt;.partial" to <paramref name="path"/>, replacing an older file.</summary>
        internal static void CommitPartial(string path)
        {
            File.Move(PartialPath(path), path, true);
        }

        /// <summary>Deletes "&lt;path&gt;.partial" if it exists (after a failure).</summary>
        internal static void DiscardPartial(string path)
        {
            try
            {
                File.Delete(PartialPath(path));
            }
            catch (IOException)
            {
                // Best effort: a leftover .partial file is ignored by import and overwritten by the next export.
            }
            catch (UnauthorizedAccessException)
            {
            }
        }
    }
}
