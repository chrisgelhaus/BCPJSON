using Microsoft.Data.SqlClient;
using Xunit;

namespace bcpJson.Tests
{
    public class ResilienceTests : IDisposable
    {
        private readonly string folder = Path.Combine(Path.GetTempPath(), "bcpjson-tests", Guid.NewGuid().ToString("N"));

        public ResilienceTests()
        {
            Directory.CreateDirectory(this.folder);
        }

        public void Dispose()
        {
            Directory.Delete(this.folder, true);
        }

        private sealed class Transient : Exception
        {
            public Transient() : base("blip") { }
        }

        [Fact]
        public void Transient_failures_are_retried_until_success()
        {
            int calls = 0;
            var waits = new List<TimeSpan>();
            int result = Program.WithRetries("t", 3, () =>
            {
                if (++calls < 3)
                {
                    throw new Transient();
                }
                return 42;
            }, ex => ex is Transient, waits.Add);

            Assert.Equal(42, result);
            Assert.Equal(3, calls);
            Assert.Equal(new[] { TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(4) }, waits);
        }

        [Fact]
        public void Retries_give_up_after_the_limit()
        {
            int calls = 0;
            Assert.Throws<Transient>(() => Program.WithRetries<int>("t", 2, () => { calls++; throw new Transient(); }, ex => ex is Transient, _ => { }));
            Assert.Equal(3, calls);
        }

        [Fact]
        public void Other_failures_are_not_retried()
        {
            int calls = 0;
            Assert.Throws<InvalidOperationException>(() => Program.WithRetries<int>("t", 5, () => { calls++; throw new InvalidOperationException(); }, ex => ex is Transient, _ => { }));
            Assert.Equal(1, calls);
        }

        [Fact]
        public void Zero_retries_runs_once()
        {
            int calls = 0;
            Assert.Throws<Transient>(() => Program.WithRetries<int>("t", 0, () => { calls++; throw new Transient(); }, ex => ex is Transient, _ => { }));
            Assert.Equal(1, calls);
        }

        [Fact]
        public void Backoff_doubles_up_to_thirty_seconds()
        {
            Assert.Equal(new[] { 2.0, 4, 8, 16, 30, 30 }, Enumerable.Range(0, 6).Select(a => Program.RetryDelay(a).TotalSeconds));
            Assert.Equal(30, Program.RetryDelay(1000).TotalSeconds);
        }

        [Theory]
        [InlineData(1205, true)]   // deadlock victim
        [InlineData(4060, true)]   // cannot open database
        [InlineData(40613, true)]  // Azure SQL database unavailable
        [InlineData(10054, true)]  // connection reset
        [InlineData(208, false)]   // invalid object name
        [InlineData(2627, false)]  // duplicate key
        [InlineData(18456, false)] // login failed
        public void Transient_error_numbers(int number, bool transient)
        {
            Assert.Equal(transient, Program.IsTransientSqlErrorNumber(number));
        }

        [Fact]
        public void Non_sql_exceptions_are_not_transient_sql_errors()
        {
            Assert.False(Program.IsTransientSqlError(new IOException("disk")));
            Assert.False(Program.IsTransientSqlError(new InvalidOperationException("x", new TimeoutException())));
        }

        [Fact]
        public void Retry_provider_matches_the_setting()
        {
            Assert.Equal(1, SqlConnectionFactory.CreateRetryProvider(0).RetryLogic?.NumberOfTries ?? 1);
            Assert.Equal(4, SqlConnectionFactory.CreateRetryProvider(3).RetryLogic.NumberOfTries);
            Assert.Equal(60, SqlConnectionFactory.CreateRetryProvider(59).RetryLogic.NumberOfTries);
        }

        [Fact]
        public void Failed_opens_are_not_cached_by_the_pool()
        {
            var builder = new SqlConnectionStringBuilder(SqlConnectionFactory.Build("srv", "db", "", "", false, false));
            Assert.Equal(PoolBlockingPeriod.NeverBlock, builder.PoolBlockingPeriod);
        }

        [Fact]
        public void Connections_get_a_retry_provider()
        {
            using (SqlConnection conn = SqlConnectionFactory.Create("Server=localhost"))
            {
                Assert.NotNull(conn.RetryLogicProvider);
            }
        }

        [Fact]
        public void Partial_file_replaces_the_target_only_on_commit()
        {
            string path = Path.Combine(this.folder, "dbo.T.json");
            File.WriteAllText(path, "old");
            File.WriteAllText(Program.PartialPath(path), "new");
            Assert.Equal("old", File.ReadAllText(path));

            Program.CommitPartial(path);
            Assert.Equal("new", File.ReadAllText(path));
            Assert.False(File.Exists(Program.PartialPath(path)));
        }

        [Fact]
        public void Discarding_a_partial_file_keeps_the_target()
        {
            string path = Path.Combine(this.folder, "dbo.T.bcp");
            File.WriteAllText(path, "complete");
            File.WriteAllText(Program.PartialPath(path), "trunc");
            Program.DiscardPartial(path);
            Program.DiscardPartial(path); // no file: no error
            Assert.Equal("complete", File.ReadAllText(path));
            Assert.False(File.Exists(Program.PartialPath(path)));
        }

        [Theory]
        [InlineData("dbo.T.json", true)]
        [InlineData("dbo.T.bcp", true)]
        [InlineData("bcpjson-manifest.json", false)]
        [InlineData("BCPJSON-MANIFEST.JSON", false)]
        [InlineData("dbo.T.json.partial", false)]
        public void Import_skips_manifest_and_partial_files(string name, bool importable)
        {
            Assert.Equal(importable, Program.IsImportableFile(name));
        }
    }
}
