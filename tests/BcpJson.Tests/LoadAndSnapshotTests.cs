using Microsoft.Data.SqlClient;
using Xunit;

namespace bcpJson.Tests
{
    public class LoadAndSnapshotTests
    {
        [Fact]
        public void Table_lock_is_on_by_default()
        {
            SqlBulkCopyOptions options = Program.LoadOptions(noTableLock: false);
            Assert.True(options.HasFlag(SqlBulkCopyOptions.TableLock));
            Assert.True(options.HasFlag(SqlBulkCopyOptions.KeepIdentity));
            Assert.True(options.HasFlag(SqlBulkCopyOptions.KeepNulls));
            Assert.Equal(" -h \"TABLOCK\"", Program.BcpLoadHints(noTableLock: false));
        }

        [Fact]
        public void No_table_lock_keeps_identity_and_nulls()
        {
            SqlBulkCopyOptions options = Program.LoadOptions(noTableLock: true);
            Assert.False(options.HasFlag(SqlBulkCopyOptions.TableLock));
            Assert.True(options.HasFlag(SqlBulkCopyOptions.KeepIdentity));
            Assert.True(options.HasFlag(SqlBulkCopyOptions.KeepNulls));
            Assert.Equal("", Program.BcpLoadHints(noTableLock: true));
        }

        [Fact]
        public void Snapshot_name_has_utc_timestamp()
        {
            Assert.Equal("Sales_bcpjson_20261120211530", Program.SnapshotName("Sales", new DateTime(2026, 11, 20, 21, 15, 30, DateTimeKind.Utc)));
        }

        [Fact]
        public void Snapshot_name_fits_in_sysname()
        {
            string name = Program.SnapshotName(new string('d', 128), DateTime.UtcNow);
            Assert.Equal(128, name.Length);
            Assert.Contains("_bcpjson_", name);
        }

        [Theory]
        [InlineData(@"C:\Data\Sales.mdf", @"C:\Data\Sales_snap_Sales_Data.ss")]
        [InlineData("/var/opt/mssql/data/Sales.mdf", "/var/opt/mssql/data/Sales_snap_Sales_Data.ss")]
        [InlineData("Sales.mdf", "Sales_snap_Sales_Data.ss")]
        public void Snapshot_file_sits_next_to_the_source_file(string physical, string expected)
        {
            Assert.Equal(expected, Program.SnapshotFilePath(physical, "Sales_snap", "Sales_Data"));
        }

        [Fact]
        public void Snapshot_file_name_replaces_unsafe_characters()
        {
            Assert.Equal("/data/My_DB_snap_log_cal_name_.ss", Program.SnapshotFilePath("/data/x.mdf", "My DB_snap", "log/cal:name*"));
        }

        [Fact]
        public void Create_snapshot_sql_quotes_names_and_lists_every_data_file()
        {
            string sql = Program.BuildCreateSnapshotSql("O'Brien]DB", "snap", new[]
            {
                new Program.DataFile("primary", @"D:\db\a.mdf"),
                new Program.DataFile("second'ary", @"E:\db\b.ndf")
            });

            Assert.Equal(
                "CREATE DATABASE [snap] ON " +
                @"(NAME = [primary], FILENAME = N'D:\db\snap_primary.ss'), " +
                @"(NAME = [second'ary], FILENAME = N'E:\db\snap_second_ary.ss') " +
                "AS SNAPSHOT OF [O'Brien]]DB];",
                sql);
        }
    }
}
