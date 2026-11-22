using Xunit;

namespace bcpJson.Tests
{
    public sealed class BcpFormatFileTests : IDisposable
    {
        private readonly string folder = Path.Combine(Path.GetTempPath(), "bcpjson-tests", Guid.NewGuid().ToString("N"));

        public BcpFormatFileTests()
        {
            Directory.CreateDirectory(folder);
        }

        public void Dispose()
        {
            Directory.Delete(folder, true);
        }

        [Fact]
        public void Prefers_non_xml_format_file()
        {
            File.WriteAllText(Path.Combine(folder, "dbo.T-c.fmt"), "");
            File.WriteAllText(Path.Combine(folder, "dbo.T-c.xml"), "");
            Assert.Equal("dbo.T-c.fmt", Program.FindBcpFormatFile(folder, "dbo.T"));
        }

        [Fact]
        public void Falls_back_to_legacy_xml_format_file()
        {
            File.WriteAllText(Path.Combine(folder, "dbo.T-c.xml"), "");
            Assert.Equal("dbo.T-c.xml", Program.FindBcpFormatFile(folder, "dbo.T"));
        }

        private static string BcpIn(string schema, string table, bool noTableLock = false, int batchSize = 10000)
        {
            return Program.BcpInArguments(schema, table, "x.bcp", "x-c.fmt", "-S \"srv\" -d \"db\" -T", noTableLock, "x_import_error.log", batchSize);
        }

        [Fact]
        public void Bcp_in_batch_size_follows_the_setting()
        {
            Assert.Contains(" -q -b 50000 -h ", BcpIn("dbo", "T", batchSize: 50000));
            // 0: no -b, so bcp loads the whole file in one transaction.
            string args = BcpIn("dbo", "T", batchSize: 0);
            Assert.DoesNotContain(" -b ", args);
            Assert.Contains(" -E -k -q -h \"TABLOCK\" -e ", args);
        }

        [Theory]
        [InlineData("dbo", "T", "\"dbo.T\"")]
        [InlineData("plain", "Numbers", "\"plain.Numbers\"")]
        [InlineData("my sch", "odd ] t'x", "\"my sch.odd ] t'x\"")]
        public void Bcp_in_uses_quoted_identifiers_with_an_unbracketed_name(string schema, string table, string expectedTable)
        {
            string args = BcpIn(schema, table);
            // bcp requires the table first and the direction second.
            Assert.StartsWith(expectedTable + " in \"x.bcp\" -f \"x-c.fmt\" ", args);
            Assert.Contains(" -E -k -q -b 10000 -h \"TABLOCK\" -e \"x_import_error.log\"", args);
        }

        [Theory]
        [InlineData("dbo", "my.table", "\"[dbo].[my.table]\"")]
        [InlineData("s.x", "T", "\"[s.x].[T]\"")]
        [InlineData("dbo", "a\"b", "\"[dbo].[a\"b]\"")]
        public void Bcp_in_falls_back_to_brackets_without_q_for_names_bcp_cannot_split(string schema, string table, string expectedTable)
        {
            string args = BcpIn(schema, table);
            Assert.StartsWith(expectedTable + " in ", args);
            Assert.DoesNotContain(" -q", args);
        }

        [Fact]
        public void Bcp_in_without_table_lock_has_no_hint()
        {
            string args = BcpIn("dbo", "T", noTableLock: true);
            Assert.DoesNotContain("TABLOCK", args);
            Assert.Contains(" -E -k -q -b 10000 -e ", args);
        }

        [Fact]
        public void Returns_null_when_missing()
        {
            Assert.Null(Program.FindBcpFormatFile(folder, "dbo.T"));
        }
    }
}
