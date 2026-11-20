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

        [Theory]
        [InlineData("dbo", "T", "\"dbo.T\" -q")]
        [InlineData("plain", "Numbers", "\"plain.Numbers\" -q")]
        [InlineData("my sch", "odd ] t'x", "\"my sch.odd ] t'x\" -q")]
        public void Bcp_in_uses_quoted_identifiers_with_an_unbracketed_name(string schema, string table, string expected)
        {
            Assert.Equal(expected, Program.BcpInTarget(schema, table));
        }

        [Theory]
        [InlineData("dbo", "my.table", "\"[dbo].[my.table]\"")]
        [InlineData("s.x", "T", "\"[s.x].[T]\"")]
        [InlineData("dbo", "a\"b", "\"[dbo].[a\"b]\"")]
        public void Bcp_in_falls_back_to_brackets_without_q_for_names_bcp_cannot_split(string schema, string table, string expected)
        {
            Assert.Equal(expected, Program.BcpInTarget(schema, table));
        }

        [Fact]
        public void Returns_null_when_missing()
        {
            Assert.Null(Program.FindBcpFormatFile(folder, "dbo.T"));
        }
    }
}
