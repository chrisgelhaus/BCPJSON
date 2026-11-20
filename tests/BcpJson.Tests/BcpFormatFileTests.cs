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

        [Fact]
        public void Returns_null_when_missing()
        {
            Assert.Null(Program.FindBcpFormatFile(folder, "dbo.T"));
        }
    }
}
