using System.IO.Compression;
using Xunit;

namespace bcpJson.Tests
{
    public class CompressionTests : IDisposable
    {
        private readonly string folder = Path.Combine(Path.GetTempPath(), "bcpjson-tests", Guid.NewGuid().ToString("N"));

        public CompressionTests()
        {
            Directory.CreateDirectory(this.folder);
        }

        public void Dispose()
        {
            Directory.Delete(this.folder, true);
        }

        [Theory]
        [InlineData(null, true)]
        [InlineData("none", true)]
        [InlineData("GZIP", true)]
        [InlineData("zstd", false)]
        public void Compression_methods(string method, bool valid)
        {
            Assert.Equal(valid, FileCompression.IsValidMethod(method));
        }

        [Theory]
        [InlineData(null, CompressionLevel.Fastest)]
        [InlineData("fastest", CompressionLevel.Fastest)]
        [InlineData("Optimal", CompressionLevel.Optimal)]
        [InlineData("smallest", CompressionLevel.SmallestSize)]
        public void Compression_levels(string level, CompressionLevel expected)
        {
            Assert.Equal(expected, FileCompression.ParseLevel(level));
        }

        [Fact]
        public void Unknown_level_is_rejected()
        {
            Assert.Null(FileCompression.ParseLevel("max"));
        }

        [Theory]
        [InlineData("dbo.T.json", "dbo.T")]
        [InlineData("dbo.T.json.gz", "dbo.T")]
        [InlineData("dbo.T.bcp.GZ", "dbo.T")]
        [InlineData("my.schema.T.bcp", "my.schema.T")]
        public void Import_table_name_ignores_gz(string file, string table)
        {
            Assert.Equal(table, Program.ImportBaseName(file));
        }

        [Fact]
        public void Compressed_text_round_trips_as_utf8_without_bom()
        {
            string path = Path.Combine(this.folder, "dbo.T.json.gz");
            string text = "[{\"Name\":\"Zoë 東京 \\u0000\"}\r\n]";
            using (StreamWriter writer = FileCompression.CreateText(path, CompressionLevel.Fastest))
            {
                writer.Write(text);
            }

            using (var raw = new GZipStream(File.OpenRead(path), CompressionMode.Decompress))
            using (var bytes = new MemoryStream())
            {
                raw.CopyTo(bytes);
                Assert.NotEqual(0xEF, bytes.ToArray()[0]);
            }
            using (StreamReader reader = FileCompression.OpenText(path))
            {
                Assert.Equal(text, reader.ReadToEnd());
            }
        }

        [Fact]
        public void Plain_files_are_written_and_read_unchanged()
        {
            string path = Path.Combine(this.folder, "dbo.T.json");
            using (StreamWriter writer = FileCompression.CreateText(path, null))
            {
                writer.Write("[]");
            }
            Assert.Equal(new byte[] { (byte)'[', (byte)']' }, File.ReadAllBytes(path));
            using (StreamReader reader = FileCompression.OpenText(path))
            {
                Assert.Equal("[]", reader.ReadToEnd());
            }
        }

        [Fact]
        public void Files_compress_and_decompress_byte_for_byte()
        {
            string source = Path.Combine(this.folder, "dbo.T.bcp");
            byte[] data = Enumerable.Range(0, 300000).Select(i => (byte)(i % 7 == 0 ? '|' : 'a' + i % 26)).ToArray();
            File.WriteAllBytes(source, data);

            string gz = source + ".gz";
            FileCompression.CompressFile(source, gz, CompressionLevel.Fastest);
            Assert.True(new FileInfo(gz).Length < data.Length / 5);

            string back = Path.Combine(this.folder, "back.bcp");
            FileCompression.DecompressFile(gz, back);
            Assert.Equal(data, File.ReadAllBytes(back));
        }
    }
}
