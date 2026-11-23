using Xunit;

namespace bcpJson.Tests
{
    public class PartFilesTests
    {
        [Theory]
        [InlineData("dbo.T.part0001.json", "dbo.T", 1)]
        [InlineData("dbo.T.part0200.json.gz", "dbo.T", 200)]
        [InlineData("dbo.T.part0003.bcp", "dbo.T", 3)]
        [InlineData("dbo.T.json", "dbo.T", 0)]
        [InlineData("dbo.T.partial.json", "dbo.T.partial", 0)]   // not four digits: an ordinary name
        [InlineData("dbo.T.part01.json", "dbo.T.part01", 0)]
        public void Part_names(string file, string table, int part)
        {
            Assert.Equal(table, PartFiles.TableNameOf(file));
            Assert.Equal(table, Program.ImportBaseName(file));
            Assert.Equal(part, PartFiles.PartNumberOf(file));
        }

        [Fact] public void PartName_pads_to_four_digits() => Assert.Equal("dbo.T.part0007", PartFiles.PartName("dbo.T", 7));

        [Fact]
        public void Groups_parts_by_table_in_part_order()
        {
            var groups = PartFiles.GroupImportFiles(new[] { "dbo.B.json", "dbo.A.part0002.json", "dbo.A.part0010.json", "dbo.A.part0001.json" }, null);
            Assert.Equal(new[] { "dbo.A", "dbo.B" }, groups.Select(g => g.TableName).OrderBy(n => n, StringComparer.Ordinal));
            Assert.Equal(new[] { "dbo.A.part0001.json", "dbo.A.part0002.json", "dbo.A.part0010.json" }, groups.Single(g => g.TableName == "dbo.A").Files);
        }

        [Fact]
        public void Target_table_puts_every_file_in_one_group()
        {
            var group = Assert.Single(PartFiles.GroupImportFiles(new[] { "x.part0002.json", "x.part0001.json" }, "dbo.Target"));
            Assert.Equal("dbo.Target", group.TableName);
            Assert.Equal(new[] { "x.part0001.json", "x.part0002.json" }, group.Files);
        }
    }
}
