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

        private static ExportManifest Manifest(params ManifestTable[] tables) => new ExportManifest { Tables = tables.ToList() };

        private static ManifestTable Entry(string name, params string[] files) =>
            new ManifestTable { Name = name, Files = files.Select(f => new ManifestFile { Name = f }).ToList() };

        private static ManifestTable SplitEntry(string name, int partCount, int recorded)
        {
            ManifestTable entry = Entry(name, Enumerable.Range(1, recorded).Select(p => PartFiles.PartName(name, p) + ".json").ToArray());
            entry.Split = new ManifestSplit
            {
                PartCount = partCount,
                Parts = Enumerable.Range(1, recorded).Select(p => new ManifestPart { Part = p }).ToList()
            };
            return entry;
        }

        [Fact]
        public void Manifest_skips_files_it_does_not_list()
        {
            var files = new[] { "dbo.T.part0001.json", "dbo.T.part0002.json", "dbo.T.part0003.json", "dbo.T.json" };
            ImportSelection selection = PartFiles.SelectImportFiles(files, Manifest(SplitEntry("dbo.T", 2, 2)), "m.json");
            Assert.Equal(new[] { "dbo.T.part0001.json", "dbo.T.part0002.json" }, selection.Files);
            Assert.Empty(selection.Errors);
            Assert.Equal(new[]
            {
                "dbo.T: skipping dbo.T.part0003.json: not part of the export in m.json.",
                "dbo.T: skipping dbo.T.json: not part of the export in m.json."
            }, selection.Warnings);
        }

        [Fact]
        public void Manifest_refuses_an_incomplete_split()
        {
            var files = new[] { "dbo.T.part0001.json", "dbo.T.part0002.json" };
            ImportSelection selection = PartFiles.SelectImportFiles(files, Manifest(SplitEntry("dbo.T", 4, 2)), "m.json");
            Assert.Empty(selection.Files);
            Assert.Equal(new[] { "dbo.T: the export is incomplete (2 of 4 parts); finish it with export --resume." }, selection.Errors);
        }

        [Fact]
        public void Files_the_manifest_does_not_know_are_kept()
        {
            var files = new[] { "dbo.Hand.json", "dbo.T.json" };
            ImportSelection selection = PartFiles.SelectImportFiles(files, Manifest(Entry("dbo.T", "dbo.T.json", "dbo.T-c.fmt")), "m.json");
            Assert.Equal(files, selection.Files);
            Assert.Empty(selection.Warnings);
            Assert.Empty(selection.Errors);
        }

        [Fact]
        public void Mixed_whole_and_part_files_are_refused_without_a_manifest()
        {
            var files = new[] { "dbo.T.part0001.json", "dbo.T.json.gz", "dbo.U.json" };
            ImportSelection selection = PartFiles.SelectImportFiles(files, null, null);
            Assert.Equal(new[] { "dbo.T: both dbo.T.json.gz and part files are present; remove the files of the older export." }, selection.Errors);
            Assert.Empty(selection.Files);
        }

        [Fact]
        public void Manifest_resolves_a_mix_before_it_is_refused()
        {
            var files = new[] { "dbo.T.part0001.json", "dbo.T.part0002.json", "dbo.T.json" };
            ImportSelection selection = PartFiles.SelectImportFiles(files, Manifest(Entry("dbo.T", "dbo.T.json")), "m.json");
            Assert.Equal(new[] { "dbo.T.json" }, selection.Files);
            Assert.Empty(selection.Errors);
            Assert.Equal(2, selection.Warnings.Count);
        }

        [Fact]
        public void Unsplit_files_without_a_manifest_are_kept()
        {
            var files = new[] { "dbo.T.part0001.json", "dbo.T.part0002.json" };
            ImportSelection selection = PartFiles.SelectImportFiles(files, null, null);
            Assert.Equal(files, selection.Files);
            Assert.Empty(selection.Errors);
        }

        [Theory]
        [InlineData(new[] { "dbo.T.part0001.json" }, new[] { "dbo.T.json", "dbo.T.json.gz", "dbo.T.part0002.json", "dbo.T.part0001.json.partial", "dbo.T-c.fmt", "dbo.U.json", "dbo.T.bcp", "bcpjson-manifest.json" },
            new[] { "dbo.T.json", "dbo.T.json.gz", "dbo.T.part0002.json" })]
        [InlineData(new[] { "dbo.T.json" }, new[] { "dbo.T.json", "dbo.T.part0001.json.gz", "dbo.Tx.json" }, new[] { "dbo.T.part0001.json.gz" })]
        public void Leftovers_are_data_files_of_the_table_this_run_does_not_write(string[] written, string[] folder, string[] expected)
        {
            Assert.Equal(expected, PartFiles.Leftovers(folder, "dbo.T", ".json", written));
        }
    }
}
