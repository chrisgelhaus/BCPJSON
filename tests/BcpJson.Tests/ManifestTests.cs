using Xunit;

namespace bcpJson.Tests
{
    public class ManifestTests : IDisposable
    {
        private readonly string folder = Path.Combine(Path.GetTempPath(), "bcpjson-tests", Guid.NewGuid().ToString("N"));

        public ManifestTests()
        {
            Directory.CreateDirectory(this.folder);
        }

        public void Dispose()
        {
            Directory.Delete(this.folder, true);
        }

        private ManifestTable Table(string name, long rows, params (string File, string Content)[] files)
        {
            var table = new ManifestTable { Name = name, Rows = rows, Exported = DateTime.UtcNow };
            foreach (var (file, content) in files)
            {
                File.WriteAllText(Path.Combine(this.folder, file), content);
                table.Files.Add(ManifestFile.Describe(this.folder, file));
            }
            return table;
        }

        private ExportManifest Manifest()
        {
            var manifest = new ExportManifest
            {
                Tool = ExportManifest.ToolVersion(),
                Format = "json",
                Server = "srv",
                Database = "Sales",
                Started = new DateTime(2026, 11, 21, 12, 0, 0, DateTimeKind.Utc)
            };
            manifest.Record(Table("dbo.A", 2, ("dbo.A.json", "[{\"Id\":1}\n,{\"Id\":2}\n]")), this.folder);
            manifest.Record(Table("dbo.B", 0, ("dbo.B.json", "[]")), this.folder);
            manifest.Completed = new DateTime(2026, 11, 21, 12, 5, 0, DateTimeKind.Utc);
            manifest.Save(this.folder);
            return manifest;
        }

        [Fact]
        public void Describe_records_size_and_sha256()
        {
            File.WriteAllText(Path.Combine(this.folder, "x.json"), "abc");
            ManifestFile file = ManifestFile.Describe(this.folder, "x.json");
            Assert.Equal(3, file.Bytes);
            Assert.Equal("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad", file.Sha256);
        }

        [Fact]
        public void Saved_manifest_loads_back()
        {
            Manifest();
            Assert.False(File.Exists(Program.PartialPath(ExportManifest.PathIn(this.folder))));

            ExportManifest loaded = ExportManifest.Load(this.folder);
            Assert.Equal("json", loaded.Format);
            Assert.Equal("Sales", loaded.Database);
            Assert.Equal(DateTimeKind.Utc, loaded.Started.Kind);
            Assert.NotNull(loaded.Completed);
            Assert.Equal(new[] { "dbo.A", "dbo.B" }, loaded.Tables.Select(t => t.Name));
            Assert.Equal(2, loaded.Find("dbo.A").Rows);
            Assert.Contains("\"sha256\"", File.ReadAllText(ExportManifest.PathIn(this.folder)));
        }

        [Fact]
        public void Missing_manifest_loads_as_null()
        {
            Assert.Null(ExportManifest.Load(this.folder));
        }

        [Fact]
        public void Record_replaces_a_table_and_keeps_names_sorted()
        {
            ExportManifest manifest = Manifest();
            manifest.Record(Table("dbo.0", 1, ("dbo.0.json", "[1]")), this.folder);
            manifest.Record(Table("dbo.A", 5, ("dbo.A.json", "[5]")), this.folder);

            ExportManifest loaded = ExportManifest.Load(this.folder);
            Assert.Equal(new[] { "dbo.0", "dbo.A", "dbo.B" }, loaded.Tables.Select(t => t.Name));
            Assert.Equal(5, loaded.Find("dbo.A").Rows);
        }

        [Fact]
        public void Intact_needs_every_file_with_the_recorded_size()
        {
            ExportManifest manifest = Manifest();
            Assert.True(manifest.IsIntact("dbo.A", this.folder, true));
            Assert.False(manifest.IsIntact("dbo.Missing", this.folder, false));

            File.AppendAllText(Path.Combine(this.folder, "dbo.A.json"), " ");
            Assert.False(manifest.IsIntact("dbo.A", this.folder, false));

            File.Delete(Path.Combine(this.folder, "dbo.B.json"));
            Assert.False(manifest.IsIntact("dbo.B", this.folder, false));
        }

        [Fact]
        public void Same_size_change_is_caught_only_by_the_hash()
        {
            ExportManifest manifest = Manifest();
            File.WriteAllText(Path.Combine(this.folder, "dbo.A.json"), "[{\"Id\":1}\n,{\"Id\":3}\n]");
            Assert.True(manifest.IsIntact("dbo.A", this.folder, false));
            Assert.False(manifest.IsIntact("dbo.A", this.folder, true));
        }

        [Fact]
        public void Resume_needs_the_same_format_compression_database_and_query()
        {
            ExportManifest manifest = Manifest();
            Assert.Null(manifest.ResumeConflict("JSON", "none", "sales", null));
            Assert.Contains("json, not bcp", manifest.ResumeConflict("bcp", "none", "Sales", null));
            Assert.Contains("--compress none, not gzip", manifest.ResumeConflict("json", "gzip", "Sales", null));
            Assert.Contains("'Sales', not 'Other'", manifest.ResumeConflict("json", "none", "Other", null));
            Assert.Contains("--source-query", manifest.ResumeConflict("json", "none", "Sales", "SELECT 1"));

            manifest.Compression = "gzip";
            Assert.Null(manifest.ResumeConflict("json", "GZIP", "Sales", null));
            Assert.Contains("--compress gzip, not none", manifest.ResumeConflict("json", null, "Sales", null));
        }

        [Fact]
        public void Manifests_without_compression_are_uncompressed_exports()
        {
            ExportManifest manifest = Manifest();
            manifest.Compression = null;
            Assert.Null(manifest.ResumeConflict("json", "none", "Sales", null));
        }

        [Fact]
        public void Verify_warns_about_unlisted_part_files()
        {
            ExportManifest manifest = Manifest();
            File.WriteAllText(Path.Combine(this.folder, "dbo.T.part0001.json"), "[]");
            Program.VerifyResult result = Program.CheckExport(this.folder, manifest);
            Assert.Empty(result.Problems);
            Assert.Contains(result.Warnings, w => w.StartsWith("dbo.T.part0001.json:"));
        }

        [Fact]
        public void Verify_warns_about_unlisted_compressed_data_files()
        {
            ExportManifest manifest = Manifest();
            manifest.Completed = DateTime.UtcNow;
            File.WriteAllText(Path.Combine(this.folder, "dbo.Old.json.gz"), "x");
            File.WriteAllText(Path.Combine(this.folder, "dbo.Old.bcp.gz"), "x");
            File.WriteAllText(Path.Combine(this.folder, "notes.gz"), "x");
            Program.VerifyResult result = Program.CheckExport(this.folder, manifest);
            Assert.Contains(result.Warnings, w => w.StartsWith("dbo.Old.json.gz:"));
            Assert.Contains(result.Warnings, w => w.StartsWith("dbo.Old.bcp.gz:"));
            Assert.DoesNotContain(result.Warnings, w => w.StartsWith("notes.gz:"));
        }

        [Fact]
        public void Verify_passes_an_intact_complete_export()
        {
            Program.VerifyResult result = Program.CheckExport(this.folder, Manifest());
            Assert.Empty(result.Problems);
            Assert.Empty(result.Warnings);
            Assert.Equal(2, result.FilesChecked);
            Assert.Equal(2, result.Rows);
        }

        [Fact]
        public void Verify_reports_changed_missing_and_incomplete()
        {
            ExportManifest manifest = Manifest();
            manifest.Completed = null;
            File.WriteAllText(Path.Combine(this.folder, "dbo.A.json"), "[{\"Id\":1}\n,{\"Id\":3}\n]");
            File.Delete(Path.Combine(this.folder, "dbo.B.json"));

            Program.VerifyResult result = Program.CheckExport(this.folder, manifest);
            Assert.Equal(3, result.Problems.Count);
            Assert.Contains(result.Problems, p => p.Contains("did not complete"));
            Assert.Contains(result.Problems, p => p.StartsWith("dbo.A.json: SHA-256"));
            Assert.Contains(result.Problems, p => p == "dbo.B.json: missing.");
        }

        private static ManifestTable Shell(int parts)
        {
            return new ManifestTable { Name = "dbo.T", Split = new ManifestSplit { Kind = "histogram", Column = "Id", Type = "bigint", PartCount = parts, Parts = new List<ManifestPart>() } };
        }

        private void RecordPart(ExportManifest m, int p, string content)
        {
            string name = string.Format("dbo.T.part{0:0000}.json", p);
            File.WriteAllText(Path.Combine(this.folder, name), content);
            m.RecordPart(Shell(3), ManifestPart.From(new KeyRange { Part = p }, 10 * p, new[] { name }),
                new[] { ManifestFile.Describe(this.folder, name) }, this.folder);
        }

        [Fact]
        public void Parts_recorded_concurrently_merge_into_one_entry()
        {
            var m = new ExportManifest { Format = "json" };
            Parallel.For(1, 4, p => RecordPart(m, p, new string('x', p)));
            ManifestTable t = ExportManifest.Load(this.folder).Find("dbo.T");
            Assert.Equal(60, t.Rows);
            Assert.Equal(new[] { 1, 2, 3 }, t.Split.Parts.Select(p => p.Part));
            Assert.Equal(3, t.Files.Count);
            Assert.True(m.IsIntact("dbo.T", this.folder, true));
        }

        [Fact]
        public void Missing_or_damaged_parts_are_redone()
        {
            var m = new ExportManifest { Format = "json" };
            RecordPart(m, 1, "a");
            RecordPart(m, 2, "bb");
            File.WriteAllText(Path.Combine(this.folder, "dbo.T.part0002.json"), "different length");
            Assert.Equal(new[] { 2, 3 }, m.PartsToRedo("dbo.T", this.folder));
            Assert.False(m.IsIntact("dbo.T", this.folder, false));
            Assert.Empty(m.PartsToRedo("dbo.Other", this.folder));
        }

        [Fact]
        public void Unsplit_entries_have_no_split_key()
        {
            Manifest();
            string json = File.ReadAllText(ExportManifest.PathIn(this.folder));
            Assert.DoesNotContain("split", json);
        }

        [Fact]
        public void Verify_reports_a_split_table_with_missing_parts()
        {
            var m = new ExportManifest { Format = "json", Completed = DateTime.UtcNow };
            RecordPart(m, 1, "a");
            RecordPart(m, 2, "bb");
            Program.VerifyResult result = Program.CheckExport(this.folder, m);
            Assert.Contains("dbo.T: 2 of 3 parts exported.", result.Problems);
        }

        [Fact]
        public void Part_ranges_round_trip()
        {
            var range = new KeyRange { Part = 4, Lower = "1", LowerInclusive = true, Upper = "9", UpperInclusive = false, IncludeNulls = true };
            KeyRange back = ManifestPart.From(range, 5, new[] { "f" }).ToRange();
            Assert.Equal(4, back.Part);
            Assert.Equal("1", back.Lower);
            Assert.True(back.LowerInclusive);
            Assert.Equal("9", back.Upper);
            Assert.False(back.UpperInclusive);
            Assert.True(back.IncludeNulls);
        }

        [Fact]
        public void Verify_warns_about_unlisted_and_partial_files()
        {
            ExportManifest manifest = Manifest();
            File.WriteAllText(Path.Combine(this.folder, "dbo.Old.json"), "[]");
            File.WriteAllText(Path.Combine(this.folder, "dbo.C.json.partial"), "[");
            File.WriteAllText(Path.Combine(this.folder, "dbo.A_output.log"), "log");

            Program.VerifyResult result = Program.CheckExport(this.folder, manifest);
            Assert.Empty(result.Problems);
            Assert.Equal(2, result.Warnings.Count);
            Assert.Contains(result.Warnings, w => w.StartsWith("dbo.C.json.partial"));
            Assert.Contains(result.Warnings, w => w.StartsWith("dbo.Old.json"));
        }
    }
}
