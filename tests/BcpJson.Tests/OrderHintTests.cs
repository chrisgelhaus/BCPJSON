using Newtonsoft.Json.Linq;
using Xunit;

namespace bcpJson.Tests
{
    public class OrderHintTests : IDisposable
    {
        private readonly string folder = Path.Combine(Path.GetTempPath(), "bcpjson-tests", Guid.NewGuid().ToString("N"));

        public OrderHintTests()
        {
            Directory.CreateDirectory(this.folder);
        }

        public void Dispose()
        {
            Directory.Delete(this.folder, true);
        }

        private static ManifestKeyColumn Key(string column, string type = "int", bool descending = false, string collation = null)
        {
            return new ManifestKeyColumn { Column = column, Type = type, Descending = descending, Collation = collation };
        }

        [Fact]
        public void Same_columns_types_directions_and_collations_match()
        {
            var sorted = new List<ManifestKeyColumn> { Key("Code", "varchar", false, "Latin1_General_CS_AS"), Key("Seq", "int", true) };
            var clustered = new List<ManifestKeyColumn> { Key("code", "VARCHAR", false, "latin1_general_cs_as"), Key("Seq", "int", true) };
            Assert.True(ManifestKeyColumn.SameOrder(sorted, clustered));
        }

        [Fact]
        public void Any_difference_means_no_hint()
        {
            var sorted = new List<ManifestKeyColumn> { Key("Code", "varchar", false, "Latin1_General_CS_AS"), Key("Seq", "int", true) };

            Assert.False(ManifestKeyColumn.SameOrder(sorted, null));
            Assert.False(ManifestKeyColumn.SameOrder(null, sorted));
            Assert.False(ManifestKeyColumn.SameOrder(new List<ManifestKeyColumn>(), new List<ManifestKeyColumn>()));
            // Clustered on a prefix of the key, or a longer key.
            Assert.False(ManifestKeyColumn.SameOrder(sorted, sorted.Take(1).ToList()));
            Assert.False(ManifestKeyColumn.SameOrder(sorted.Take(1).ToList(), sorted));
            // Columns swapped, other column, direction, type, collation.
            Assert.False(ManifestKeyColumn.SameOrder(sorted, new List<ManifestKeyColumn> { sorted[1], sorted[0] }));
            Assert.False(ManifestKeyColumn.SameOrder(sorted, new List<ManifestKeyColumn> { Key("Name", "varchar", false, "Latin1_General_CS_AS"), sorted[1] }));
            Assert.False(ManifestKeyColumn.SameOrder(sorted, new List<ManifestKeyColumn> { sorted[0], Key("Seq", "int", false) }));
            Assert.False(ManifestKeyColumn.SameOrder(sorted, new List<ManifestKeyColumn> { Key("Code", "nvarchar", false, "Latin1_General_CS_AS"), sorted[1] }));
            Assert.False(ManifestKeyColumn.SameOrder(sorted, new List<ManifestKeyColumn> { Key("Code", "varchar", false, "Latin1_General_CI_AS"), sorted[1] }));
        }

        [Fact]
        public void Sql_variant_keys_never_get_a_hint()
        {
            var key = new List<ManifestKeyColumn> { Key("V", "sql_variant") };
            Assert.False(ManifestKeyColumn.SameOrder(key, key));
        }

        [Fact]
        public void Sort_key_is_found_by_file_name()
        {
            var manifest = new ExportManifest();
            manifest.Tables.Add(new ManifestTable
            {
                Name = "dbo.T",
                Files = new List<ManifestFile> { new ManifestFile { Name = "dbo.T.json.gz" } },
                OrderedBy = new List<ManifestKeyColumn> { Key("Id") }
            });
            manifest.Tables.Add(new ManifestTable { Name = "dbo.Heap", Files = new List<ManifestFile> { new ManifestFile { Name = "dbo.Heap.json" } } });

            Assert.Equal("Id", Program.SortKeyOf(manifest, "dbo.T.json.gz").Single().Column);
            Assert.Null(Program.SortKeyOf(manifest, "dbo.Heap.json"));
            Assert.Null(Program.SortKeyOf(manifest, "dbo.Other.json"));
            Assert.Null(Program.SortKeyOf(null, "dbo.T.json.gz"));
        }

        [Fact]
        public void Manifest_keeps_the_sort_key_and_omits_it_when_there_is_none()
        {
            var manifest = new ExportManifest { Format = "json" };
            manifest.Record(new ManifestTable { Name = "dbo.T", OrderedBy = new List<ManifestKeyColumn> { Key("Code", "varchar", true, "Latin1_General_CS_AS") } }, this.folder);
            manifest.Record(new ManifestTable { Name = "dbo.U" }, this.folder);

            JObject json = JObject.Parse(File.ReadAllText(ExportManifest.PathIn(this.folder)));
            Assert.Null(json["tables"][1]["orderedBy"]);
            Assert.Equal("Code", (string)json["tables"][0]["orderedBy"][0]["column"]);

            ManifestKeyColumn key = ExportManifest.Load(this.folder).Find("dbo.T").OrderedBy.Single();
            Assert.Equal("varchar", key.Type);
            Assert.True(key.Descending);
            Assert.Equal("Latin1_General_CS_AS", key.Collation);
            Assert.Null(ExportManifest.Load(this.folder).Find("dbo.U").OrderedBy);
        }

        [Fact]
        public void Spatial_text_with_an_srid_prefix_is_rebuilt_with_that_srid()
        {
            string sql = Program.ClrFromText("geography", "[Geo]");
            Assert.Equal("CASE WHEN [Geo] LIKE N'SRID=%;%' THEN geography::STGeomFromText(SUBSTRING([Geo], CHARINDEX(N';', [Geo]) + 1, 2147483647), "
                + "CAST(SUBSTRING([Geo], 6, CHARINDEX(N';', [Geo]) - 6) AS INT)) ELSE geography::Parse([Geo]) END", sql);
            Assert.Contains("geometry::STGeomFromText", Program.ClrFromText("geometry", "[G]"));
        }

        [Fact]
        public void Hierarchyid_is_parsed_as_before()
        {
            Assert.Equal("hierarchyid::Parse([Node])", Program.ClrFromText("hierarchyid", "[Node]"));
        }
    }
}
