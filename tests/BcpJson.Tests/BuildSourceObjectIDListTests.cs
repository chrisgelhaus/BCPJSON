using Xunit;

namespace bcpJson.Tests
{
    public class BuildSourceObjectIDListTests
    {
        private const string AllTablesInsert = "AND t.is_ms_shipped = 0;";

        private static string Build(string[] includeTables = null, string[] excludeTables = null, string[] includeSchemas = null, string[] excludeSchemas = null)
        {
            return Program.BuildSourceObjectIDList(includeTables, excludeTables, includeSchemas, excludeSchemas);
        }

        [Fact]
        public void No_filters_selects_all_user_tables()
        {
            string sql = Build();
            Assert.Contains(AllTablesInsert, sql);
            Assert.DoesNotContain("DELETE", sql);
            Assert.EndsWith("ORDER BY schema_name, table_name;" + Environment.NewLine, sql);
        }

        [Fact]
        public void Empty_and_whitespace_entries_are_ignored()
        {
            string sql = Build(includeTables: new[] { "", "  " }, excludeSchemas: new[] { " " });
            Assert.Contains(AllTablesInsert, sql);
            Assert.DoesNotContain(" IN (N'", sql);
            Assert.DoesNotContain("DELETE", sql);
        }

        [Fact]
        public void Literal_include_tables_use_one_IN_list()
        {
            string sql = Build(includeTables: new[] { "dbo.A", " sales.B " });
            Assert.DoesNotContain(AllTablesInsert, sql);
            Assert.Contains("(s.name + N'.' + t.name) IN (N'dbo.A', N'sales.B')", sql);
        }

        [Fact]
        public void Wildcard_include_becomes_LIKE_with_escaping()
        {
            string sql = Build(includeTables: new[] { "dbo.my_*" });
            Assert.Contains("(s.name + N'.' + t.name) LIKE N'dbo.my[_]%'", sql);
            Assert.DoesNotContain("t.name) IN (", sql);
        }

        [Fact]
        public void Literal_names_are_not_LIKE_escaped()
        {
            string sql = Build(includeTables: new[] { "dbo.my_table" });
            Assert.Contains("IN (N'dbo.my_table')", sql);
        }

        [Fact]
        public void Include_schemas_filter_on_schema_name()
        {
            string sql = Build(includeSchemas: new[] { "sales", "hr*" });
            Assert.DoesNotContain(AllTablesInsert, sql);
            Assert.Contains("s.name IN (N'sales')", sql);
            Assert.Contains("s.name LIKE N'hr%'", sql);
        }

        [Fact]
        public void Excludes_only_start_from_all_tables_then_delete()
        {
            string sql = Build(excludeTables: new[] { "dbo.Log*" }, excludeSchemas: new[] { "staging" });
            Assert.Contains(AllTablesInsert, sql);
            Assert.Contains("DELETE @objectIDs WHERE ID IN (SELECT t.object_id FROM sys.tables t INNER JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE t.type = 'U' AND (s.name + N'.' + t.name) LIKE N'dbo.Log%');", sql);
            Assert.Contains("AND s.name IN (N'staging'));", sql);
        }

        [Fact]
        public void Excludes_run_after_includes()
        {
            string sql = Build(includeSchemas: new[] { "dbo" }, excludeTables: new[] { "dbo.Big" });
            Assert.True(sql.IndexOf("INSERT @objectIDs (ID) SELECT", StringComparison.Ordinal) < sql.IndexOf("DELETE", StringComparison.Ordinal));
        }

        [Fact]
        public void Quotes_in_names_are_escaped()
        {
            string sql = Build(includeTables: new[] { "dbo.x'); DROP TABLE y;--" });
            Assert.Contains("IN (N'dbo.x''); DROP TABLE y;--')", sql);
        }
    }
}
