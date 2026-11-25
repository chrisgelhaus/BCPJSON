using Xunit;

namespace bcpJson.Tests
{
    public class HeapLoadScriptTests
    {
        [Theory]
        [InlineData("split", "Big", "split.Big.load-as-heap.sql")]
        [InlineData("we]ird", "Tab le", "we]ird.Tab le.load-as-heap.sql")]
        public void File_name(string schema, string table, string expected)
        {
            Assert.Equal(expected, HeapLoadScript.FileName(schema, table));
        }

        [Theory]
        [InlineData(1, "bcpjson_load_as_heap_001")]
        [InlineData(42, "bcpjson_load_as_heap_042")]
        [InlineData(999, "bcpjson_load_as_heap_999")]
        public void Marker_name(int ordinal, string expected)
        {
            Assert.Equal(expected, HeapLoadScript.MarkerName(ordinal));
        }

        private static readonly DateTime When = new DateTime(2026, 11, 24, 8, 5, 3, DateTimeKind.Utc);

        [Fact]
        public void Build_creates_missing_indexes_in_order_then_drops_markers()
        {
            string text = HeapLoadScript.Build("split", "Big", new[] { ("PK_Big", "CREATE A"), ("IX_Big", "CREATE B") }, When);
            Assert.Contains("split.Big at 2026-11-24 08:05:03 UTC", text);
            Assert.Contains("rerunnable", text);

            Assert.Contains("IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[split].[Big]') AND name = N'PK_Big')\nBEGIN\nCREATE A\nEND\nGO\n", text);
            Assert.Contains("IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[split].[Big]') AND name = N'IX_Big')\nBEGIN\nCREATE B\nEND\nGO\n", text);

            string drop1 = "IF EXISTS (SELECT 1 FROM sys.extended_properties WHERE class = 1 AND major_id = OBJECT_ID(N'[split].[Big]') AND minor_id = 0 AND name = N'bcpjson_load_as_heap_001')\n"
                + "    EXEC sys.sp_dropextendedproperty @name = N'bcpjson_load_as_heap_001', @level0type = N'SCHEMA', @level0name = N'split', @level1type = N'TABLE', @level1name = N'Big';\nGO\n";
            string drop2 = drop1.Replace("_001", "_002");
            Assert.Contains(drop1, text);
            Assert.Contains(drop2, text);

            int a = text.IndexOf("CREATE A", StringComparison.Ordinal);
            int b = text.IndexOf("CREATE B", StringComparison.Ordinal);
            int m1 = text.IndexOf(drop1, StringComparison.Ordinal);
            int m2 = text.IndexOf(drop2, StringComparison.Ordinal);
            Assert.True(a < b && b < m1 && m1 < m2);
            Assert.EndsWith(drop2, text);
        }

        [Fact]
        public void Build_escapes_quotes_and_brackets_in_names()
        {
            string text = HeapLoadScript.Build("we]ird", "O'Brien", new[] { ("IX_'x]", "CREATE A") }, When);
            Assert.Contains("OBJECT_ID(N'[we]]ird].[O''Brien]') AND name = N'IX_''x]')", text);
            Assert.Contains("@level0name = N'we]ird', @level1type = N'TABLE', @level1name = N'O''Brien';", text);
        }
    }
}
