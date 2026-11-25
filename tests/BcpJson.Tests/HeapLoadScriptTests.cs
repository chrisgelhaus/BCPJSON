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

        [Fact]
        public void Build_orders_statements()
        {
            string text = HeapLoadScript.Build("[split].[Big]", new DateTime(2026, 11, 24, 8, 5, 3, DateTimeKind.Utc),
                new[] { "CREATE A", "CREATE B" });
            Assert.Contains("[split].[Big] at 2026-11-24 08:05:03 UTC", text);
            Assert.Contains("\nCREATE A\nGO\n", text);
            Assert.True(text.IndexOf("CREATE A") < text.IndexOf("CREATE B"));
            Assert.EndsWith("CREATE B\nGO\n", text);
        }
    }
}
