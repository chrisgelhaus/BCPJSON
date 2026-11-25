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

        private const string SetHeader = "SET QUOTED_IDENTIFIER ON; SET ANSI_NULLS ON; SET ANSI_PADDING ON; SET ANSI_WARNINGS ON; SET CONCAT_NULL_YIELDS_NULL ON; SET ARITHABORT ON; SET NUMERIC_ROUNDABORT OFF;\nGO\n";

        private static string Drop(string marker)
        {
            return "    IF EXISTS (SELECT 1 FROM sys.extended_properties WHERE class = 1 AND major_id = OBJECT_ID(N'[split].[Big]') AND minor_id = 0 AND name = N'" + marker + "')\n"
                + "        EXEC sys.sp_dropextendedproperty @name = N'" + marker + "', @level0type = N'SCHEMA', @level0name = N'split', @level1type = N'TABLE', @level1name = N'Big';\n";
        }

        [Fact]
        public void Build_starts_with_comment_then_use_then_set_options()
        {
            string text = HeapLoadScript.Build("Sales DB", "split", "Big", new[] { ("PK_Big", "CREATE A") }, When);
            Assert.StartsWith("-- Written by bcpJson import --load-as-heap for split.Big at 2026-11-24 08:05:03 UTC,\n-- in database Sales DB. If the database now has another name", text);
            Assert.Contains("rerunnable", text);

            string header = "\nUSE [Sales DB];\nGO\n" + SetHeader;
            int use = text.IndexOf(header, StringComparison.Ordinal);
            Assert.True(use > 0, "USE and SET header missing");
            // Only comment lines come before USE; the index batches come after the SET block.
            Assert.All(text.Substring(0, use).Split('\n'), line => Assert.StartsWith("--", line));
            Assert.True(use < text.IndexOf("NOT EXISTS", StringComparison.Ordinal));
        }

        [Fact]
        public void Build_creates_missing_indexes_in_order_then_drops_markers_in_one_guarded_batch()
        {
            string text = HeapLoadScript.Build("db", "split", "Big", new[] { ("PK_Big", "CREATE A"), ("IX_Big", "CREATE B") }, When);

            string a = "IF DB_NAME() = N'db' AND NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[split].[Big]') AND name = N'PK_Big')\nBEGIN\nCREATE A\nEND\nGO\n";
            string b = "IF DB_NAME() = N'db' AND NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'[split].[Big]') AND name = N'IX_Big')\nBEGIN\nCREATE B\nEND\nGO\n";
            string markers = "\nIF DB_NAME() = N'db' AND (SELECT COUNT(*) FROM sys.indexes WHERE object_id = OBJECT_ID(N'[split].[Big]') AND name IN (N'PK_Big', N'IX_Big')) = 2\n"
                + "BEGIN\n"
                + Drop("bcpjson_load_as_heap_001")
                + Drop("bcpjson_load_as_heap_002")
                + "END\n"
                + "ELSE IF DB_NAME() = N'db'\n"
                + "    RAISERROR(N'Not every index exists; the markers are kept. Fix the cause and run this script again.', 16, 1);\n"
                + "GO\n";

            int ia = text.IndexOf(a, StringComparison.Ordinal);
            int ib = text.IndexOf(b, StringComparison.Ordinal);
            int im = text.IndexOf(markers, StringComparison.Ordinal);
            Assert.True(ia > 0 && ia < ib && ib < im);
            // The guarded marker batch is the last one, and the markers are dropped nowhere else.
            Assert.EndsWith(markers, text);
            Assert.Equal(1, CountOf(text, "sp_dropextendedproperty @name = N'bcpjson_load_as_heap_001'"));
            Assert.Equal(1, CountOf(text, "sp_dropextendedproperty @name = N'bcpjson_load_as_heap_002'"));
            Assert.Equal(1, CountOf(text, "RAISERROR(N'Not every index exists"));
        }

        [Fact]
        public void Build_marker_batch_counts_a_single_index()
        {
            string text = HeapLoadScript.Build("db", "split", "Big", new[] { ("PK_Big", "CREATE A") }, When);
            Assert.Contains("AND name IN (N'PK_Big')) = 1\nBEGIN\n" + Drop("bcpjson_load_as_heap_001") + "END\nELSE IF DB_NAME() = N'db'\n    RAISERROR(N'Not every index exists; ", text);
            Assert.DoesNotContain("bcpjson_load_as_heap_002", text);
        }

        [Fact]
        public void Build_escapes_quotes_and_brackets_in_names()
        {
            string text = HeapLoadScript.Build("d]b", "we]ird", "O'Brien", new[] { ("IX_'x]", "CREATE A") }, When);
            Assert.Contains("\nUSE [d]]b];\nGO\n", text);
            Assert.Contains("IF DB_NAME() = N'd]b' AND NOT EXISTS", text);
            Assert.Contains("OBJECT_ID(N'[we]]ird].[O''Brien]') AND name = N'IX_''x]')", text);
            Assert.Contains("AND name IN (N'IX_''x]')) = 1\n", text);
            Assert.Contains("@level0name = N'we]ird', @level1type = N'TABLE', @level1name = N'O''Brien';", text);
        }

        [Fact]
        public void Build_stops_in_another_database_with_a_message_and_guards_every_batch()
        {
            string text = HeapLoadScript.Build("Sales%DB", "split", "Big", new[] { ("PK_Big", "CREATE A"), ("IX_Big", "CREATE B") }, When);
            // Right after the SET block: an error naming the database (passed as a %s argument, so a '%' in it is safe).
            Assert.Contains(SetHeader + "IF DB_NAME() <> N'Sales%DB'\n    RAISERROR(N'This script is for database %s, which is not the current database; nothing was changed.", text);
            Assert.Contains("', 16, 1, N'Sales%DB');\nGO\n", text);
            // Every batch that changes something checks the database first.
            Assert.Equal(2, CountOf(text, "IF DB_NAME() = N'Sales%DB' AND NOT EXISTS"));
            Assert.Equal(1, CountOf(text, "IF DB_NAME() = N'Sales%DB' AND (SELECT COUNT(*)"));
            Assert.Equal(1, CountOf(text, "ELSE IF DB_NAME() = N'Sales%DB'\n    RAISERROR(N'Not every index exists"));
        }

        [Fact]
        public void Build_keeps_control_characters_out_of_comments()
        {
            string text = HeapLoadScript.Build("d\nb", "s\r\nx", "t\nDROP TABLE x", new[] { ("PK", "CREATE A") }, When);
            int use = text.IndexOf("\nUSE ", StringComparison.Ordinal);
            Assert.All(text.Substring(0, use).Split('\n'), line => Assert.StartsWith("--", line));
            Assert.Contains("for s??x.t?DROP TABLE x at ", text);
            Assert.Contains("-- in database d?b.", text);
        }

        [Theory]
        [InlineData("plain", "plain")]
        [InlineData("a\tb\u0000c", "a?b?c")]
        public void Comment_text(string name, string expected)
        {
            Assert.Equal(expected, HeapLoadScript.CommentText(name));
        }

        [Fact]
        public void Write_uses_utf8_with_bom()
        {
            string path = Path.Combine(Path.GetTempPath(), "bcpjson-test-" + Guid.NewGuid().ToString("N") + HeapLoadScript.Suffix);
            const string content = "-- Ünïcödé 日本\n";
            try
            {
                HeapLoadScript.Write(path, content);
                byte[] bytes = File.ReadAllBytes(path);
                Assert.Equal(new byte[] { 0xEF, 0xBB, 0xBF }, bytes.Take(3).ToArray());
                Assert.Equal(content, File.ReadAllText(path));
            }
            finally
            {
                File.Delete(path);
            }
        }

        private static int CountOf(string text, string part)
        {
            int count = 0;
            for (int i = text.IndexOf(part, StringComparison.Ordinal); i >= 0; i = text.IndexOf(part, i + part.Length, StringComparison.Ordinal))
            {
                count++;
            }
            return count;
        }
    }
}
