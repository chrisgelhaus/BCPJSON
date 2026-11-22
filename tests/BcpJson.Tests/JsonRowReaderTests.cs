using System.Data.SqlTypes;
using Xunit;

namespace bcpJson.Tests
{
    public class JsonRowReaderTests
    {
        private static readonly string[] Names = { "Id", "Name", "Amount", "Stamp" };
        private static readonly Type[] Types = { typeof(int), typeof(string), typeof(SqlDecimal), typeof(DateTime) };

        private static JsonRowReader Reader(string json)
        {
            return new JsonRowReader(new StringReader(json), Names, Types);
        }

        [Fact]
        public void Reads_rows_with_target_types()
        {
            using (JsonRowReader reader = Reader("[{\"Id\":1,\"Name\":\"a\",\"Amount\":12345678901234567890123456789.123456789,\"Stamp\":\"2026-11-22T10:00:00\"}\r\n,{\"Id\":2,\"Name\":null,\"Amount\":null,\"Stamp\":null}\r\n]"))
            {
                Assert.Equal(4, reader.FieldCount);
                Assert.Equal("Amount", reader.GetName(2));
                Assert.Equal(typeof(SqlDecimal), reader.GetFieldType(2));
                Assert.Equal(1, reader.GetOrdinal("name"));

                Assert.True(reader.Read());
                Assert.Equal(1, reader.GetInt32(0));
                Assert.Equal("a", reader.GetString(1));
                Assert.Equal("12345678901234567890123456789.123456789", ((SqlDecimal)reader.GetValue(2)).ToString());
                Assert.Equal(new DateTime(2026, 11, 22, 10, 0, 0), reader.GetDateTime(3));

                Assert.True(reader.Read());
                Assert.Equal(2, reader.GetValue(0));
                Assert.True(reader.IsDBNull(1));
                Assert.True(reader.IsDBNull(2));
                Assert.True(reader.IsDBNull(3));

                Assert.False(reader.Read());
                Assert.False(reader.Read());
                Assert.Equal(2, reader.RowCount);
            }
        }

        [Fact]
        public void Missing_properties_are_null_and_unknown_ones_are_skipped()
        {
            using (JsonRowReader reader = Reader("[{\"Id\":1,\"Name\":\"a\"},{\"Extra\":{\"x\":[1,2]},\"Id\":2}]"))
            {
                Assert.True(reader.Read());
                Assert.True(reader.Read());
                Assert.Equal(2, reader.GetInt32(0));
                // Values from the previous row must not leak into this one.
                Assert.True(reader.IsDBNull(1));
                Assert.False(reader.Read());
            }
        }

        [Fact]
        public void Empty_array_has_no_rows()
        {
            using (JsonRowReader reader = Reader("[]"))
            {
                Assert.False(reader.Read());
                Assert.Equal(0, reader.RowCount);
            }
        }

        [Fact]
        public void A_file_that_is_not_an_array_is_rejected()
        {
            Assert.Throws<InvalidDataException>(() => Reader("{\"Id\":1}"));
        }

        [Fact]
        public void A_truncated_file_fails_instead_of_loading_part_of_it_silently()
        {
            using (JsonRowReader reader = Reader("[{\"Id\":1}\r\n,{\"Id\":2"))
            {
                Assert.True(reader.Read());
                Assert.ThrowsAny<Exception>(() => reader.Read());
            }
            using (JsonRowReader reader = Reader("[{\"Id\":1}\r\n"))
            {
                Assert.True(reader.Read());
                Assert.Throws<InvalidDataException>(() => reader.Read());
            }
        }

        [Fact]
        public void Bad_values_name_the_line_and_column()
        {
            using (JsonRowReader reader = Reader("[{\"Id\":1}\r\n,{\"Id\":\"x\"}\r\n]"))
            {
                Assert.True(reader.Read());
                var ex = Assert.Throws<InvalidDataException>(() => reader.Read());
                Assert.Contains("Line 2, column Id", ex.Message);
            }
        }

        [Fact]
        public void GetValues_copies_the_current_row()
        {
            using (JsonRowReader reader = Reader("[{\"Id\":7,\"Name\":\"n\"}]"))
            {
                Assert.True(reader.Read());
                var values = new object[4];
                Assert.Equal(4, reader.GetValues(values));
                Assert.Equal(7, values[0]);
                Assert.Equal("n", values[1]);
                Assert.Equal(DBNull.Value, values[2]);
            }
        }
    }
}
