using Xunit;

namespace bcpJson.Tests
{
    public class KeyRangeTests
    {
        static SplitColumn Col(string type, short len = 8, byte p = 0, byte s = 0, bool nullable = false) =>
            new SplitColumn { Name = "K", TypeName = type, MaxLength = len, Precision = p, Scale = s, Nullable = nullable };

        [Fact]
        public void Sql_types_and_styles()
        {
            Assert.Equal("decimal(18,4)", Col("decimal", 9, 18, 4).SqlType);
            Assert.Equal("nvarchar(30)", Col("nvarchar", 60).SqlType);
            Assert.Equal("varchar(max)", Col("varchar", -1).SqlType);
            Assert.Equal("datetime2(7)", Col("datetime2", 8, 27, 7).SqlType);
            Assert.Equal("binary(16)", Col("binary", 16).SqlType);
            Assert.Equal(126, Col("datetime2", 8, 27, 7).ConvertStyle);
            Assert.Equal(3, Col("float").ConvertStyle);
            Assert.Null(Col("time", 5, 16, 7).ConvertStyle);
            Assert.False(SplitColumn.IsSplittableType("sql_variant"));
            Assert.False(SplitColumn.IsSplittableType("hierarchyid"));
        }

        [Fact]
        public void Middle_part_is_half_open()
        {
            var r = new KeyRange { Part = 2, Lower = "100", LowerInclusive = true, Upper = "200", UpperInclusive = false };
            Assert.Equal("([K] >= CONVERT(bigint, N'100') AND [K] < CONVERT(bigint, N'200'))", r.Predicate(Col("bigint")));
        }

        [Fact]
        public void First_part_takes_nulls_and_last_part_is_open()
        {
            var first = new KeyRange { Part = 1, Upper = "2026-01-01T00:00:00", UpperInclusive = false, IncludeNulls = true };
            Assert.Equal("([K] < CONVERT(datetime2(7), N'2026-01-01T00:00:00', 126) OR [K] IS NULL)", first.Predicate(Col("datetime2", 8, 27, 7, nullable: true)));
            var last = new KeyRange { Part = 3, Lower = "5", LowerInclusive = false };   // RANGE LEFT: lower bound exclusive
            Assert.Equal("([K] > CONVERT(int, N'5'))", last.Predicate(Col("int", 4)));
        }

        [Fact]
        public void More_sql_types()
        {
            Assert.Equal("nvarchar(max)", Col("nvarchar", -1).SqlType);
            Assert.Equal("varbinary(max)", Col("varbinary", -1).SqlType);
            Assert.Equal("time(7)", Col("time", 5, 16, 7).SqlType);
        }

        [Fact]
        public void Inclusive_upper_and_nulls_only()
        {
            var r = new KeyRange { Part = 1, Upper = "9", UpperInclusive = true };
            Assert.Equal("([K] <= CONVERT(int, N'9'))", r.Predicate(Col("int", 4)));
            var n = new KeyRange { Part = 1, IncludeNulls = true };
            Assert.Equal("([K] IS NULL)", n.Predicate(Col("int", 4, nullable: true)));
        }

        [Fact]
        public void Value_expression_styles()
        {
            Assert.Equal("CONVERT(binary(16), N'0xAB', 1)", KeyRange.ValueExpression(Col("binary", 16), "0xAB"));
            Assert.Equal("CONVERT(int, N'7')", KeyRange.ValueExpression(Col("int", 4), "7"));
        }

        [Theory]
        [InlineData("a\"b", true)]
        [InlineData("a\\b", true)]
        [InlineData("a\nb", true)]
        [InlineData("a b]c", false)]
        public void Unsafe_column_names_are_refused(string name, bool unsafeName)
        {
            var col = Col("int", 4);
            col.Name = name;
            Assert.Equal(!unsafeName, col.HasCommandLineSafeName);
            var r = new KeyRange { Part = 2, Lower = "1", LowerInclusive = true };
            if (unsafeName)
                Assert.Throws<InvalidOperationException>(() => r.Predicate(col));
            else
                Assert.NotNull(r.Predicate(col));
        }

        [Fact]
        public void Unbounded_range_selects_everything()
        {
            Assert.Equal("1 = 1", new KeyRange { Part = 1 }.Predicate(Col("int", 4)));
        }

        [Theory]
        [InlineData("ab", "N'ab'")]
        [InlineData("O'Brien", "N'O''Brien'")]
        [InlineData("say \"hi\"", "(N'say ' + NCHAR(34) + N'hi' + NCHAR(34) + N'')")]
        [InlineData(@"C:\x", "(N'C:' + NCHAR(92) + N'x')")]
        [InlineData("a\"\\b", "(N'a' + NCHAR(34) + N'' + NCHAR(92) + N'b')")]
        [InlineData("\"a", "(N'' + NCHAR(34) + N'a')")]
        [InlineData("", "N''")]
        [InlineData("x\r\ny", "(N'x' + NCHAR(13) + N'' + NCHAR(10) + N'y')")]
        [InlineData("a\tb", "(N'a' + NCHAR(9) + N'b')")]
        public void Text_literals_are_safe_on_a_command_line(string value, string literal)
        {
            Assert.Equal(literal, KeyRange.TextLiteral(value));
        }
    }
}
