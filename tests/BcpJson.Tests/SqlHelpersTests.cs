using Xunit;

namespace bcpJson.Tests
{
    public class SqlHelpersTests
    {
        [Theory]
        [InlineData("dbo", "[dbo]")]
        [InlineData("a]b", "[a]]b]")]
        [InlineData("]]", "[]]]]]")]
        [InlineData("has space", "[has space]")]
        [InlineData("[x]", "[[x]]]")]
        [InlineData("", "[]")]
        public void QuoteName_brackets_and_doubles_closing_bracket(string input, string expected)
        {
            Assert.Equal(expected, Program.QuoteName(input));
        }

        [Theory]
        [InlineData("abc", "N'abc'")]
        [InlineData("O'Brien", "N'O''Brien'")]
        [InlineData("''", "N''''''")]
        [InlineData("", "N''")]
        [InlineData("x'); DROP TABLE t;--", "N'x''); DROP TABLE t;--'")]
        public void SqlLiteral_wraps_as_unicode_and_doubles_quotes(string input, string expected)
        {
            Assert.Equal(expected, Program.SqlLiteral(input));
        }

        [Theory]
        [InlineData("dbo.*", "N'dbo.%'")]
        [InlineData("*", "N'%'")]
        [InlineData("dbo.my_*", "N'dbo.my[_]%'")]
        [InlineData("100%*", "N'100[%]%'")]
        [InlineData("a[b]*", "N'a[[]b]%'")]
        [InlineData("O'*", "N'O''%'")]
        public void LikeLiteral_maps_star_to_percent_and_escapes_like_metacharacters(string input, string expected)
        {
            Assert.Equal(expected, Program.LikeLiteral(input));
        }

        [Fact]
        public void TableRef_names()
        {
            var table = new Program.TableRef(42, "sales", "order]items");
            Assert.Equal("sales.order]items", table.DisplayName);
            Assert.Equal("[sales].[order]]items]", table.QuotedName);
        }
    }
}
