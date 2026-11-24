using Xunit;

namespace bcpJson.Tests
{
    public class CopySelectTests
    {
        static readonly string[] Columns = { "Id", "Name" };

        [Fact]
        public void Whole_table_select_is_unchanged()
        {
            Assert.Equal("SELECT [Id], [Name] FROM [dbo].[T]", Program.CopySelectSql(Columns, "[dbo].[T]", false, null, null));
            Assert.Equal("SELECT [Id], [Name] FROM [dbo].[T] WITH (NOLOCK)", Program.CopySelectSql(Columns, "[dbo].[T]", true, null, null));
        }

        [Fact]
        public void Range_select_adds_the_predicate()
        {
            var column = new SplitColumn { Name = "Id", TypeName = "int", MaxLength = 4 };
            var range = new KeyRange { Part = 2, Lower = "10", LowerInclusive = true, Upper = "20", UpperInclusive = false };
            Assert.Equal("SELECT [Id], [Name] FROM [dbo].[T] WITH (NOLOCK) WHERE ([Id] >= CONVERT(int, N'10') AND [Id] < CONVERT(int, N'20'))",
                Program.CopySelectSql(Columns, "[dbo].[T]", true, range, column));
        }
    }
}
