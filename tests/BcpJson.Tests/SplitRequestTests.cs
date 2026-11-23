using Xunit;

namespace bcpJson.Tests
{
    public class SplitRequestTests
    {
        [Fact]
        public void Parses_patterns_and_part_counts()
        {
            Assert.True(SplitRequest.TryParse(new[] { "dbo.History:32", "Sales.Order*", "odd:name:4" }, out var r, out _));
            Assert.Equal(new SplitRequest("dbo.History", 32), r[0]);
            Assert.Equal(new SplitRequest("Sales.Order*", 8), r[1]);
            Assert.Equal(new SplitRequest("odd:name", 4), r[2]);       // the last ":" followed by digits is the count
        }

        [Theory]
        [InlineData("dbo.T:1")]
        [InlineData("dbo.T:201")]
        [InlineData("dbo.T:0")]
        [InlineData(":4")]
        [InlineData("")]
        public void Rejects_bad_values(string value)
        {
            Assert.False(SplitRequest.TryParse(new[] { value }, out _, out string error));
            Assert.Contains("--split-tables", error);
        }

        [Fact]
        public void First_matching_pattern_wins()
        {
            var r = new List<SplitRequest> { new("dbo.History", 32), new("dbo.*", 4) };
            Assert.Equal(32, SplitRequest.PartsFor(r, "DBO.history"));
            Assert.Equal(4, SplitRequest.PartsFor(r, "dbo.Other"));
            Assert.Null(SplitRequest.PartsFor(r, "sales.X"));
        }

        [Theory]
        [InlineData("export")]
        [InlineData("copy")]
        public void Snapshot_warning_names_the_verb(string verb)
        {
            Assert.Equal("--split-tables without --snapshot: the parts of a table are read at different moments. Use --snapshot for an exact, consistent " + verb + ".",
                SplitRequest.SnapshotWarning(true, false, verb));
        }

        [Fact]
        public void No_snapshot_warning_when_not_needed()
        {
            Assert.Null(SplitRequest.SnapshotWarning(false, false, "export"));
            Assert.Null(SplitRequest.SnapshotWarning(true, true, "copy"));
        }
    }
}
