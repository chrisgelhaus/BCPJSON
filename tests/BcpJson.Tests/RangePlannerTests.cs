using Xunit;

namespace bcpJson.Tests
{
    public class RangePlannerTests
    {
        static List<RangePlanner.HistogramStep> Steps(params (double range, double eq)[] s) =>
            s.Select((x, i) => new RangePlanner.HistogramStep((i * 10).ToString(), x.range, x.eq)).ToList();

        [Fact]
        public void Even_histogram_splits_evenly()
        {
            // 10 steps of 100 rows each on the step key: rows below step j = 100 * j, total 1000.
            var steps = Steps(Enumerable.Repeat((0.0, 100.0), 10).ToArray());
            Assert.Equal(new[] { 2, 5, 7 }, RangePlanner.ChooseHistogramSteps(steps, 4)); // targets 250/500/750; ties (200 vs 300, 700 vs 800) take the lower step
        }

        [Fact]
        public void Fewer_steps_than_parts_gives_fewer_parts()
        {
            Assert.Equal(new[] { 1, 2 }, RangePlanner.ChooseHistogramSteps(Steps((0, 5), (0, 5), (0, 5)), 8));
        }

        [Fact]
        public void One_dominant_key_cannot_be_split()
        {
            Assert.Empty(RangePlanner.ChooseHistogramSteps(Steps((0, 1000000)), 4));
        }

        [Fact]
        public void Histogram_ranges_cover_everything_and_descending_numbers_from_the_top()
        {
            var asc = RangePlanner.HistogramRanges(new[] { "10", "20" }, nullable: true, descending: false);
            Assert.Equal(3, asc.Count);
            Assert.True(asc[0].IncludeNulls); Assert.Null(asc[0].Lower); Assert.Equal("10", asc[0].Upper); Assert.False(asc[0].UpperInclusive);
            Assert.Equal("10", asc[1].Lower); Assert.True(asc[1].LowerInclusive); Assert.Equal("20", asc[1].Upper);
            Assert.Null(asc[2].Upper);
            var desc = RangePlanner.HistogramRanges(new[] { "10", "20" }, nullable: false, descending: true);
            Assert.Equal("20", desc[0].Lower); Assert.Null(desc[0].Upper); Assert.Equal(1, desc[0].Part);   // highest values first
            Assert.Null(desc[2].Lower); Assert.Equal(3, desc[2].Part);
        }

        [Fact]
        public void Descending_nulls_go_with_the_lowest_range_which_is_numbered_last()
        {
            var desc = RangePlanner.HistogramRanges(new[] { "10", "20" }, nullable: true, descending: true);
            Assert.False(desc[0].IncludeNulls);
            Assert.False(desc[1].IncludeNulls);
            Assert.True(desc[2].IncludeNulls);
            Assert.Null(desc[2].Lower);
        }

        [Theory]
        [InlineData(new long[] { 100, 100, 100, 100 }, 2, "1-2,3-4")]
        [InlineData(new long[] { 0, 100, 100, 0, 100, 100, 0 }, 2, "1-3,4-7")]
        [InlineData(new long[] { 10, 1000, 10, 10 }, 3, "1-2,3-3,4-4")]
        [InlineData(new long[] { 100, 0, 100 }, 5, "1-1,2-3")]
        [InlineData(new long[] { 0, 0, 0 }, 4, "1-3")]
        public void Partitions_are_grouped_contiguously(long[] rows, int parts, string expected)
        {
            Assert.Equal(expected, string.Join(",", RangePlanner.GroupPartitions(rows, parts).Select(g => g.First + "-" + g.Last)));
        }

        [Fact]
        public void Partition_ranges_follow_range_left_and_right()
        {
            // 4 partitions, boundaries a|b|c; groups (1-2),(3-4).
            var right = RangePlanner.PartitionRanges(new[] { "a", "b", "c" }, true, new[] { (1, 2), (3, 4) }, nullable: true);
            Assert.Equal("b", right[0].Upper); Assert.False(right[0].UpperInclusive); Assert.True(right[0].IncludeNulls);
            Assert.Equal("b", right[1].Lower); Assert.True(right[1].LowerInclusive); Assert.Null(right[1].Upper);
            var left = RangePlanner.PartitionRanges(new[] { "a", "b", "c" }, false, new[] { (1, 2), (3, 4) }, nullable: false);
            Assert.True(left[0].UpperInclusive); Assert.False(left[1].LowerInclusive);
        }
    }
}
