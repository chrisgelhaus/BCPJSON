namespace bcpJson.Tests
{
    public class TableSplitTests
    {
        static SplitColumn Col(string type = "bigint", bool nullable = false, bool descending = false, string name = "Id") =>
            new SplitColumn { Name = name, TypeName = type, MaxLength = 8, Nullable = nullable, Descending = descending };

        static List<RangePlanner.HistogramStep> Steps(params (string key, double range, double eq)[] s) =>
            s.Select(x => new RangePlanner.HistogramStep(x.key, x.range, x.eq)).ToList();

        static List<RangePlanner.HistogramStep> Uneven() => Steps(("10", 0, 100), ("20", 0, 100), ("30", 0, 100), ("40", 0, 300));

        [Fact]
        public void Histogram_split_estimates_rows_per_part()
        {
            // Rows below each step: 0, 100, 200, 300 of 600. Targets 200 and 400 pick steps "30" and "40".
            TableSplit split = TableSplit.FromHistogram(Col(), Uneven(), 3, out string reason);
            Assert.Null(reason);
            Assert.Equal(TableSplit.HistogramKind, split.Kind);
            Assert.Equal(new[] { "30", "40" }, split.Boundaries);
            Assert.Equal(new[] { 1, 2, 3 }, split.Ranges.Select(r => r.Part));
            Assert.Equal(new long[] { 200, 100, 300 }, split.EstimatedRows);
        }

        [Fact]
        public void Descending_histogram_split_numbers_parts_from_the_top()
        {
            TableSplit split = TableSplit.FromHistogram(Col(descending: true), Uneven(), 3, out _);
            Assert.Equal(new[] { "30", "40" }, split.Boundaries);   // still ascending
            Assert.Equal("40", split.Ranges[0].Lower);
            Assert.Null(split.Ranges[0].Upper);
            Assert.Equal(new long[] { 300, 100, 200 }, split.EstimatedRows);
        }

        [Fact]
        public void Histogram_null_step_is_never_a_boundary()
        {
            var steps = Steps((null, 0, 500), ("10", 0, 100), ("20", 0, 100));
            TableSplit split = TableSplit.FromHistogram(Col(nullable: true), steps, 2, out _);
            Assert.DoesNotContain(null, split.Boundaries);
            Assert.True(split.Ranges[0].IncludeNulls);
        }

        [Fact]
        public void Histogram_reasons()
        {
            Assert.Null(TableSplit.FromHistogram(Col(), Steps(), 4, out string empty));
            Assert.Equal("no statistics histogram on the clustered index (run UPDATE STATISTICS)", empty);
            Assert.Null(TableSplit.FromHistogram(Col(), Steps(("1", 0, 1000)), 4, out string single));
            Assert.Equal("the key has too few distinct values", single);
        }

        [Fact]
        public void Partition_split_groups_partitions_and_cuts_on_their_boundaries()
        {
            // 7 partitions, the first and last empty (RANGE RIGHT, monthly).
            var boundaries = new[] { "2026-01-01T00:00:00", "2026-02-01T00:00:00", "2026-03-01T00:00:00", "2026-04-01T00:00:00", "2026-05-01T00:00:00", "2026-06-01T00:00:00" };
            var rows = new long[] { 0, 10, 10, 10, 10, 10, 0 };
            TableSplit split = TableSplit.FromPartitions(Col("datetime2"), "pf", true, boundaries, rows, 8, out string reason);
            Assert.Null(reason);
            Assert.Equal(TableSplit.PartitionKind, split.Kind);
            Assert.Equal("pf", split.PartitionFunction);
            Assert.True(split.RangeRight);
            Assert.Equal(5, split.Ranges.Count);
            Assert.Equal(new long[] { 10, 10, 10, 10, 10 }, split.EstimatedRows);
            Assert.Equal(split.Ranges.Skip(1).Select(r => r.Lower), split.Boundaries);
            Assert.Null(split.Ranges[0].Lower);
            Assert.Null(split.Ranges[4].Upper);
        }

        [Fact]
        public void Partition_reasons()
        {
            Assert.Null(TableSplit.FromPartitions(Col(), "pf", true, new[] { "1", null }, new long[] { 1, 1, 1 }, 4, out string nullBoundary));
            Assert.Equal("a partition boundary is NULL", nullBoundary);
            Assert.Null(TableSplit.FromPartitions(Col(), "pf", true, new[] { "1", "2" }, new long[] { 0, 30, 0 }, 4, out string single));
            Assert.Equal("the key has too few distinct values", single);
        }

        [Fact]
        public void Column_reasons()
        {
            Assert.Null(TableSplit.ColumnProblem(Col(), false));
            Assert.Equal("split column type sql_variant is not supported", TableSplit.ColumnProblem(Col("sql_variant"), false));
            Assert.Equal("varchar split column with a code page other than the database's", TableSplit.ColumnProblem(Col("varchar"), true));
            Assert.Equal("split column name contains characters bcp cannot take on its command line", TableSplit.ColumnProblem(Col(name: "a\"b"), false));
        }

        [Theory]
        [InlineData("bigint")]
        [InlineData("decimal(18,4)")]
        [InlineData("nvarchar(30)")]
        [InlineData("varchar(20)")]
        [InlineData("varbinary(max)")]
        [InlineData("datetime2(7)")]
        [InlineData("datetimeoffset(3)")]
        [InlineData("time(0)")]
        public void Column_types_parse_back_from_the_manifest(string sqlType)
        {
            Assert.Equal(sqlType, TableSplit.ParseColumn("K", sqlType).SqlType);
        }

        [Fact]
        public void Manifest_round_trip_rebuilds_the_same_ranges()
        {
            var steps = Steps((null, 0, 5), ("10", 0, 100), ("20", 0, 100), ("30", 0, 100), ("40", 0, 100));
            foreach (bool descending in new[] { false, true })
            {
                TableSplit planned = TableSplit.FromHistogram(Col("decimal", nullable: true, descending: descending), steps, 4, out _);
                planned.Column.Precision = 18;
                planned.Column.Scale = 4;
                AssertSameRanges(planned, TableSplit.FromManifest(planned.ToManifest()));
            }

            var partitioned = TableSplit.FromPartitions(Col("int", nullable: true), "pf", false, new[] { "1", "2", "3", "4" }, new long[] { 5, 5, 0, 5, 5 }, 3, out _);
            ManifestSplit manifest = partitioned.ToManifest();
            Assert.Equal("partition", manifest.Kind);
            Assert.Empty(manifest.Parts);
            Assert.Equal(partitioned.Ranges.Count, manifest.PartCount);
            AssertSameRanges(partitioned, TableSplit.FromManifest(manifest));
        }

        [Fact]
        public void Manifest_without_boundaries_is_rejected()
        {
            var split = new ManifestSplit { Kind = "histogram", Column = "Id", Type = "bigint", PartCount = 3 };
            Assert.Throws<InvalidDataException>(() => TableSplit.FromManifest(split));
        }

        [Fact]
        public void Manifest_estimates_come_from_recorded_parts()
        {
            TableSplit planned = TableSplit.FromHistogram(Col(), Steps(("10", 0, 100), ("20", 0, 100), ("30", 0, 100)), 3, out _);
            ManifestSplit manifest = planned.ToManifest();
            manifest.Parts.Add(ManifestPart.From(planned.Ranges[1], 77, new[] { "f" }));
            Assert.Equal(new long[] { -1, 77, -1 }, TableSplit.FromManifest(manifest).EstimatedRows);
        }

        static void AssertSameRanges(TableSplit expected, TableSplit actual)
        {
            Assert.Equal(expected.Kind, actual.Kind);
            Assert.Equal(expected.Column.SqlType, actual.Column.SqlType);
            Assert.Equal(expected.Ranges.Count, actual.Ranges.Count);
            for (int i = 0; i < expected.Ranges.Count; i++)
            {
                Assert.Equal(expected.Ranges[i].Predicate(expected.Column), actual.Ranges[i].Predicate(actual.Column));
                Assert.Equal(expected.Ranges[i].Part, actual.Ranges[i].Part);
            }
        }
    }
}
