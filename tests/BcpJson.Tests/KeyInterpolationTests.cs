using System.Globalization;
using System.Text.RegularExpressions;

namespace bcpJson.Tests
{
    public class KeyInterpolationTests
    {
        static SplitColumn Col(string type, byte scale = 0, byte precision = 0, short length = 8) =>
            new SplitColumn { Name = "K", TypeName = type, Scale = scale, Precision = precision, MaxLength = length };

        static List<RangePlanner.HistogramStep> Steps(params (string key, double range, double eq)[] s) =>
            s.Select(x => new RangePlanner.HistogramStep(x.key, x.range, x.eq)).ToList();

        // What FULLSCAN gives an identity-like key 1..200000: three steps.
        static List<RangePlanner.HistogramStep> Identity() => Steps(("1", 0, 1), ("100000", 99998, 1), ("200000", 99999, 1));

        [Fact]
        public void Evenly_spaced_bigint_gets_every_part()
        {
            var cuts = KeyInterpolation.Boundaries(Col("bigint"), Identity(), 8);
            Assert.Equal(7, cuts.Count);
            for (int k = 1; k <= 7; k++)
            {
                long value = long.Parse(cuts[k - 1].Key, CultureInfo.InvariantCulture);   // whole numbers, no decimal point
                Assert.InRange(value, k * 25000L - 2, k * 25000L + 2);
                Assert.InRange(cuts[k - 1].Below, k * 25000 - 2, k * 25000 + 2);
            }
            Assert.Equal(cuts.Select(c => long.Parse(c.Key, CultureInfo.InvariantCulture)).OrderBy(v => v).Distinct(), cuts.Select(c => long.Parse(c.Key, CultureInfo.InvariantCulture)));
        }

        [Fact]
        public void Identity_key_splits_into_the_requested_parts()
        {
            TableSplit split = TableSplit.FromHistogram(Col("bigint"), Identity(), 8, out string reason);
            Assert.Null(reason);
            Assert.Equal(8, split.Ranges.Count);
            Assert.All(split.EstimatedRows, r => Assert.InRange(r, 24990, 25010));
        }

        [Fact]
        public void Bigint_keeps_full_precision()
        {
            var steps = Steps(("9223372036854775000", 0, 1), ("9223372036854775800", 799, 1));
            var cuts = KeyInterpolation.Boundaries(Col("bigint"), steps, 2);
            Assert.Equal("9223372036854775400", cuts.Single().Key);
        }

        [Fact]
        public void Datetime2_boundaries_have_the_scale_digits()
        {
            var steps = Steps(("2026-01-01T00:00:00", 0, 1), ("2026-01-02T00:00:00", 999, 1));
            var cuts = KeyInterpolation.Boundaries(Col("datetime2", scale: 7), steps, 2);
            string key = cuts.Single().Key;
            Assert.Matches(new Regex(@"^2026-01-01T1[12]:\d\d:\d\d\.\d{7}$"), key);

            var three = KeyInterpolation.Boundaries(Col("datetime2", scale: 3), steps, 2).Single().Key;
            Assert.Matches(new Regex(@"^2026-01-01T1[12]:\d\d:\d\d\.\d{3}$"), three);
            var zero = KeyInterpolation.Boundaries(Col("datetime2", scale: 0), steps, 2).Single().Key;
            Assert.Matches(new Regex(@"^2026-01-01T1[12]:\d\d:\d\d$"), zero);
        }

        [Fact]
        public void Datetime_boundaries_have_three_digits_on_exact_values()
        {
            var steps = Steps(("2026-01-01T00:00:00", 0, 1), ("2026-01-01T00:00:01.997", 998, 1));
            string key = KeyInterpolation.Boundaries(Col("datetime"), steps, 2).Single().Key;
            Assert.Matches(new Regex(@"^2026-01-01T00:00:0[01]\.\d\d0$"), key);   // multiples of 10 ms are exact datetime values
        }

        [Fact]
        public void Other_date_types_and_offsets()
        {
            var days = Steps(("2026-01-01", 0, 1), ("2026-12-31", 3640, 10));
            Assert.Matches(new Regex(@"^2026-0[67]-\d\d$"), KeyInterpolation.Boundaries(Col("date"), days, 2).Single().Key);

            var minutes = Steps(("2026-01-01T00:00:00", 0, 1), ("2026-01-01T10:00:00", 599, 1));
            Assert.Matches(new Regex(@"^2026-01-01T0[45]:\d\d:00$"), KeyInterpolation.Boundaries(Col("smalldatetime"), minutes, 2).Single().Key);

            var offsets = Steps(("2026-01-01T00:00:00.000+05:30", 0, 1), ("2026-01-02T00:00:00.000+05:30", 999, 1));
            string key = KeyInterpolation.Boundaries(Col("datetimeoffset", scale: 3), offsets, 2).Single().Key;
            Assert.Matches(new Regex(@"^2026-01-01T1[12]:\d\d:\d\d\.\d{3}\+05:30$"), key);
        }

        [Fact]
        public void Decimal_and_money_round_to_their_scale()
        {
            var steps = Steps(("0.0000", 0, 1), ("1.0000", 999, 1));
            Assert.Matches(new Regex(@"^0\.5\d{3}$"), KeyInterpolation.Boundaries(Col("decimal", scale: 4, precision: 18), steps, 2).Single().Key);
            Assert.Matches(new Regex(@"^0\.5\d{3}$"), KeyInterpolation.Boundaries(Col("money"), steps, 2).Single().Key);
        }

        [Fact]
        public void Boundaries_stay_strictly_inside_their_step()
        {
            // Keys 1 and 2: no whole number between them, so the range rows cannot be cut there; the step key is the
            // only usable boundary, and it is used once.
            var steps = Steps(("1", 0, 1), ("2", 1000, 1));
            Assert.Equal(new[] { "2" }, KeyInterpolation.Boundaries(Col("int"), steps, 4).Select(c => c.Key));
        }

        [Fact]
        public void Equal_rows_fall_back_to_the_nearest_step_key()
        {
            var steps = Steps(("10", 0, 100), ("20", 0, 100), ("30", 0, 100), ("40", 0, 300));
            Assert.Equal(new[] { "30", "40" }, KeyInterpolation.Boundaries(Col("int"), steps, 3).Select(c => c.Key));
        }

        [Fact]
        public void Strings_and_out_of_range_numbers_use_step_keys()
        {
            Assert.Null(KeyInterpolation.Boundaries(Col("varchar", length: 20), Steps(("a", 0, 1), ("m", 500, 1)), 2));
            Assert.Null(KeyInterpolation.Boundaries(Col("float"), Steps(("1", 0, 1), ("9", 500, 1)), 2));
            var huge = Steps(("0", 0, 1), ("99999999999999999999999999999999999999", 500, 1));
            Assert.Null(KeyInterpolation.Boundaries(Col("decimal", precision: 38), huge, 2));

            TableSplit split = TableSplit.FromHistogram(Col("varchar", length: 20), Steps(("a", 0, 1), ("m", 500, 1), ("z", 500, 1)), 2, out _);
            Assert.Equal(new[] { "m" }, split.Boundaries);
        }

        [Fact]
        public void Planning_failure_exports_the_table_whole()
        {
            Assert.Null(Program.SplitOrWhole("dbo.T", () => throw new InvalidOperationException("boom.")));
            Assert.Null(Program.SplitOrWhole("dbo.T", () => (null, "no reason")));
            var planned = TableSplit.FromHistogram(Col("bigint"), Identity(), 2, out _);
            Assert.Same(planned, Program.SplitOrWhole("dbo.T", () => (planned, null)));
        }
    }
}
