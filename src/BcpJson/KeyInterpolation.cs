using System.Globalization;

namespace bcpJson
{
    /// <summary>
    /// Boundaries inside histogram steps for number and date/time split columns. A step stands for RANGE_ROWS rows spread
    /// evenly between the previous step's key and its own, plus EQ_ROWS rows on its key. A target row count that falls
    /// inside that spread gets a key interpolated there, so an identity-like key, whose histogram has only a few steps,
    /// still splits into the parts asked for.
    /// </summary>
    internal static class KeyInterpolation
    {
        /// <summary>A boundary key as text (accepted by CONVERT(type, text, ConvertStyle)) and the estimated rows below it.</summary>
        internal sealed record Cut(string Key, double Below);

        /// <summary>How the keys of one type map to numbers (decimal: whole numbers, or ticks for dates) and back to text.</summary>
        private sealed class Scale
        {
            public Func<string, decimal?> Parse;
            public decimal Unit;
            public Func<decimal, string, string> Format;   // value and the text of the step key below it (for its offset)
        }

        /// <summary>
        /// Ascending boundaries for <paramref name="parts"/> parts, or null when the column type is not interpolated or
        /// a key cannot be read (for example a decimal(38) beyond System.Decimal); callers then use the step keys.
        /// </summary>
        internal static List<Cut> Boundaries(SplitColumn column, IReadOnlyList<RangePlanner.HistogramStep> steps, int parts)
        {
            Scale scale = ScaleFor(column);
            if (scale == null)
            {
                return null;
            }

            try
            {
                var positions = new decimal?[steps.Count];
                var keyBelow = new double[steps.Count];       // rows below each step key
                var rangeStart = new double[steps.Count];     // rows below the first row of each step's range
                double total = 0;
                for (int j = 0; j < steps.Count; j++)
                {
                    if (steps[j].Key != null)
                    {
                        positions[j] = scale.Parse(steps[j].Key);
                        if (positions[j] == null)
                        {
                            return null;
                        }
                    }
                    rangeStart[j] = total;
                    keyBelow[j] = total + steps[j].RangeRows;
                    total += steps[j].RangeRows + steps[j].EqualRows;
                }

                var cuts = new List<Cut>();
                decimal? last = null;
                for (int k = 1; k < parts; k++)
                {
                    double target = k * total / parts;
                    (string Key, decimal Position, double Below)? cut = Interpolate(steps, positions, rangeStart, scale, target, last)
                        ?? NearestStepKey(steps, positions, keyBelow, total, target, last);
                    if (cut == null || cut.Value.Below <= 0 || cut.Value.Below >= total)
                    {
                        continue;
                    }
                    cuts.Add(new Cut(cut.Value.Key, cut.Value.Below));
                    last = cut.Value.Position;
                }
                return cuts;
            }
            catch (Exception ex) when (ex is OverflowException || ex is ArgumentOutOfRangeException)
            {
                return null;
            }
        }

        /// <summary>A key strictly inside the step range holding the target, after rounding to the type, or null.</summary>
        private static (string, decimal, double)? Interpolate(IReadOnlyList<RangePlanner.HistogramStep> steps, decimal?[] positions,
            double[] rangeStart, Scale scale, double target, decimal? last)
        {
            int previous = -1;
            for (int j = 0; j < steps.Count; j++)
            {
                if (positions[j] == null)
                {
                    continue;
                }
                double rows = steps[j].RangeRows;
                if (previous >= 0 && rows > 0 && target > rangeStart[j] && target < rangeStart[j] + rows)
                {
                    decimal low = positions[previous].Value, high = positions[j].Value;
                    decimal fraction = (decimal)((target - rangeStart[j]) / rows);
                    decimal value = Math.Round((low + (high - low) * fraction) / scale.Unit, 0, MidpointRounding.AwayFromZero) * scale.Unit;
                    if (value <= low || value >= high || (last.HasValue && value <= last.Value))
                    {
                        return null;
                    }
                    double below = rangeStart[j] + rows * (double)((value - low) / (high - low));
                    return (scale.Format(value, steps[previous].Key), value, below);
                }
                previous = j;
            }
            return null;
        }

        /// <summary>The step key whose rows-below is closest to the target (ties: the lower), above the last boundary.</summary>
        private static (string, decimal, double)? NearestStepKey(IReadOnlyList<RangePlanner.HistogramStep> steps, decimal?[] positions,
            double[] keyBelow, double total, double target, decimal? last)
        {
            int best = -1;
            for (int j = 0; j < steps.Count; j++)
            {
                if (positions[j] == null || keyBelow[j] <= 0 || keyBelow[j] >= total || (last.HasValue && positions[j].Value <= last.Value))
                {
                    continue;
                }
                if (best < 0 || Math.Abs(keyBelow[j] - target) < Math.Abs(keyBelow[best] - target))
                {
                    best = j;
                }
            }
            return best < 0 ? null : (steps[best].Key, positions[best].Value, keyBelow[best]);
        }

        private static Scale ScaleFor(SplitColumn column)
        {
            switch (column.TypeName.ToLowerInvariant())
            {
                case "tinyint":
                case "smallint":
                case "int":
                case "bigint":
                    return NumberScale(0);
                case "decimal":
                case "numeric":
                    return column.Scale <= 28 ? NumberScale(column.Scale) : null;
                case "money":
                case "smallmoney":
                    return NumberScale(4);
                case "date":
                    return DateScale(TimeSpan.TicksPerDay, "yyyy'-'MM'-'dd");
                case "smalldatetime":
                    return DateScale(TimeSpan.TicksPerMinute, "yyyy'-'MM'-'dd'T'HH':'mm':'ss");
                case "datetime":
                    // Multiples of 10 ms are exact datetime values (1/300 s ticks), so the text survives CONVERT unchanged.
                    return DateScale(10 * TimeSpan.TicksPerMillisecond, "yyyy'-'MM'-'dd'T'HH':'mm':'ss'.'fff");
                case "datetime2":
                    return column.Scale <= 7 ? DateScale(FractionUnit(column.Scale), TimeFormat(column.Scale)) : null;
                case "datetimeoffset":
                    return column.Scale <= 7 ? OffsetScale(FractionUnit(column.Scale), TimeFormat(column.Scale) + "zzz") : null;
                default:
                    return null;   // strings, uniqueidentifier, binary, bit, float/real, time: step keys
            }
        }

        private static long FractionUnit(int scale)
        {
            long unit = 1;
            for (int i = scale; i < 7; i++)
            {
                unit *= 10;
            }
            return unit;
        }

        private static string TimeFormat(int scale)
        {
            return "yyyy'-'MM'-'dd'T'HH':'mm':'ss" + (scale > 0 ? "'.'" + new string('f', scale) : string.Empty);
        }

        private static Scale NumberScale(int digits)
        {
            return new Scale
            {
                Parse = text => decimal.TryParse(text, NumberStyles.AllowLeadingSign | NumberStyles.AllowDecimalPoint, CultureInfo.InvariantCulture, out decimal v) ? v : (decimal?)null,
                Unit = digits == 0 ? 1m : 1m / Pow10(digits),
                Format = (value, _) => value.ToString("F" + digits.ToString(CultureInfo.InvariantCulture), CultureInfo.InvariantCulture)
            };
        }

        private static decimal Pow10(int digits)
        {
            decimal p = 1m;
            for (int i = 0; i < digits; i++)
            {
                p *= 10m;
            }
            return p;
        }

        private static Scale DateScale(long unitTicks, string format)
        {
            return new Scale
            {
                Parse = text => DateTime.TryParse(text, CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime d) && !text.Contains('+') && !text.EndsWith("Z", StringComparison.Ordinal)
                    ? d.Ticks : (decimal?)null,
                Unit = unitTicks,
                Format = (value, _) => new DateTime((long)value, DateTimeKind.Unspecified).ToString(format, CultureInfo.InvariantCulture)
            };
        }

        private static Scale OffsetScale(long unitTicks, string format)
        {
            return new Scale
            {
                Parse = text => DateTimeOffset.TryParse(text, CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTimeOffset d) ? d.UtcTicks : (decimal?)null,
                Unit = unitTicks,
                Format = (value, lowerKey) =>
                {
                    // The boundary is a UTC instant; it is written in the offset of the key below it.
                    TimeSpan offset = DateTimeOffset.Parse(lowerKey, CultureInfo.InvariantCulture, DateTimeStyles.None).Offset;
                    return new DateTimeOffset((long)value, TimeSpan.Zero).ToOffset(offset).ToString(format, CultureInfo.InvariantCulture);
                }
            };
        }
    }
}
