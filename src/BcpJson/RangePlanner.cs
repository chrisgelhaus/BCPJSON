namespace bcpJson
{
    /// <summary>Chooses key ranges for splitting a table from statistics histograms or partition boundaries.</summary>
    internal static class RangePlanner
    {
        internal sealed record HistogramStep(string Key, double RangeRows, double EqualRows);

        /// <summary>
        /// Indexes of the histogram steps whose keys become range boundaries. Each boundary is the step whose
        /// estimated rows-below is closest to an even share of the total; ties take the lower step. Fewer than
        /// parts - 1 indexes come back when the histogram cannot be split that finely.
        /// </summary>
        public static List<int> ChooseHistogramSteps(IReadOnlyList<HistogramStep> steps, int parts)
        {
            var below = new double[steps.Count];
            double running = 0;
            for (int j = 0; j < steps.Count; j++)
            {
                below[j] = running + steps[j].RangeRows;
                running += steps[j].RangeRows + steps[j].EqualRows;
            }
            double total = running;

            var chosen = new List<int>();
            for (int k = 1; k < parts; k++)
            {
                double target = k * total / parts;
                int last = chosen.Count == 0 ? -1 : chosen[chosen.Count - 1];
                int best = -1;
                for (int j = last + 1; j < steps.Count; j++)
                {
                    if (below[j] <= 0 || below[j] >= total) continue;
                    if (last >= 0 && below[j] <= below[last]) continue;
                    if (best < 0 || Math.Abs(below[j] - target) < Math.Abs(below[best] - target))
                        best = j;
                }
                if (best < 0) break;
                chosen.Add(best);
            }
            return chosen;
        }

        /// <summary>
        /// Ranges for ascending boundary keys: half-open [lower, upper), the first open below and the last open above.
        /// Parts are numbered in key order, or from the highest values when the key is descending. NULLs go with
        /// the range that has no lower bound (they sort first ascending, last descending).
        /// </summary>
        public static List<KeyRange> HistogramRanges(IReadOnlyList<string> boundaries, bool nullable, bool descending)
        {
            int count = boundaries.Count + 1;
            var ranges = new List<KeyRange>(count);
            for (int i = 0; i < count; i++)
            {
                ranges.Add(new KeyRange
                {
                    Lower = i == 0 ? null : boundaries[i - 1],
                    LowerInclusive = i > 0,
                    Upper = i == count - 1 ? null : boundaries[i],
                    UpperInclusive = false,
                    IncludeNulls = nullable && i == 0
                });
            }
            if (descending)
                ranges.Reverse();
            for (int i = 0; i < ranges.Count; i++)
                ranges[i].Part = i + 1;
            return ranges;
        }

        /// <summary>
        /// Groups consecutive partitions (1-based, inclusive) into at most parts groups of similar row counts.
        /// </summary>
        public static List<(int First, int Last)> GroupPartitions(IReadOnlyList<long> rowsPerPartition, int parts)
        {
            var groups = new List<(int First, int Last)>();
            var groupRows = new List<long>();
            long remaining = 0;
            foreach (long rows in rowsPerPartition) remaining += rows;

            int first = 1;
            long current = 0;
            for (int p = 1; p <= rowsPerPartition.Count; p++)
            {
                current += rowsPerPartition[p - 1];
                int groupsLeft = parts - groups.Count;
                bool rowsAfter = remaining - current > 0;
                if (groupsLeft > 1 && rowsAfter && current > 0 && current >= (double)remaining / groupsLeft)
                {
                    groups.Add((first, p));
                    groupRows.Add(current);
                    remaining -= current;
                    current = 0;
                    first = p + 1;
                }
            }
            if (first <= rowsPerPartition.Count)
            {
                groups.Add((first, rowsPerPartition.Count));
                groupRows.Add(current);
            }

            // A trailing group without rows adds nothing: fold it into the previous one.
            while (groups.Count > 1 && groupRows[groups.Count - 1] == 0)
            {
                int n = groups.Count;
                groups[n - 2] = (groups[n - 2].First, groups[n - 1].Last);
                groups.RemoveAt(n - 1);
                groupRows.RemoveAt(n - 1);
            }
            return groups;
        }

        /// <summary>
        /// Ranges for groups of partitions. boundaryValues holds the partition function boundaries (partitions - 1).
        /// RANGE RIGHT: lower bound inclusive, upper exclusive; RANGE LEFT: lower exclusive, upper inclusive.
        /// NULLs sort into partition 1, so they go with the group holding it.
        /// </summary>
        public static List<KeyRange> PartitionRanges(IReadOnlyList<string> boundaryValues, bool rangeRight,
            IReadOnlyList<(int First, int Last)> groups, bool nullable)
        {
            int partitionCount = boundaryValues.Count + 1;
            var ranges = new List<KeyRange>(groups.Count);
            for (int i = 0; i < groups.Count; i++)
            {
                var (first, last) = groups[i];
                ranges.Add(new KeyRange
                {
                    Part = i + 1,
                    Lower = first > 1 ? boundaryValues[first - 2] : null,
                    LowerInclusive = first > 1 && rangeRight,
                    Upper = last < partitionCount ? boundaryValues[last - 1] : null,
                    UpperInclusive = last < partitionCount && !rangeRight,
                    IncludeNulls = nullable && first == 1
                });
            }
            return ranges;
        }
    }
}
