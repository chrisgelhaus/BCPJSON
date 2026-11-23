using Microsoft.Data.SqlClient;
using System.Data;
using System.Globalization;

namespace bcpJson
{
    /// <summary>
    /// How a table is cut into key ranges (--split-tables): the split column, the ranges in part order, the ascending
    /// values the key is cut at and the estimated rows of each part.
    /// </summary>
    internal sealed class TableSplit
    {
        public const string HistogramKind = "histogram";
        public const string PartitionKind = "partition";

        private const string TooFewValues = "the key has too few distinct values";

        public SplitColumn Column { get; set; }

        /// <summary>histogram or partition.</summary>
        public string Kind { get; set; }
        public string PartitionFunction { get; set; }
        public bool? RangeRight { get; set; }

        /// <summary>The Ranges.Count - 1 cut values, ascending whatever the key direction.</summary>
        public List<string> Boundaries { get; set; } = new List<string>();
        public List<KeyRange> Ranges { get; set; } = new List<KeyRange>();

        /// <summary>Estimated rows per range (same order as Ranges); -1 when unknown.</summary>
        public List<long> EstimatedRows { get; set; } = new List<long>();

        /// <summary>Why the column cannot be a split column, or null when it can.</summary>
        internal static string ColumnProblem(SplitColumn column, bool codePageDiffersFromDatabase)
        {
            if (!SplitColumn.IsSplittableType(column.TypeName))
            {
                return string.Format("split column type {0} is not supported", column.TypeName);
            }
            string type = column.TypeName.ToLowerInvariant();
            if ((type == "varchar" || type == "char") && codePageDiffersFromDatabase)
            {
                // Boundaries travel as nvarchar text and are converted with the database's code page.
                return "varchar split column with a code page other than the database's";
            }
            if (!column.HasCommandLineSafeName)
            {
                return "split column name contains characters bcp cannot take on its command line";
            }
            return null;
        }

        /// <summary>
        /// Ranges from the clustered index's statistics histogram (steps in ascending key order, also for a
        /// descending key). Number and date/time keys get boundaries interpolated inside the steps
        /// (<see cref="KeyInterpolation"/>); other keys cut at step keys. Null plus a reason when the histogram is empty
        /// or yields a single range.
        /// </summary>
        internal static TableSplit FromHistogram(SplitColumn column, IReadOnlyList<RangePlanner.HistogramStep> steps, int parts, out string reason)
        {
            reason = null;
            if (steps.Count == 0)
            {
                reason = "no statistics histogram on the clustered index (run UPDATE STATISTICS)";
                return null;
            }

            List<KeyInterpolation.Cut> cuts = KeyInterpolation.Boundaries(column, steps, parts) ?? StepKeyCuts(steps, parts);
            if (cuts.Count == 0)
            {
                reason = TooFewValues;
                return null;
            }

            double total = steps.Sum(s => s.RangeRows + s.EqualRows);
            var estimates = new List<long>();
            for (int i = 0; i <= cuts.Count; i++)
            {
                double from = i == 0 ? 0 : cuts[i - 1].Below;
                double to = i == cuts.Count ? total : cuts[i].Below;
                estimates.Add((long)Math.Round(Math.Max(0, to - from)));
            }
            if (column.Descending)
            {
                estimates.Reverse();
            }

            List<string> boundaries = cuts.Select(c => c.Key).ToList();
            return new TableSplit
            {
                Column = column,
                Kind = HistogramKind,
                Boundaries = boundaries,
                Ranges = RangePlanner.HistogramRanges(boundaries, column.Nullable, column.Descending),
                EstimatedRows = estimates
            };
        }

        /// <summary>Boundaries at histogram step keys, with the rows below each.</summary>
        private static List<KeyInterpolation.Cut> StepKeyCuts(IReadOnlyList<RangePlanner.HistogramStep> steps, int parts)
        {
            var below = new double[steps.Count];
            double running = 0;
            for (int j = 0; j < steps.Count; j++)
            {
                below[j] = running + steps[j].RangeRows;
                running += steps[j].RangeRows + steps[j].EqualRows;
            }
            // A NULL step (nullable key) holds the NULLs, which go with the first range; it is never a boundary.
            return RangePlanner.ChooseHistogramSteps(steps, parts)
                .Where(i => steps[i].Key != null)
                .Select(i => new KeyInterpolation.Cut(steps[i].Key, below[i]))
                .ToList();
        }

        /// <summary>
        /// Ranges along partition boundaries: contiguous partitions grouped by their row counts. boundaryValues are the
        /// partition function's values in boundary_id order (partitions - 1 of them). Null plus a reason when a
        /// boundary is NULL or everything lands in one group.
        /// </summary>
        internal static TableSplit FromPartitions(SplitColumn column, string partitionFunction, bool rangeRight,
            IReadOnlyList<string> boundaryValues, IReadOnlyList<long> rowsPerPartition, int parts, out string reason)
        {
            reason = null;
            if (boundaryValues.Any(v => v == null))
            {
                reason = "a partition boundary is NULL";
                return null;
            }
            if (rowsPerPartition.Count != boundaryValues.Count + 1)
            {
                throw new InvalidOperationException(string.Format(CultureInfo.InvariantCulture,
                    "The table has {0} partitions but its partition function {1} boundaries.", rowsPerPartition.Count, boundaryValues.Count));
            }

            List<(int First, int Last)> groups = RangePlanner.GroupPartitions(rowsPerPartition, parts);
            if (groups.Count < 2)
            {
                reason = TooFewValues;
                return null;
            }

            return new TableSplit
            {
                Column = column,
                Kind = PartitionKind,
                PartitionFunction = partitionFunction,
                RangeRight = rangeRight,
                Boundaries = groups.Take(groups.Count - 1).Select(g => boundaryValues[g.Last - 1]).ToList(),
                Ranges = RangePlanner.PartitionRanges(boundaryValues, rangeRight, groups, column.Nullable),
                EstimatedRows = groups.Select(g => Enumerable.Range(g.First, g.Last - g.First + 1).Sum(p => rowsPerPartition[p - 1])).ToList()
            };
        }

        /// <summary>The manifest's description of this split, with no parts recorded yet.</summary>
        public ManifestSplit ToManifest()
        {
            return new ManifestSplit
            {
                Kind = this.Kind,
                Column = this.Column.Name,
                Type = this.Column.SqlType,
                Descending = this.Column.Descending,
                Nullable = this.Column.Nullable,
                PartitionFunction = this.PartitionFunction,
                RangeRight = this.RangeRight,
                PartCount = this.Ranges.Count,
                Boundaries = new List<string>(this.Boundaries),
                Parts = new List<ManifestPart>()
            };
        }

        /// <summary>
        /// The split recorded in a manifest (--resume reuses it, whatever the data looks like now). Estimates are the
        /// recorded rows of each part, -1 for parts not recorded. Throws InvalidDataException when it is incomplete.
        /// </summary>
        public static TableSplit FromManifest(ManifestSplit split)
        {
            if (split.Boundaries == null || split.PartCount < 2 || split.Boundaries.Count != split.PartCount - 1 || split.Boundaries.Any(b => b == null))
            {
                throw new InvalidDataException(string.Format(CultureInfo.InvariantCulture,
                    "The manifest's split of column {0} does not list its {1} part(s)' boundaries.", split.Column, split.PartCount));
            }

            SplitColumn column = ParseColumn(split.Column, split.Type);
            column.Nullable = split.Nullable;
            column.Descending = split.Descending;

            List<KeyRange> ranges;
            if (string.Equals(split.Kind, HistogramKind, StringComparison.Ordinal))
            {
                ranges = RangePlanner.HistogramRanges(split.Boundaries, column.Nullable, column.Descending);
            }
            else if (string.Equals(split.Kind, PartitionKind, StringComparison.Ordinal) && split.RangeRight.HasValue)
            {
                // The cut values act as the boundaries of PartCount partitions, one per part.
                var groups = Enumerable.Range(1, split.PartCount).Select(p => (p, p)).ToList();
                ranges = RangePlanner.PartitionRanges(split.Boundaries, split.RangeRight.Value, groups, column.Nullable);
            }
            else
            {
                throw new InvalidDataException(string.Format("The manifest's split kind '{0}' is not known.", split.Kind));
            }

            return new TableSplit
            {
                Column = column,
                Kind = split.Kind,
                PartitionFunction = split.PartitionFunction,
                RangeRight = split.RangeRight,
                Boundaries = new List<string>(split.Boundaries),
                Ranges = ranges,
                EstimatedRows = ranges.Select(r => split.Parts?.FirstOrDefault(p => p.Part == r.Part)?.Rows ?? -1).ToList()
            };
        }

        /// <summary>A split column from its name and T-SQL type as the manifest records it (bigint, decimal(18,4), nvarchar(30)...).</summary>
        internal static SplitColumn ParseColumn(string name, string sqlType)
        {
            string text = (sqlType ?? string.Empty).Trim();
            int open = text.IndexOf('(');
            string typeName = open < 0 ? text : text.Substring(0, open);
            string[] args = open < 0 || !text.EndsWith(")", StringComparison.Ordinal)
                ? Array.Empty<string>()
                : text.Substring(open + 1, text.Length - open - 2).Split(',').Select(a => a.Trim()).ToArray();

            var column = new SplitColumn { Name = name, TypeName = typeName };
            try
            {
                switch (typeName.ToLowerInvariant())
                {
                    case "decimal":
                    case "numeric":
                        column.Precision = byte.Parse(args[0], CultureInfo.InvariantCulture);
                        column.Scale = byte.Parse(args[1], CultureInfo.InvariantCulture);
                        break;
                    case "datetime2":
                    case "datetimeoffset":
                    case "time":
                        column.Scale = byte.Parse(args[0], CultureInfo.InvariantCulture);
                        break;
                    case "nchar":
                    case "nvarchar":
                        column.MaxLength = IsMax(args[0]) ? (short)-1 : (short)(2 * short.Parse(args[0], CultureInfo.InvariantCulture));
                        break;
                    case "char":
                    case "varchar":
                    case "binary":
                    case "varbinary":
                        column.MaxLength = IsMax(args[0]) ? (short)-1 : short.Parse(args[0], CultureInfo.InvariantCulture);
                        break;
                }
            }
            catch (Exception ex) when (ex is IndexOutOfRangeException || ex is FormatException || ex is OverflowException)
            {
                throw new InvalidDataException(string.Format("The manifest's split column type '{0}' cannot be read.", sqlType), ex);
            }

            if (!SplitColumn.IsSplittableType(typeName) || !string.Equals(column.SqlType, text, StringComparison.OrdinalIgnoreCase))
            {
                throw new InvalidDataException(string.Format("The manifest's split column type '{0}' cannot be read.", sqlType));
            }
            return column;
        }

        private static bool IsMax(string arg)
        {
            return string.Equals(arg, "max", StringComparison.OrdinalIgnoreCase);
        }
    }

    partial class Program
    {
        /// <summary>
        /// Rows per table (object_id), from sys.partitions (heap or clustered index). Sizes the export work items.
        /// </summary>
        internal static Dictionary<int, long> TableRowCounts(SqlConnection conn)
        {
            const string sql = @"SET NOCOUNT ON;
SELECT p.object_id, SUM(p.rows) FROM sys.partitions p WHERE p.index_id IN (0, 1) GROUP BY p.object_id;";
            var counts = new Dictionary<int, long>();
            using (var cmd = new SqlCommand(sql, conn))
            {
                cmd.CommandTimeout = 0;
                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        counts[reader.GetInt32(0)] = Convert.ToInt64(reader.GetValue(1), CultureInfo.InvariantCulture);
                    }
                }
            }
            return counts;
        }

        /// <summary>
        /// Plans the key ranges of a table: along partition boundaries when the table (heap or clustered index) is
        /// partitioned, otherwise from the statistics histogram of a clustered rowstore index on its first key column.
        /// Returns null and the reason when the table cannot be split.
        /// </summary>
        internal static TableSplit PlanSplit(SqlConnection conn, TableRef table, int parts, out string reason)
        {
            const string shapeSql = @"SET NOCOUNT ON;
SELECT TOP (1) i.index_id, i.type, ds.type, pf.function_id, pf.name, pf.boundary_value_on_right
FROM sys.indexes i
     INNER JOIN sys.data_spaces ds ON ds.data_space_id = i.data_space_id
     LEFT JOIN sys.partition_schemes ps ON ps.data_space_id = i.data_space_id
     LEFT JOIN sys.partition_functions pf ON pf.function_id = ps.function_id
WHERE i.object_id = @objectId AND i.index_id IN (0, 1)
ORDER BY i.index_id DESC;";

            int indexId;
            byte indexType;
            bool partitioned;
            int functionId = 0;
            string functionName = null;
            bool rangeRight = false;
            using (var cmd = new SqlCommand(shapeSql, conn))
            {
                cmd.Parameters.Add("@objectId", SqlDbType.Int).Value = table.ObjectId;
                using (var reader = cmd.ExecuteReader())
                {
                    if (!reader.Read())
                    {
                        throw new InvalidOperationException("Could not read the table's storage.");
                    }
                    indexId = reader.GetInt32(0);
                    indexType = reader.GetByte(1);
                    partitioned = string.Equals(reader.GetString(2).Trim(), "PS", StringComparison.OrdinalIgnoreCase) && !reader.IsDBNull(3);
                    if (partitioned)
                    {
                        functionId = reader.GetInt32(3);
                        functionName = reader.GetString(4);
                        rangeRight = reader.GetBoolean(5);
                    }
                }
            }

            if (!partitioned && indexType != 1)
            {
                reason = "no clustered rowstore index and not partitioned";
                return null;
            }

            SplitColumn column = ReadSplitColumn(conn, table.ObjectId, indexId, partitioned, out bool codePageDiffers);
            reason = TableSplit.ColumnProblem(column, codePageDiffers);
            if (reason != null)
            {
                return null;
            }

            // The type comes from the catalog and passed IsSplittableType, so it is safe to put into the SQL text.
            string style = column.ConvertStyle.HasValue ? ", " + column.ConvertStyle.Value.ToString(CultureInfo.InvariantCulture) : string.Empty;
            if (partitioned)
            {
                string boundarySql = "SET NOCOUNT ON; SELECT CONVERT(nvarchar(4000), CAST(prv.value AS " + column.SqlType + ")" + style + @")
FROM sys.partition_range_values prv WHERE prv.function_id = @functionId ORDER BY prv.boundary_id;";
                var boundaries = new List<string>();
                using (var cmd = new SqlCommand(boundarySql, conn))
                {
                    cmd.Parameters.Add("@functionId", SqlDbType.Int).Value = functionId;
                    using (var reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            boundaries.Add(reader.IsDBNull(0) ? null : reader.GetString(0));
                        }
                    }
                }

                const string rowsSql = @"SET NOCOUNT ON;
SELECT p.partition_number, SUM(p.rows) FROM sys.partitions p
WHERE p.object_id = @objectId AND p.index_id IN (0, 1)
GROUP BY p.partition_number ORDER BY p.partition_number;";
                var rows = new List<long>();
                using (var cmd = new SqlCommand(rowsSql, conn))
                {
                    cmd.Parameters.Add("@objectId", SqlDbType.Int).Value = table.ObjectId;
                    using (var reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            rows.Add(Convert.ToInt64(reader.GetValue(1), CultureInfo.InvariantCulture));
                        }
                    }
                }

                return TableSplit.FromPartitions(column, functionName, rangeRight, boundaries, rows, parts, out reason);
            }

            // The clustered index's statistics have stats_id 1. Steps come in ascending key order, also for a DESC key.
            string histogramSql = "SET NOCOUNT ON; SELECT CONVERT(nvarchar(4000), CAST(h.range_high_key AS " + column.SqlType + ")" + style + @"), h.range_rows, h.equal_rows
FROM sys.dm_db_stats_histogram(@objectId, 1) h ORDER BY h.step_number;";
            var steps = new List<RangePlanner.HistogramStep>();
            using (var cmd = new SqlCommand(histogramSql, conn))
            {
                cmd.Parameters.Add("@objectId", SqlDbType.Int).Value = table.ObjectId;
                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        steps.Add(new RangePlanner.HistogramStep(
                            reader.IsDBNull(0) ? null : reader.GetString(0),
                            Convert.ToDouble(reader.GetValue(1), CultureInfo.InvariantCulture),
                            Convert.ToDouble(reader.GetValue(2), CultureInfo.InvariantCulture)));
                    }
                }
            }
            return TableSplit.FromHistogram(column, steps, parts, out reason);
        }

        /// <summary>
        /// The split column: the partitioning column (partition_ordinal 1, ascending) or the first clustered key column.
        /// codePageDiffers is set when a string column's collation uses another code page than the database's.
        /// </summary>
        private static SplitColumn ReadSplitColumn(SqlConnection conn, int objectId, int indexId, bool partitioned, out bool codePageDiffers)
        {
            string sql = @"SET NOCOUNT ON;
SELECT c.name, TYPE_NAME(c.system_type_id), c.max_length, c.precision, c.scale, c.is_nullable, ic.is_descending_key, c.collation_name,
       CASE WHEN c.collation_name IS NOT NULL
                 AND CAST(COLLATIONPROPERTY(c.collation_name, 'CodePage') AS int)
                     <> CAST(COLLATIONPROPERTY(CONVERT(sysname, DATABASEPROPERTYEX(DB_NAME(), 'Collation')), 'CodePage') AS int)
            THEN 1 ELSE 0 END
FROM sys.index_columns ic
     INNER JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
WHERE ic.object_id = @objectId AND ic.index_id = @indexId AND " + (partitioned ? "ic.partition_ordinal = 1" : "ic.key_ordinal = 1") + ";";

            using (var cmd = new SqlCommand(sql, conn))
            {
                cmd.Parameters.Add("@objectId", SqlDbType.Int).Value = objectId;
                cmd.Parameters.Add("@indexId", SqlDbType.Int).Value = indexId;
                using (var reader = cmd.ExecuteReader())
                {
                    if (!reader.Read())
                    {
                        throw new InvalidOperationException(partitioned ? "Could not find the partitioning column." : "Could not find the clustered index key.");
                    }
                    codePageDiffers = reader.GetInt32(8) == 1;
                    return new SplitColumn
                    {
                        Name = reader.GetString(0),
                        TypeName = reader.GetString(1),
                        MaxLength = reader.GetInt16(2),
                        Precision = reader.GetByte(3),
                        Scale = reader.GetByte(4),
                        Nullable = reader.GetBoolean(5),
                        // Partition ranges always run in ascending partition order.
                        Descending = !partitioned && reader.GetBoolean(6),
                        Collation = reader.IsDBNull(7) ? null : reader.GetString(7)
                    };
                }
            }
        }
    }
}
