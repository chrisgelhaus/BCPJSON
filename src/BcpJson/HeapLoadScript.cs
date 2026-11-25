namespace bcpJson
{
    /// <summary>Names and text of the recovery script and markers used by import --load-as-heap.</summary>
    internal static class HeapLoadScript
    {
        internal const string Suffix = ".load-as-heap.sql";
        internal const string MarkerPrefix = "bcpjson_load_as_heap_";

        internal static string FileName(string schema, string table)
        {
            return schema + "." + table + Suffix;
        }

        internal static string MarkerName(int ordinal)
        {
            return MarkerPrefix + ordinal.ToString("000", System.Globalization.CultureInfo.InvariantCulture);
        }

        /// <summary>
        /// The recovery script: each saved index (creation order) created only if missing, then every marker dropped.
        /// Rerunnable; every name reaches the SQL through <see cref="Program.SqlLiteral"/> / <see cref="Program.QuoteName"/>.
        /// </summary>
        internal static string Build(string schema, string table, IEnumerable<(string Name, string Statement)> indexes, DateTime utc)
        {
            string objectLiteral = Program.SqlLiteral(Program.QuoteName(schema) + "." + Program.QuoteName(table));
            var sb = new System.Text.StringBuilder();
            sb.Append("-- Written by bcpJson import --load-as-heap for ").Append(schema).Append('.').Append(table).Append(" at ")
                .Append(utc.ToString("yyyy-MM-dd HH:mm:ss", System.Globalization.CultureInfo.InvariantCulture)).Append(" UTC.\n");
            sb.Append("-- Run this script to recreate the indexes if the import was interrupted. It is rerunnable:\n");
            sb.Append("-- each index is created only if it is missing, and the markers are removed at the end.\n");
            int count = 0;
            foreach ((string name, string statement) in indexes)
            {
                count++;
                sb.Append('\n')
                    .Append("IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(").Append(objectLiteral)
                    .Append(") AND name = ").Append(Program.SqlLiteral(name)).Append(")\n")
                    .Append("BEGIN\n").Append(statement).Append('\n').Append("END\n").Append("GO\n");
            }
            for (int i = 1; i <= count; i++)
            {
                string marker = Program.SqlLiteral(MarkerName(i));
                sb.Append('\n')
                    .Append("IF EXISTS (SELECT 1 FROM sys.extended_properties WHERE class = 1 AND major_id = OBJECT_ID(").Append(objectLiteral)
                    .Append(") AND minor_id = 0 AND name = ").Append(marker).Append(")\n")
                    .Append("    EXEC sys.sp_dropextendedproperty @name = ").Append(marker)
                    .Append(", @level0type = N'SCHEMA', @level0name = ").Append(Program.SqlLiteral(schema))
                    .Append(", @level1type = N'TABLE', @level1name = ").Append(Program.SqlLiteral(table)).Append(";\n")
                    .Append("GO\n");
            }
            return sb.ToString();
        }
    }
}
