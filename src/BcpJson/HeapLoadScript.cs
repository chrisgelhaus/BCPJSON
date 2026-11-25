namespace bcpJson
{
    /// <summary>Names and text of the recovery script and markers used by import --load-as-heap.</summary>
    internal static class HeapLoadScript
    {
        internal const string Suffix = ".load-as-heap.sql";
        internal const string MarkerPrefix = "bcpjson_load_as_heap_";
        internal const string MarkersKeptMessage = "Not every index exists; the markers are kept. Fix the cause and run this script again.";

        /// <summary>The session options index DDL needs (filtered indexes, indexes on computed columns), so the script runs the same in any tool.</summary>
        internal const string SetOptions = "SET QUOTED_IDENTIFIER ON; SET ANSI_NULLS ON; SET ANSI_PADDING ON; SET ANSI_WARNINGS ON; SET CONCAT_NULL_YIELDS_NULL ON; SET ARITHABORT ON; SET NUMERIC_ROUNDABORT OFF;";

        internal static string FileName(string schema, string table)
        {
            return schema + "." + table + Suffix;
        }

        internal static string MarkerName(int ordinal)
        {
            return MarkerPrefix + ordinal.ToString("000", System.Globalization.CultureInfo.InvariantCulture);
        }

        /// <summary>
        /// The recovery script: USE the database, set the session options, create each saved index (creation order) only if
        /// missing, then, in one final batch, drop every marker, but only when every saved index exists; otherwise RAISERROR
        /// and keep them (sqlcmd and SSMS carry on after a failed batch). Rerunnable; every name reaches the SQL through
        /// <see cref="Program.SqlLiteral"/> / <see cref="Program.QuoteName"/>.
        /// </summary>
        internal static string Build(string database, string schema, string table, IEnumerable<(string Name, string Statement)> indexes, DateTime utc)
        {
            string objectLiteral = Program.SqlLiteral(Program.QuoteName(schema) + "." + Program.QuoteName(table));
            var sb = new System.Text.StringBuilder();
            sb.Append("-- Written by bcpJson import --load-as-heap for ").Append(schema).Append('.').Append(table).Append(" at ")
                .Append(utc.ToString("yyyy-MM-dd HH:mm:ss", System.Globalization.CultureInfo.InvariantCulture)).Append(" UTC.\n");
            sb.Append("-- Run this script to recreate the indexes if the import was interrupted. It is rerunnable:\n");
            sb.Append("-- each index is created only if it is missing, and the markers are removed at the end, only once every index exists.\n");
            sb.Append("USE ").Append(Program.QuoteName(database)).Append(";\nGO\n");
            sb.Append(SetOptions).Append("\nGO\n");
            var names = new List<string>();
            foreach ((string name, string statement) in indexes)
            {
                names.Add(name);
                sb.Append('\n')
                    .Append("IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(").Append(objectLiteral)
                    .Append(") AND name = ").Append(Program.SqlLiteral(name)).Append(")\n")
                    .Append("BEGIN\n").Append(statement).Append('\n').Append("END\n").Append("GO\n");
            }
            if (names.Count == 0)
            {
                return sb.ToString();
            }

            sb.Append('\n')
                .Append("IF (SELECT COUNT(*) FROM sys.indexes WHERE object_id = OBJECT_ID(").Append(objectLiteral)
                .Append(") AND name IN (").Append(string.Join(", ", names.Select(Program.SqlLiteral))).Append(")) = ")
                .Append(names.Count.ToString(System.Globalization.CultureInfo.InvariantCulture)).Append('\n')
                .Append("BEGIN\n");
            for (int i = 1; i <= names.Count; i++)
            {
                string marker = Program.SqlLiteral(MarkerName(i));
                sb.Append("    IF EXISTS (SELECT 1 FROM sys.extended_properties WHERE class = 1 AND major_id = OBJECT_ID(").Append(objectLiteral)
                    .Append(") AND minor_id = 0 AND name = ").Append(marker).Append(")\n")
                    .Append("        EXEC sys.sp_dropextendedproperty @name = ").Append(marker)
                    .Append(", @level0type = N'SCHEMA', @level0name = ").Append(Program.SqlLiteral(schema))
                    .Append(", @level1type = N'TABLE', @level1name = ").Append(Program.SqlLiteral(table)).Append(";\n");
            }
            sb.Append("END\n")
                .Append("ELSE\n")
                .Append("    RAISERROR(").Append(Program.SqlLiteral(MarkersKeptMessage)).Append(", 16, 1);\n")
                .Append("GO\n");
            return sb.ToString();
        }

        /// <summary>Writes the script as UTF-8 with a BOM, so Windows sqlcmd reads non-ASCII names correctly.</summary>
        internal static void Write(string path, string text)
        {
            File.WriteAllText(path, text, new System.Text.UTF8Encoding(true));
        }
    }
}
