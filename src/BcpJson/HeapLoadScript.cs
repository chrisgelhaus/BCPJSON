namespace bcpJson
{
    /// <summary>Names and text of the recovery script and markers used by import and copy --load-as-heap.</summary>
    internal static class HeapLoadScript
    {
        internal const string Suffix = ".load-as-heap.sql";
        internal const string MarkerPrefix = "bcpjson_load_as_heap_";
        internal const string MarkersKeptMessage = "Not every index exists; the markers are kept. Fix the cause and run this script again.";
        internal const string WrongDatabaseMessage = "This script is for database %s, which is not the current database; nothing was changed. If the database now has another name, replace its name in this script (the USE line and the DB_NAME() checks).";

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
        /// and keep them (sqlcmd and SSMS carry on after a failed batch). Every batch checks DB_NAME(), so a failed USE
        /// (renamed or restored database) changes nothing in whatever database the session is left in. Rerunnable; every
        /// name reaches the SQL through <see cref="Program.SqlLiteral"/> / <see cref="Program.QuoteName"/>, and names in
        /// comments go through <see cref="CommentText"/>.
        /// </summary>
        internal static string Build(string verb, string database, string schema, string table, IEnumerable<(string Name, string Statement)> indexes, DateTime utc)
        {
            string objectLiteral = Program.SqlLiteral(Program.QuoteName(schema) + "." + Program.QuoteName(table));
            string databaseLiteral = Program.SqlLiteral(database);
            string inDatabase = "IF DB_NAME() = " + databaseLiteral;
            var sb = new System.Text.StringBuilder();
            sb.Append("-- Written by bcpJson ").Append(verb).Append(" --load-as-heap for ").Append(CommentText(schema + "." + table)).Append(" at ")
                .Append(utc.ToString("yyyy-MM-dd HH:mm:ss", System.Globalization.CultureInfo.InvariantCulture)).Append(" UTC,\n");
            sb.Append("-- in database ").Append(CommentText(database)).Append(". If the database now has another name, replace its name in the USE line and the DB_NAME() checks.\n");
            sb.Append("-- Run this script to recreate the indexes if the ").Append(verb).Append(" was interrupted. It is rerunnable:\n");
            sb.Append("-- each index is created only if it is missing, and the markers are removed at the end, only once every index exists.\n");
            sb.Append("USE ").Append(Program.QuoteName(database)).Append(";\nGO\n");
            sb.Append(SetOptions).Append("\nGO\n");
            sb.Append("IF DB_NAME() <> ").Append(databaseLiteral).Append('\n')
                .Append("    RAISERROR(").Append(Program.SqlLiteral(WrongDatabaseMessage)).Append(", 16, 1, ").Append(databaseLiteral).Append(");\n")
                .Append("GO\n");
            var names = new List<string>();
            foreach ((string name, string statement) in indexes)
            {
                names.Add(name);
                sb.Append('\n')
                    .Append(inDatabase).Append(" AND NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(").Append(objectLiteral)
                    .Append(") AND name = ").Append(Program.SqlLiteral(name)).Append(")\n")
                    .Append("BEGIN\n").Append(statement).Append('\n').Append("END\n").Append("GO\n");
            }
            if (names.Count == 0)
            {
                return sb.ToString();
            }

            sb.Append('\n')
                .Append(inDatabase).Append(" AND (SELECT COUNT(*) FROM sys.indexes WHERE object_id = OBJECT_ID(").Append(objectLiteral)
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
                .Append("ELSE ").Append(inDatabase).Append('\n')
                .Append("    RAISERROR(").Append(Program.SqlLiteral(MarkersKeptMessage)).Append(", 16, 1);\n")
                .Append("GO\n");
            return sb.ToString();
        }

        /// <summary>A name as it may appear in a <c>--</c> comment: control characters (a newline would end the comment) become '?'.</summary>
        internal static string CommentText(string name)
        {
            return new string(name.Select(c => char.IsControl(c) ? '?' : c).ToArray());
        }

        /// <summary>Writes the script as UTF-8 with a BOM, so Windows sqlcmd reads non-ASCII names correctly.</summary>
        internal static void Write(string path, string text)
        {
            File.WriteAllText(path, text, new System.Text.UTF8Encoding(true));
        }
    }
}
