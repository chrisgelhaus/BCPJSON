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

        internal static string Build(string label, DateTime utc, IEnumerable<string> statements)
        {
            var sb = new System.Text.StringBuilder();
            sb.Append("-- Written by bcpJson import --load-as-heap for ").Append(label).Append(" at ")
                .Append(utc.ToString("yyyy-MM-dd HH:mm:ss", System.Globalization.CultureInfo.InvariantCulture)).Append(" UTC.\n");
            sb.Append("-- Run this script to recreate the indexes if the import was interrupted.\n");
            sb.Append("-- Each statement fails harmlessly if its index already exists.\n");
            foreach (string statement in statements)
            {
                sb.Append('\n').Append(statement).Append('\n').Append("GO\n");
            }
            return sb.ToString();
        }
    }
}
