using System.Globalization;

namespace bcpJson
{
    /// <summary>A column of the key a table is split on (clustered key or primary key column).</summary>
    internal sealed class SplitColumn
    {
        private static readonly HashSet<string> SplittableTypes = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            "tinyint", "smallint", "int", "bigint", "decimal", "numeric", "money", "smallmoney", "bit", "float", "real",
            "date", "time", "datetime", "datetime2", "smalldatetime", "datetimeoffset",
            "char", "varchar", "nchar", "nvarchar", "uniqueidentifier", "binary", "varbinary"
        };

        public string Name { get; set; }
        /// <summary>System type name (sys.types name of the system type).</summary>
        public string TypeName { get; set; }
        /// <summary>sys.columns.max_length: bytes, -1 for max.</summary>
        public short MaxLength { get; set; }
        public byte Precision { get; set; }
        public byte Scale { get; set; }
        public bool Nullable { get; set; }
        public bool Descending { get; set; }
        public string Collation { get; set; }

        /// <summary>The column type as T-SQL, for example decimal(18,4), nvarchar(30), varchar(max).</summary>
        public string SqlType
        {
            get
            {
                switch (TypeName.ToLowerInvariant())
                {
                    case "decimal":
                    case "numeric":
                        return TypeName + "(" + Precision.ToString(CultureInfo.InvariantCulture) + "," + Scale.ToString(CultureInfo.InvariantCulture) + ")";
                    case "datetime2":
                    case "datetimeoffset":
                    case "time":
                        return TypeName + "(" + Scale.ToString(CultureInfo.InvariantCulture) + ")";
                    case "nchar":
                    case "nvarchar":
                        return TypeName + "(" + (MaxLength < 0 ? "max" : (MaxLength / 2).ToString(CultureInfo.InvariantCulture)) + ")";
                    case "char":
                    case "varchar":
                    case "binary":
                    case "varbinary":
                        return TypeName + "(" + (MaxLength < 0 ? "max" : MaxLength.ToString(CultureInfo.InvariantCulture)) + ")";
                    default:
                        return TypeName;
                }
            }
        }

        /// <summary>CONVERT style that round-trips the type through text, or null when the default is fine.</summary>
        public int? ConvertStyle
        {
            get
            {
                switch (TypeName.ToLowerInvariant())
                {
                    case "datetime":
                    case "datetime2":
                    case "smalldatetime":
                    case "date":
                    case "datetimeoffset":
                        return 126;
                    case "float":
                    case "real":
                        return 3;
                    case "money":
                    case "smallmoney":
                        return 2;
                    case "binary":
                    case "varbinary":
                        return 1;
                    default:
                        return null;
                }
            }
        }

        public static bool IsSplittableType(string typeName)
        {
            return typeName != null && SplittableTypes.Contains(typeName);
        }
    }

    /// <summary>One part of a split table: a key range. A null bound means unbounded.</summary>
    internal sealed class KeyRange
    {
        public int Part { get; set; }
        public string Lower { get; set; }
        public bool LowerInclusive { get; set; }
        public string Upper { get; set; }
        public bool UpperInclusive { get; set; }
        public bool IncludeNulls { get; set; }

        /// <summary>WHERE predicate selecting the rows of this range.</summary>
        public string Predicate(SplitColumn column)
        {
            string name = Program.QuoteName(column.Name);
            var bounds = new List<string>();
            if (Lower != null)
                bounds.Add(name + (LowerInclusive ? " >= " : " > ") + ValueExpression(column, Lower));
            if (Upper != null)
                bounds.Add(name + (UpperInclusive ? " <= " : " < ") + ValueExpression(column, Upper));

            string text = string.Join(" AND ", bounds);
            if (IncludeNulls)
                text = text.Length == 0 ? name + " IS NULL" : text + " OR " + name + " IS NULL";
            return text.Length == 0 ? "1 = 1" : "(" + text + ")";
        }

        /// <summary>
        /// String literal that survives a command line: double quotes and backslashes are built with NCHAR()
        /// so they never appear in the SQL text.
        /// </summary>
        public static string TextLiteral(string value)
        {
            if (value.IndexOf('"') < 0 && value.IndexOf('\\') < 0)
                return Program.SqlLiteral(value);

            var parts = new List<string>();
            var current = new System.Text.StringBuilder();
            foreach (char c in value)
            {
                if (c == '"' || c == '\\')
                {
                    parts.Add(Program.SqlLiteral(current.ToString()));
                    parts.Add("NCHAR(" + (c == '"' ? "34" : "92") + ")");
                    current.Clear();
                }
                else
                {
                    current.Append(c);
                }
            }
            parts.Add(Program.SqlLiteral(current.ToString()));
            return "(" + string.Join(" + ", parts) + ")";
        }

        /// <summary>CONVERT(&lt;type&gt;, &lt;text literal&gt;[, style]) producing a typed boundary value.</summary>
        public static string ValueExpression(SplitColumn column, string text)
        {
            string style = column.ConvertStyle.HasValue ? ", " + column.ConvertStyle.Value.ToString(CultureInfo.InvariantCulture) : "";
            return "CONVERT(" + column.SqlType + ", " + TextLiteral(text) + style + ")";
        }
    }
}
