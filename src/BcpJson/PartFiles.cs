using System.Globalization;

namespace bcpJson
{
    /// <summary>The data files of one table, in load order.</summary>
    internal sealed record ImportGroup(string TableName, List<string> Files);

    /// <summary>Names of the part files a split table is exported to (&lt;schema&gt;.&lt;table&gt;.part0001.json) and how import groups them.</summary>
    internal static class PartFiles
    {
        private const string Marker = ".part";
        private const int DigitCount = 4;

        /// <summary>"dbo.T" and 1 give "dbo.T.part0001".</summary>
        internal static string PartName(string tableName, int part)
        {
            return tableName + Marker + part.ToString("D" + DigitCount, CultureInfo.InvariantCulture);
        }

        /// <summary>The part number in a data file name, or 0 when the file is not a part file.</summary>
        internal static int PartNumberOf(string fileName)
        {
            string stem = StemOf(fileName);
            return SplitPart(stem, out _);
        }

        /// <summary>The table a data file belongs to: the name without .gz, the extension and a .partNNNN suffix.</summary>
        internal static string TableNameOf(string fileName)
        {
            string stem = StemOf(fileName);
            SplitPart(stem, out string table);
            return table;
        }

        /// <summary>Groups files by table (all in one group named targetTable when it is given); files are ordered by part number, then name.</summary>
        internal static List<ImportGroup> GroupImportFiles(IEnumerable<string> fileNames, string targetTable)
        {
            IEnumerable<IGrouping<string, string>> groups = fileNames.GroupBy(
                f => string.IsNullOrEmpty(targetTable) ? TableNameOf(f) : targetTable,
                StringComparer.OrdinalIgnoreCase);
            return groups
                .Select(g => new ImportGroup(
                    string.IsNullOrEmpty(targetTable) ? TableNameOf(g.First()) : targetTable,
                    g.OrderBy(PartNumberOf).ThenBy(f => f, StringComparer.Ordinal).ToList()))
                .ToList();
        }

        private static string StemOf(string fileName)
        {
            return Path.GetFileNameWithoutExtension(FileCompression.WithoutGzipExtension(fileName));
        }

        private static int SplitPart(string stem, out string table)
        {
            table = stem;
            int cut = stem.Length - Marker.Length - DigitCount;
            if (cut < 0 || string.CompareOrdinal(stem, cut, Marker, 0, Marker.Length) != 0)
            {
                return 0;
            }
            for (int i = cut + Marker.Length; i < stem.Length; i++)
            {
                if (stem[i] < '0' || stem[i] > '9')
                {
                    return 0;
                }
            }
            table = stem.Substring(0, cut);
            return int.Parse(stem.AsSpan(cut + Marker.Length), NumberStyles.None, CultureInfo.InvariantCulture);
        }
    }
}
