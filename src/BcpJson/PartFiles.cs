using System.Globalization;

namespace bcpJson
{
    /// <summary>The data files of one table, in load order.</summary>
    internal sealed record ImportGroup(string TableName, List<string> Files);

    /// <summary>The files of an import group to load (in group order), the files skipped (Warnings) and the reasons the group is refused (Errors).</summary>
    internal sealed record ImportSelection(List<string> Files, List<string> Warnings, List<string> Errors);

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

        /// <summary>
        /// Which files of an import group to load. With a manifest (<paramref name="manifestName"/> names it in messages),
        /// a file of a table the manifest has an entry for is skipped unless the entry lists it, and a table whose split
        /// export is incomplete is refused. Files of tables the manifest does not know are kept. Then, with or without a
        /// manifest, a table that still has both a whole-table file and part files is refused. Any refusal refuses the
        /// whole group (Files is empty), so nothing of it is loaded.
        /// </summary>
        internal static ImportSelection SelectImportFiles(IReadOnlyList<string> files, ExportManifest manifest, string manifestName)
        {
            var selection = new ImportSelection(new List<string>(), new List<string>(), new List<string>());
            var kept = new List<string>();
            var incomplete = new HashSet<string>(StringComparer.Ordinal);
            foreach (string file in files)
            {
                string table = TableNameOf(file);
                ManifestTable entry = manifest == null ? null : FindEntry(manifest, table);
                if (entry == null)
                {
                    kept.Add(file);
                    continue;
                }
                if (!entry.Files.Any(f => string.Equals(f.Name, file, StringComparison.Ordinal)))
                {
                    selection.Warnings.Add(string.Format("{0}: skipping {1}: not part of the export in {2}.", table, file, manifestName));
                    continue;
                }
                if (entry.Split != null && entry.Split.Parts.Count < entry.Split.PartCount && incomplete.Add(table))
                {
                    selection.Errors.Add(string.Format("{0}: the export is incomplete ({1} of {2} parts); finish it with export --resume.",
                        table, entry.Split.Parts.Count, entry.Split.PartCount));
                }
                kept.Add(file);
            }

            foreach (IGrouping<string, string> table in kept.GroupBy(TableNameOf, StringComparer.OrdinalIgnoreCase))
            {
                string whole = table.FirstOrDefault(f => PartNumberOf(f) == 0);
                if (whole != null && table.Any(f => PartNumberOf(f) > 0))
                {
                    selection.Errors.Add(string.Format("{0}: both {1} and part files are present; remove the files of the older export.", table.Key, whole));
                }
            }

            if (selection.Errors.Count == 0)
            {
                selection.Files.AddRange(kept);
            }
            return selection;
        }

        /// <summary>The manifest entry of a table: an exact match, else one that differs only in case.</summary>
        private static ManifestTable FindEntry(ExportManifest manifest, string table)
        {
            return manifest.Tables.FirstOrDefault(t => string.Equals(t.Name, table, StringComparison.Ordinal))
                ?? manifest.Tables.FirstOrDefault(t => string.Equals(t.Name, table, StringComparison.OrdinalIgnoreCase));
        }

        /// <summary>
        /// Data files of <paramref name="table"/> in <paramref name="folderFiles"/> (with <paramref name="extension"/>,
        /// compressed or not, whole or part) that are not among <paramref name="written"/>: left from an earlier export.
        /// </summary>
        internal static List<string> Leftovers(IEnumerable<string> folderFiles, string table, string extension, IEnumerable<string> written)
        {
            var writing = new HashSet<string>(written, StringComparer.OrdinalIgnoreCase);
            return folderFiles
                .Where(f => !writing.Contains(f)
                    && FileCompression.WithoutGzipExtension(f).EndsWith(extension, StringComparison.OrdinalIgnoreCase)
                    && string.Equals(TableNameOf(f), table, StringComparison.OrdinalIgnoreCase))
                .ToList();
        }

        /// <summary>The file name without .gz and its extension: "dbo.T.part0001.json.gz" gives "dbo.T.part0001".</summary>
        internal static string StemOf(string fileName)
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
