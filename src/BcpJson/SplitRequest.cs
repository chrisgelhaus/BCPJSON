using System.Text.RegularExpressions;

namespace bcpJson
{
    /// <summary>One --split-tables entry: a schema.table pattern (* wildcard) and how many key ranges to cut it into.</summary>
    internal sealed record SplitRequest(string Pattern, int Parts)
    {
        public const int DefaultParts = 8;
        public const int MinParts = 2;
        public const int MaxParts = 200;

        /// <summary>Parses values like "dbo.History:32" or "Sales.Order*". The last ":" followed by digits is the part count.</summary>
        public static bool TryParse(IEnumerable<string> values, out List<SplitRequest> requests, out string error)
        {
            requests = new List<SplitRequest>();
            error = null;

            foreach (string raw in values ?? Enumerable.Empty<string>())
            {
                string value = raw == null ? string.Empty : raw.Trim();
                string pattern = value;
                int parts = DefaultParts;

                int colon = value.LastIndexOf(':');
                if (colon >= 0 && colon < value.Length - 1 && value.Skip(colon + 1).All(char.IsAsciiDigit))
                {
                    pattern = value.Substring(0, colon);
                    if (!int.TryParse(value.Substring(colon + 1), out parts))
                    {
                        parts = int.MaxValue;
                    }
                }

                if (pattern.Length == 0 || value.EndsWith(":"))
                {
                    error = string.Format("--split-tables: '{0}' has no table name. Use schema.table[:parts].", raw);
                    return false;
                }

                if (parts < MinParts || parts > MaxParts)
                {
                    error = string.Format("--split-tables: '{0}' asks for an invalid number of parts; use {1} to {2}.", raw, MinParts, MaxParts);
                    return false;
                }

                requests.Add(new SplitRequest(pattern, parts));
            }

            return true;
        }

        /// <summary>Part count of the first request matching the table (schema.table, case-insensitive, * wildcard); null = don't split.</summary>
        public static int? PartsFor(IReadOnlyList<SplitRequest> requests, string tableDisplayName)
        {
            if (requests == null)
            {
                return null;
            }

            foreach (SplitRequest request in requests)
            {
                string regex = "^" + Regex.Escape(request.Pattern).Replace("\\*", ".*") + "$";
                if (Regex.IsMatch(tableDisplayName, regex, RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Singleline))
                {
                    return request.Parts;
                }
            }

            return null;
        }

        /// <summary>The warning for --split-tables without --snapshot, or null when none is due. verb is "export" or "copy".</summary>
        public static string SnapshotWarning(bool splitRequested, bool snapshot, string verb)
        {
            if (!splitRequested || snapshot)
            {
                return null;
            }

            return "--split-tables without --snapshot: the parts of a table are read at different moments. Use --snapshot for an exact, consistent " + verb + ".";
        }
    }
}
