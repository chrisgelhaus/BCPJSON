using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Security.Cryptography;

namespace bcpJson
{
    /// <summary>
    /// bcpjson-manifest.json, written into the export folder: what was exported, from where, and the size and SHA-256
    /// of every file. Saved after each table, so an interrupted export can be resumed (--resume), and checked by the
    /// verify verb without a database.
    /// </summary>
    internal sealed class ExportManifest
    {
        public const string FileName = "bcpjson-manifest.json";

        private static readonly JsonSerializerSettings Settings = new JsonSerializerSettings
        {
            ContractResolver = new DefaultContractResolver { NamingStrategy = new CamelCaseNamingStrategy() },
            Formatting = Formatting.Indented,
            DateTimeZoneHandling = DateTimeZoneHandling.Utc,
            NullValueHandling = NullValueHandling.Include
        };

        private readonly object sync = new object();

        public string Tool { get; set; }
        public string Format { get; set; }

        /// <summary>none or gzip (--compress). Manifests from before --compress have no value, which means none.</summary>
        public string Compression { get; set; }
        public string Server { get; set; }
        public string Database { get; set; }
        public string Query { get; set; }
        public bool Snapshot { get; set; }
        public DateTime Started { get; set; }

        /// <summary>Set when every table was exported without errors; null while running or after a failed run.</summary>
        public DateTime? Completed { get; set; }

        public List<ManifestTable> Tables { get; set; } = new List<ManifestTable>();

        public static string PathIn(string folder)
        {
            return Path.Combine(folder, FileName);
        }

        public static string ToolVersion()
        {
            string version = typeof(ExportManifest).Assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion;
            return "bcpjson " + (version ?? "unknown");
        }

        /// <summary>The manifest in <paramref name="folder"/>, or null if there is none. Throws if it is unreadable.</summary>
        public static ExportManifest Load(string folder)
        {
            string path = PathIn(folder);
            if (!File.Exists(path))
            {
                return null;
            }
            ExportManifest manifest = JsonConvert.DeserializeObject<ExportManifest>(File.ReadAllText(path), Settings);
            if (manifest == null)
            {
                throw new InvalidDataException(path + " is empty.");
            }
            manifest.Tables ??= new List<ManifestTable>();
            return manifest;
        }

        /// <summary>Writes the manifest atomically (via a .partial file), so a crash never leaves it half-written.</summary>
        public void Save(string folder)
        {
            lock (this.sync)
            {
                string path = PathIn(folder);
                File.WriteAllText(Program.PartialPath(path), JsonConvert.SerializeObject(this, Settings));
                Program.CommitPartial(path);
            }
        }

        /// <summary>Adds or replaces a table's entry (thread-safe) and saves the manifest.</summary>
        public void Record(ManifestTable table, string folder)
        {
            lock (this.sync)
            {
                this.Tables.RemoveAll(t => string.Equals(t.Name, table.Name, StringComparison.Ordinal));
                this.Tables.Add(table);
                this.Tables.Sort((a, b) => string.CompareOrdinal(a.Name, b.Name));
                this.Save(folder);
            }
        }

        public ManifestTable Find(string name)
        {
            lock (this.sync)
            {
                return this.Tables.FirstOrDefault(t => string.Equals(t.Name, name, StringComparison.Ordinal));
            }
        }

        /// <summary>
        /// Why a resumed export cannot continue this manifest (another format, compression, database or query), or null if it can.
        /// </summary>
        public string ResumeConflict(string format, string compression, string database, string query)
        {
            if (!string.Equals(this.Format, format, StringComparison.OrdinalIgnoreCase))
            {
                return string.Format("the existing export is {0}, not {1}", this.Format, format);
            }
            string existing = FileCompression.Normalize(this.Compression);
            if (existing != FileCompression.Normalize(compression))
            {
                return string.Format("the existing export uses --compress {0}, not {1}", existing, FileCompression.Normalize(compression));
            }
            if (!string.Equals(this.Database ?? string.Empty, database ?? string.Empty, StringComparison.OrdinalIgnoreCase))
            {
                return string.Format("the existing export is from database '{0}', not '{1}'", this.Database, database);
            }
            if (!string.Equals(this.Query ?? string.Empty, query ?? string.Empty, StringComparison.Ordinal))
            {
                return "the existing export was made with a different --source-query";
            }
            return null;
        }

        /// <summary>True when the table has an entry and all its files exist with the recorded size (and hash, if asked).</summary>
        public bool IsIntact(string name, string folder, bool checkHash)
        {
            ManifestTable table = this.Find(name);
            return table != null && table.Files.Count > 0 && table.Files.All(f => f.Problem(folder, checkHash) == null);
        }
    }

    internal sealed class ManifestTable
    {
        public string Name { get; set; }

        /// <summary>Rows exported, or -1 when bcp did not report a count.</summary>
        public long Rows { get; set; }

        public DateTime Exported { get; set; }
        public List<ManifestFile> Files { get; set; } = new List<ManifestFile>();

        /// <summary>
        /// JSON table exports: the primary key the rows are sorted by. Import uses it to tell SQL Server the rows arrive
        /// in clustered index order. Null (and left out of the file) for BCP, query exports, tables without a primary key
        /// and exports from before this was recorded.
        /// </summary>
        [JsonProperty(NullValueHandling = NullValueHandling.Ignore)]
        public List<ManifestKeyColumn> OrderedBy { get; set; }
    }

    /// <summary>One key column: name, system type, direction and collation (null for non-string types).</summary>
    internal sealed class ManifestKeyColumn
    {
        public string Column { get; set; }
        public string Type { get; set; }
        public bool Descending { get; set; }
        public string Collation { get; set; }

        /// <summary>
        /// True when rows sorted by <paramref name="sortedBy"/> are also in <paramref name="clustered"/> order: the same
        /// columns in the same order, with the same type, direction and collation (collation decides string order).
        /// sql_variant keys never match: values re-imported as nvarchar can sort differently.
        /// </summary>
        public static bool SameOrder(IReadOnlyList<ManifestKeyColumn> sortedBy, IReadOnlyList<ManifestKeyColumn> clustered)
        {
            if (sortedBy == null || clustered == null || sortedBy.Count == 0 || sortedBy.Count != clustered.Count)
            {
                return false;
            }
            for (int i = 0; i < sortedBy.Count; i++)
            {
                ManifestKeyColumn a = sortedBy[i], b = clustered[i];
                if (a == null || b == null
                    || !string.Equals(a.Column, b.Column, StringComparison.OrdinalIgnoreCase)
                    || !string.Equals(a.Type, b.Type, StringComparison.OrdinalIgnoreCase)
                    || string.Equals(a.Type, "sql_variant", StringComparison.OrdinalIgnoreCase)
                    || a.Descending != b.Descending
                    || !string.Equals(a.Collation ?? string.Empty, b.Collation ?? string.Empty, StringComparison.OrdinalIgnoreCase))
                {
                    return false;
                }
            }
            return true;
        }
    }

    internal sealed class ManifestFile
    {
        public string Name { get; set; }
        public long Bytes { get; set; }
        public string Sha256 { get; set; }

        /// <summary>Size and SHA-256 of a file in the export folder.</summary>
        public static ManifestFile Describe(string folder, string name)
        {
            string path = Path.Combine(folder, name);
            using (FileStream stream = File.OpenRead(path))
            {
                return new ManifestFile
                {
                    Name = name,
                    Bytes = stream.Length,
                    Sha256 = Convert.ToHexString(SHA256.HashData(stream)).ToLowerInvariant()
                };
            }
        }

        /// <summary>What is wrong with this file in <paramref name="folder"/>, or null if it matches the manifest.</summary>
        public string Problem(string folder, bool checkHash)
        {
            string path = Path.Combine(folder, this.Name);
            if (!File.Exists(path))
            {
                return string.Format("{0}: missing.", this.Name);
            }
            long bytes = new FileInfo(path).Length;
            if (bytes != this.Bytes)
            {
                return string.Format("{0}: {1:N0} bytes, manifest says {2:N0}.", this.Name, bytes, this.Bytes);
            }
            if (checkHash)
            {
                string actual = Describe(folder, this.Name).Sha256;
                if (!string.Equals(actual, this.Sha256, StringComparison.OrdinalIgnoreCase))
                {
                    return string.Format("{0}: SHA-256 {1} does not match the manifest ({2}).", this.Name, actual, this.Sha256);
                }
            }
            return null;
        }
    }
}
