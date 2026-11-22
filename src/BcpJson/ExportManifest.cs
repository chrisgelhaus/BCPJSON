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
        /// Why a resumed export cannot continue this manifest (another format, database or query), or null if it can.
        /// </summary>
        public string ResumeConflict(string format, string database, string query)
        {
            if (!string.Equals(this.Format, format, StringComparison.OrdinalIgnoreCase))
            {
                return string.Format("the existing export is {0}, not {1}", this.Format, format);
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
