using System.IO;
using System.IO.Compression;
using System.Text;

namespace bcpJson
{
    /// <summary>
    /// gzip for export data files (--compress gzip). Exports write &lt;name&gt;.json.gz / &lt;name&gt;.bcp.gz; imports
    /// recognise the .gz extension and decompress on the fly (JSON) or to a temporary file (bcp).
    /// </summary>
    internal static class FileCompression
    {
        public const string None = "none";
        public const string Gzip = "gzip";
        public const string GzipExtension = ".gz";
        public const string DefaultLevel = "fastest";

        private const int BufferSize = 1 << 16;

        /// <summary>UTF-8 without a byte order mark, as JSON exports have always been written.</summary>
        private static readonly Encoding Utf8 = new UTF8Encoding(false);

        public static bool IsValidMethod(string method)
        {
            return string.IsNullOrEmpty(method)
                || method.Equals(None, StringComparison.OrdinalIgnoreCase)
                || method.Equals(Gzip, StringComparison.OrdinalIgnoreCase);
        }

        public static bool IsGzip(string method)
        {
            return Gzip.Equals(method, StringComparison.OrdinalIgnoreCase);
        }

        /// <summary>Canonical name of a --compress value: "gzip" or "none" (null/empty = none).</summary>
        public static string Normalize(string method)
        {
            return IsGzip(method) ? Gzip : None;
        }

        /// <summary>Maps --compression-level (fastest, optimal, smallest; null = fastest) to a .NET level; null if unknown.</summary>
        public static CompressionLevel? ParseLevel(string level)
        {
            switch ((level ?? DefaultLevel).Trim().ToLowerInvariant())
            {
                case "fastest":
                    return CompressionLevel.Fastest;
                case "optimal":
                    return CompressionLevel.Optimal;
                case "smallest":
                    return CompressionLevel.SmallestSize;
                default:
                    return null;
            }
        }

        public static bool IsCompressedFile(string name)
        {
            return name.EndsWith(GzipExtension, StringComparison.OrdinalIgnoreCase);
        }

        /// <summary>The file name without a trailing .gz.</summary>
        public static string WithoutGzipExtension(string name)
        {
            return IsCompressedFile(name) ? name.Substring(0, name.Length - GzipExtension.Length) : name;
        }

        /// <summary>A text writer for an export file, gzip-compressed when <paramref name="level"/> is set.</summary>
        public static StreamWriter CreateText(string path, CompressionLevel? level)
        {
            Stream stream = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.None, BufferSize);
            if (level.HasValue)
            {
                stream = new GZipStream(stream, level.Value);
            }
            return new StreamWriter(stream, Utf8, BufferSize);
        }

        /// <summary>A text reader for an import file; .gz files are decompressed while reading.</summary>
        public static StreamReader OpenText(string path)
        {
            Stream stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read, BufferSize);
            if (IsCompressedFile(path))
            {
                stream = new GZipStream(stream, CompressionMode.Decompress);
            }
            return new StreamReader(stream, Utf8, true, BufferSize);
        }

        public static void CompressFile(string source, string destination, CompressionLevel level)
        {
            using (var input = new FileStream(source, FileMode.Open, FileAccess.Read, FileShare.Read, BufferSize))
            using (var output = new FileStream(destination, FileMode.Create, FileAccess.Write, FileShare.None, BufferSize))
            using (var gzip = new GZipStream(output, level))
            {
                input.CopyTo(gzip, BufferSize);
            }
        }

        public static void DecompressFile(string source, string destination)
        {
            using (var input = new FileStream(source, FileMode.Open, FileAccess.Read, FileShare.Read, BufferSize))
            using (var gzip = new GZipStream(input, CompressionMode.Decompress))
            using (var output = new FileStream(destination, FileMode.Create, FileAccess.Write, FileShare.None, BufferSize))
            {
                gzip.CopyTo(output, BufferSize);
            }
        }
    }
}
