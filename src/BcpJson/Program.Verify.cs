using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace bcpJson
{
    partial class Program
    {
        /// <summary>Outcome of checking an export folder against its manifest.</summary>
        internal sealed record VerifyResult(List<string> Problems, List<string> Warnings, int FilesChecked, long Rows);

        /// <summary>
        /// Checks every file of the manifest (present, recorded size, SHA-256) and that the export completed.
        /// Data files in the folder that the manifest does not list (for example left by an older export) are warnings.
        /// </summary>
        internal static VerifyResult CheckExport(string folder, ExportManifest manifest)
        {
            var problems = new List<string>();
            var warnings = new List<string>();
            int files = 0;

            if (manifest.Completed == null)
            {
                problems.Add("The export did not complete (no completion time in the manifest); rerun it with --resume.");
            }

            foreach (ManifestTable table in manifest.Tables)
            {
                if (table.Split != null && table.Split.Parts.Count < table.Split.PartCount)
                {
                    problems.Add(string.Format("{0}: {1} of {2} parts exported.", table.Name, table.Split.Parts.Count, table.Split.PartCount));
                }
                foreach (ManifestFile file in table.Files)
                {
                    files++;
                    string problem = file.Problem(folder, true);
                    if (problem != null)
                    {
                        problems.Add(problem);
                    }
                }
            }

            var listed = new HashSet<string>(manifest.Tables.SelectMany(t => t.Files).Select(f => f.Name), StringComparer.OrdinalIgnoreCase);
            foreach (string path in Directory.EnumerateFiles(folder).OrderBy(p => p, StringComparer.Ordinal))
            {
                string name = System.IO.Path.GetFileName(path);
                if (name.Equals(ExportManifest.FileName, StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }
                if (name.EndsWith(PartialSuffix, StringComparison.OrdinalIgnoreCase))
                {
                    warnings.Add(string.Format("{0}: unfinished file from an interrupted export.", name));
                }
                else if (IsExportDataFile(name) && !listed.Contains(name))
                {
                    warnings.Add(string.Format("{0}: not part of this export (not in the manifest).", name));
                }
            }

            long rows = manifest.Tables.Where(t => t.Rows > 0).Sum(t => t.Rows);
            return new VerifyResult(problems, warnings, files, rows);
        }

        private static bool IsExportDataFile(string name)
        {
            name = FileCompression.WithoutGzipExtension(name);
            return name.EndsWith(".json", StringComparison.OrdinalIgnoreCase)
                || name.EndsWith(".bcp", StringComparison.OrdinalIgnoreCase)
                || name.EndsWith(BcpFormatFileSuffix, StringComparison.OrdinalIgnoreCase)
                || name.EndsWith(LegacyBcpFormatFileSuffix, StringComparison.OrdinalIgnoreCase);
        }

        private static int VerifyExport(VerifyOptions opts)
        {
            SetLogFile(opts.LogFile);
            string folder = string.IsNullOrEmpty(opts.Path) ? Directory.GetCurrentDirectory() : opts.Path;

            ExportManifest manifest;
            try
            {
                manifest = Directory.Exists(folder) ? ExportManifest.Load(folder) : null;
            }
            catch (Exception ex)
            {
                PostToConsole(string.Format("Cannot read {0}: {1}", ExportManifest.PathIn(folder), ex.Message), InfoLevel.Error);
                return 1;
            }
            if (manifest == null)
            {
                PostToConsole(string.Format("No {0} in {1}.", ExportManifest.FileName, folder), InfoLevel.Error);
                return 1;
            }

            PostToConsole(string.Format("{0} export of {1} ({2}), started {3:u} by {4}.",
                manifest.Format, string.IsNullOrEmpty(manifest.Query) ? manifest.Database : "a query", manifest.Server, manifest.Started, manifest.Tool));

            VerifyResult result = CheckExport(folder, manifest);
            foreach (string warning in result.Warnings)
            {
                PostToConsole(warning, InfoLevel.Warning);
            }
            foreach (string problem in result.Problems)
            {
                PostToConsole(problem, InfoLevel.Error);
            }

            if (result.Problems.Count == 0)
            {
                PostToConsole(string.Format("Verified: {0} table(s), {1} file(s), {2:N0} rows; every file matches the manifest.",
                    manifest.Tables.Count, result.FilesChecked, result.Rows), InfoLevel.Success);
            }
            return ExitCode();
        }
    }
}
