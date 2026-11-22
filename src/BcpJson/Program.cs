using CommandLine;
using CommandLine.Text;
using Microsoft.Data.SqlClient;
using System;
using System.ComponentModel;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace bcpJson
{
    /// <summary>
    /// Entry point and shared plumbing. Verb implementations live in the other Program.*.cs partial files:
    /// Program.Export.cs, Program.Import.cs, Program.Copy.cs, Program.Sql.cs.
    /// </summary>
    partial class Program
    {
        private static readonly object ConsoleLock = new object();
        private static int errorCount;

        /// <summary>When set, every console message is also appended to this file.</summary>
        internal static string LogFilePath;

        static int Main(string[] args)
        {
            // HelpWriter = null: the parser would write all help/version/error text to stderr. ReportParseErrors
            // writes requested help and version to stdout instead, and only real usage errors to stderr.
            using (var parser = new Parser(settings => settings.HelpWriter = null))
            {
                ParserResult<object> result = parser.ParseArguments<ExportOptions, ImportOptions, CopyOptions, VerifyOptions>(args);
                return result.MapResult(
                    (ExportOptions opts) => ExportData(opts),
                    (ImportOptions opts) => ImportData(opts),
                    (CopyOptions opts) => CopyData(opts),
                    (VerifyOptions opts) => VerifyExport(opts),
                    errors => ReportParseErrors(result, errors, Console.Out, Console.Error));
            }
        }

        /// <summary>
        /// Writes the parser's help text for a run that did not reach a verb: --help and --version go to
        /// <paramref name="stdout"/> (exit 0); usage errors go to <paramref name="stderr"/> (exit 1).
        /// </summary>
        internal static int ReportParseErrors(ParserResult<object> result, IEnumerable<Error> errors, TextWriter stdout, TextWriter stderr)
        {
            var list = errors.ToList();
            int exitCode = ExitCodeForParseErrors(list);
            // Same text the parser would have written: version heading, verb list, option help or error messages.
            (exitCode == 0 ? stdout : stderr).Write(HelpText.AutoBuild(result));
            return exitCode;
        }

        /// <summary>
        /// CommandLineParser reports --help and --version as "errors"; those are successful runs (exit 0).
        /// Anything else (unknown verb, missing required option, bad value) is exit 1.
        /// </summary>
        internal static int ExitCodeForParseErrors(IEnumerable<Error> errors)
        {
            var list = errors.ToList();
            return list.IsHelp() || list.IsVersion() ? 0 : 1;
        }

        private static int ExportData(ExportOptions opts)
        {
            SetLogFile(opts.outputLogFile);
            if (!opts.Valid())
            {
                return 1;
            }

            string format = (opts.FileFormat ?? string.Empty).ToLowerInvariant();
            if (format != "bcp" && format != "json")
            {
                PostToConsole(string.Format("Unsupported file format '{0}'. Use bcp or json.", opts.FileFormat), InfoLevel.Error);
                return 1;
            }

            ExportManifest manifest = OpenManifest(opts, format);
            if (manifest == null)
            {
                return 1;
            }

            Stopwatch sw = Stopwatch.StartNew();
            DatabaseSnapshot snapshot = null;
            long result;
            try
            {
                if (opts.Snapshot)
                {
                    snapshot = CreateSnapshot(opts.srcServer, opts.srcDatabase, opts.srcLogin, opts.srcPassword, opts.TrustServerCertificate);
                    if (snapshot == null)
                    {
                        return 1;
                    }
                    opts.srcDatabase = snapshot.Name;
                }

                if (format == "bcp")
                {
                    result = opts.IsQueryExport ? ExportQueryToBCP(opts, manifest) : ExportToBCP(opts, manifest);
                }
                else
                {
                    result = opts.IsQueryExport ? ExportQueryToJSON(opts, manifest) : ExportToJSON(opts, manifest);
                }
            }
            finally
            {
                snapshot?.Dispose();
            }
            sw.Stop();

            if (ExitCode() == 0)
            {
                // Completed marks the export as whole; verify reports an export without it as incomplete.
                manifest.Completed = DateTime.UtcNow;
                try
                {
                    manifest.Save(opts.exportPath);
                }
                catch (Exception ex)
                {
                    PostToConsole(string.Format("Could not write {0}: {1}", ExportManifest.FileName, ex.Message), InfoLevel.Error);
                }
            }
            else
            {
                PostToConsole("The export is incomplete; rerun with --resume to export only what is missing.", InfoLevel.Warning);
            }
            PostToConsole(string.Format("Total Exported: {0:N0}\tRows in {1}", result, sw.Elapsed), errorCount > 0 ? InfoLevel.Warning : InfoLevel.Success);
            return ExitCode();
        }

        /// <summary>
        /// The manifest this export records into: the existing one with --resume (after checking it describes the same
        /// export), otherwise a new one. Saved immediately, so even an export that fails at once leaves an
        /// (incomplete) manifest. Null after reporting an error.
        /// </summary>
        private static ExportManifest OpenManifest(ExportOptions opts, string format)
        {
            try
            {
                if (opts.Resume)
                {
                    ExportManifest existing = ExportManifest.Load(opts.exportPath);
                    if (existing == null)
                    {
                        PostToConsole(string.Format("--resume: no {0} in {1}; exporting everything.", ExportManifest.FileName, opts.exportPath), InfoLevel.Warning);
                    }
                    else
                    {
                        string conflict = existing.ResumeConflict(format, opts.srcDatabase, opts.Query);
                        if (conflict != null)
                        {
                            PostToConsole(string.Format("Cannot resume: {0}. Use another --export-path, or export without --resume.", conflict), InfoLevel.Error);
                            return null;
                        }
                        if (opts.Snapshot)
                        {
                            PostToConsole("--resume with --snapshot: tables exported by the earlier run come from an earlier point in time.", InfoLevel.Warning);
                        }
                        existing.Tool = ExportManifest.ToolVersion();
                        existing.Completed = null;
                        existing.Save(opts.exportPath);
                        return existing;
                    }
                }

                var manifest = new ExportManifest
                {
                    Tool = ExportManifest.ToolVersion(),
                    Format = format,
                    Server = opts.srcServer,
                    Database = opts.srcDatabase,
                    Query = opts.Query,
                    Snapshot = opts.Snapshot,
                    Started = DateTime.UtcNow
                };
                manifest.Save(opts.exportPath);
                return manifest;
            }
            catch (Exception ex)
            {
                PostToConsole(string.Format("{0} in {1}: {2}", ExportManifest.FileName, opts.exportPath, ex.Message), InfoLevel.Error);
                return null;
            }
        }

        private static int ImportData(ImportOptions opts)
        {
            SetLogFile(opts.LogFile);
            if (!opts.Valid())
            {
                return 1;
            }

            Stopwatch sw = Stopwatch.StartNew();
            long result;
            switch ((opts.FileFormat ?? string.Empty).ToLowerInvariant())
            {
                case "bcp":
                    result = ImportFromBCP(opts);
                    break;
                case "json":
                    result = ImportFromJSON(opts);
                    break;
                default:
                    PostToConsole(string.Format("Unsupported file format '{0}'. Use bcp or json.", opts.FileFormat), InfoLevel.Error);
                    return 1;
            }
            sw.Stop();
            PostToConsole(string.Format("Total Imported: {0:N0}\tRows in {1}", result, sw.Elapsed), errorCount > 0 ? InfoLevel.Warning : InfoLevel.Success);
            return ExitCode();
        }

        private static int CopyData(CopyOptions opts)
        {
            SetLogFile(opts.outputLogFile);
            if (!opts.Valid())
            {
                return 1;
            }

            Stopwatch sw = Stopwatch.StartNew();
            DatabaseSnapshot snapshot = null;
            long result;
            try
            {
                if (opts.Snapshot)
                {
                    snapshot = CreateSnapshot(opts.srcServer, opts.srcDatabase, opts.srcLogin, opts.srcPassword, opts.TrustServerCertificate);
                    if (snapshot == null)
                    {
                        return 1;
                    }
                    opts.srcDatabase = snapshot.Name;
                }

                result = BulkCopyTables(opts);
            }
            finally
            {
                snapshot?.Dispose();
            }
            sw.Stop();
            PostToConsole(string.Format("Total Copied: {0:N0}\tRows in {1}", result, sw.Elapsed), errorCount > 0 ? InfoLevel.Warning : InfoLevel.Success);
            return ExitCode();
        }

        /// <summary>
        /// ParallelOptions for a --max-tasks value. Zero or negative means unlimited (-1); ParallelOptions itself
        /// rejects 0 and negatives other than -1.
        /// </summary>
        internal static ParallelOptions Parallelism(int maxTasks)
        {
            return new ParallelOptions { MaxDegreeOfParallelism = maxTasks > 0 ? maxTasks : -1 };
        }

        /// <summary>
        /// SqlBulkCopy options for loads: identity values and NULLs are always kept; TABLOCK unless --no-table-lock.
        /// </summary>
        internal static SqlBulkCopyOptions LoadOptions(bool noTableLock)
        {
            var options = SqlBulkCopyOptions.KeepIdentity | SqlBulkCopyOptions.KeepNulls;
            return noTableLock ? options : options | SqlBulkCopyOptions.TableLock;
        }

        /// <summary>bcp in load hint matching <see cref="LoadOptions"/>: -h "TABLOCK" unless --no-table-lock.</summary>
        internal static string BcpLoadHints(bool noTableLock)
        {
            return noTableLock ? string.Empty : " -h \"TABLOCK\"";
        }

        /// <summary>0 when no errors were reported during the run, otherwise 1.</summary>
        private static int ExitCode()
        {
            return Volatile.Read(ref errorCount) > 0 ? 1 : 0;
        }

        private static void SetLogFile(string path)
        {
            if (!string.IsNullOrEmpty(path))
            {
                LogFilePath = Path.GetFullPath(path);
            }
        }

        /// <summary>
        /// The bcp utility, found on PATH. No extension, so it resolves to bcp.exe on Windows and to the
        /// mssql-tools18 bcp on Linux/macOS.
        /// </summary>
        private const string BcpTool = "bcp";

        /// <summary>Non-XML bcp format file written next to each .bcp export (works with Windows and Linux bcp).</summary>
        internal const string BcpFormatFileSuffix = "-c.fmt";

        /// <summary>XML format file written by exports before the Linux-compatible change; still accepted on import.</summary>
        internal const string LegacyBcpFormatFileSuffix = "-c.xml";

        /// <summary>
        /// Runs an external tool (bcp) and returns its exit code, or -1 if it could not be started.
        /// When <paramref name="outputLogFile"/> is given, the tool's standard output is saved to that file in the
        /// working directory (the Linux bcp has no -o switch, so the log is written here for every platform).
        /// </summary>
        private static int ExecuteProcess(string command, string arguments, string workingDirectory, string outputLogFile = null)
        {
            var startInfo = new ProcessStartInfo
            {
                FileName = command,
                Arguments = arguments,
                UseShellExecute = false,
                CreateNoWindow = true,
                RedirectStandardOutput = true,
                WorkingDirectory = workingDirectory
            };

            try
            {
                using (Process process = Process.Start(startInfo))
                {
                    string output = process.StandardOutput.ReadToEnd();
                    process.WaitForExit();

                    if (!string.IsNullOrEmpty(outputLogFile))
                    {
                        File.WriteAllText(Path.Combine(workingDirectory, outputLogFile), output);
                    }

                    if (process.ExitCode != 0 && !string.IsNullOrWhiteSpace(output))
                    {
                        PostToConsole(output.Trim(), InfoLevel.Warning);
                    }

                    return process.ExitCode;
                }
            }
            catch (Win32Exception ex)
            {
                PostToConsole(string.Format("Could not start {0}: {1}. Make sure it is installed and on PATH.", command, ex.Message), InfoLevel.Error);
                return -1;
            }
        }

        public enum InfoLevel
        {
            Info,
            Warning,
            Success,
            Error
        }

        /// <summary>
        /// Thread-safe colored console output (and optional log file). Error messages make the process exit with code 1.
        /// </summary>
        internal static void PostToConsole(string message, InfoLevel messageLevel = InfoLevel.Info)
        {
            if (messageLevel == InfoLevel.Error)
            {
                Interlocked.Increment(ref errorCount);
            }

            ConsoleColor consoleColor;
            switch (messageLevel)
            {
                case InfoLevel.Warning:
                    consoleColor = ConsoleColor.Yellow;
                    break;
                case InfoLevel.Success:
                    consoleColor = ConsoleColor.Green;
                    break;
                case InfoLevel.Error:
                    consoleColor = ConsoleColor.Red;
                    break;
                default:
                    consoleColor = ConsoleColor.Cyan;
                    break;
            }

            lock (ConsoleLock)
            {
                Console.ForegroundColor = consoleColor;
                Console.WriteLine(message);
                Console.ResetColor();

                if (!string.IsNullOrEmpty(LogFilePath))
                {
                    try
                    {
                        File.AppendAllText(LogFilePath, string.Format("{0:yyyy-MM-dd HH:mm:ss}\t{1}\t{2}{3}", DateTime.Now, messageLevel, message, Environment.NewLine));
                    }
                    catch (IOException)
                    {
                        // Logging must never stop a data load.
                    }
                }
            }
        }
    }
}
