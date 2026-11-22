using CommandLine;

namespace bcpJson
{
    [Verb("verify", HelpText = "Check an export folder against its bcpjson-manifest.json (no database needed): every file present with the recorded size and SHA-256, and the export complete.")]
    class VerifyOptions
    {
        [Option("path", Required = false, HelpText = "Export folder to verify. Defaults to the current directory.")]
        public string Path { get; set; }

        [Option("log-file", Required = false, HelpText = "Append console messages to this log file.")]
        public string LogFile { get; set; }
    }
}
