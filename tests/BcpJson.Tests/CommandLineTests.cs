using CommandLine;
using Xunit;

namespace bcpJson.Tests
{
    public class CommandLineTests
    {
        private static ParserResult<object> Parse(params string[] args)
        {
            var parser = new Parser(settings => settings.HelpWriter = null);
            return parser.ParseArguments<ExportOptions, ImportOptions, CopyOptions>(args);
        }

        private static T Parsed<T>(params string[] args)
        {
            ParserResult<object> result = Parse(args);
            var parsed = Assert.IsType<Parsed<object>>(result);
            return Assert.IsType<T>(parsed.Value);
        }

        [Fact]
        public void Export_defaults()
        {
            var opts = Parsed<ExportOptions>("export", "--source-database", "Sales");
            Assert.Equal("localhost", opts.srcServer);
            Assert.Equal("Sales", opts.srcDatabase);
            Assert.Equal("bcp", opts.FileFormat);
            Assert.Equal("query", opts.OutputName);
            Assert.False(opts.NoLock);
            Assert.False(opts.TrustServerCertificate);
            Assert.Empty(opts.IncludeTablesList);
        }

        [Fact]
        public void Export_filters_split_on_commas()
        {
            var opts = Parsed<ExportOptions>("export", "--include-tables", "dbo.A,sales.*", "--exclude-schemas", "staging,tmp", "--file-format", "json", "--nolock", "--trust-server-certificate");
            Assert.Equal(new[] { "dbo.A", "sales.*" }, opts.IncludeTablesList);
            Assert.Equal(new[] { "staging", "tmp" }, opts.ExcludeSchemaList);
            Assert.Equal("json", opts.FileFormat);
            Assert.True(opts.NoLock);
            Assert.True(opts.TrustServerCertificate);
        }

        [Fact]
        public void Export_login_and_password()
        {
            var opts = Parsed<ExportOptions>("export", "--source-login", "sa", "--source-password", "p;w", "--log-file", "out.log");
            Assert.Equal("sa", opts.srcLogin);
            Assert.Equal("p;w", opts.srcPassword);
            Assert.Equal("out.log", opts.outputLogFile);
        }

        [Fact]
        public void Import_requires_file_format_and_source_file()
        {
            Assert.IsType<NotParsed<object>>(Parse("import"));
            Assert.IsType<NotParsed<object>>(Parse("import", "--file-format", "json"));

            var opts = Parsed<ImportOptions>("import", "--file-format", "json", "--source-file", "*.json", "--truncate-target");
            Assert.Equal("json", opts.FileFormat);
            Assert.Equal("*.json", opts.SourceFile);
            Assert.Equal("localhost", opts.TargetSQLServer);
            Assert.True(opts.TruncateTarget);
        }

        [Fact]
        public void Copy_parses_source_and_target()
        {
            var opts = Parsed<CopyOptions>("copy", "--source-server", "a", "--target-server", "b", "--target-database", "Db", "--include-schemas", "dbo");
            Assert.Equal("a", opts.srcServer);
            Assert.Equal("b", opts.TargetSQLServer);
            Assert.Equal("Db", opts.TargetSQLDatabase);
            Assert.Equal(new[] { "dbo" }, opts.IncludeSchemaList);
        }

        [Fact]
        public void Max_tasks_is_available_on_every_verb()
        {
            Assert.Equal(-1, Parsed<ExportOptions>("export").MaxParallelTasks);
            Assert.Equal(3, Parsed<ExportOptions>("export", "--max-tasks", "3").MaxParallelTasks);
            Assert.Equal(2, Parsed<ImportOptions>("import", "--file-format", "json", "--source-file", "*.json", "--max-tasks", "2").MaxParallelTasks);
            Assert.Equal(5, Parsed<CopyOptions>("copy", "--max-tasks", "5").MaxParallelTasks);
        }

        [Fact]
        public void Table_lock_and_snapshot_options()
        {
            Assert.False(Parsed<ImportOptions>("import", "--file-format", "bcp", "--source-file", "x.bcp").NoTableLock);
            Assert.True(Parsed<ImportOptions>("import", "--file-format", "bcp", "--source-file", "x.bcp", "--no-table-lock").NoTableLock);

            var copy = Parsed<CopyOptions>("copy", "--no-table-lock", "--snapshot");
            Assert.True(copy.NoTableLock);
            Assert.True(copy.Snapshot);
            Assert.False(Parsed<CopyOptions>("copy").Snapshot);

            Assert.True(Parsed<ExportOptions>("export", "--snapshot").Snapshot);
            Assert.False(Parsed<ExportOptions>("export").Snapshot);
        }

        [Theory]
        [InlineData(0, "--version")]
        [InlineData(0, "--help")]
        [InlineData(0, "export", "--help")]
        [InlineData(0, "import", "--help")]
        [InlineData(0, "copy", "--help")]
        [InlineData(1, "backup")]
        [InlineData(1)]
        [InlineData(1, "import", "--file-format", "json")]
        public void Help_and_version_exit_zero_real_errors_exit_one(int expected, params string[] args)
        {
            var notParsed = Assert.IsType<NotParsed<object>>(Parse(args));
            Assert.Equal(expected, Program.ExitCodeForParseErrors(notParsed.Errors));
        }

        [Fact]
        public void Unknown_verb_fails()
        {
            Assert.IsType<NotParsed<object>>(Parse("backup"));
        }
    }
}
